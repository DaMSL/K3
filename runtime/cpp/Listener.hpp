#ifndef K3_RUNTIME_LISTENER_H
#define K3_RUNTIME_LISTENER_H

#include <atomic>
#include <functional>
#include <memory>
#include <unordered_set>
#include <boost/array.hpp>
#include <boost/thread/condition_variable.hpp>
#include <boost/thread/locks.hpp>
#include <boost/thread/lock_types.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/thread.hpp>

#include <Common.hpp>
#include <Queue.hpp>
#include <IOHandle.hpp>
#include <Endpoint.hpp>

namespace K3 {

  using namespace std;

  using std::atomic_uint;

  using boost::condition_variable;
  using boost::lock_guard;
  using boost::mutex;
  using boost::unique_lock;

  //--------------------------------------------
  // A reference counter for listener instances.

  class ListenerCounter : public atomic_uint {
  public:
    ListenerCounter() : atomic_uint(0) {}

    void registerListener() { this->fetch_add(1); }
    void deregisterListener() { this->fetch_sub(1); }
    unsigned int operator()() { return this->load(); }
  };

  //---------------------------------------------------------------
  // Control data structures for multi-threaded listener execution.

  class ListenerControl {
  public:

    ListenerControl(shared_ptr<mutex> m, shared_ptr<condition_variable> c,
                    shared_ptr<ListenerCounter> i)
      : listenerCounter(i), msgAvailable(false), msgAvailMutex(m), msgAvailCondition(c)
    {}

    // Waits on the message available condition variable.
    void waitForMessage()
    {
      unique_lock<mutex> lock(*msgAvailMutex);
      while ( !msgAvailable ) { msgAvailCondition->wait(lock); }
    }

    // Notifies one waiter on the message available condition variable.
    void messageAvailable()
    {
      {
        lock_guard<mutex> lock(*msgAvailMutex);
        msgAvailable = true;
      }
      msgAvailCondition->notify_one();
    }

    shared_ptr<ListenerCounter> counter() { return listenerCounter; }

    shared_ptr<mutex> msgMutex() { return msgAvailMutex; }
    shared_ptr<condition_variable> msgCondition() { return msgAvailCondition; }

  protected:
    shared_ptr<ListenerCounter> listenerCounter;

    bool msgAvailable;
    shared_ptr<mutex> msgAvailMutex;
    shared_ptr<condition_variable> msgAvailCondition;
  };

  //------------
  // Listeners

  // Abstract base class for listeners.
  template<typename NContext, typename NEndpoint>
  class Listener {
    public:
      Listener(Identifier n,
               shared_ptr<NContext> ctxt,
               shared_ptr<MessageQueues> q,
               shared_ptr<Endpoint> ep,
               shared_ptr<ListenerControl> ctrl,
               shared_ptr<InternalCodec> c)
        : name(n), ctxt_(ctxt), queues(q), 
          endpoint_(ep), control_(ctrl), transfer_codec(c),
          listenerLog(shared_ptr<LogMT>(new LogMT("Listener_"+n)))
      {
        if ( endpoint_ ) {
          IOHandle::SourceDetails source = ep->handle()->networkSource();
          nEndpoint_ = dynamic_pointer_cast<NEndpoint>(get<1>(source));
          handle_codec = get<0>(source);
        }
      }

      shared_ptr<ListenerControl> control() { return control_; }

    protected:
      Identifier name;

      shared_ptr<NContext> ctxt_;
      shared_ptr<MessageQueues> queues;
      shared_ptr<Endpoint> endpoint_;
      shared_ptr<NEndpoint> nEndpoint_;

      shared_ptr<Codec> handle_codec;
      shared_ptr<InternalCodec> transfer_codec;
        // We assume this wire description performs the framing necessary
        // for partial network messages.

      shared_ptr<ListenerControl> control_;
      shared_ptr<LogMT> listenerLog;
  };

  namespace Asio
  {
    using namespace boost::asio;
    using namespace boost::log;
    using namespace boost::system;

    using boost::system::error_code;
    using boost::thread;

    template<typename NContext, typename NEndpoint>
    using BaseListener = ::K3::Listener<NContext, NEndpoint>;

    // TODO: close method, terminating all incoming connections to this acceptor.
    class Listener : public BaseListener<NContext, NEndpoint>, public basic_lockable_adapter<mutex> {
    public:
      typedef basic_lockable_adapter<mutex> llockable;
      typedef list<shared_ptr<NConnection> > ConnectionList;
      typedef externally_locked<shared_ptr<ConnectionList>, Listener> ConcurrentConnectionList;

      Listener(Identifier n,
               shared_ptr<NContext> ctxt,
               shared_ptr<MessageQueues> q,
               shared_ptr<Endpoint> ep,
               shared_ptr<ListenerControl> ctrl,
               shared_ptr<InternalCodec> c)
        : BaseListener<NContext, NEndpoint>(n, ctxt, q, ep, ctrl, c),
          llockable(), connections_(emptyConnections())
      {
        if ( this->nEndpoint_ && this->handle_codec
                && this->ctxt_ && this->ctxt_->service_threads )
        {
          acceptConnection();
          thread_ = shared_ptr<thread>(this->ctxt_->service_threads->create_thread(*(this->ctxt_)));     

        } else {
          listenerLog->logAt(trivial::error, "Invalid listener arguments.");
        }
      }

      ~Listener() {
        if (ctxt_ && thread_) {
          ctxt_->service_threads->remove_thread(thread_.get());
        }
      }

    protected:
      shared_ptr<thread> thread_;
      shared_ptr<externally_locked<shared_ptr<ConnectionList>, Listener> > connections_;

      //---------
      // Helpers.

      shared_ptr<ConcurrentConnectionList> emptyConnections()
      {
        shared_ptr<ConnectionList> l = shared_ptr<ConnectionList>(new ConnectionList());
        return shared_ptr<ConcurrentConnectionList>(new ConcurrentConnectionList(*this, l));
      }

      //---------------------
      // Endpoint execution.

      void acceptConnection()
      { 
        if ( this->endpoint_ && this->handle_codec ) {
          shared_ptr<NConnection> nextConnection = shared_ptr<NConnection>(new NConnection(this->ctxt_));
          this->nEndpoint_->acceptor()->async_accept(*(nextConnection->socket()),
            [=] (const error_code& ec) {
              if ( !ec ) { registerConnection(nextConnection); }
              else { this->listenerLog->logAt(trivial::error, "Failed"); }
              // recursive call:
              acceptConnection();
            });
        }
        else { this->listenerLog->logAt(trivial::error, "Invalid listener endpoint or wire description"); }
      }

      void registerConnection(shared_ptr<NConnection> c)
      {
        if ( connections_ ) {
          {
            strict_lock<Listener> guard(*this);
            connections_->get(guard)->push_back(c);
          }

          // Notify subscribers of socket accept event.
          if ( this->endpoint_ ) {
            this->endpoint_->subscribers()->notifyEvent(EndpointNotification::SocketAccept, nullptr);
          }

          // Start connection.
          receiveMessages(c);
        }
      }

      void deregisterConnection(shared_ptr<NConnection> c)
      {
        if ( connections_ ) {
          strict_lock<Listener> guard(*this);
          connections_->get(guard)->remove(c);
        }
      }

      void receiveMessages(shared_ptr<NConnection> c) {
        if ( c && c->socket()) {
          // TODO: extensible buffer size.
          // We use a local variable for the socket buffer since multiple threads
          // may invoke this handler simultaneously (i.e. for different connections).
          typedef boost::array<char, 8192> SocketBuffer;
          shared_ptr<SocketBuffer> buffer_(new SocketBuffer());
          c->socket()->async_read_some(buffer(buffer_->c_array(), buffer_->size()),
            [=](const error_code& ec, std::size_t bytes_transferred) {
              if (!ec || (ec == boost::asio::error::eof && bytes_transferred > 0 )) {
                // Unpack buffer, check if it returns a valid message, and pass that to the processor.
                // We assume the processor notifies subscribers regarding socket data events.
                shared_ptr<Value> v = this->handle_codec->decode(string(buffer_->c_array(), bytes_transferred));
                while (v) {
                  listenerLog->logAt(trivial::trace, string("Listener Received data: ")+*v);
                  bool t = this->endpoint_->do_push(v, this->queues, this->transfer_codec);
                  if (t) {
                    this->control_->messageAvailable();
                  }
                  v = this->handle_codec->decode("");
                }

                // Recursive invocation for the next message.

                receiveMessages(c);
              } else {
                deregisterConnection(c);
                listenerLog->logAt(trivial::error, string("Connection error: ")+ec.message());
              }
            });

        } else { listenerLog->logAt(trivial::error, "Invalid listener connection"); }
      }
    };
  }

  namespace Nanomsg
  {
    using std::atomic_bool;

    template<typename NContext, typename NEndpoint>
    using BaseListener = ::K3::Listener<NContext, NEndpoint>;

    class Listener : public BaseListener<NContext, NEndpoint>
    {
    public:
      Listener(Identifier n,
               shared_ptr<NContext> ctxt,
               shared_ptr<MessageQueues> q,
               shared_ptr<Endpoint> ep,
               shared_ptr<ListenerControl> ctrl,
               shared_ptr<InternalCodec> c)
        : BaseListener<NContext, NEndpoint>(n, ctxt, q, ep, ctrl, c)
      {
        if ( this->nEndpoint_ && this->handle_codec && this->ctxt_ && this->ctxt_->listenerThreads ) {
          // Instantiate a new thread to listen for messages on the nanomsg
          // socket, tracking it in the network context.
          terminated_ = false;
          thread_ = shared_ptr<thread>(this->ctxt_->listenerThreads->create_thread(*this));
        } else {
          listenerLog->logAt(trivial::error, "Invalid listener arguments.");
        }
      }

      Listener(const Listener& other)
        : BaseListener<NContext, NEndpoint>(other.name, other.ctxt_, other.queues,
                                            other.endpoint_, other.control_, other.transfer_codec)
      {
        this->thread_ = other.thread_;
        this->senders = other.senders;
        this->terminated_.store(other.terminated_.load());
      }

      void operator()() {
        typedef boost::array<char, 8192> SocketBuffer;
        SocketBuffer buffer_;

        while ( !terminated_ ) {
          // Receive a message.
          int bytes = nn_recv(this->nEndpoint_->acceptor(), buffer_.c_array(), buffer_.static_size, 0);
          if ( bytes >= 0 ) {
            // Decode, process.
            shared_ptr<Value> v = this->handle_codec->decode(string(buffer_.c_array(), buffer_.static_size));
            if ( v ) {
              // Simulate accept events for nanomsg.
              refreshSenders(v);
              bool t = this->endpoint_->do_push(v, this->queues, this->transfer_codec);

              if (t) {
                this->control_->messageAvailable();
              }
            }
          } else {
            listenerLog->logAt(trivial::error, string("Error receiving message: ") + nn_strerror(nn_errno()));
            terminate();
          }
        }
      }

      void terminate() { terminated_ = true; }

    protected:
      shared_ptr<thread> thread_;
      atomic_bool terminated_;
      unordered_set<string> senders;

      // TODO: simulate socket accept notifications. Since nanomsg is not connection-oriented,
      // we simulate connections based on the arrival of messages from unseen addresses.
      // TODO: break assumption that value is a Message.
      void refreshSenders(shared_ptr<Value> v) {
        /*
        if ( senders.find(v->address()) == senders.end() ) {
          senders.insert(v->address());
          endpoint_->subscribers()->notifyEvent(EndpointNotification::SocketAccept);
        }

        // TODO: remove addresses from the recipients pool based on a timeout.
        // TODO: time out stale senders.
        */
      }
    };
  }
}

#endif
// vim:set sw=2 ts=2 sts=2:
