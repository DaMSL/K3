#ifndef K3_RUNTIME_LISTENER_H
#define K3_RUNTIME_LISTENER_H

#include <atomic>
#include <functional>
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

  //--------------------------------------------
  // A reference counter for listener instances.

  class ListenerCounter : public std::atomic_uint {
  public:
    ListenerCounter() : std::atomic_uint(0) {}

    void registerListener() { this->fetch_add(1); }
    void deregisterListener() { this->fetch_sub(1); }
    unsigned int operator()() { return this->load(); }
  };

  //---------------------------------------------------------------
  // Control data structures for multi-threaded listener execution.

  class ListenerControl {
  public:

    ListenerControl(shared_ptr<ListenerCounter> i)
      : listenerCounter(i)
    {}

    shared_ptr<ListenerCounter> counter() { return listenerCounter; }


  protected:
    shared_ptr<ListenerCounter> listenerCounter;

    bool msgAvailable;
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
               shared_ptr<InternalFraming> f)
        : name(n), ctxt_(ctxt), queues(q),
          endpoint_(ep), control_(ctrl), transfer_frame(f),
          listenerLog(shared_ptr<LogMT>(new LogMT("Listener_"+n)))
      {
        if ( endpoint_ ) {
          IOHandle::SourceDetails source = ep->handle()->networkSource();
          nEndpoint_ = dynamic_pointer_cast<NEndpoint>(get<1>(source));
          handle_frame = get<0>(source);
        }
      }

      shared_ptr<ListenerControl> control() { return control_; }
      shared_ptr<Framing> newFraming() { }

    protected:
      Identifier name;

      shared_ptr<NContext> ctxt_;
      shared_ptr<MessageQueues> queues;
      shared_ptr<Endpoint> endpoint_;
      shared_ptr<NEndpoint> nEndpoint_;

      shared_ptr<Framing> handle_frame;
      shared_ptr<InternalFraming> transfer_frame;

      shared_ptr<ListenerControl> control_;
      shared_ptr<LogMT> listenerLog;
  };

  namespace Asio
  {
    template<typename NContext, typename NEndpoint>
    using BaseListener = ::K3::Listener<NContext, NEndpoint>;

    // TODO: close method, terminating all incoming connections to this acceptor.
    class Listener : public BaseListener<NContext, NEndpoint>, public boost::basic_lockable_adapter<boost::mutex> {
    public:
      typedef basic_lockable_adapter<boost::mutex> llockable;
      typedef list<shared_ptr<NConnection> > ConnectionList;
      typedef boost::externally_locked<shared_ptr<ConnectionList>, Listener> ConcurrentConnectionList;

      Listener(Identifier n,
               shared_ptr<NContext> ctxt,
               shared_ptr<MessageQueues> q,
               shared_ptr<Endpoint> ep,
               shared_ptr<ListenerControl> ctrl,
               shared_ptr<InternalFraming> f)
        : BaseListener<NContext, NEndpoint>(n, ctxt, q, ep, ctrl, f),
          llockable(), connections_(emptyConnections())
      {
        if ( this->nEndpoint_ && this->handle_frame
                && this->ctxt_ && this->ctxt_->service_threads )
        {
          acceptConnection();
          thread_ = shared_ptr<boost::thread>(this->ctxt_->service_threads->create_thread(*(this->ctxt_)));

        } else {
          listenerLog->logAt(boost::log::trivial::error, "Invalid listener arguments.");
        }
      }

      ~Listener() {
        if (ctxt_ && thread_) {
          ctxt_->service_threads->remove_thread(thread_.get());
        }
      }

    protected:
      shared_ptr<boost::thread> thread_;
      shared_ptr<boost::externally_locked<shared_ptr<ConnectionList>, Listener> > connections_;

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
        if ( this->endpoint_ && this->handle_frame ) {
          shared_ptr<NConnection> nextConnection = shared_ptr<NConnection>(new NConnection(this->ctxt_));
          this->nEndpoint_->acceptor()->async_accept(*(nextConnection->socket()),
            [=] (const boost::system::error_code& ec) {
              if ( !ec ) {
                registerConnection(nextConnection);
                //this->listenerLog->logAt(boost::log::trivial::trace, "Listener Registered a connection");

              }
              else {
                this->listenerLog->logAt(boost::log::trivial::error, "Failed to accept a connection: " + ec.message());
              }
              // recursive call:
              acceptConnection();
            });
        }
        else { this->listenerLog->logAt(boost::log::trivial::error, "Invalid listener endpoint or wire description"); }
      }

      void registerConnection(shared_ptr<NConnection> c)
      {
        if ( connections_ ) {
          {
            boost::strict_lock<Listener> guard(*this);
            connections_->get(guard)->push_back(c);
          }

          // Notify subscribers of socket accept event.
          if ( this->endpoint_ ) {
            this->endpoint_->subscribers()->notifyEvent(EndpointNotification::SocketAccept, nullptr);
          }

          // Start connection, with a new frame(buffer)
          shared_ptr<Framing> frme = handle_frame->freshClone();
          receiveMessages(c, frme);
        }
      }

      void deregisterConnection(shared_ptr<NConnection> c)
      {
        if ( connections_ ) {
          boost::strict_lock<Listener> guard(*this);
          connections_->get(guard)->remove(c);
        }
      }

      void receiveMessages(shared_ptr<NConnection> c, shared_ptr<Framing> frme) {
        if ( c && c->socket()) {
          // TODO: extensible buffer size.
          // We use a local variable for the socket buffer since multiple threads
          // may invoke this handler simultaneously (i.e. for different connections).

          typedef boost::array<char, 1024*1024> SocketBuffer;
          shared_ptr<SocketBuffer> buffer_ = shared_ptr<SocketBuffer>(new SocketBuffer());
          c->socket()->async_read_some(buffer(buffer_->c_array(), buffer_->size()),
            [=](const boost::system::error_code& ec, std::size_t bytes_transferred) {

                // Capture the buffer in closure to keep the pointer count > 0
                // until the callback has finished
                shared_ptr<SocketBuffer> keep_alive = buffer_;

                if (!ec || (ec == boost::asio::error::eof && bytes_transferred > 0 )) {
                // Add network data to the frame's buffer.
                // We assume the processor notifies subscribers regarding socket data events.
                shared_ptr<Value> v(frme->decode(buffer_->c_array(), bytes_transferred));
                // Exhaust the frame's buffer
                while (v) {

                  bool t = endpoint_->do_push(v, queues, transfer_frame);
                  // Attempt to decode a buffered value
                  v = frme->decode("");
                }

                // Recursive invocation for the next message.
                receiveMessages(c, frme);

              } else {
                deregisterConnection(c);
                listenerLog->logAt(boost::log::trivial::error, string("Connection error: ")+ec.message());
              }
            });

        } else { listenerLog->logAt(boost::log::trivial::error, "Invalid listener connection"); }
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
               shared_ptr<InternalFraming> f)
        : BaseListener<NContext, NEndpoint>(n, ctxt, q, ep, ctrl, f)
      {
        if ( this->nEndpoint_ && this->handle_frame && this->ctxt_ && this->ctxt_->listenerThreads ) {
          // Instantiate a new thread to listen for messages on the nanomsg
          // socket, tracking it in the network context.
          terminated_ = false;
          thread_ = shared_ptr<boost::thread>(this->ctxt_->listenerThreads->create_thread(*this));
        } else {
          listenerLog->logAt(boost::log::trivial::error, "Invalid listener arguments.");
        }
      }

      Listener(const Listener& other)
        : BaseListener<NContext, NEndpoint>(other.name, other.ctxt_, other.queues,
                                            other.endpoint_, other.control_, other.transfer_frame)
      {
        this->thread_ = other.thread_;
        this->senders = other.senders;
        this->terminated_.store(other.terminated_.load());
      }

      void operator()() {
        typedef boost::array<char, 8192> SocketBuffer;
        SocketBuffer buffer_;
        shared_ptr<Framing> frme = this->handle_frame->freshClone();
        while ( !terminated_ ) {
          // Receive a message.
          int bytes = nn_recv(this->nEndpoint_->acceptor(), buffer_.c_array(), buffer_.static_size, 0);
          if ( bytes >= 0 ) {
            // Decode, process.
            shared_ptr<Value> v = frme->decode(string(buffer_.c_array(), buffer_.static_size));
            while ( v ) {
              // Simulate accept events for nanomsg.
              refreshSenders(v);
              bool t = this->endpoint_->do_push(v, this->queues, this->transfer_frame);

              v = frme->decode("");
            }
          } else {
            listenerLog->logAt(boost::log::trivial::error, string("Error receiving message: ") + nn_strerror(nn_errno()));
            terminate();
          }
        }
      }

      void terminate() { terminated_ = true; }

    protected:
      shared_ptr<boost::thread> thread_;
      atomic_bool terminated_;
      std::unordered_set<string> senders;

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
