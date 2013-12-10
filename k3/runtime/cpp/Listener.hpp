#ifndef K3_RUNTIME_LISTENER_H
#define K3_RUNTIME_LISTENER_H

#include <atomic>
#include <memory>
#include <boost/thread/condition_variable.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/thread.hpp>
#include <k3/runtime/cpp/Common.hpp>
#include <k3/runtime/cpp/Queue.hpp>
#include <k3/runtime/cpp/IOHandle.hpp>
#include <k3/runtime/cpp/Endpoint.hpp>

namespace K3 {
  
  using namespace std;
  using namespace boost;

  using std::atomic_uint;
  using boost::mutex;
  using boost::condition_variable;

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
      unique_lock<mutex> lock(msgAvailMutex);
      while ( !msgAvailable ) { msgAvailCondition.wait(lock); }
    }

    // Notifies one waiter on the message available condition variable.
    void messageAvailable()
    {
      {
        lock_guard<mutex> lock(msgAvailMutex);
        msgAvailable = true;
      }
      msgAvailCondition.notify_one();
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

  //-------------------------------
  // Listener processor base class.

  template<typename Value>
  class ListenerProcessor : public virtual LogMT {
  public:
    ListenerProcessor(shared_ptr<ListenerControl> c, shared_ptr<Endpoint<Value> > e)
      : LogMT("ListenerProcessor"), control(c), endpoint(e)
    {}

    // A callable processing function that must be implemented by subclasses.
    virtual void operator()() = 0;
  
  protected:
    shared_ptr<ListenerControl> control;
    shared_ptr<Endpoint<Value> > endpoint;
  };

  //------------------------------------
  // Listener processor implementations.

  template<typename Value>
  class InternalListenerProcessor : public ListenerProcessor<Message<Value> > {
  public:
    InternalListenerProcessor(shared_ptr<MessageQueues> q,
                              shared_ptr<ListenerControl> c,
                              shared_ptr<Endpoint<Value> > e)
      : ListenerProcessor<Message<Value> >(c, e), engineQueues(q)
    {}

    void operator()() {
      if ( control && endpoint && engineQueues )
      {
        // Enqueue from the endpoint's buffer into the engine queues,
        // and signal message availability.
        endpoint->buffer()->enqueue(engineQueues);
        control->messageAvailable();
      }
      else { logAt(boost::log::trivial::error, "Invalid listener processor members"); }
    }

  protected:
    shared_ptr<MessageQueues> engineQueues;
  };
  
  template<typename Value>
  class ExternalListenerProcessor : public ListenerProcessor<Value> {
  public:
    ExternalListenerProcessor(shared_ptr<ListenerControl> c, shared_ptr<Endpoint<Value> > e)
      : ListenerProcessor<Value>(c, e)
    {}

    void operator()() {
      if ( control && endpoint )
      {
        // Notify the endpoint's subscribers of a socket data event,
        // and signal message availability.
        endpoint->subscribers()->notifyEvent(EndpointBindings::SocketData);
        control->messageAvailable();
      }
      else { logAt(boost::log::trivial::error, "Invalid listener processor members"); }
    }
  };

  //------------
  // Listeners

  namespace Asio
  {
    using namespace boost::asio;
    using namespace boost::log;
    using namespace boost::system;

    // TODO: close method, terminating all incoming connections to this acceptor.
    template<typename Value>
    class Listener : public basic_lockable_adapter<mutex>, public virtual LogMT
    {
    public:
      typedef basic_lockable_adapter<mutex> llockable;
      typedef list<shared_ptr<NConnection> > ConnectionList
      typedef externally_locked<shared_ptr<ConnectionList>, Listener> ConcurrentConnectionList;

      Listener(Identifier n,
               shared_ptr<NContext> ctxt,
               shared_ptr<Endpoint> ep,
               shared_ptr<ListenerControl> ctrl,
               shared_ptr<ListenerProcessor<Value> > p)
        : llockable(), LogMT("Listener_"+n),
          name(n), endpoint_(ep), control_(ctrl), processor_(p), connections_(emptyConnections())
      {
        if ( endpoint_ ) {
          IOHandle::SourceDetails source = ep->handle()->networkSource();
          nEndpoint_ = get<0>(source);
          wireDesc_ = get<1>(source);
        }

        if ( nEndpoint_ && wireDesc_ && ctxt && ctxt->service_threads ) {
          acceptConnection();
          thread_ = shared_ptr<thread>(ctxt->service_threads->create_thread<NContext>(*ctxt));
        } else {
          logAt(trivial::error, "Invalid listener arguments.");
        }
      }

    protected:
      Identifier name;
      
      shared_ptr<Endpoint> endpoint_;
      shared_ptr<NEndpoint> nEndpoint_;
      shared_ptr<WireDesc<Value> > wireDesc_;
        // We assume this wire description performs the framing necessary
        // for partial network messages.
      
      shared_ptr<thread> thread_;
      shared_ptr<ListenerControl> control_;
      shared_ptr<ListenerProcessor<Value> > processor_;

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

      void acceptConnection(shared_ptr<NContext> ctxt)
      {
        if ( endpoint_ && wireDesc_ ) {
          shared_ptr<NConnection> nextConnection = shared_ptr<NConnection>(new NConnection(ctxt));
          
          nEndpoint_->acceptor()->async_accept(nextConnection,
            [=] (const error_code& ec) {
              if ( !ec ) { registerConnection(nextConnection); }
              else { logAt(trivial::error, string("Failed to accept a connection: ")+ec.message()); }
            });

          acceptConnection(ctxt);
        }
        else { logAt(trivial::error, "Invalid listener endpoint or wire description"); }
      }

      void registerConnection(shared_ptr<NConnection> c)
      {
        if ( connections_ ) {
          {
            strict_lock<Listener> guard(*this);
            connections_->get(guard)->push_back(c);
          }
          
          // Notify subscribers of socket accept event.
          if ( endpoint_ ) { 
            endpoint_->subscribers()->notifyEvent(EndpointBindings::SocketAccept);
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

      void receiveMessages(shared_ptr<NConnection> c)
      {
        if ( c && c->socket() && processor_ )
        {
          // TODO: extensible buffer size.
          // We use a local variable for the socket buffer since multiple threads
          // may invoke this handler simultaneously (i.e. for different connections).
          typedef SocketBuffer array<char, 8192>;
          shared_ptr<SocketBuffer> buffer_(new SocketBuffer());

          async_read(c->socket(), buffer(buffer_->c_array(), buffer_->size()),
            [=](const error_code& ec, std::size_t bytes_transferred)
            {
              if ( !ec )
              {
                // Unpack buffer, check if it returns a valid message, and pass that to the processor.
                // We assume the processor notifies subscribers regarding socket data events.
                shared_ptr<Value> v = wireDesc_->unpack(string(buffer_->c_array(), buffer_->size()));                
                if ( v ) { 
                  // Add the value to the endpoint's buffer, and invoke the listener processor.
                  endpoint_->buffer()->append(v);
                  (*processor)();
                }
                
                // Recursive invocation for the next message.
                receiveMessages(c);
              }
              else 
              {
                deregisterConnection(c);
                logAt(trivial::error, string("Connection error: ")+ec.message());
              }
            });
        }
        else { logAt(trivial::error, "Invalid listener connection") }
      }
    };
  }
} 

#endif