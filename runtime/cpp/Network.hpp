#ifndef K3_RUNTIME_NETWORK_HPP
#define K3_RUNTIME_NETWORK_HPP

#include <ios>
#include <queue>
#include <thread>
#include <chrono>

#include <system_error>
#include <boost/asio.hpp>
#include <boost/iostreams/categories.hpp>
#include <boost/iostreams/stream.hpp>
#include <boost/system/error_code.hpp>
#include <boost/thread/thread.hpp>
#include <external/nanomsg/nn.h>
#include <external/nanomsg/pipeline.h>
#include <external/nanomsg/tcp.h>

#include <Common.hpp>

class EndpointBuffer;

using namespace boost::iostreams;

using boost::thread_group;
using std::bind;
using std::endl;
using std::cin;
using std::cout;
using std::cerr;

namespace K3
{

  //-------------------------------------------------
  // Abstract base classes for low-level networking.

  template<typename NContext, typename Acceptor>
  class NEndpoint : public virtual LogMT {
  public:
    NEndpoint(const string& logId, shared_ptr<NContext> ctxt) : LogMT(logId) {}
    NEndpoint(const char* logId, shared_ptr<NContext> ctxt) : LogMT(logId) {}

    virtual Acceptor acceptor() = 0;
    virtual void close() = 0;
  };

  template<typename NContext, typename Socket>
  class NConnection : public virtual LogMT
  {
  public:
    NConnection(const string& logId, shared_ptr<NContext> ctxt, Socket s)
      : LogMT(logId)
    {}

    NConnection(const char* logId, shared_ptr<NContext> ctxt, Socket s)
      : LogMT(logId)
    {}

    virtual Socket socket() = 0;
    virtual void close() = 0;
    virtual void write(shared_ptr<Value> ) = 0;

    // TODO
    //virtual bool has_write() = 0;
  };


  //-------------------------------------------------
  // Boost ASIO low-level networking implementation.

  namespace Asio
  {
    using namespace boost::asio;
    // Boost implementation of a network context.
    class NContext
    {
    public:
      NContext() {
        service = shared_ptr<io_service>(new io_service());
        service_threads = shared_ptr<thread_group>(new thread_group());
      }

      NContext(size_t concurrency) {
        service = shared_ptr<io_service>(new io_service(concurrency));
        service_threads = shared_ptr<thread_group>(new thread_group());
      }

      NContext(shared_ptr<io_service> ios) : service(ios) {}

      void operator()() { if ( service ) { service->run();} }

      shared_ptr<io_service> service;
      shared_ptr<thread_group> service_threads;
    };

    // Low-level endpoint class. This is a wrapper around a Boost tcp acceptor.
    class NEndpoint : public ::K3::NEndpoint<NContext, shared_ptr<ip::tcp::acceptor> >
    {
    public:
      typedef shared_ptr<ip::tcp::acceptor> Acceptor;

      NEndpoint(shared_ptr<NContext> ctxt, Address addr)
        : ::K3::NEndpoint<NContext, Acceptor>("NEndpoint", ctxt), LogMT("NEndpoint")
      {
        if ( ctxt ) {
          ip::tcp::endpoint ep(get<0>(addr), get<1>(addr));
	  for(int retries=4; retries > 0; retries--) {
	    try {
              acceptor_ = shared_ptr<ip::tcp::acceptor>(new ip::tcp::acceptor(*(ctxt->service)));
	      acceptor_->open(ep.protocol());
	      boost::asio::socket_base::reuse_address option(true);
	      acceptor_->set_option(option);
	      acceptor_->bind(ep);
	      acceptor_->listen();
	      break;
	    }
	    catch (std::exception e) {
              logAt(boost::log::trivial::warning, e.what());
	      if (retries > 1) {
                logAt(boost::log::trivial::warning, "Trying again in 30 seconds");
		std::this_thread::sleep_for (std::chrono::seconds(30));
	      }
	      else {
                logAt(boost::log::trivial::warning, "Aborting");
		throw e;
	      }
	    }
	  }
        } else {
          logAt(boost::log::trivial::warning, "Invalid network context in constructing an NEndpoint");
        }
      }

      Acceptor acceptor() { return acceptor_; }

      void close() { if ( acceptor_ ) { acceptor_->close(); } }

    protected:
      Acceptor acceptor_;
    };

    // Low-level connection class. This is a wrapper around a Boost tcp socket.
    class NConnection : public ::K3::NConnection<NContext, shared_ptr<ip::tcp::socket> >
    {
    public:
      typedef shared_ptr<ip::tcp::socket> Socket;

      // null ptr for EndpointBuffer
      NConnection(shared_ptr<NContext> ctxt)
        : NConnection(ctxt, Socket(new ip::tcp::socket(*(ctxt->service))))
      {}

      // null ptr for EndpointBuffer
      NConnection(shared_ptr<NContext> ctxt, Address addr)
        : NConnection(ctxt, Socket(new ip::tcp::socket(*(ctxt->service))))
      {
        if ( ctxt ) {
          if ( socket_ ) {
            ip::tcp::endpoint ep(::std::get<0>(addr), ::std::get<1>(addr));
	    boost::system::error_code error;
	    socket_->connect(ep, error);
            if (!error) {
              connected_ = true;
              //logAt(boost::log::trivial::warning, "connected");
              //BOOST_LOG(*this) << "Connected! ";

            } else {
              BOOST_LOG(*this) << "Connect error: " << error.message();
	      throw std::runtime_error("Connect error");
            }
          } else { logAt(boost::log::trivial::warning, "Uninitialized socket in constructing an NConnection"); }
        } else { logAt(boost::log::trivial::warning, "Invalid network context in constructing an NConnection"); }
      }

      Socket socket() { return socket_; }

      bool connected() { return connected_; }

      void close() { if ( socket_ ) { socket_->close(); } }

      void write(shared_ptr<Value>  val) {
        // TODO switch to scoped locks
        // If the loop is already running, just add the message to the queue
        mut.lock();
        if (busy) {
          while(buffer_->size() > 1000000) {
            logAt(boost::log::trivial::warning, "Too many messages on outgoing queue: waiting...");
            mut.unlock();
            boost::this_thread::sleep_for( boost::chrono::seconds(1) );
            mut.lock();
          }
          buffer_->push(val);
        }
        // Otherwise, start the loop
        else {
          busy = true;
          async_write_loop(val);
        }
        mut.unlock();
      }

      void async_write_loop(shared_ptr<Value> val) {
        size_t desired = val->length();
        // Write the value out to the socket
        async_write(*socket_, boost::asio::buffer(*val,
          desired),
        [=](boost::system::error_code ec, size_t s)
        {
          // Capture the buffer in closure to keep its pointer count > 0
          // until this callback has been executed
          shared_ptr<Value> keep_alive = val;
          // Check for errors:
          if (ec || (s != desired)) {
            BOOST_LOG(*(static_cast<LogMT*>(this))) << "Error on write: " << ec.message()
              << " wrote  " << s << " out of " << desired << " bytes" << endl;
          }

          // Determine loop status:
          mut.lock();
          // If the buffer is empty, terminate the loop for now.
          if (buffer_->empty()) {
            busy = false;
          }
          // Otherwise, pop the next value and recurse
          else {
            shared_ptr<Value> newval = buffer_->front();
            buffer_->pop();
            async_write_loop(newval);
          }
          mut.unlock();
        });
      }

    protected:
      NConnection(shared_ptr<NContext> ctxt, Socket s)
        : ::K3::NConnection<NContext, Socket>("NConnection", ctxt, s),
          LogMT("NConnection"), socket_(s), connected_(false), busy(false),
          buffer_(new std::queue<shared_ptr<Value>>())
      {}
      // use mutex to operate on queues and busy atomically
      boost::mutex mut;
      bool busy;
      shared_ptr<std::queue<shared_ptr<Value>>> buffer_;
      Socket socket_;
      bool connected_;
    };
  }


  //----------------------------------------------
  // Nanomsg low-level networking implementation.
  //
  // Currently, this is a blocking implementation.

  namespace Nanomsg
  {
    class NContext {
    public:
      NContext() {
        listenerThreads = shared_ptr<thread_group>(new thread_group());
      }

      // K3 Nanomsg endpoints use the TCP transport.
      string urlOfAddress(Address addr) {
        return string("tcp://") + addressAsString(addr);
      }

      shared_ptr<thread_group> listenerThreads;
    };

    class NEndpoint : public ::K3::NEndpoint<NContext, int>
    {
    public:
      typedef int Acceptor;

      NEndpoint(shared_ptr<NContext> ctxt, Address addr)
        : ::K3::NEndpoint<NContext, Acceptor>("NEndpoint", ctxt), LogMT("NEndpoint"),
          socket_(nn_socket(AF_SP, NN_PULL))
      {
        if ( ctxt && socket_ >= 0 ) {
          string bindAddr = ctxt->urlOfAddress(addr);
          int bDone = nn_bind(socket_, bindAddr.c_str());
          if ( bDone < 0 ) {
            socket_ = -1;
            logAt(boost::log::trivial::error,
              string("Failed to bind endpoint to address ")
                + addressAsString(addr) + " : " + nn_strerror(nn_errno()));
          }
        }
      }

      Acceptor acceptor() { return socket_; }

      void close()
      {
        if ( socket_ >= 0 ) {
          int cDone = nn_close(socket_);
          if ( cDone < 0 ) {
            logAt(boost::log::trivial::error,
              string("Failed to close endpoint: ") + nn_strerror(nn_errno()));
          }
        }
      }

    protected:
      int socket_;
    };

    class NConnection : public ::K3::NConnection<NContext, int>
    {
    public:
      typedef int Socket;

      NConnection(shared_ptr<NContext> ctxt) : NConnection(ctxt, nn_socket(AF_SP, NN_PUSH)) {}

      NConnection(shared_ptr<NContext> ctxt, Address addr) : NConnection(ctxt)
      {
        if ( ctxt && socket_ >= 0 ) {
          string connectAddr = ctxt->urlOfAddress(addr);
          int cDone = nn_connect(socket_, connectAddr.c_str());
          if ( cDone < 0 ) {
            this->close();
            logAt(boost::log::trivial::error,
              string("Failed to connect to address "
                + addressAsString(addr) + " : " + nn_strerror(nn_errno())));
          }
          else { endpointId_ = cDone; }
        }
      }

      Socket socket() { return socket_; }

      void close()
      {
        if ( socket_ >= 0 ) {
          int cDone = nn_close(socket_);
          if ( cDone < 0 ) {
            logAt(boost::log::trivial::error,
              string("Failed to close connection: ") + nn_strerror(nn_errno()));
          }
        }
      }

      void write(const string& val) {
        size_t n = val.length();
        int wDone = nn_send(socket_, &val, n, 0);
        if ( wDone < 0 ) {
          int err = nn_errno();
          string errStr(nn_strerror(err));
          logAt(boost::log::trivial::error, string("Failed to write to socket: " + errStr));
        }
        return;
      }

    protected:
      NConnection(shared_ptr<NContext> ctxt, Socket s)
        : ::K3::NConnection<NContext, Socket>("NConnection", ctxt, s),
          LogMT("NConnection"), socket_(s)
      {
        if ( socket_ < 0 ) {
          logAt(boost::log::trivial::error,
            string("Failed in connection socket construction: ") + nn_strerror(nn_errno()));
        }
      }

      Socket socket_;
      int endpointId_;
    };
  }

  //-----------------------------------------
  // Low-level networking library selection.

  namespace Net = K3::Asio;
}

#endif
