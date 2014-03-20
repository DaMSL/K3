#ifndef K3_RUNTIME_NETWORK_HPP
#define K3_RUNTIME_NETWORK_HPP

#include <ios>
#include <memory>
#include <system_error>
#include <tuple>
#include <boost/asio.hpp>
#include <boost/iostreams/categories.hpp>
#include <boost/iostreams/stream.hpp>
#include <boost/system/error_code.hpp>
#include <boost/thread/thread.hpp>
#include <nanomsg/nn.h>
#include <nanomsg/pipeline.h>
#include <nanomsg/tcp.h>

#include <Common.hpp>

namespace K3
{
  using namespace std;

  using namespace boost::iostreams;
  using namespace boost::asio;

  using boost::thread_group;
  using std::bind;

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
    virtual void write(const string&) = 0;

    // TODO
    //virtual bool has_write() = 0;
  };


  //-------------------------------------------------
  // Boost ASIO low-level networking implementation.

  namespace Asio
  { 
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

      void operator()() { if ( service ) { service->run(); } }

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
          acceptor_ = shared_ptr<ip::tcp::acceptor>(new ip::tcp::acceptor(*(ctxt->service), ep));
        } else {
          logAt(warning, "Invalid network context in constructing an NEndpoint");
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

      NConnection(shared_ptr<NContext> ctxt)
        : NConnection(ctxt, Socket(new ip::tcp::socket(*(ctxt->service))))
      {}

      NConnection(shared_ptr<NContext> ctxt, Address addr)
        : NConnection(ctxt, Socket(new ip::tcp::socket(*(ctxt->service))))
      {
        if ( ctxt ) {
          if ( socket_ ) {
            ip::tcp::endpoint ep(::std::get<0>(addr), ::std::get<1>(addr));
            socket_->async_connect(ep,
              [=] (const boost::system::error_code& error) {
                if (!error) {
                  connected_ = true;
                  BOOST_LOG(*this) << "connected to " << ::K3::addressAsString(addr);

                } else {
                  BOOST_LOG(*this) << "Connect error: " << error.message();
                }
              } );
          } else { logAt(warning, "Uninitialized socket in constructing an NConnection"); }
        } else { logAt(warning, "Invalid network context in constructing an NConnection"); }
      }

      Socket socket() { return socket_; }

      bool connected() { return connected_; }

      void close() { if ( socket_ ) { socket_->close(); } }

      void write(const string& val) { 
        size_t desired = val.length();
        async_write(*socket_, boost::asio::buffer(val,
          desired),
        [=](boost::system::error_code ec, size_t s)
        {
          if (!ec && (s == desired)) {
            BOOST_LOG(*this) << "Successfully sent: " <<  val;
          }
          else {
            BOOST_LOG(*this) << "Error on write: " << ec.message()
                             << " sent : " << s << " bytes" << endl;
          }
        });
      }

    protected:
      NConnection(shared_ptr<NContext> ctxt, Socket s)
        : ::K3::NConnection<NContext, Socket>("NConnection", ctxt, s),
          LogMT("NConnection"), socket_(s)
      {}
      
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
            logAt(trivial::error, 
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
            logAt(trivial::error, 
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
            logAt(trivial::error,
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
            logAt(trivial::error,
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
          logAt(trivial::error, string("Failed to write to socket: " + errStr));
        }
        return;
      }

    protected:
      NConnection(shared_ptr<NContext> ctxt, Socket s)
        : ::K3::NConnection<NContext, Socket>("NConnection", ctxt, s),
          LogMT("NConnection"), socket_(s)
      {
        if ( socket_ < 0 ) {
          logAt(trivial::error,
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
