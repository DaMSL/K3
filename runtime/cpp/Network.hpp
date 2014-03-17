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
#include "runtime/cpp/Common.hpp"

namespace K3
{
  using namespace std;

  using boost::thread_group;
  using namespace boost::iostreams;
  using namespace boost::asio;

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

  template<typename NContext, typename Device, typename Socket>
  class NConnection : public stream<Device>, public virtual LogMT
  {
  public:
    NConnection(const string& logId, shared_ptr<NContext> ctxt, Socket s)
      : stream<Device>(s), LogMT(logId)
    {}
    
    NConnection(const char* logId, shared_ptr<NContext> ctxt, Socket s)
      : stream<Device>(s), LogMT(logId)
    {}

    virtual Socket socket() = 0;
    virtual void close() = 0;
  };


  //-------------------------------------------------
  // Boost ASIO low-level networking implementation.

  namespace Asio
  {
    class AsioDeviceException : public system_error {
    public:
      AsioDeviceException(const error_code& ec) : system_error(ec) {}
      
      AsioDeviceException(const error_code& ec, const string& msg)
        : system_error(ec, msg)
      {}
    };

    class AsioDevice
    {
    public:
      typedef char char_type;
      typedef bidirectional_device_tag category;

      AsioDevice(shared_ptr<ip::tcp::socket> s) : socket(s) {}

      streamsize read(char* s, streamsize n) 
      {
        boost::system::error_code ec;
        std::size_t rval = socket->read_some(boost::asio::buffer(s, n), ec);
        if (!ec) { return rval; }
        else if (ec == boost::asio::error::eof) { return -1; }
        else { 
          throw AsioDeviceException(make_error_code(static_cast<errc>(ec.value())), ec.message());
        }
      }
      
      streamsize write(const char* s, streamsize n)
      {
        boost::system::error_code ec;
        std::size_t rval = socket->write_some(boost::asio::buffer(s, n), ec);
        if (!ec) { return rval; }
        else if (ec == boost::asio::error::eof) { return -1; }
        else {
          throw AsioDeviceException(make_error_code(static_cast<errc>(ec.value())), ec.message());
        }
      }

    protected:
      shared_ptr<ip::tcp::socket> socket;
    };

    // Boost implementation of a network context.
    class NContext
    {
    public:
      NContext(Address addr) {
        service = shared_ptr<io_service>(new io_service());
        service_threads = shared_ptr<thread_group>(new thread_group());
      }

      NContext(Address addr, size_t concurrency) {
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
    class NConnection : public ::K3::NConnection<NContext, AsioDevice, shared_ptr<ip::tcp::socket> >
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
            shared_ptr<LogMT> logger(static_cast<LogMT*>(this));
            socket_->async_connect(ep,
              [=,&addr] (const boost::system::error_code& error) { 
                BOOST_LOG(*logger) << "connected to " << ::K3::addressAsString(addr);
              } );
          } else { logAt(warning, "Uninitialized socket in constructing an NConnection"); }
        } else { logAt(warning, "Invalid network context in constructing an NConnection"); }
      }

      Socket socket() { return socket_; }

      void close() { if ( socket_ ) { socket_->close(); } }

    protected:
      NConnection(shared_ptr<NContext> ctxt, Socket s)
        : ::K3::NConnection<NContext, AsioDevice, Socket>("NConnection", ctxt, s),
          LogMT("NConnection"), socket_(s)
      {}
      
      Socket socket_;
    };
  }


  //----------------------------------------------
  // Nanomsg low-level networking implementation.
  //
  // Currently, this is a blocking implementation.

  namespace Nanomsg
  {
    class NanomsgDeviceException : public system_error {
    public:
      NanomsgDeviceException(const error_code& ec) : system_error(ec) {}
      
      NanomsgDeviceException(const error_code& ec, const string& msg)
        : system_error(ec, msg)
      {}
    };

    class NanomsgDevice
    {
    public:
      typedef char char_type;
      typedef bidirectional_device_tag category;

      NanomsgDevice(int s) : socket(s) {}

      streamsize read(char* s, streamsize n) 
      {
        int rDone = nn_recv(socket, s, n, 0);
        if ( rDone < 0 ) {
          int err = nn_errno();
          string errStr(nn_strerror(err));
          throw NanomsgDeviceException(make_error_code(static_cast<errc>(err)), errStr);
        }
        return rDone;
      }
      
      streamsize write(const char* s, streamsize n)
      {
        int wDone = nn_send(socket, s, n, 0);
        if ( wDone < 0 ) {
          int err = nn_errno();
          string errStr(nn_strerror(err));
          throw NanomsgDeviceException(make_error_code(static_cast<errc>(err)), errStr);
        }
        return wDone;
      }

    protected:
      int socket;
    };

    class NContext {
    public:
      NContext() {
        listenerThreads = shared_ptr<thread_group>(new thread_group());
      }

      // K3 Nanomsg endpoints use the TCP transport.
      string urlOfAddress(Address addr) { return string("tcp://") + addressAsString(addr); }

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

    class NConnection : public ::K3::NConnection<NContext, NanomsgDevice, int>
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

    protected:
      NConnection(shared_ptr<NContext> ctxt, Socket s)
        : ::K3::NConnection<NContext, NanomsgDevice, Socket>("NConnection", ctxt, s),
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
