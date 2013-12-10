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
#include <k3/runtime/cpp/Common.hpp>

namespace K3
{
  using namespace std;
  using namespace boost;

  using namespace boost::iostreams;
  using namespace boost::asio;

  template<typename NContext>
  class NEndpoint : public virtual LogMT {
  public:
    NEndpoint(const string& logId, shared_ptr<NContext> ctxt) : LogMT(logId) {}
    NEndpoint(const char* logId, shared_ptr<NContext> ctxt) : LogMT(logId) {}
  };

  template<typename NContext>
  class NConnection : public virtual LogMT {
  public:
    NConnection(const string& logId, shared_ptr<NContext> ctxt) : LogMT(logId) {}
    NConnection(const char* logId, shared_ptr<NContext> ctxt) : LogMT(logId) {}
  };

  namespace Asio
  {
    //--------------------------
    // Low-level networking.

    class AsioDeviceException : public ios_base::failure {
    public:
      AsioDeviceException(const string& msg) : ios_base::failure(msg) {}
      
      AsioDeviceException(const string& msg, const error_code& ec)
        : ios_base::failure(msg, ec)
      {}
    };

    class AsioDevice {
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
          throw AsioDeviceException(ec.message(), 
                  make_error_code(static_cast<errc>(ec.value())));
        }
      }
      
      streamsize write(const char* s, streamsize n)
      {
        boost::system::error_code ec;
        std::size_t rval = socket->write_some(boost::asio::buffer(s, n), ec);
        if (!ec) { return rval; }
        else if (ec == boost::asio::error::eof) { return -1; }
        else {
          throw AsioDeviceException(ec.message(),
                  make_error_code(static_cast<errc>(ec.value())));
        }
      }

    protected:
      shared_ptr<ip::tcp::socket> socket;
    };

    // Boost implementation of a network context.
    class NContext {
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

    // TODO: close method for IOHandle
    // Low-level endpoint class. This is a wrapper around a Boost tcp acceptor.
    class NEndpoint : public ::K3::NEndpoint<NContext>
    {
    public:
      NEndpoint(shared_ptr<NContext> ctxt, Address addr)
        : ::K3::NEndpoint<NContext>("NEndpoint", ctxt), LogMT("NEndpoint")
      {
        if ( ctxt ) {
          ip::tcp::endpoint ep(get<0>(addr), get<1>(addr));
          acceptor_ = shared_ptr<ip::tcp::acceptor>(new ip::tcp::acceptor(*(ctxt->service), ep));
        } else {
          logAt(warning, "Invalid network context in constructing an NEndpoint");
        }
      }

      shared_ptr<ip::tcp::acceptor> acceptor() { return acceptor_; }
      void close() { if ( acceptor_ ) { acceptor_->close(); } }

    protected:
      shared_ptr<ip::tcp::acceptor> acceptor_;
    };

    // TODO: close method for IOHandle
    // Low-level connection class. This is a wrapper around a Boost tcp socket.
    class NConnection : public stream<AsioDevice>, public ::K3::NConnection<NContext>
    {
    public:
      NConnection(shared_ptr<NContext> ctxt)
        : NConnection(ctxt, shared_ptr<ip::tcp::socket>(new ip::tcp::socket(*(ctxt->service))))
      {}

      NConnection(shared_ptr<NContext> ctxt, Address addr)
        : NConnection(ctxt, shared_ptr<ip::tcp::socket>(new ip::tcp::socket(*(ctxt->service))))
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

      shared_ptr<ip::tcp::socket> socket() { return socket_; }

      void close() { if ( socket_ ) { socket_->close(); } }

    protected:
      NConnection(shared_ptr<NContext> ctxt, shared_ptr<ip::tcp::socket> s)
        : ::K3::NConnection<NContext>("NConnection", ctxt), 
          stream<AsioDevice>(s), LogMT("NConnection"), socket_(s)
      {}
      
      shared_ptr<ip::tcp::socket> socket_;
    };
  }

  // TODO: nanomsg implementation.
  namespace Nanomsg {}
}

#endif