#ifndef K3_RUNTIME_NETWORK_HPP
#define K3_RUNTIME_NETWORK_HPP

#include <ios>
#include <memory>
#include <system_error>
#include <boost/asio.hpp>
#include <boost/iostreams/categories.hpp>
#include <boost/iostreams/stream.hpp>
#include <boost/system/error_code.hpp>

namespace K3
{
  using namespace std;
  using namespace boost;

  using namespace boost::iostreams;
  using namespace boost::asio;

  template<typename NContext>
  class NEndpoint : public virtual LogMT {
  public:
    NEndpoint(string logId, shared_ptr<NContext> ctxt) : LogMT(logId) {}
  };

  template<typename NContext>
  class NConnection : public virtual LogMT {
  public:
    NConnection(string logId, shared_ptr<NContext> ctxt) : LogMT(logId) {}
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

    class NContext {
    public:
      shared_ptr<io_service> service;
      ip::address address;
      unsigned short port;
    };

    class NEndpoint : public ::K3::NEndpoint<NContext>
    {
    public:
      NEndpoint(shared_ptr<NContext> ctxt)
        : ::K3::NEndpoint<NContext>("NEndpoint", ctxt), LogMT("NEndpoint")
      {
        if ( ctxt ) {
          ip::tcp::endpoint ep(ctxt->address, ctxt->port);
          acceptor = shared_ptr<ip::tcp::acceptor>(new ip::tcp::acceptor(*(ctxt->service), ep));
        } else {
          logAt(warning, "Invalid network context in constructing an NEndpoint");
        }
      }
    
    protected:
      shared_ptr<ip::tcp::acceptor> acceptor;
    };

    class NConnection : public stream<AsioDevice>, public ::K3::NConnection<NContext>
    {
    public:
      NConnection(shared_ptr<NContext> ctxt)
        : NConnection(ctxt, shared_ptr<ip::tcp::socket>(new ip::tcp::socket(*(ctxt->service))))
      {
        if ( ctxt ) {
          if ( socket ) {
            ip::tcp::endpoint ep(ctxt->address, ctxt->port);
            shared_ptr<LogMT> logger(static_cast<LogMT*>(this));
            socket->async_connect(ep,
              [=](const boost::system::error_code& error) { 
                BOOST_LOG(*logger) << "connected to " << ctxt->address.to_string() << ctxt->port;
              } );
          } else { logAt(warning, "Uninitialized socket in constructing an NConnection"); }
        } else { logAt(warning, "Invalid network context in constructing an NConnection"); }
      }

    protected:
      NConnection(shared_ptr<NContext> ctxt, shared_ptr<ip::tcp::socket> s)
        : ::K3::NConnection<NContext>("NConnection", ctxt), 
          stream<AsioDevice>(s), LogMT("NConnection"), socket(s)
      {}
      
      shared_ptr<ip::tcp::socket> socket;
    };
  }

  // TODO: nanomsg implementation.
  namespace Nanomsg {}
}

#endif