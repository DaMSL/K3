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

    class NEndpoint : public virtual LogMT
    {
    public:
      NEndpoint(shared_ptr<io_service> ios, ip::address addr, unsigned short port) 
        : LogMT("NEndpoint")
      {
        ip::tcp::endpoint ep(addr, port);
        acceptor = shared_ptr<ip::tcp::acceptor>(new ip::tcp::acceptor(*ios, ep));
      }
    
    protected:
      shared_ptr<ip::tcp::acceptor> acceptor;
    };

    class NConnection : public stream<AsioDevice>, public virtual LogMT
    {
    public:
      NConnection(shared_ptr<io_service> ios, ip::address addr, unsigned short port)
        : NConnection(shared_ptr<ip::tcp::socket>(new ip::tcp::socket(*ios)))
      {
        if ( socket ) {
          ip::tcp::endpoint ep(addr, port);
          shared_ptr<LogMT> logger(static_cast<LogMT*>(this));
          socket->async_connect(ep,
            [=](const boost::system::error_code& error) { 
              BOOST_LOG(*logger) << "connected to " << addr.to_string() << port;
            } );
        }
      }

    protected:
      NConnection(shared_ptr<ip::tcp::socket> s)
        : stream<AsioDevice>(s), LogMT("NConnection"), socket(s)
      {}
      
      shared_ptr<ip::tcp::socket> socket;
    };
  }

  // TODO: nanomsg implementation.
  namespace Nanomsg {}
}

#endif