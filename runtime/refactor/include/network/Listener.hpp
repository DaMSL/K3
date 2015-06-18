#ifndef K3_LISTENER
#define K3_LISTENER

#include <memory>

#include "network/NetworkManager.hpp"

namespace K3 {

typedef std::function<shared_ptr<IncomingConnection>(
    asio::io_service&, CodecFormat)> IncomingConnectionFactory;

class Peer;
class Listener : public std::enable_shared_from_this<Listener> {
 public:
  Listener(asio::io_service&, const Address& address, shared_ptr<Peer>,
           CodecFormat, shared_ptr<IncomingConnectionFactory>);
  void acceptConnection(shared_ptr<ErrorHandler>);
  int numConnections();

 protected:
  void registerConnection(shared_ptr<IncomingConnection>);

  Address address_;
  shared_ptr<Peer> peer_;
  shared_ptr<boost::asio::ip::tcp::acceptor> acceptor_;
  CodecFormat format_;
  shared_ptr<IncomingConnectionFactory> conn_factory_;
  shared_ptr<IncomingConnectionMap> in_conns_;
};

}  // namespace K3

#endif
