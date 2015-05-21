#ifndef K3_LISTENER
#define K3_LISTENER

#include "NetworkManager.hpp"

typedef std::function<shared_ptr<IncomingConnection>(shared_ptr<MessageHandler>,
    shared_ptr<ErrorHandler>)> IncomingConnectionFactory;

class Peer;
class Listener {
 public:
  Listener(shared_ptr<Peer> peer, shared_ptr<IncomingConnectionFactory> in_constructor);
  int numConnections();

 protected:
  void acceptConnection();
  void registerConnection(shared_ptr<IncomingConnection> c);

  shared_ptr<Peer> peer_;
  shared_ptr<boost::asio::ip::tcp::acceptor> acceptor_;
  shared_ptr<IncomingConnectionFactory> conn_factory_;
  ConcurrentSet<shared_ptr<IncomingConnection>> in_conns_;
};

#endif
