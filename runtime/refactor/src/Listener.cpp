#include "Listener.hpp"
#include "Peer.hpp"

Listener::Listener(shared_ptr<Peer> peer,
                   shared_ptr<IncomingConnectionFactory> in_factory)
  : in_conns_() {
  peer_ = peer;
  auto service = NetworkManager::getInstance().getIOService();
  acceptor_ = make_shared<boost::asio::ip::tcp::acceptor>(*service);
  conn_factory_ = in_factory;

  // Accept connections at the peer's address
  Address addr = peer_->address();
  auto ip = boost::asio::ip::address_v4(std::get<0>(addr));
  boost::asio::ip::tcp::endpoint ep(ip, std::get<1>(addr));
  acceptor_->open(ep.protocol());
  boost::asio::socket_base::reuse_address option(true);
  acceptor_->set_option(option);
  acceptor_->bind(ep);
  acceptor_->listen();
  acceptConnection();
}

void Listener::acceptConnection() {
  shared_ptr<MessageHandler> rcv_m_handler = make_shared<MessageHandler>(
  [this] (shared_ptr<Message> m) {
    peer_->enqueue(m);
  });

  // The error handler needs to be set after the connection is created
  // since it caputres the connection pointer
  shared_ptr<IncomingConnection> conn = (*conn_factory_)(rcv_m_handler, nullptr);
  shared_ptr<ErrorHandler> rcv_e_handler;
  rcv_e_handler = make_shared<ErrorHandler>([this, conn] (const boost::system::error_code& ec) {
    if (ec) {
      in_conns_.erase(conn);
    }
  });
  conn->setErrorHandler(rcv_e_handler);

  acceptor_->async_accept(*conn->getSocket(), [this, conn] (const boost::system::error_code& ec) {
    if (ec) {
      throw std::runtime_error("Listener acceptConnection(): " + ec.message());
    }

    registerConnection(conn);
    acceptConnection();
  });
}

void Listener::registerConnection(shared_ptr<IncomingConnection> c) {
  in_conns_.insert(c);
  c->receiveMessages();
}

int Listener::numConnections() {
  return in_conns_.size();
}
