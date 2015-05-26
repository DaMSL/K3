#include <string>

#include "Listener.hpp"
#include "IncomingConnection.hpp"
#include "Peer.hpp"

namespace K3 {

Listener::Listener(asio::io_service& service,
                   const Address& address,
                   shared_ptr<Peer> peer,
                   CodecFormat format,
                   shared_ptr<IncomingConnectionFactory> in_factory) {
  address_ = address;
  peer_ = peer;
  acceptor_ = make_shared<boost::asio::ip::tcp::acceptor>(service);
  format_ = format;
  in_conns_ = make_shared<IncomingConnectionMap>();
  conn_factory_ = in_factory;

  // Prepare to accept connections
  auto ip = boost::asio::ip::address_v4(std::get<0>(address_));
  boost::asio::ip::tcp::endpoint ep(ip, std::get<1>(address_));
  acceptor_->open(ep.protocol());
  boost::asio::socket_base::reuse_address option(true);
  acceptor_->set_option(option);
  acceptor_->bind(ep);
  acceptor_->listen();
}

void Listener::acceptConnection(shared_ptr<ErrorHandler> acpt_e_handler) {
  shared_ptr<IncomingConnection> conn = (*conn_factory_)(acceptor_->get_io_service(), format_);

  shared_ptr<Listener> this_shared = shared_from_this();
  acceptor_->async_accept(*conn->getSocket(), [this_shared, conn, acpt_e_handler]
  (const boost::system::error_code& ec) {
    if (ec) {
      (*acpt_e_handler)(ec);
    } else {
      this_shared->registerConnection(conn);
      this_shared->acceptConnection(acpt_e_handler);
    }
  });
}

void Listener::registerConnection(shared_ptr<IncomingConnection> c) {
  string s = c->getSocket()->remote_endpoint().address().to_string();
  unsigned short port = c->getSocket()->remote_endpoint().port();
  Address a = make_address(s, port);
  in_conns_->insert(a, c);

  shared_ptr<Peer> peer = peer_;
  shared_ptr<MessageHandler> m_handler = make_shared<MessageHandler>(
  [peer] (shared_ptr<Message> m) {
    peer->enqueue(m);
  });

  shared_ptr<IncomingConnectionMap> conn_map = in_conns_;
  shared_ptr<ErrorHandler> e_handler;
  e_handler = make_shared<ErrorHandler>([conn_map, a] (const boost::system::error_code& ec) {
    conn_map->erase(a);
  });

  c->receiveMessages(m_handler, e_handler);
}

int Listener::numConnections() {
  return in_conns_->size();
}

}  // namespace K3
