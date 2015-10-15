#include <string>
#include <thread>
#include <chrono>

#include "core/Peer.hpp"
#include "core/ProgramContext.hpp"
#include "network/Listener.hpp"
#include "network/IncomingConnection.hpp"

namespace K3 {

Listener::Listener(asio::io_service& service, const Address& address,
                   shared_ptr<Peer> peer, CodecFormat format,
                   shared_ptr<IncomingConnectionFactory> in_factory) {
  address_ = address;
  peer_ = peer;
  format_ = format;
  in_conns_ = make_shared<IncomingConnectionMap>();
  conn_factory_ = in_factory;

  // Prepare to accept connections
  for (int tries = 24; tries > 0; tries--) {
    try {
      acceptor_ = make_shared<boost::asio::ip::tcp::acceptor>(service);
      auto ip = boost::asio::ip::address_v4(address_.ip);
      boost::asio::ip::tcp::endpoint ep(ip, address_.port);
      acceptor_->open(ep.protocol());
      boost::asio::socket_base::reuse_address option(true);
      acceptor_->set_option(option);
      acceptor_->bind(ep);
      acceptor_->listen();
    }
    catch (std::exception& e) {
      std::cout << "Failed to bind to port: " << address.port
                << ". Retrying in 5 seconds" << std::endl;
      if (tries > 0) {
        std::this_thread::sleep_for(std::chrono::seconds(5));
      } else {
        throw e;
      }
    }
  }
}

void Listener::acceptConnection(shared_ptr<ErrorHandler> acpt_e_handler) {
  shared_ptr<IncomingConnection> conn =
      (*conn_factory_)(acceptor_->get_io_service(), format_);

  shared_ptr<Listener> this_shared = shared_from_this();
  acceptor_->async_accept(
      *conn->getSocket(),
      [this_shared, conn, acpt_e_handler](const boost::system::error_code& ec) {
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
  auto tok = peer_->getQueue().newProducerToken();

  shared_ptr<MessageHandler> m_handler = make_shared<MessageHandler>(
      [peer, tok](std::unique_ptr<Message> m) {
        auto d = peer->getContext().__getDispatcher(std::move(m->value_), m->trigger_);
        #ifdef K3DEBUG
        d->trigger_ = m->trigger_;
        d->source_ = m->source_;
        d->destination_ = m->destination_;
        #endif
        peer->getQueue().enqueue(tok, std::move(d));
      });

  shared_ptr<IncomingConnectionMap> conn_map = in_conns_;
  shared_ptr<ErrorHandler> e_handler;
  e_handler = make_shared<ErrorHandler>([conn_map, a](
      const boost::system::error_code& ec) { conn_map->erase(a); });

  c->receiveMessages(m_handler, e_handler);
}

int Listener::numConnections() { return in_conns_->size(); }

}  // namespace K3
