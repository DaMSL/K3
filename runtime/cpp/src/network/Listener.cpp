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
                   IncomingConnectionFactory in_factory) :
  address_(address), peer_(peer), format_(format),
  acceptor_(service), conn_factory_(in_factory) {
  auto ip = boost::asio::ip::address_v4(address_.ip);
  boost::asio::ip::tcp::endpoint ep(ip, address_.port);

  // Prepare to accept connections
  for (int tries = 24; tries > 0; tries--) {
    try {
      acceptor_.open(ep.protocol());
      acceptor_.set_option(boost::asio::socket_base::reuse_address(true));
      acceptor_.bind(ep);
      acceptor_.listen();
      break;
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


void Listener::registerConnection(shared_ptr<InternalIncomingConnection> c) {
  string s = c->getSocket().remote_endpoint().address().to_string();
  unsigned short port = c->getSocket().remote_endpoint().port();
  Address a = make_address(s, port);
  in_conns_.insert(a, c);

  auto tok = peer_->getQueue().newProducerToken();

  auto m_handler = [peer = peer_, tok](std::unique_ptr<Message> m) {
        auto d = peer->getContext().__getDispatcher(std::move(m->value_), m->trigger_);
        #ifdef K3MESSAGETRACE
        d->source_ = m->source_;
        d->destination_ = m->destination_;
        #endif
        #if defined(K3MESSAGETRACE) || defined(K3TRIGGERTIMES)
        d->trigger_ = m->trigger_;
        #endif
        peer->getQueue().enqueue(tok, std::move(d));
      };

  auto e_handler = [this_shared = shared_from_this(), a](const boost::system::error_code& ec) {
        this_shared->in_conns_.erase(a);
        if (ec.message() != "End of file") {
          std::cout << "Encountered invalid message. Closing connection.\n";
          std::cout << ec.message() << "\n";
        }
      };

  c->receiveMessages(std::move(m_handler), std::move(e_handler));
}

int Listener::numConnections() { return in_conns_.size(); }

}  // namespace K3
