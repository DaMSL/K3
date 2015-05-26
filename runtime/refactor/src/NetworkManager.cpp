#include "NetworkManager.hpp"
#include "Peer.hpp"
#include "IncomingConnection.hpp"
#include "OutgoingConnection.hpp"
#include "Listener.hpp"

namespace K3 {

NetworkManager::NetworkManager() {
  io_service_ = make_shared<asio::io_service>();
  work_ = make_shared<asio::io_service::work>(*io_service_);
  threads_ = make_shared<boost::thread_group>();
  internal_listeners_ = make_shared<ListenerMap>();
  external_listeners_ = make_shared<ListenerMap>();
  internal_out_conns_ = make_shared<InternalConnectionMap>();
  external_out_conns_ = make_shared<ExternalConnectionMap>();
  internal_format_ = CodecFormat::BoostBinary;
  running_ = true;
  addThread();
}

NetworkManager::~NetworkManager() {
  if (running_) {
    stop();
    join();
  }
}

void NetworkManager::stop() {
  work_.reset();
  io_service_->stop();
}

void NetworkManager::join() {
  if (running_) {
    threads_->join_all();
    running_ = false;
  }
  return;
}

void NetworkManager::listenInternal(shared_ptr<Peer> peer) {
  // Construct Listener
  auto fac_ptr = make_shared<IncomingConnectionFactory>(
  [] (asio::io_service& service, CodecFormat format) {
    return make_shared<InternalIncomingConnection>(service, format);
  });
  auto listen_addr = peer->address();
  auto listener = make_shared<Listener>(*io_service_, listen_addr, peer, internal_format_, fac_ptr);
  internal_listeners_->insert(listen_addr, listener);

  // Accept connections, removing the listener upon error
  shared_ptr<ListenerMap> listener_map = internal_listeners_;
  auto e_handler = make_shared<ErrorHandler>([listener_map, listen_addr] (boost_error ec) {
    std::cout << "Listener error handler: " << ec.message() << std::endl;
    listener_map->erase(listen_addr);
  });
  listener->acceptConnection(e_handler);
}

void NetworkManager::listenExternal(shared_ptr<Peer> peer, Address listen_addr, TriggerID trig, CodecFormat format) {
  // Consrtuct Listener
  auto peer_addr = peer->address();
  auto fac_ptr = make_shared<IncomingConnectionFactory>(
  [peer_addr, trig] (asio::io_service& service, CodecFormat format) {
    return make_shared<ExternalIncomingConnection>(service, format, peer_addr, trig);
  });
  auto listener = make_shared<Listener>(*io_service_, listen_addr, peer, format, fac_ptr);
  external_listeners_->insert(listen_addr, listener);

  // Accept connections, removing the listener upon error
  shared_ptr<ListenerMap> listener_map = external_listeners_;
  auto e_handler = make_shared<ErrorHandler>([listener_map, listen_addr] (boost_error ec) {
    std::cout << "Listener error handler: " << ec.message() << std::endl;
    listener_map->erase(listen_addr);
  });
  listener->acceptConnection(e_handler);
}

void NetworkManager::sendInternal(shared_ptr<NetworkMessage> pm) {
  // Check for an existing connection, creating one if necessary
  shared_ptr<InternalOutgoingConnection> c = internal_out_conns_->lookup(pm->destination());
  if (!c) {
    c = connectInternal(pm->destination());
  }

  // Send, removing the connection upon error
  shared_ptr<InternalConnectionMap> conn_map = internal_out_conns_;
  auto e_handler = make_shared<ErrorHandler>([conn_map, pm] (boost_error ec) {
    std::cout << "Send error: " << ec.message() << std::endl;
    conn_map->erase(pm->destination());
  });

  c->send(pm, e_handler);
}

void NetworkManager::sendExternal(const Address& addr, shared_ptr<PackedValue> pm) {
  // Check for an existing connection, creating one if necessary
  shared_ptr<ExternalOutgoingConnection> c = external_out_conns_->lookup(addr);
  if (!c) {
    c = connectExternal(addr);
  }

  // Send, removing the connection upon error
  shared_ptr<ExternalConnectionMap> conn_map = external_out_conns_;
  auto e_handler = make_shared<ErrorHandler>([addr, conn_map] (boost_error ec) {
    std::cout << "Send error: " << ec.message() << std::endl;
    conn_map->erase(addr);
  });

  c->send(pm, e_handler);
}

shared_ptr<Listener> NetworkManager::getListener(const Address& a) {
  if (auto res = internal_listeners_->lookup(a)) {
    return res;
  } else {
    throw std::runtime_error("NetworkManager getListener(): failed");
  }
}

CodecFormat NetworkManager::internalFormat() {
  return internal_format_;
}

shared_ptr<InternalOutgoingConnection> NetworkManager::connectInternal(const Address& a) {
  auto c = make_shared<InternalOutgoingConnection>(a, *io_service_);
  internal_out_conns_->insert(a, c);
  return c;
}

shared_ptr<ExternalOutgoingConnection> NetworkManager::connectExternal(const Address& a) {
  auto c = make_shared<ExternalOutgoingConnection>(a, *io_service_);
  external_out_conns_->insert(a, c);
  return c;
}

void NetworkManager::addThread() {
  threads_->create_thread([this] () {
    io_service_->run();
  });
}

}  // namespace K3
