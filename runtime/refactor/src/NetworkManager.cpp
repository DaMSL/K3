#include "NetworkManager.hpp"
#include "Peer.hpp"
#include "Listener.hpp"

NetworkManager& NetworkManager::getInstance() {
  static NetworkManager instance;
  return instance;
}

void NetworkManager::run() {
  io_service_ = make_shared<asio::io_service>();
  work_ = make_shared<asio::io_service::work>(*io_service_);
  threads_ = make_shared<boost::thread_group>();
  internal_listeners_ = make_shared<ConcurrentMap<Address, Listener>>();
  internal_out_conns_ = make_shared<ConcurrentMap<Address, InternalOutgoingConnection>>();
  internal_format_ = CodecFormat::BoostBinary;
  addThread();
}

void NetworkManager::stop() {
  work_.reset();
  io_service_->stop();
}

void NetworkManager::join() {
  threads_->join_all();
  internal_listeners_.reset();
  internal_out_conns_.reset();
  return;
}

void NetworkManager::listenInternal(shared_ptr<Peer> peer) {
  auto fac_ptr = make_shared<IncomingConnectionFactory>(
  [] (shared_ptr<MessageHandler> m, shared_ptr<ErrorHandler> e) {
    return make_shared<InternalIncomingConnection>(m, e);
  });

  auto listener = make_shared<Listener>(peer, fac_ptr);
  internal_listeners_->insert(peer->address(), listener);
}

void NetworkManager::connectInternal(const Address& a) {
  shared_ptr<InternalOutgoingConnection> c = make_shared<InternalOutgoingConnection>(a);
  internal_out_conns_->insert(a, c);
}

void NetworkManager::sendInternal(shared_ptr<NetworkMessage> pm) {
  shared_ptr<InternalOutgoingConnection> c = internal_out_conns_->lookup(pm->destination());
  if (!c) {
    connectInternal(pm->destination());
  }
  c = internal_out_conns_->lookup(pm->destination());

  c->send(pm);
}

shared_ptr<asio::io_service> NetworkManager::getIOService() {
  return io_service_;
}

shared_ptr<Listener> NetworkManager::getListener(const Address& a) {
  return internal_listeners_->lookup(a);
}

CodecFormat NetworkManager::internalFormat() {
  return internal_format_;
}

void NetworkManager::addThread() {
  threads_->create_thread([this] () {
    io_service_->run();
  });
}
