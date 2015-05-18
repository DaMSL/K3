#include <map>
#include <list>

#include "Engine.hpp"

Engine& Engine::getInstance() {
  static Engine instance;
  return instance;
}

void Engine::initialize(const list<Address>& peer_addrs, ContextConstructor c) {
  context_constructor_ = make_unique<ContextConstructor>(c);
  for (auto addr : peer_addrs) {
    peers_[addr] = make_unique<Peer>(true);
  }
  num_total_peers_ = peer_addrs.size();
}

void Engine::send(unique_ptr<Message> m) {
  auto it = peers_.find(m->destination());
  if (it != peers_.end()) {
    it->second->enqueue(std::move(m));
  } else {
    throw std::runtime_error("Engine send(): cannot send non-local messages yet!");
  }
}

void Engine::run() {
  for (auto& it : peers_) {
    it.second->run([this] () {
      registerPeer();
    });
  }

  // Busy-wait until all peers are ready
  while (num_total_peers_ > num_ready_peers_.load()) continue;

  return;
}

void Engine::sendSentinel() {
  for (auto& it : peers_) {
    unique_ptr<Message> m;
    m = make_unique<Message>(it.first, it.first, -1, make_unique<SentinelValue>());
    send(std::move(m));
  }
}

void Engine::join() {
  for (auto& it : peers_) {
    it.second->join();
  }
}

void Engine::cleanup() {
  peers_ = map<Address, unique_ptr<Peer>>();
  num_total_peers_ = 0;
  num_ready_peers_.store(0);
}

Peer* Engine::getPeer(const Address& a) {
  Peer* p;
  auto it = peers_.find(a);
  if (it != peers_.end()) {
    p = it->second.get();
  } else {
    throw std::runtime_error("Engine getPeer(): failed to find the provided address");
  }
  return p;
}

void Engine::registerPeer() {
  num_ready_peers_++;
}

ContextConstructor* Engine::getContextConstructor() {
  if (!context_constructor_) {
    throw std::runtime_error("Engine getContextConstructor(): null constructor pointer");
  }
  return context_constructor_.get();
}
