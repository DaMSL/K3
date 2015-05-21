#include <memory>

#include "Peer.hpp"
#include "Engine.hpp"
#include "NetworkManager.hpp"

Peer::Peer(const Address& addr, shared_ptr<ContextFactory> fac,
           const YAML::Node& peer_config,
           std::function<void()> ready_callback) {
  address_ = addr;

  auto work = [this, fac, peer_config, ready_callback] () {
    queue_ = make_shared<Queue>();
    context_ = (*fac)();
    context_->__patch(peer_config);
    ready_callback();

    try {
      while (true) {
        shared_ptr<Message> m = queue_->dequeue();
        m->value()->dispatchIntoContext(context_.get(), m->trigger());
      }
    } catch (EndOfProgramException e) {
      return;
    }
  };

  thread_ = make_shared<thread>(work);
}

void Peer::start() {
  if (!context_) {
    throw std::runtime_error("Peer start(): null context ptr");
  }

  context_->__processRole();
}

void Peer::enqueue(shared_ptr<Message> m) {
  if (!queue_) {
    throw std::runtime_error("Peer enqueue(): null queue ptr");
  }
  queue_->enqueue(std::move(m));
}


void Peer::join() {
  if (!thread_) {
    throw std::runtime_error("Peer join(): not running ");
  }

  thread_->join();
  return;
}

Address Peer::address() {
  return address_;
}

shared_ptr<ProgramContext> Peer::getContext() {
  return context_;
}
