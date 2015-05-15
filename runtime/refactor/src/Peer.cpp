#include "Peer.hpp"

Peer::Peer(unique_ptr<ProgramContext> c) {
  context_ = std::move(c);
  queue_ = make_unique<Queue>();
}

void Peer::enqueue(unique_ptr<Message> m) {
  queue_->enqueue(std::move(m));
}

void Peer::run() {
  if (thread_) {
    throw std::runtime_error("Peer run() invalid: already running ");
  }

  auto loop = [this] () {
    try {
      while (true) {
        context_->dispatch(queue_->dequeue());
      }
    } catch (EndOfProgramException e) {
      return;
    }
  };

  thread_ = make_unique<thread>(loop);
}

void Peer::join() {
  if (!thread_) {
    throw std::runtime_error("Peer join() invalid: not running ");
  }

  thread_->join();
  return;
}
