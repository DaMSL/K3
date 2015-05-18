#include "Peer.hpp"
#include "Engine.hpp"

Peer::Peer() {
  initialize();
}

Peer::Peer(bool skip_init) {
  if (!skip_init) {
    initialize();
  }
}

void Peer::initialize() {
  ContextConstructor* constructor = Engine::getInstance().getContextConstructor();
  context_ = (*constructor)();
  queue_ = make_unique<Queue>();
}


void Peer::enqueue(unique_ptr<Message> m) {
  queue_->enqueue(std::move(m));
}


void Peer::run(std::function<void()> registerCallback) {
  if (thread_) {
    throw std::runtime_error("Peer run(): already running ");
  }

  auto work = [this, registerCallback] () {
    if (!queue_ && !context_) {
      initialize();
    }
    registerCallback();

    try {
      while (true) {
        unique_ptr<Message> m = queue_->dequeue();
        m->value()->dispatchIntoContext(context_.get(), m->trigger());
      }
    } catch (EndOfProgramException e) {
      return;
    }
  };

  thread_ = make_unique<thread>(work);
}

void Peer::run() {
  return run([] () {
    return;
  });
}

void Peer::join() {
  if (!thread_) {
    throw std::runtime_error("Peer join(): not running ");
  }

  thread_->join();
  return;
}

ProgramContext* Peer::context() {
  if (context_) {
    return context_.get();
  } else {
    throw std::runtime_error("Peer context(): null context pointer");
  }
}
