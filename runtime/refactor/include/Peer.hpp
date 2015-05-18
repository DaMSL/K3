#ifndef K3_PEER
#define K3_PEER

// Peers own a message Queue and a ProgramContext.
// They run an event loop in their own thread, repeatedly pulling
// messages from the Queue and dispatching them to the ProgramContext.
// A peer stops processing when a special SentinelValue is dispatched.

#include <memory>
#include <thread>

#include "Message.hpp"
#include "ProgramContext.hpp"
#include "Queue.hpp"

using std::make_unique;
using std::thread;

template <class ContextImpl>
class Peer {
 public:
  Peer() {
    initialize();
  }

  explicit Peer(bool skip_init) {
    if (!skip_init) {
      initialize();
    }
  }

  void initialize() {
    context_ = make_unique<ContextImpl>();
    queue_ = make_unique<Queue>();
  }

  void enqueue(unique_ptr<Message> m) {
    queue_->enqueue(std::move(m));
  }

  void run(std::function<void()> registerCallback) {
    if (thread_) {
      throw std::runtime_error("Peer run() invalid: already running ");
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

  void run() {
    return run([] () {
      return;
    });
  }

  void join() {
    if (!thread_) {
      throw std::runtime_error("Peer join() invalid: not running ");
    }

    thread_->join();
    return;
  }

  ProgramContext* context() {
    return context_.get();
  }

 protected:
  unique_ptr<thread> thread_;
  unique_ptr<ProgramContext> context_;
  unique_ptr<Queue> queue_;
};

#endif
