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

class Peer {
 public:
  Peer();
  explicit Peer(bool skip_init);
  void initialize();
  void enqueue(unique_ptr<Message> m);
  void run(std::function<void()> registerCallback);
  void run();
  void join();
  ProgramContext* context();

 protected:
  unique_ptr<thread> thread_;
  unique_ptr<ProgramContext> context_;
  unique_ptr<Queue> queue_;
  ContextConstructor context_constructor_;
};

#endif
