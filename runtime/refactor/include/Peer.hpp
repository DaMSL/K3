#ifndef K3_PEER
#define K3_PEER

// Peers own a message Queue and a ProgramContext.
// They run an event loop in their own thread, repeatedly pulling
// messages from the Queue and dispatching them to the ProgramContext.
// A peer stops processing when a special SentinelValue is dispatched.

#include <memory>
#include <thread>

#include "yaml-cpp/yaml.h"

#include "Message.hpp"
#include "ProgramContext.hpp"
#include "Queue.hpp"

namespace K3 {

using std::shared_ptr;
using std::thread;

class Peer {
 public:
  Peer(const Address& addr,
       shared_ptr<ContextFactory> fac,
       const YAML::Node& peer_config,
       std::function<void()> ready_callback);
  void start();
  void join();
  void enqueue(shared_ptr<Message> m);
  Address address();
  shared_ptr<ProgramContext> getContext();

 protected:
  Address address_;
  shared_ptr<thread> thread_;
  shared_ptr<Queue> queue_;
  shared_ptr<ProgramContext> context_;
};

}  // namespace K3

#endif
