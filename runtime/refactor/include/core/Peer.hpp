#ifndef K3_PEER
#define K3_PEER

// Peers own a message Queue and a ProgramContext.
// They run an event loop in their own thread, repeatedly pulling
// messages from the Queue and dispatching them to the ProgramContext.
// A peer stops processing when a special SentinelValue is dispatched.

#include <atomic>
#include <memory>

#include "boost/thread.hpp"
#include "boost/chrono.hpp"

#include "spdlog/spdlog.h"
#include "yaml-cpp/yaml.h"

#include "types/Message.hpp"
#include "ProgramContext.hpp"
#include "types/Queue.hpp"

namespace K3 {

using std::shared_ptr;

class Peer {
 public:
  Peer(const Address& addr, shared_ptr<ContextFactory> fac,
       const YAML::Node& peer_config, std::function<void()> ready_callback);
  void start();
  void processRole();
  void join();
  void enqueue(shared_ptr<Message> m);

  bool finished();
  Address address();
  shared_ptr<ProgramContext> getContext();

 protected:
  void logMessage(const Message& m);
  void logGlobals();

  Address address_;
  shared_ptr<spdlog::logger> logger_;
  shared_ptr<boost::thread> thread_;
  shared_ptr<Queue> queue_;
  shared_ptr<ProgramContext> context_;

  std::atomic<bool> start_processing_;
  std::atomic<bool> finished_;
};

}  // namespace K3

#endif
