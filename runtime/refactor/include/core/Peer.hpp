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
       const YAML::Node& peer_config, std::function<void()> ready_callback,
       const string& json_path, bool json_final_only);
  void start();
  void processRole();
  void join();
  void enqueue(shared_ptr<Message> m);

  template <class F>
  void logJson(int trigger, const Address& src, F f);
  bool finished();
  Address address();
  shared_ptr<ProgramContext> getContext();

 protected:
  void logMessage(const Message& m);
  void logGlobals(const Message& m);

  Address address_;
  shared_ptr<spdlog::logger> logger_;
  shared_ptr<boost::thread> thread_;
  shared_ptr<Queue> queue_;
  shared_ptr<ProgramContext> context_;

  std::atomic<bool> start_processing_;
  std::atomic<bool> finished_;

  shared_ptr<std::ofstream> json_globals_log_;
  shared_ptr<std::ofstream> json_messages_log_;
  bool json_final_state_only_;
  int message_counter_;
};

template <class F>
void Peer::logJson(int trigger, const Address& src, F f) {
  if (json_messages_log_ && !json_final_state_only_) {
    auto t = std::chrono::system_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(t.time_since_epoch());
    auto time =  elapsed.count();
    *json_messages_log_ << message_counter_ << "|";
    *json_messages_log_ << K3::serialization::json::encode<Address>(address_) << "|";
    *json_messages_log_ << ProgramContext::__triggerName(trigger) << "|";
    *json_messages_log_ << K3::serialization::json::encode<Address>(src) << "|";
    *json_messages_log_ << f() << "|";
    *json_messages_log_ << time << std::endl;
    message_counter_++;
  }
}

}  // namespace K3

#endif
