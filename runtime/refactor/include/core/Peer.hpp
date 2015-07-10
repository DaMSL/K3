#ifndef K3_PEER
#define K3_PEER

#include <atomic>
#include <string>
#include <memory>
#include <thread>
#include <chrono>
#include <vector>

#include <spdlog/spdlog.h>
#include <yaml-cpp/yaml.h>

#include "types/Dispatcher.hpp"
#include "types/Queue.hpp"

namespace K3 {

class ProgramContext;
class Peer {
 public:
  // Core Interface
  Peer(const Address& addr, shared_ptr<ContextFactory> fac,
       const YAML::Node& peer_config, std::function<void()> ready_callback,
       const string& json_path, bool json_final_only);
  void start();
  void processRole();
  void join();
  void enqueue(std::unique_ptr<Dispatcher> m);

  // Utilities
  bool finished();
  Address address();
  shared_ptr<ProgramContext> getContext();

 protected:
  // Helper Functions
  void processBatch();
  void logMessage(const Dispatcher& d);
  void logGlobals(bool final);

  // Components
  shared_ptr<spdlog::logger> logger_;
  shared_ptr<std::thread> thread_;
  shared_ptr<Queue> queue_;
  shared_ptr<ProgramContext> context_;

  // Configuration
  shared_ptr<std::ofstream> json_globals_log_;
  shared_ptr<std::ofstream> json_messages_log_;
  bool json_final_state_only_;

  // State
  Address address_;
  std::atomic<bool> start_processing_;
  std::atomic<bool> finished_;
  int message_counter_;
  vector<unique_ptr<Dispatcher>> batch_;
};

}  // namespace K3

#endif
