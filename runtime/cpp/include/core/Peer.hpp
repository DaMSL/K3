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

#include "Options.hpp"
#include "types/Dispatcher.hpp"
#include "types/Queue.hpp"

namespace K3 {

class ProgramContext;
class Peer {
 public:
  // Core Interface
  Peer(shared_ptr<ContextFactory> fac, const YAML::Node& peer_config,
       std::function<void()> ready_callback, const JSONOptions& json);
  void start();
  void processRole();
  void join();

  Queue& getQueue();

  // Utilities
  bool finished();
  Address address();
  ProgramContext& getContext() {
    return *context_;
  }
  void printStatistics();

 protected:
  // Helper Functions
  void processBatch();
  void logMessage(const Dispatcher& d);
  void logGlobals(bool final);

  // Components
  shared_ptr<ProgramContext> context_;
  shared_ptr<spdlog::logger> logger_;
  shared_ptr<std::thread> thread_;
  shared_ptr<Queue> queue_;

  // Configuration
  JSONOptions json_opts_;
  shared_ptr<std::ofstream> json_globals_log_;
  shared_ptr<std::ofstream> json_messages_log_;

  // State
  Address address_;
  std::atomic<bool> start_processing_;
  std::atomic<bool> finished_;
  vector<unique_ptr<Dispatcher>> batch_;

  // Statistics
  std::vector<TriggerStatistics> statistics_;
  int message_counter_;
};

}  // namespace K3

#endif
