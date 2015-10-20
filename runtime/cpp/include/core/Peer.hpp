#ifndef K3_PEER
#define K3_PEER

#include <atomic>
#include <string>
#include <memory>
#include <thread>
#include <chrono>
#include <map>
#include <vector>

#ifdef _WIN32
#include <winsock2.h>
#endif //_WIN32
#include <spdlog/spdlog.h>
#include <yaml-cpp/yaml.h>

#include "Options.hpp"
#include "Common.hpp"
#include "types/Dispatcher.hpp"
#include "types/Queue.hpp"
#include "types/Pool.hpp"

namespace K3 {

class ProgramContext;

class Outbox {
  public:
    Outbox () { }
    void stash(const Address& a, Pool::unique_ptr<Dispatcher> d, const PeerMap& peers) {
      auto& ref = pending_[a];
      ref.push_back(std::move(d));
      if (ref.size() >= 1000) {
        flushOne(a, ref, peers);
      }
    }
    void flushOne(const Address& addr, vector<Pool::unique_ptr<Dispatcher>>& batch, const PeerMap& peers);
    void flushAll(const PeerMap& peers);
 protected:
    std::map<Address, std::vector<Pool::unique_ptr<Dispatcher>>> pending_;
};

class Peer {
 public:
  // Core Interface
  Peer(PeerMap& peers, shared_ptr<ContextFactory> fac, const YAML::Node& peer_config,
       std::function<void()> ready_callback, const JSONOptions& json);
  void start();
  void processRole();
  void join();

  Queue& getQueue() { return *queue_; }
  Outbox& getOutbox() { return outbox_; }

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
  Outbox outbox_;
  const PeerMap& peers_;

  // Configuration
  JSONOptions json_opts_;
  shared_ptr<std::ofstream> json_globals_log_;
  shared_ptr<std::ofstream> json_messages_log_;

  // State
  Address address_;
  std::atomic<bool> start_processing_;
  std::atomic<bool> finished_;
  vector<Pool::unique_ptr<Dispatcher>> batch_;

  // Statistics
  std::vector<TriggerStatistics> statistics_;
  int message_counter_;
};

}  // namespace K3

#endif
