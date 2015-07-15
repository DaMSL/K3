#ifndef K3_ENGINE
#define K3_ENGINE

#include <atomic>
#include <vector>
#include <map>
#include <memory>

#include <spdlog/spdlog.h>

#include "Common.hpp"
#include "Options.hpp"
#include "network/NetworkManager.hpp"
#include "storage/StorageManager.hpp"
#include "serialization/Yaml.hpp"
#include "serialization/Codec.hpp"
#include "Peer.hpp"

namespace K3 {

class ProgramContext;
class Engine {
 public:
  // Core Interface
  Engine(const Options& opts);
  ~Engine();
  template <class Context>
  void run();
  void stop();
  void join();
  void send(const Address& src, const Address& dst, TriggerID trig,
            unique_ptr<NativeValue> v, shared_ptr<Codec> cdec);

  // Accessors
  ProgramContext& getContext(const Address& addr);
  NetworkManager& getNetworkManager();
  StorageManager& getStorageManager();

 protected:
  // Components
  shared_ptr<spdlog::logger> logger_;
  NetworkManager network_manager_;
  StorageManager storage_manager_;
  shared_ptr<const map<Address, shared_ptr<Peer>>> peers_;

  // Configuration
  Options options_;

  // State
  std::atomic<bool> running_;
  std::atomic<int> ready_peers_;
  int total_peers_;
};

template <class Context>
void Engine::run() {
  if (running_) {
    throw std::runtime_error("Engine run(): already running");
  }
  logger_->info("The Engine has started.");

  // Create peers from their command line arguments
  auto tmp_peers = make_shared<map<Address, shared_ptr<Peer>>>();
  auto context_factory = make_shared<ContextFactory>(
      [this]() { return make_shared<Context>(*this); });
  auto ready_callback = [this]() { ready_peers_++; };
  vector<YAML::Node> nodes = serialization::yaml::parsePeers(options_.peer_strs_);
  for (auto node : nodes) {
    Address addr = serialization::yaml::meFromYAML(node);
    auto p = make_shared<Peer>(addr, context_factory, node, ready_callback,
                               options_.json_folder_,
                               options_.json_final_state_only_);
    if (tmp_peers->find(addr) != tmp_peers->end()) {
      throw std::runtime_error("Engine createPeers(): Duplicate address: " +
                               addr.toString());
    }
    (*tmp_peers)[addr] = p;
  }
  peers_ = tmp_peers;
  total_peers_ = peers_->size();

  // Wait for peers to check in
  while (total_peers_ > ready_peers_) continue;
  logger_->info("All peers are ready.");

  // Accept network connections and send initial messages
  for (auto& it : *peers_) {
    network_manager_.listenInternal(it.second);
    it.second->processRole();
  }

  // All roles are processed: signal peers to start message loop
  for (auto& it : *peers_) {
    it.second->start();
  }
  running_ = true;
  return;
}

}  // namespace K3
#endif
