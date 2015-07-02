#ifndef K3_ENGINE
#define K3_ENGINE

#include <atomic>
#include <vector>
#include <map>
#include <memory>
#include <string>

#include <spdlog/spdlog.h>

#include "Common.hpp"
#include "Options.hpp"
#include "network/NetworkManager.hpp"
#include "storage/StorageManager.hpp"
#include "serialization/Codec.hpp"
#include "Peer.hpp"

namespace K3 {

class Engine {
 public:
  // Core Interface
  Engine();
  ~Engine();
  template <class Context> void run(const Options& opts);
  void stop();
  void join();
  void send(const MessageHeader& m, unique_ptr<NativeValue> v,
            shared_ptr<Codec> cdec);

  // Utilities
  bool running();
  shared_ptr<Peer> getPeer(const Address& addr);
  NetworkManager& getNetworkManager();
  StorageManager& getStorageManager();

  // Configuration
  void toggleLocalSends(bool enabled);
  void setLogLevel(int level);

 protected:
  // Helper Functions
  Address meFromYAML(const YAML::Node& peer_config);
  shared_ptr<map<Address, shared_ptr<Peer>>> createPeers(
      const Options& opts, shared_ptr<ContextFactory> context_factory);

  // Components
  shared_ptr<spdlog::logger> logger_;
  NetworkManager network_manager_;
  StorageManager storage_manager_;
  shared_ptr<const map<Address, shared_ptr<Peer>>> peers_;

  // Configuration
  bool local_sends_enabled_;

  // State
  std::atomic<bool> running_;
  std::atomic<int> ready_peers_;
  int total_peers_;
};

template <class Context>
void Engine::run(const Options& opts) {
  if (running_) {
    throw std::runtime_error("Engine run(): already running");
  }

  // Configuration
  setLogLevel(opts.log_level_);
  toggleLocalSends(opts.local_sends_enabled_);
  logger_->info("The Engine has started.");

  // Peers start their own thread, create a context, check in
  auto context_factory = make_shared<ContextFactory>(
      [this]() { return make_shared<Context>(*this); });
  peers_ = createPeers(opts, context_factory);
  total_peers_ = peers_->size();
  while (total_peers_ > ready_peers_) continue;
  logger_->info("All peers are ready.");

  // This must happen AFTER peers_ has been initialized
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
