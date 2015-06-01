#ifndef K3_ENGINE
#define K3_ENGINE

#include <atomic>
#include <vector>
#include <map>
#include <memory>
#include <string>

#include "Common.hpp"
#include "network/NetworkManager.hpp"
#include "storage/StorageManager.hpp"
#include "Peer.hpp"

namespace K3 {

class Engine {
 public:
  // Core
  Engine();
  ~Engine();
  template <class Context>
  void run(const vector<string>& peer_configs);
  void stop();
  void join();
  void send(const MessageHeader& m, shared_ptr<NativeValue> v,
            shared_ptr<Codec> cdec);

  // Utilities
  shared_ptr<Peer> getPeer(const Address& addr);
  void toggleLocalSends(bool enabled);
  shared_ptr<NetworkManager> getNetworkManager();
  shared_ptr<StorageManager> getStorageManager();
  bool running();

 protected:
  Address meFromYAML(const YAML::Node& peer_config);
  shared_ptr<map<Address, shared_ptr<Peer>>> createPeers(
      const vector<string>& peer_configs,
      shared_ptr<ContextFactory> context_factory);

  shared_ptr<NetworkManager> network_manager_;
  shared_ptr<StorageManager> storage_manager_;
  shared_ptr<const map<Address, shared_ptr<Peer>>> peers_;
  bool local_sends_enabled_;
  std::atomic<bool> running_;
  std::atomic<int> ready_peers_;
  int total_peers_;
};

template <class Context>
void Engine::run(const vector<string>& peer_configs) {
  if (running_) {
    throw std::runtime_error("Engine run(): already running");
  }
  running_ = true;

  // Create peers: Peers start their own thread, create a context
  // and check in
  auto context_factory = make_shared<ContextFactory>(
      [this]() { return make_shared<Context>(*this); });
  peers_ = createPeers(peer_configs, context_factory);

  total_peers_ = peers_->size();
  while (total_peers_ > ready_peers_) continue;

  // Once peers check in, start a listener for each peer
  // and allow each peer to send its initial message (processRole)
  // This must happen AFTER peers_ has been initialized
  for (auto it : *peers_) {
    network_manager_->listenInternal(it.second);
    it.second->processRole();
  }

  // Once all roles have been processed, peers can start to process messages
  for (auto it : *peers_) {
    it.second->start();
  }
  return;
}

}  // namespace K3

#endif
