#ifndef K3_ENGINE
#define K3_ENGINE

#include <atomic>
#include <list>
#include <map>
#include <memory>
#include <string>

#include "Common.hpp"
#include "NetworkManager.hpp"
#include "Peer.hpp"

namespace K3 {

class Engine {
 public:
  // Core
  Engine();
  ~Engine();
  template <class Context> void run(const list<string>& peer_configs);
  void stop();
  void join();
  void send(const MessageHeader& m, shared_ptr<NativeValue> v, shared_ptr<Codec> cdec);

  // Utilities
  shared_ptr<Peer> getPeer(const Address& addr);
  void toggleLocalSends(bool enabled);
  shared_ptr<NetworkManager> getNetworkManager();
  bool running();

 protected:
  Address meFromYAML(const YAML::Node& peer_config);
  shared_ptr<map<Address, shared_ptr<Peer>>> createPeers(const list<string>& peer_configs,
                                                         shared_ptr<ContextFactory> context_factory);

  shared_ptr<NetworkManager> network_manager_;
  shared_ptr<const map<Address, shared_ptr<Peer>>> peers_;
  bool local_sends_enabled_;
  std::atomic<bool> running_;
  std::atomic<int> ready_peers_;
  int total_peers_;
};

template <class Context>
void Engine::run(const list<string>& peer_configs) {
  if (running_) {
    throw std::runtime_error("Engine run(): already running");
  }
  running_ = true;

  // Create peers
  total_peers_ = peer_configs.size();
  auto context_factory = make_shared<ContextFactory>([this] () {
    return make_shared<Context>(*this);
  });
  peers_ = createPeers(peer_configs, context_factory);

  // Wait for peers to initialize and check-in as ready
  while (total_peers_ > ready_peers_) continue;

  // Signal all peers to start
  for (auto it : *peers_) {
    network_manager_->listenInternal(it.second);
    it.second->start();
  }

  return;
}

}  // namespace K3

#endif
