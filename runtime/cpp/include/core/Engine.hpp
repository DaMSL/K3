#ifndef K3_ENGINE
#define K3_ENGINE

#include <atomic>
#include <vector>
#include <map>

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
  template <class T>
  void send(const Address& src, const Address& dst, TriggerID trig,
            const T& value);
  // Accessors
  shared_ptr<ProgramContext> getContext(const Address& addr);
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
  auto ctxt_fac = make_shared<ContextFactory>(
      [this]() { return make_shared<Context>(*this); });
  auto rdy_callback = [this]() { ready_peers_++; };

  auto tmp_peers = make_shared<map<Address, shared_ptr<Peer>>>();
  vector<YAML::Node> nodes =
      serialization::yaml::parsePeers(options_.peer_strs_);
  for (auto node : nodes) {
    Address addr = serialization::yaml::meFromYAML(node);
    if (tmp_peers->find(addr) != tmp_peers->end()) {
      throw std::runtime_error("Engine createPeers(): Duplicate address: " +
                               addr.toString());
    }
    auto p = make_shared<Peer>(ctxt_fac, node, rdy_callback, options_.json_);
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

unique_ptr<Dispatcher> getDispatcher(shared_ptr<Peer>, unique_ptr<NativeValue>, TriggerID trig);
string getTriggerName(int);

template <class T>
void Engine::send(const Address& src, const Address& dst, TriggerID trig,
                  const T& value) {
  if (!peers_) {
    throw std::runtime_error(
        "Engine send(): Can't send before peers_ is initialized");
  }
  if (logger_->level() <= spdlog::level::debug) {
    logger_->debug() << "Message: " << src.toString() << " --> "
                    << dst.toString() << " @"
                    << getTriggerName(trig);
  }
  auto it = peers_->find(dst);
  if (options_.local_sends_enabled_ && it != peers_->end()) {
    // Direct enqueue for local messages
    unique_ptr<NativeValue> nv = std::make_unique<TNativeValue<T>>(value);
    auto d = getDispatcher(it->second, std::move(nv), trig);
#ifdef K3DEBUG
    d->trigger_ = trig;
    d->source_ = src;
    d->destination_ = dst;
#endif
    it->second->getQueue().enqueue(std::move(d));
  } else {
    // Serialize and send over the network, otherwise
    unique_ptr<PackedValue> pv = pack<T>(value, K3_INTERNAL_FORMAT);
    shared_ptr<NetworkMessage> m =
        make_shared<NetworkMessage>(trig, std::move(pv));
#ifdef K3DEBUG
    m->source_ = src;
    m->destination_ = dst;
#endif
    network_manager_.sendInternal(dst, m);
  }
}

}  // namespace K3
#endif
