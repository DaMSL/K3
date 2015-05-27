#include <map>
#include <string>
#include <vector>

#include "yaml-cpp/yaml.h"

#include "core/Engine.hpp"
#include "core/Peer.hpp"
#include "serialization/Codec.hpp"
#include "network/NetworkManager.hpp"

namespace K3 {

Engine::Engine() {
  network_manager_ = make_shared<NetworkManager>();
  peers_ = nullptr;  // Intialized during run()
  running_ = false;
  ready_peers_ = 0;
  total_peers_ = 0;
  local_sends_enabled_ = true;
}

Engine::~Engine() {
  if (running_) {
    stop();
    join();
  }
}

void Engine::stop() {
  // Place a Sentintel on each Peer's queue
  for (auto& it : *peers_) {
    auto m = make_shared<Message>(it.first, it.first, -1,
                                  make_shared<SentinelValue>());
    it.second->enqueue(std::move(m));
  }

  network_manager_->stop();
}

void Engine::join() {
  if (running_) {
    for (auto& it : *peers_) {
      it.second->join();
    }

    network_manager_->join();
    running_ = false;
  }
}

void Engine::send(const MessageHeader& header, shared_ptr<NativeValue> value,
                  shared_ptr<Codec> codec) {
  auto it = peers_->find(header.destination());

  if (local_sends_enabled_ && it != peers_->end()) {
    // Direct enqueue for local messages
    auto m = make_shared<Message>(header, value);
    it->second->enqueue(m);
  } else {
    // Serialize and send over the network, otherwise
    shared_ptr<PackedValue> pv = codec->pack(*value);
    shared_ptr<NetworkMessage> m = make_shared<NetworkMessage>(header, pv);
    network_manager_->sendInternal(m);
  }
}

shared_ptr<Peer> Engine::getPeer(const Address& addr) {
  auto it = peers_->find(addr);
  if (it != peers_->end()) {
    return it->second;
  } else {
    throw std::runtime_error("Engine getPeer(): Peer not found");
  }
}

void Engine::toggleLocalSends(bool enabled) {
  if (running_) {
    throw std::runtime_error("Engine toggleLocalSends(): Already running");
  } else {
    local_sends_enabled_ = enabled;
  }
}

shared_ptr<NetworkManager> Engine::getNetworkManager() {
  return network_manager_;
}

bool Engine::running() { return running_.load(); }

Address Engine::meFromYAML(const YAML::Node& peer_config) {
  if (!peer_config.IsMap()) {
    throw std::runtime_error("Engine run(): Invalid YAML. Not a map: " +
                             YAML::Dump(peer_config));
  }
  if (!peer_config["me"]) {
    string err =
        "Engine run(): Invalid YAML. Missing 'me': " + YAML::Dump(peer_config);
    throw std::runtime_error(err);
  }
  auto addr = peer_config["me"].as<Address>();
  return addr;
}

shared_ptr<map<Address, shared_ptr<Peer>>> Engine::createPeers(
    const vector<string>& peer_configs, shared_ptr<ContextFactory> factory) {
  auto result = make_shared<map<Address, shared_ptr<Peer>>>();

  for (auto config : peer_configs) {
    YAML::Node node = YAML::Load(config);
    Address addr = meFromYAML(node);
    auto ready_callback = [this]() { ready_peers_++; };
    auto p = make_shared<Peer>(addr, factory, node, ready_callback);
    (*result)[addr] = p;
  }

  return result;
}

}  // namespace K3
