#include <map>
#include <list>
#include <string>

#include "yaml-cpp/yaml.h"

#include "Codec.hpp"
#include "Engine.hpp"
#include "NetworkManager.hpp"
#include "Peer.hpp"

using std::list;


Engine& Engine::getInstance() {
  static Engine instance;
  return instance;
}



void Engine::run(const list<std::string>& peer_configs, shared_ptr<ContextFactory> f) {
  // Reset members
  peers_ = map<Address, shared_ptr<Peer>>();
  context_factory_ = f;
  total_peers_ = peer_configs.size();
  ready_peers_.store(0);
  logger = spdlog::stdout_logger_mt("stdout");

  logger->info("Initializing engine");

  NetworkManager::getInstance().run();

  // Parse peer configurations
  for (auto config : peer_configs) {
    YAML::Node node = YAML::Load(config);

    if (!node.IsMap()) {
      throw std::runtime_error("Engine initialize(): Invalid YAML. Not a map: " + YAML::Dump(node));
    }
    if (!node["me"]) {
      std::string err = "Engine initialize(): Invalid YAML. Missing 'me': " + YAML::Dump(node);
      throw std::runtime_error(err);
    }

    Address addr = make_address(node);
    auto ready_callback = [this] () {
      ready_peers_++;
    };
    auto p = make_shared<Peer>(addr, context_factory_, node, ready_callback);
    peers_[addr] = p;
  }

  // Wait for peers to initialize and check-in as ready
  while (total_peers_ > ready_peers_.load()) continue;

  // Signal all peers to start
  logger->info("Waiting for peers");
  for (auto it : peers_) {
    NetworkManager::getInstance().listenInternal(it.second);
    it.second->start();
  }

  // Engine is running once all peers have checked in
  running_.store(true);
  logger->info("Initialization Complete");
  return;
}

void Engine::stop() {
  // Place a Sentintel on each Peer's queue
  for (auto& it : peers_) {
    auto m = make_shared<Message>(it.first, it.first , -1, make_shared<SentinelValue>());
    it.second->enqueue(std::move(m));
  }

  // TODO(jbw) Restore all configuration parameters to defaults
  local_sends_enabled_ = true;
  running_.store(false);
}

void Engine::join() {
  for (auto& it : peers_) {
    it.second->join();
  }

  NetworkManager::getInstance().stop();
  NetworkManager::getInstance().join();
}

void Engine::send(const MessageHeader& header,
                  shared_ptr<NativeValue> value,
                  shared_ptr<Codec> codec) {
  auto it = peers_.find(header.destination());

  if (local_sends_enabled_ && it != peers_.end()) {
    // Direct enqueue for local messages
    auto m = make_shared<Message>(header, value);
    it->second->enqueue(m);
  } else {
    // Serialize and send over the network, otherwise
    shared_ptr<PackedValue> pv = codec->pack(*value);
    shared_ptr<NetworkMessage> m = make_shared<NetworkMessage>(header, pv);
    NetworkManager::getInstance().sendInternal(m);
  }
}

void Engine::toggleLocalSend(bool enable) {
  if (running_) {
    throw std::runtime_error("Engine toggleLocalSend(): Engine already running");
  }

  local_sends_enabled_ = enable;
}

shared_ptr<Peer> Engine::getPeer(const Address& addr) {
  auto it = peers_.find(addr);
  if (it != peers_.end()) {
    return it->second;
  } else {
    throw std::runtime_error("Engine getPeer(): Peer not found");
  }
}

bool Engine::running() {
  return running_.load();
}
