#include <map>
#include <string>
#include <vector>

#include "yaml-cpp/yaml.h"

#include "core/Engine.hpp"
#include "core/Peer.hpp"
#include "serialization/Codec.hpp"
#include "types/Dispatcher.hpp"

namespace K3 {

Engine::Engine() : network_manager_(), storage_manager_() {
  logger_ = spdlog::get("engine");
  if (!logger_) {
    logger_ = spdlog::stdout_logger_mt("engine");
    spdlog::set_level(spdlog::level::warn);
    spdlog::set_pattern("[%T.%f %l %n] %v");
  }
  peers_ = NULL;  // Initialized during run()
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
    auto d = make_unique<SentinelDispatcher>(); 
    it.second->enqueue(std::move(d));
  }
  network_manager_.stop();
  logger_->info("Stopping the Engine.");
}

void Engine::join() {
  if (running_ && peers_) {
    for (auto& it : *peers_) {
      it.second->join();
    }
  }
  network_manager_.stop();
  network_manager_.join();
  running_ = false;
  logger_->info("The Engine has Joined.");
}

void Engine::send(const MessageHeader& header, unique_ptr<NativeValue> value,
                  shared_ptr<Codec> codec) {
  if (!peers_) {
    throw std::runtime_error(
        "Engine send(): Can't send before peers_ is initialized");
  }
  if (logger_->level() >= spdlog::level::info) {
    logger_->info() << "Message: " << header.source().toString() << " --> "
                    << header.destination().toString() << " @"
                    << ProgramContext::__triggerName(header.trigger());
  }
  auto it = peers_->find(header.destination());
  if (local_sends_enabled_ && it != peers_->end()) {
    // Direct enqueue for local messages
    auto d = it->second->getContext()->__getDispatcher(std::move(value), header.trigger());
    #ifdef K3DEBUG
    d->header_ = header;
    #endif
    it->second->enqueue(std::move(d));
  } else {
    // Serialize and send over the network, otherwise
    unique_ptr<PackedValue> pv = codec->pack(*value);
    shared_ptr<NetworkMessage> m = make_shared<NetworkMessage>(header, std::move(pv));
    network_manager_.sendInternal(m);
  }
}

bool Engine::running() { return running_; }

shared_ptr<Peer> Engine::getPeer(const Address& addr) {
  auto it = peers_->find(addr);
  if (it != peers_->end()) {
    return it->second;
  } else {
    throw std::runtime_error("Engine getPeer(): Peer not found");
  }
}

NetworkManager& Engine::getNetworkManager() {
  return network_manager_;
}

StorageManager& Engine::getStorageManager() {
  return storage_manager_;
}

void Engine::toggleLocalSends(bool enabled) {
  if (running_) {
    throw std::runtime_error("Engine toggleLocalSends(): Already running");
  } else {
    local_sends_enabled_ = enabled;
  }
}

void Engine::setLogLevel(int level) {
  switch (level) {
    case 1:
      spdlog::set_level(spdlog::level::info);
      break;
    case 2:
      spdlog::set_level(spdlog::level::debug);
      break;
    case 3:
      spdlog::set_level(spdlog::level::trace);
      break;
    default:
      spdlog::set_level(spdlog::level::warn);
      break;
  }
}

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
    const Options& opts, shared_ptr<ContextFactory> factory) {
  auto result = make_shared<map<Address, shared_ptr<Peer>>>();
  // Parse YAML to grab 'me' address for each peer
  auto peer_configs = opts.peer_strs_;
  vector<YAML::Node> nodes;
  for (auto config : peer_configs) {
    if (config.length() == 0) {
      throw std::runtime_error(
          "Engine createPeers(): Empty YAML peer configuration");
    } else if (config[0] != '{') {
      for (auto n : YAML::LoadAllFromFile(config)) {
        nodes.push_back(n);
      }
    } else {
      nodes.push_back(YAML::Load(config));
    }
  }
  // Construct each peer, place in map
  for (auto node : nodes) {
    Address addr = meFromYAML(node);
    auto ready_callback = [this]() { ready_peers_++; };
    auto p = make_shared<Peer>(addr, factory, node, ready_callback,
                               opts.json_folder_, opts.json_final_state_only_);
    if (result->find(addr) != result->end()) {
      throw std::runtime_error(
          "Engine createPeers(): Duplicate peer address: " + addr.toString());
    }
    (*result)[addr] = p;
  }
  return result;
}
}  // namespace K3
