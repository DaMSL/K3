#include <map>
#include <string>
#include <vector>

#include "yaml-cpp/yaml.h"

#include "core/Engine.hpp"
#include "core/Peer.hpp"
#include "core/ProgramContext.hpp"
#include "serialization/Codec.hpp"
#include "types/Dispatcher.hpp"

namespace K3 {

Engine::Engine(const Options& opts)
    : network_manager_(),
      storage_manager_(),
      peers_(),
      options_(opts),
      running_(false),
      ready_peers_(0),
      total_peers_(0) {
  logger_ = spdlog::get("engine");
  if (!logger_) {
    logger_ = spdlog::stdout_logger_mt("engine");
    spdlog::set_level(spdlog::level::warn);
    spdlog::set_pattern("[%T.%f %l %n] %v");
  }

  switch (opts.log_level_) {
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
    it.second->getQueue().enqueue(std::move(d));
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
#ifdef K3DEBUG
  for (auto& it : *peers_) {
    it.second->printStatistics();
  }
#endif
  network_manager_.stop();
  network_manager_.join();
  running_ = false;
  logger_->info("The Engine has Joined.");
}

shared_ptr<ProgramContext> Engine::getContext(const Address& addr) {
  auto it = peers_->find(addr);
  if (it != peers_->end()) {
    return it->second->getContext();
  } else {
    throw std::runtime_error("Engine getPeer(): Peer not found");
  }
}

NetworkManager& Engine::getNetworkManager() { return network_manager_; }

StorageManager& Engine::getStorageManager() { return storage_manager_; }

unique_ptr<Dispatcher> getDispatcher(shared_ptr<Peer> p, unique_ptr<NativeValue> nv, TriggerID trig) {
 return p->getContext()->__getDispatcher(std::move(nv), trig);
}

string getTriggerName(int trig) {
  return ProgramContext::__triggerName(trig);
}
}  // namespace K3
