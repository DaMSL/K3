#ifndef K3_ENGINE
#define K3_ENGINE

#include <atomic>
#include <vector>
#include <unordered_map>

#ifdef _WIN32
#include <winsock2.h>
#endif //_WIN32
#include <spdlog/spdlog.h>

#include <boost/date_time/posix_time/posix_time.hpp>

#include "Common.hpp"
#include "Hash.hpp"
#include "Options.hpp"
#include "network/NetworkManager.hpp"
#include "storage/StorageManager.hpp"
#include "serialization/Yaml.hpp"
#include "serialization/Codec.hpp"
#include "Peer.hpp"
#include "types/Pool.hpp"

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
  template <class T, class TrigDispatcher, class Context>
  void send(const Address& src, const Address& dst, TriggerID trig,
            const T& value, Outbox& outbox, Context& context);
  template <class T, class TrigDispatcher, class Context>
  void send(const Address& src, const Address& dst, TriggerID trig,
            T&& value, Outbox& outbox, Context& context);

  // Delayed sends, in microseconds from now.
  template <class T, class TrigDispatcher, class Context>
  void delayedSend(const Address& src, const Address& dst, TriggerID trig,
                   const T& value, Outbox& outbox, Context& context, const TimerType& tmty, int delay);
  template <class T, class TrigDispatcher, class Context>
  void delayedSend(const Address& src, const Address& dst, TriggerID trig,
                   T&& value, Outbox& outbox, Context& context, const TimerType& tmty, int delay);

  // Accessors
  ProgramContext& getContext(const Address& addr);
  NetworkManager& getNetworkManager();
  StorageManager& getStorageManager();

 protected:
  // Components
  shared_ptr<spdlog::logger> logger_;
  NetworkManager network_manager_;
  StorageManager storage_manager_;
  shared_ptr<const PeerMap> peers_;

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
      [this](Peer& p) { return make_shared<Context>(*this, p); });
  auto rdy_callback = [this]() { ready_peers_++; };

  auto tmp_peers = new std::map<Address, shared_ptr<Peer>>();
  vector<YAML::Node> nodes =
      serialization::yaml::parsePeers(options_.peer_strs_);
  for (auto node : nodes) {
    Address addr = serialization::yaml::meFromYAML(node);
    if (tmp_peers->find(addr) != tmp_peers->end()) {
      throw std::runtime_error("Engine createPeers(): Duplicate address: " +
                               addr.toString());
    }
    auto p = make_shared<Peer>(*tmp_peers, ctxt_fac, node, rdy_callback, options_.json_);
    (*tmp_peers)[addr] = p;
  }
  peers_ = shared_ptr<const PeerMap>(tmp_peers);
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

Pool::unique_ptr<Dispatcher> getDispatcher(Peer& p, Pool::unique_ptr<NativeValue>, TriggerID trig);
string getTriggerName(int);

template <class T, class TrigDispatcher, class Context>
void Engine::send(const Address& src, const Address& dst, TriggerID trig,
                  T&& value, Outbox& outbox, Context& context) {
  //if (!peers_) {
  //  throw std::runtime_error(
  //      "Engine send(): Can't send before peers_ is initialized");
  //}
  //if (logger_->level() <= spdlog::level::debug) {
  //  logger_->debug() << "Message: " << src.toString() << " --> "
  //                  << dst.toString() << " @"
  //                  << getTriggerName(trig);
  //}
  auto it = peers_->find(dst);
  if (options_.local_sends_enabled_ && it != peers_->end()) {
    // Stash in outbox local messages
    Pool::unique_ptr<NativeValue> nv = Pool::getInstance().make_unique_subclass<NativeValue, TNativeValue<T>>(std::move(value));
    auto d = Pool::getInstance().make_unique_subclass<Dispatcher, TrigDispatcher>(dynamic_cast<Context&>(it->second->getContext()), std::move(nv)); 
    //auto d = getDispatcher(*it->second, std::move(nv), trig);
#ifdef K3DEBUG
    d->trigger_ = trig;
    d->source_ = src;
    d->destination_ = dst;
#endif
    outbox.stash(dst, std::move(d), *peers_);
    //it->second->getQueue().enqueue(std::move(d));
  } else {
    // Serialize and send over the network, otherwise
    Pool::unique_ptr<PackedValue> pv = pack<T>(value, K3_INTERNAL_FORMAT);
    shared_ptr<NetworkMessage> m =
        make_shared<NetworkMessage>(trig, std::move(pv));
#ifdef K3DEBUG
    m->source_ = src;
    m->destination_ = dst;
#endif
    network_manager_.sendInternal(dst, m);
  }
}

template <class T, class TrigDispatcher, class Context>
void Engine::send(const Address& src, const Address& dst, TriggerID trig,
                  const T& value, Outbox& outbox, Context& c)
{
  send<T, TrigDispatcher>(src, dst, trig, T(value), outbox, c);
}


// Delayed sends, based on timers.
template <class T, class TrigDispatcher, class Context>
void Engine::delayedSend(const Address& src, const Address& dst, TriggerID trig,
                         T&& value, Outbox& outbox, Context& context, const TimerType& tmty, int delay)
{
  if (!peers_) {
    throw std::runtime_error(
        "Engine send(): Can't send before peers_ is initialized");
  }
  if (logger_->level() <= spdlog::level::debug) {
    logger_->debug() << "Delayed Message: " << src.toString() << " --> "
                     << dst.toString() << " @"
                     << getTriggerName(trig);
  }

  auto timer_key = network_manager_.timerKey(tmty, src, dst, trig);
  network_manager_.addTimer(timer_key, boost::posix_time::microseconds(delay));

  auto cb = [this, timer_key, src, dst, trig, value = std::move(value), &outbox, &context]
            (const boost::system::error_code& error) mutable
  {
    if ( !error ) {
      this->network_manager_.removeTimer(timer_key);
      this->send<T, TrigDispatcher, Context>(src, dst, trig, std::move(value), outbox, context);
    }
  };
  network_manager_.asyncWaitTimer(timer_key, cb);
}

template <class T, class TrigDispatcher, class Context>
void Engine::delayedSend(const Address& src, const Address& dst, TriggerID trig,
                         const T& value, Outbox& outbox, Context& context, const TimerType& tmty, int delay)
{
  delayedSend<T, TrigDispatcher, Context>(src, dst, trig, T(value), outbox, tmty, delay, context);
}

}  // namespace K3
#endif
