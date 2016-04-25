#include "Common.hpp"
#include "core/Peer.hpp"
#include "network/NetworkManager.hpp"
#include "network/IncomingConnection.hpp"
#include "network/OutgoingConnection.hpp"
#include "network/Listener.hpp"

namespace K3 {

std::atomic<unsigned long> NetworkManager::timer_cnt_(0);

NetworkManager::NetworkManager(int num_threads) :
  running_(true), internal_format_(K3_INTERNAL_FORMAT) {
  logger_ = spdlog::get("engine");
  if (!logger_) {
    logger_ = spdlog::stdout_logger_mt("engine");
  }
  work_ = make_unique<asio::io_service::work>(io_service_);
  internal_listeners_ = make_shared<ListenerMap>();
  internal_out_conns_ = make_shared<InternalConnectionMap>();
  for (int i = 0; i < num_threads; i ++) {
    addThread();
  }
}

NetworkManager::~NetworkManager() {
  if (running_) {
    stop();
    join();
  }
}

void NetworkManager::stop() {
  work_.reset();
  io_service_.stop();
}

void NetworkManager::join() {
  if (running_) {
    running_ = false;
    threads_.join_all();
  }
  return;
}

// A listener can keep a peer alive, so we need a shared_ptr
void NetworkManager::listenInternal(shared_ptr<Peer> peer) {
  // Construct Listener
  auto fac_ptr = 
      [](asio::io_service& service, CodecFormat format) {
        return make_shared<InternalIncomingConnection>(service, format);
      };
  auto listen_addr = peer->address();
  auto listener = make_shared<Listener>(io_service_, listen_addr, peer,
                                        internal_format_, fac_ptr);
  internal_listeners_->insert(listen_addr, listener);

  // Accept connections, removing the listener upon error
  auto e_handler =
      [listeners = internal_listeners_, listen_addr](boost_error ec) {
        throw std::runtime_error("Listener error handler: " + ec.message());
        listeners->erase(listen_addr);
      };
  listener->acceptConnection(e_handler);
}

void NetworkManager::sendInternal(const Address& dst, unique_ptr<OutNetworkMessage> pm) {
  // Check for an existing connection, creating one if necessary
  auto c = internal_out_conns_->lookupOrCreate(dst, io_service_);

  // Send, removing the connection upon error
  auto e_handler = [conn_map = internal_out_conns_, dst](boost_error ec) {
    throw std::runtime_error("Send error: "  + ec.message());
    conn_map->erase(dst);
  };

  c->send(std::move(pm), std::move(e_handler));
}

shared_ptr<Listener> NetworkManager::getListener(const Address& a) {
  if (auto res = internal_listeners_->lookup(a)) {
    return res;
  } else {
    throw std::runtime_error("NetworkManager getListener(): failed");
  }
}

shared_ptr<InternalOutgoingConnection> NetworkManager::connectInternal(
    const Address& a) {
  auto c = make_shared<InternalOutgoingConnection>(a, io_service_);
  internal_out_conns_->insert(a, c);
  return c;
}

void NetworkManager::addThread() {
  threads_.create_thread([this]() { io_service_.run(); });
}

std::shared_ptr<TimerKey>
NetworkManager::timerKey(const TimerType& ty, const Address& src, const Address& dst, const TriggerID& trig)
{
  std::shared_ptr<TimerKey> key;
  switch (ty) {
    case TimerType::Delay:
      key = std::make_shared<DelayTimerT>(timer_cnt_.fetch_add(1UL));

    case TimerType::DelayOverride:
      key = std::make_shared<DelayOverrideT>(dst, trig);

    case TimerType::DelayOverrideEdge:
      key = std::make_shared<DelayOverrideEdgeT>(src, dst, trig);

    default:
      break;
  }

  if ( !key ) { throw std::runtime_error("Invalid timer type"); }
  return key;
}

void NetworkManager::addTimer(std::shared_ptr<TimerKey> k, const Delay& delay)
{
  auto& ios = io_service_;
  timers_.apply([k, &ios, &delay](auto& map) mutable {
    auto it = map.find(k);
    if ( it == map.end() ) {
      map[k] = std::move(std::make_unique<Timer>(ios, delay));
    } else {
      size_t num_expired = it->second->expires_from_now(delay);
    }
  });
}

}  // namespace K3
