#ifndef K3_NETWORKMANAGER
#define K3_NETWORKMANAGER

#include <boost/asio.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/deadline_timer.hpp>
#include <boost/thread.hpp>

#include "Common.hpp"
#include "types/Message.hpp"
#include "types/Timers.hpp"
#include "spdlog/spdlog.h"

namespace K3 {

class InternalOutgoingConnection;
class InternalIncomingConnection;
class Peer;
class Listener;

// We need shared_ptrs to connection to make sure we keep them alive
template <class Connection>
class ConnectionMap : public ConcurrentMap<Address, shared_ptr<Connection>> {
 public:
  typedef ConcurrentMap<Address, shared_ptr<Connection>> Super;
  ConnectionMap() : ConcurrentMap<Address, shared_ptr<Connection>>() {}

  shared_ptr<Connection> lookupOrCreate(const Address& addr,
                                        boost::asio::io_service& service) {
    boost::strict_lock<ConcurrentMap<Address, shared_ptr<Connection>>> lock(*this);
    auto it = Super::map_.get(lock).find(addr);
    if (it != Super::map_.get(lock).end()) {
      return it->second;
    } else {
      auto c = make_shared<Connection>(addr, service);
      Super::map_.get(lock)[addr] = c;
      return c;
    }
  }
};

typedef ConcurrentMap<Address, shared_ptr<Listener>> ListenerMap;
typedef ConnectionMap<InternalOutgoingConnection> InternalConnectionMap;
typedef ConcurrentMap<Address, shared_ptr<InternalIncomingConnection>> IncomingConnectionMap;
typedef std::function<void(std::unique_ptr<Message>)> MessageHandler;
typedef std::function<void(boost_error)> ErrorHandler;

class NetworkManager {
 public:
  // Core Interface
  NetworkManager(int threads);
  ~NetworkManager();
  void stop();
  void join();
  void listenInternal(shared_ptr<Peer> p);
  void sendInternal(const Address& dst, unique_ptr<OutNetworkMessage> pm);

  // Utilities
  shared_ptr<Listener> getListener(const Address& addr);
  CodecFormat internalFormat() { return internal_format_; }

  // Timers.
  std::shared_ptr<TimerKey>
  timerKey(const TimerType& ty, const Address& src, const Address& dst, const TriggerID& trig);

  void addTimer(std::shared_ptr<TimerKey> k, const Delay& delay);
  void removeTimer(std::shared_ptr<TimerKey> k) { timers_.erase(k); }


  template<typename F>
  void asyncWaitTimer(std::shared_ptr<TimerKey> k, F handler);

 protected:
  shared_ptr<InternalOutgoingConnection> connectInternal(const Address& a);
  void addThread();

  shared_ptr<spdlog::logger> logger_;

  // IO service related members
  asio::io_service io_service_;
  unique_ptr<asio::io_service::work> work_;
  boost::thread_group threads_;

  // Listeners
  shared_ptr<ListenerMap> internal_listeners_; // ref-count

  // Connections
  shared_ptr<InternalConnectionMap> internal_out_conns_; // ref-count
  CodecFormat internal_format_;

  // State
  std::atomic<bool> running_;

  // Timers
  TimerMap timers_;
  static std::atomic<unsigned long> timer_cnt_;
};

template<typename F>
void NetworkManager::asyncWaitTimer(std::shared_ptr<TimerKey> k, F handler) {
  timers_.apply(k, [handler = std::forward<F>(handler)](std::unique_ptr<Timer>& t) mutable {
    t->async_wait(handler);
  });
}

}  // namespace K3

#endif
