#ifndef K3_NETWORKMANAGER
#define K3_NETWORKMANAGER

// Networking for the K3 Runtime. Provides ability to start Listeners (for
// receiving messages)
// and send messages to other peers and external sinks

#include "boost/asio.hpp"
#include "boost/asio/read.hpp"
#include "boost/thread.hpp"

#include "Common.hpp"
#include "types/Message.hpp"
#include "spdlog/spdlog.h"

namespace K3 {

class InternalOutgoingConnection;
class ExternalOutgoingConnection;
class IncomingConnection;
class Peer;
class Listener;

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
typedef ConnectionMap<ExternalOutgoingConnection>
    ExternalConnectionMap;
typedef ConcurrentMap<Address, shared_ptr<IncomingConnection>>
    IncomingConnectionMap;
typedef std::function<void(shared_ptr<Message>)> MessageHandler;
typedef std::function<void(boost_error)> ErrorHandler;


class NetworkManager {
 public:
  // Core
  NetworkManager();
  ~NetworkManager();
  void stop();
  void join();
  void listenInternal(shared_ptr<Peer> p);
  void listenExternal(shared_ptr<Peer> p, Address listen_addr, TriggerID trig,
                      CodecFormat format);
  void sendInternal(shared_ptr<NetworkMessage> pm);
  void sendExternal(const Address& a, shared_ptr<PackedValue> pv);

  // Utilities
  shared_ptr<Listener> getListener(const Address& addr);
  CodecFormat internalFormat();

 protected:
  shared_ptr<InternalOutgoingConnection> connectInternal(const Address& a);
  shared_ptr<ExternalOutgoingConnection> connectExternal(const Address& a);
  void addThread();

  // IO service related members
  shared_ptr<asio::io_service> io_service_;
  shared_ptr<asio::io_service::work> work_;
  shared_ptr<boost::thread_group> threads_;
  // Listeners
  shared_ptr<ListenerMap> internal_listeners_;
  shared_ptr<ListenerMap> external_listeners_;
  // Connections
  shared_ptr<InternalConnectionMap> internal_out_conns_;
  shared_ptr<ExternalConnectionMap> external_out_conns_;
  CodecFormat internal_format_;

  // State
  std::atomic<bool> running_;

  // Logger
  shared_ptr<spdlog::logger> logger_;
};

}  // namespace K3

#endif