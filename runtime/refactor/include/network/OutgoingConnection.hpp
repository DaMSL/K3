#ifndef K3_OUTGOINGCONNECTION
#define K3_OUTGOINGCONNECTION

// Outgoing connections provide the ability to send over the wire.
// Internal connections send full messages, while External connections send
// values only to a specified address.

#include <queue>

#include "Common.hpp"
#include "NetworkManager.hpp"

namespace K3 {

// TODO(jbw) reduce duplication, NetworkMessage and PackedValue could
// provide an input/output buffer interface
class OutgoingConnection {
 public:
  OutgoingConnection(const Address& addr, asio::io_service& service);
  ~OutgoingConnection();
  shared_ptr<asio::ip::tcp::socket> getSocket();
  bool connected();

 protected:
  Address address_;
  bool connected_;
  shared_ptr<asio::io_service::strand> strand_;
  shared_ptr<asio::ip::tcp::socket> socket_;
};

class InternalOutgoingConnection
    : public OutgoingConnection,
      public enable_shared_from_this<InternalOutgoingConnection> {
 public:
  InternalOutgoingConnection(const Address& addr, asio::io_service& service)
      : OutgoingConnection(addr, service) {}
  void send(shared_ptr<NetworkMessage> pm, shared_ptr<ErrorHandler> e_handler);

 protected:
  void writeLoop(shared_ptr<NetworkMessage> pm,
                 shared_ptr<ErrorHandler> e_handler);

  std::queue<shared_ptr<NetworkMessage>> outbox_;
};

class ExternalOutgoingConnection
    : public OutgoingConnection,
      public enable_shared_from_this<ExternalOutgoingConnection> {
 public:
  ExternalOutgoingConnection(const Address& addr, asio::io_service& service)
      : OutgoingConnection(addr, service) {}
  void send(shared_ptr<PackedValue> pm, shared_ptr<ErrorHandler> e_handler);

 protected:
  void writeLoop(shared_ptr<PackedValue> pm,
                 shared_ptr<ErrorHandler> e_handler);

  std::queue<shared_ptr<PackedValue>> outbox_;
};

}  // namespace K3

#endif
