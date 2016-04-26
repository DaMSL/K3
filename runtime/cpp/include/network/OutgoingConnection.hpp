#ifndef K3_OUTGOINGCONNECTION
#define K3_OUTGOINGCONNECTION

#include <queue>

#include "Common.hpp"
#include "NetworkManager.hpp"

namespace K3 {

// TODO(jbw) reduce duplication, NetworkMessage and PackedValue could
// provide an input/output buffer interface
class OutgoingConnection {
 public:
  OutgoingConnection(const Address& addr, asio::io_service& service);

  ~OutgoingConnection() {
    if (connected_) { socket_.close(); }
  }

  asio::ip::tcp::socket& getSocket() { return socket_; }
  bool connected() { return connected_; }

 protected:
  Address address_;
  bool connected_;
  asio::ip::tcp::socket socket_;
  asio::io_service::strand strand_;
};

class InternalOutgoingConnection
    : public OutgoingConnection,
      public enable_shared_from_this<InternalOutgoingConnection> {
 public:
  InternalOutgoingConnection(const Address& addr, asio::io_service& service)
      : OutgoingConnection(addr, service) {}
  template <class E> void send(unique_ptr<OutNetworkMessage>, E); // E = ErrorHandler
  template <class E> void writeLoop(E&&); // E = ErrorHandler
 protected:
  // The outbox is necessary to make sure that we synchronize every part: the async_write calls
  // are kept in order using the strand, and the actual writing is one-at-a-time using the outbox
  std::queue<OutNetworkMessage*> outbox_;
};

template<class E> // E = ErrorHandler
void InternalOutgoingConnection::send(unique_ptr<OutNetworkMessage> pm,
                                      E e_handler) {
  if (!connected_) {
    throw std::runtime_error(
        "InternalOutgoingConnection send(): not connected");
  }

  // A strand is important to make sure we're not sending out of order
  // We need to release the unique_ptr since it currently doesn't work with lambdas
  strand_.post([this_shared=shared_from_this(), e_handler=std::move(e_handler), pm = pm.release()]() {
    this_shared->outbox_.push(pm);
    if (this_shared->outbox_.size() == 1) {
      // If we're the first ones to write, start the looping
      this_shared->writeLoop(std::move(e_handler));
    }
  });
}

template <class E> // E = ErrorHandler
void InternalOutgoingConnection::writeLoop(E&& e_handler) {
  auto callback = [this_shared=shared_from_this(), e_handler=std::forward<E>(e_handler)]
    (boost::system::error_code ec, size_t) {
    if (ec) {
      e_handler(ec);
    }
    delete this_shared->outbox_.front(); // make sure to delete the message
    this_shared->outbox_.pop();          // now remove from the queue

    if (this_shared->outbox_.size() > 0) {
      this_shared->writeLoop(std::forward<E>(e_handler));
    }
  };
  asio::async_write(getSocket(), outbox_.front()->outputBuffers(), strand_.wrap(callback));
}

}  // namespace K3

#endif
