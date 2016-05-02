#ifndef K3_INCOMINGCONNECTION
#define K3_INCOMINGCONNECTION

#include "Common.hpp"
#include "NetworkManager.hpp"

namespace K3 {

class IncomingConnection {
 public:
  IncomingConnection(asio::io_service& service,
                                       CodecFormat format) : socket_(service), format_(format) {}

  ~IncomingConnection() { socket_.close(); }

  asio::ip::tcp::socket& getSocket() { return socket_; }

 protected:
  asio::ip::tcp::socket socket_;
  CodecFormat format_;
};

// Read Messages from the network
class InternalIncomingConnection
    : public IncomingConnection,
      public enable_shared_from_this<InternalIncomingConnection> {
 public:
  InternalIncomingConnection(asio::io_service& service, CodecFormat format)
    : IncomingConnection(service, format) {}

  // M = MessageHandler, E = ErrorHandler
  template <class M, class E> void receiveMessages(M&&, E&&);
};

template <class M, class E> // M is messagehandler, E is errorHandler
void InternalIncomingConnection::receiveMessages(M&& m_handler, E&& e_handler) {
  // Create an empty NetworkMessage for reading into
  // Using a raw pointer because closures need to be copyable
  InNetworkMessage* m = new InNetworkMessage();

  // Callback for when the network header has been read (source, dest, trigger,
  // payload_length)
  auto header_callback = [this_shared = shared_from_this(), m,
       e_handler=std::forward<E>(e_handler),
       m_handler=std::forward<M>(m_handler)]
       (boost_error ec, size_t bytes) mutable {
    if (ec) {
      e_handler(ec);
      return;
    }
    if (bytes == m->networkHeaderSize()) {
      // Resize the buffer and isssue a second async read
      // Again, use a raw pointer since closures need to be copyable
      Buffer* payload_buf = new Buffer(m->payload_length_);
      auto payload_callback =
        [this_shared, m, payload_buf,
          e_handler=std::forward<E>(e_handler),
          m_handler=std::forward<M>(m_handler)]
          (boost_error ec, size_t bytes) mutable {
            if (!ec && bytes == m->payload_length_) {
              // Create a PackedValue from the buffer, and call the message handler
              auto pv = std::make_unique<BufferPackedValue>(payload_buf, this_shared->format_);
              m->setValue(std::move(pv));
              m_handler(std::unique_ptr<NetworkMessage>(m));

              // Recurse to receive the next message
              this_shared->receiveMessages(std::forward<M>(m_handler), std::forward<E>(e_handler));
            } else {
              e_handler(ec);
            }
          };

      auto buffer = asio::buffer(payload_buf->data(), payload_buf->size());
      asio::async_read(this_shared->getSocket(), buffer, std::move(payload_callback));
    }
  };

  asio::async_read(getSocket(), m->inputBuffers(), std::move(header_callback));
}

}  // namespace K3

#endif
