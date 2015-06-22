#include "network/IncomingConnection.hpp"

namespace K3 {

IncomingConnection::IncomingConnection(asio::io_service& service,
                                       CodecFormat format) {
  socket_ = make_shared<asio::ip::tcp::socket>(service);
  format_ = format;
}

IncomingConnection::~IncomingConnection() { socket_->close(); }

shared_ptr<asio::ip::tcp::socket> IncomingConnection::getSocket() {
  return socket_;
}

InternalIncomingConnection::InternalIncomingConnection(
    asio::io_service& service, CodecFormat format)
    : IncomingConnection(service, format) {}

void InternalIncomingConnection::receiveMessages(
    shared_ptr<MessageHandler> m_handler, shared_ptr<ErrorHandler> e_handler) {
  // Create an empty NetworkMessage for reading into
  shared_ptr<NetworkMessage> m = make_shared<NetworkMessage>();
  auto in_buffers = m->inputBuffers();

  shared_ptr<InternalIncomingConnection> this_shared = shared_from_this();
  // Callback for when the network header has been read (source, dest, trigger,
  // payload_length)
  auto header_callback = [this_shared, in_buffers, m, e_handler, m_handler](
      boost_error ec, size_t bytes) {
    if (ec) {
      (*e_handler)(ec);
    }
    if (bytes == m->networkHeaderSize()) {
      // Resize the buffer and isssue a second async read
      shared_ptr<Buffer> payload_buf = make_shared<Buffer>();
      payload_buf->resize(m->payload_length_);
      auto buffer = asio::buffer(payload_buf->data(), payload_buf->size());
      auto payload_callback =
          [this_shared, m, payload_buf, e_handler, m_handler](boost_error ec,
                                                              size_t bytes) {
            if (ec) {
              (*e_handler)(ec);
            }

            if (bytes == m->payload_length_) {
              // Create a PackedValue from the buffer, and call the message
              // handler
              unique_ptr<PackedValue> pv;
              pv = make_unique<BufferPackedValue>(std::move(*payload_buf),
                                                  this_shared->format_);
              m->setValue(std::move(pv));
              (*m_handler)(std::make_unique<Message>(std::move(*m)));

              // Recurse to receive the next message
              this_shared->receiveMessages(m_handler, e_handler);
            } else {
              (*e_handler)(ec);
            }
          };

      asio::async_read(*this_shared->getSocket(), buffer, payload_callback);
    }
  };

  asio::async_read(*getSocket(), *in_buffers, header_callback);
}

void ExternalIncomingConnection::receiveMessages(
    shared_ptr<MessageHandler> m_handler, shared_ptr<ErrorHandler> e_handler) {
  shared_ptr<int> header_int = make_shared<int>(0);
  auto in_buffer =
      asio::buffer(reinterpret_cast<char*>(header_int.get()), sizeof(int));

  // Callback for when the network header has been read (payload_length)
  auto this_shared = shared_from_this();
  auto header_callback =
      [this_shared, header_int, in_buffer, m_handler, e_handler](boost_error ec,
                                                                 size_t bytes) {
        if (ec) {
          (*e_handler)(ec);
        }
        if (bytes == sizeof(int)) {
          // Resize the buffer and isssue a second async read
          shared_ptr<Buffer> payload_buf = make_shared<Buffer>();
          payload_buf->resize(*header_int);
          auto buffer = asio::buffer(payload_buf->data(), payload_buf->size());
          auto payload_callback =
              [this_shared, header_int, payload_buf, m_handler, e_handler](
                  boost_error ec, size_t bytes) {
                if (ec) {
                  (*e_handler)(ec);
                }

                if (bytes == *header_int) {
                  // Create a PackedValue from the buffer, and call the message
                  // handler
                  unique_ptr<PackedValue> pv;
                  pv = make_unique<BufferPackedValue>(std::move(*payload_buf),
                                                      this_shared->format_);
                  auto m = std::make_unique<Message>(this_shared->peer_addr_,
                                                this_shared->peer_addr_,
                                                this_shared->trigger_, std::move(pv));
                  (*m_handler)(std::move(m));

                  // Recurse to receive the next message
                  this_shared->receiveMessages(m_handler, e_handler);
                } else {
                  (*e_handler)(ec);
                }
              };

          asio::async_read(*this_shared->getSocket(), buffer, payload_callback);
        }
      };

  asio::async_read(*getSocket(), in_buffer, header_callback);
}

}  // namespace K3
