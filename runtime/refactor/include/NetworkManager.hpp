#ifndef K3_NETWORKMANAGER
#define K3_NETWORKMANAGER

#include <memory>
#include <queue>
#include <vector>

#include "boost/asio.hpp"
#include "boost/asio/read.hpp"
#include "boost/thread.hpp"

#include "Common.hpp"
#include "Message.hpp"

using std::queue;
using asio::io_service;

class InternalOutgoingConnection;
class Peer;
class Listener;

typedef std::function<void(shared_ptr<Message>)> MessageHandler;
typedef std::function<void(boost_error)> ErrorHandler;

// Singleton class
class NetworkManager {
 public:
  // Core
  static NetworkManager& getInstance();
  void run();
  void stop();
  void join();
  void listenInternal(shared_ptr<Peer> p);
  void connectInternal(const Address& a);
  void sendInternal(shared_ptr<NetworkMessage> pm);
  // void listenExternal(shared_ptr<Peer> p, shared_ptr<IncomingConnectionFactory>)
  // void connectExternal(const Address& a, shared_ptr<ExternalOutgoingConnectionFactory>)
  // void sendExternal(const Address&a, shared_ptr<PackedValue> pv)

  // Utilities
  shared_ptr<asio::io_service> getIOService();
  shared_ptr<Listener> getListener(const Address& addr);
  CodecFormat internalFormat();

 private:
  // No copies of singleton
  NetworkManager() { }
  NetworkManager(const NetworkManager&) = delete;
  void operator=(const NetworkManager&) = delete;

  void addThread();

  shared_ptr<asio::io_service::work> work_;
  shared_ptr<asio::io_service> io_service_;
  shared_ptr<boost::thread_group> threads_;
  shared_ptr<ConcurrentMap<Address, Listener>> internal_listeners_;
  shared_ptr<ConcurrentMap<Address, InternalOutgoingConnection>> internal_out_conns_;
  CodecFormat internal_format_;
};

// TODO(jbw) move into seperate file, split header/cpp
class IncomingConnection {
 public:
  IncomingConnection(shared_ptr<MessageHandler> m_handler, shared_ptr<ErrorHandler> e_handler) {
    auto service = NetworkManager::getInstance().getIOService();
    socket_ = make_shared<asio::ip::tcp::socket>(*service);
    message_handler_ = m_handler;
    error_handler_ = e_handler;
  }

  shared_ptr<asio::ip::tcp::socket> getSocket() {
    return socket_;
  }

  void setErrorHandler(shared_ptr<ErrorHandler> h) {
    error_handler_ = h;
  }

  virtual void receiveMessages() = 0;

 protected:
  shared_ptr<asio::ip::tcp::socket> socket_;
  shared_ptr<MessageHandler> message_handler_;
  shared_ptr<ErrorHandler> error_handler_;
};

class InternalIncomingConnection : public IncomingConnection {
 public:
  InternalIncomingConnection(shared_ptr<MessageHandler> m_handler,
                             shared_ptr<ErrorHandler> e_handler)
    : IncomingConnection(m_handler, e_handler) { }

  virtual void receiveMessages() {
    // Create an empty NetworkMessage for reading into
    shared_ptr<NetworkMessage> m = make_shared<NetworkMessage>();
    auto in_buffers = m->inputBuffers();

    // Callback for when the network header has been read (source, dest, trigger, payload_length)
    auto header_callback = [this, in_buffers, m] (boost_error ec, size_t bytes) {
      (*error_handler_)(ec);
      if (bytes == m->networkHeaderSize()) {
        // Resize the buffer and isssue a second async read
        shared_ptr<Buffer> payload_buf = make_shared<Buffer>();
        payload_buf->resize(m->payload_length_);
        auto buffer = asio::buffer(payload_buf->data(), payload_buf->size());
        auto payload_callback = [this, m, payload_buf] (boost_error ec, size_t bytes) {
          (*error_handler_)(ec);

          if (bytes == m->payload_length_) {
            // Create a PackedValue from the buffer, and call the message handler
            auto format = NetworkManager::getInstance().internalFormat();
            shared_ptr<PackedValue> pv;
            pv = make_shared<PackedValue>(std::move(*payload_buf), format);
            m->setValue(pv);
            (*message_handler_)(m);

            // Recurse to receive the next message
            receiveMessages();
          } else {
            (*error_handler_)(ec);
          }
        };

        asio::async_read(*getSocket(), buffer, payload_callback);
      }
    };

    asio::async_read(*getSocket(), *in_buffers, header_callback);
  }
};

// TODO(jbw) error handlers to remove bad connections
class InternalOutgoingConnection {
 public:
  explicit InternalOutgoingConnection(const Address& addr) {
    auto service = NetworkManager::getInstance().getIOService();
    strand_ = make_shared<asio::io_service::strand>(*service);
    socket_ = make_shared<asio::ip::tcp::socket>(*service);

    // Connect the socket to the provided address
    asio::ip::address_v4 ip(std::get<0>(addr));
    asio::ip::tcp::endpoint ep(ip, std::get<1>(addr));
    boost::system::error_code error;
    socket_->connect(ep, error);

    if (!error) {
      connected_ = true;
    } else {
      // TODO(jbw) retries
      throw std::runtime_error("InternalOutgoingConnection(): failed to connect");
    }
  }

  shared_ptr<asio::ip::tcp::socket> getSocket() {
    return socket_;
  }

  void send(shared_ptr<NetworkMessage> pm) {
    if (!connected_) {
      throw std::runtime_error("InternalOutgoingConnection send(): not connected");
    }

    strand_->post([pm, this] () {
      if (outbox_.size() == 0) {
        writeLoop(pm);
      } else {
        outbox_.push(pm);
      }
    });
  }

  void writeLoop(shared_ptr<NetworkMessage> pm) {
    auto buffers = pm->outputBuffers();
    auto callback = [this, pm, buffers] (boost::system::error_code ec, size_t bytes) {
      if (ec) {
        throw std::runtime_error(ec.message());
      }

      if (outbox_.size() > 0) {
        shared_ptr<NetworkMessage> next = outbox_.front();
        outbox_.pop();
        writeLoop(next);
      }
    };

    asio::async_write(*getSocket(), *buffers, strand_->wrap(callback));
  }

  bool connected() {
    return connected_;
  }

 protected:
  shared_ptr<asio::io_service::strand> strand_;
  queue<shared_ptr<NetworkMessage>> outbox_;
  shared_ptr<asio::ip::tcp::socket> socket_;
  bool connected_;
};

#endif
