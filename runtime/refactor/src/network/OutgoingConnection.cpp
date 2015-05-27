#include <chrono>
#include <thread>

#include "network/OutgoingConnection.hpp"

namespace K3 {

OutgoingConnection::OutgoingConnection(const Address& addr,
                                       asio::io_service& service) {
  address_ = addr;
  strand_ = make_shared<asio::io_service::strand>(service);
  socket_ = make_shared<asio::ip::tcp::socket>(service);
  connected_ = false;

  // Attempt to connect to the address
  asio::ip::address_v4 ip(addr.ip);
  asio::ip::tcp::endpoint ep(ip, addr.port);
  boost::system::error_code error;
  for (int retries = 600; !connected_ && retries > 0; retries--) {
    socket_->connect(ep, error);
    if (!error) {
      connected_ = true;
    } else {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
      socket_ = make_shared<asio::ip::tcp::socket>(service);
    }
  }

  if (!connected_) {
    // TODO(jbw) print address
    throw std::runtime_error(
        "InternalOutgoingConnection connect(): failed to connect");
  }
}

OutgoingConnection::~OutgoingConnection() {
  if (connected_) {
    socket_->close();
  }
}

shared_ptr<asio::ip::tcp::socket> OutgoingConnection::getSocket() {
  return socket_;
}

bool OutgoingConnection::connected() { return connected_; }

void InternalOutgoingConnection::send(shared_ptr<NetworkMessage> pm,
                                      shared_ptr<ErrorHandler> e_handler) {
  if (!connected_) {
    throw std::runtime_error(
        "InternalOutgoingConnection send(): not connected");
  }

  shared_ptr<InternalOutgoingConnection> this_shared = shared_from_this();
  strand_->post([pm, this_shared, e_handler]() {
    if (this_shared->outbox_.size() == 0) {
      this_shared->writeLoop(pm, e_handler);
    } else {
      this_shared->outbox_.push(pm);
    }
  });
}

void InternalOutgoingConnection::writeLoop(shared_ptr<NetworkMessage> pm,
                                           shared_ptr<ErrorHandler> e_handler) {
  auto buffers = pm->outputBuffers();
  shared_ptr<InternalOutgoingConnection> this_shared = shared_from_this();
  auto callback = [this_shared, pm, buffers, e_handler](
      boost::system::error_code ec, size_t bytes) {
    if (ec) {
      (*e_handler)(ec);
    }

    if (this_shared->outbox_.size() > 0) {
      shared_ptr<NetworkMessage> next = this_shared->outbox_.front();
      this_shared->outbox_.pop();
      this_shared->writeLoop(next, e_handler);
    }
  };

  asio::async_write(*this_shared->getSocket(), *buffers,
                    this_shared->strand_->wrap(callback));
}

void ExternalOutgoingConnection::send(shared_ptr<PackedValue> pm,
                                      shared_ptr<ErrorHandler> e_handler) {
  if (!connected_) {
    throw std::runtime_error(
        "ExternalOutgoingConnection send(): not connected");
  }

  shared_ptr<ExternalOutgoingConnection> this_shared = shared_from_this();
  strand_->post([pm, this_shared, e_handler]() {
    if (this_shared->outbox_.size() == 0) {
      this_shared->writeLoop(pm, e_handler);
    } else {
      this_shared->outbox_.push(pm);
    }
  });
}

void ExternalOutgoingConnection::writeLoop(shared_ptr<PackedValue> pm,
                                           shared_ptr<ErrorHandler> e_handler) {
  auto buffers = make_shared<vector<asio::const_buffer>>();
  shared_ptr<int> header = make_shared<int>(pm->length());
  buffers->push_back(
      asio::const_buffer(reinterpret_cast<char*>(header.get()), sizeof(int)));
  buffers->push_back(asio::const_buffer(pm->buf(), *header));

  shared_ptr<ExternalOutgoingConnection> this_shared = shared_from_this();
  auto callback = [this_shared, pm, buffers, e_handler](
      boost::system::error_code ec, size_t bytes) {
    if (ec) {
      (*e_handler)(ec);
    }

    if (this_shared->outbox_.size() > 0) {
      shared_ptr<PackedValue> next = this_shared->outbox_.front();
      this_shared->outbox_.pop();
      this_shared->writeLoop(next, e_handler);
    }
  };

  asio::async_write(*this_shared->getSocket(), *buffers,
                    this_shared->strand_->wrap(callback));
}

}  // namespace K3
