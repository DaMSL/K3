#include <chrono>
#include <thread>
#include <vector>

#include "network/OutgoingConnection.hpp"

namespace K3 {

OutgoingConnection::OutgoingConnection(const Address& addr,
                                       asio::io_service& service) :
      address_(addr), connected_(false), strand_(service), socket_(service) {

  // Attempt to connect to the address
  asio::ip::address_v4 ip(addr.ip);
  asio::ip::tcp::endpoint ep(ip, addr.port);
  boost::system::error_code error;
  for (int retries = 12000; !connected_ && retries > 0; retries--) {
    socket_.connect(ep, error);
    if (!error) {
      connected_ = true;
    } else {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
  }

  if (!connected_) {
    throw std::runtime_error(
        "InternalOutgoingConnection connect(): failed to connect to " +
        addr.toString());
  }
}

}  // namespace K3
