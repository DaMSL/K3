#include <vector>

#include "types/Message.hpp"
#include "core/ProgramContext.hpp"

namespace K3 {

std::array<boost::asio::mutable_buffer, ARRAYSIZE>& InNetworkMessage::inputBuffers() {
  buffers_ = {
#ifdef K3MESSAGETRACE
  asio::buffer(reinterpret_cast<char*>(&source_.ip), sizeof(source_.ip)),
  asio::buffer(reinterpret_cast<char*>(&source_.port), sizeof(source_.port)),
  asio::buffer(reinterpret_cast<char*>(&destination_.ip), sizeof(destination_.ip)),
  asio::buffer(reinterpret_cast<char*>(&destination_.port), sizeof(destination_.port)),
#endif
  asio::buffer(reinterpret_cast<char*>(&trigger_), sizeof(trigger_)),
  asio::buffer(reinterpret_cast<char*>(&payload_length_), sizeof(payload_length_))
  };
  return buffers_;
}

std::array<boost::asio::const_buffer, ARRAYSIZE + 1>& OutNetworkMessage::outputBuffers() {

  buffers_ = {
#ifdef K3MESSAGETRACE
  asio::buffer(reinterpret_cast<const char*>(&source_.ip), sizeof(source_.ip)),
  asio::buffer(reinterpret_cast<const char*>(&source_.port), sizeof(source_.port)),
  asio::buffer(reinterpret_cast<const char*>(&destination_.ip), sizeof(destination_.ip)),
  asio::buffer(reinterpret_cast<const char*>(&destination_.port), sizeof(destination_.port)),
#endif
  asio::buffer(reinterpret_cast<const char*>(&trigger_), sizeof(trigger_)),
  asio::buffer(reinterpret_cast<const char*>(&payload_length_), sizeof(payload_length_)),
  asio::buffer(value_->buf(), value_->length())
  };
  return buffers_;
}

}  // namespace K3
