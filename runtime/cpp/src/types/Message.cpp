#include <vector>

#include "types/Message.hpp"
#include "core/ProgramContext.hpp"

namespace K3 {

std::array<boost::asio::mutable_buffer, ARRAYSIZE>& InNetworkMessage::inputBuffers() {
  auto it = begin(buffers_);
#ifdef K3MESSAGETRACE
  *(it++) = asio::buffer(reinterpret_cast<const char*>(&source_.ip), sizeof(source_.ip));
  *(it++) = asio::buffer(reinterpret_cast<const char*>(&source_.port), sizeof(source_.port));
  *(it++) = asio::buffer(reinterpret_cast<const char*>(&destination_.ip), sizeof(destination_.ip));
  *(it++) = asio::buffer(reinterpret_cast<const char*>(&destination_.port), sizeof(destination_.port));
#endif
  *(it++) = asio::buffer(reinterpret_cast<const char*>(&trigger_), sizeof(trigger_));
  *(it++) = asio::buffer(reinterpret_cast<const char*>(&payload_length_), sizeof(payload_length_));
  return buffers_;
}

std::array<boost::asio::const_buffer, ARRAYSIZE + 1>& NetworkMessage::outputBuffers() const {
  auto it = begin(buffers_);
#ifdef K3MESSAGETRACE
  *(it++) = asio::buffer(reinterpret_cast<const char*>(&source_.ip), sizeof(source_.ip));
  *(it++) = asio::buffer(reinterpret_cast<const char*>(&source_.port), sizeof(source_.port));
  *(it++) = asio::buffer(reinterpret_cast<const char*>(&destination_.ip), sizeof(destination_.ip));
  *(it++) = asio::buffer(reinterpret_cast<const char*>(&destination_.port), sizeof(destination_.port));
#endif
  *(it++) = asio::buffer(reinterpret_cast<const char*>(&trigger_), sizeof(trigger_));
  *(it++) = asio::buffer(reinterpret_cast<const char*>(&payload_length_), sizeof(payload_length_));
  *(it++) = asio::buffer(pv->buf(), pv->length());
  return buffers_;
}

}  // namespace K3
