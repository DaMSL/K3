#include <vector>

#include "types/Message.hpp"
#include "core/ProgramContext.hpp"

namespace K3 {

Message::Message() {}

Message::Message(TriggerID trig,
                 unique_ptr<PackedValue> val) {
  trigger_ = trig;
  value_ = std::move(val);
}


NetworkMessage::NetworkMessage() : Message() {
  payload_length_ = 0;
}

NetworkMessage::NetworkMessage(TriggerID t,
                               unique_ptr<PackedValue> v)
    : Message(t, std::move(v)) {
  payload_length_ = value_->length();
}

shared_ptr<vector<asio::const_buffer>> NetworkMessage::outputBuffers() const {
  // Wrap members in boost buffers for an immeninent call to async_write
  shared_ptr<vector<asio::const_buffer>> result;
  result = make_shared<vector<asio::const_buffer>>();

  #ifdef K3DEBUG
  // Source Address
  auto& s_ip = source_.ip;
  auto& s_port = source_.port;
  result->push_back(
      asio::buffer(reinterpret_cast<const char*>(&s_ip), sizeof(s_ip)));
  result->push_back(
      asio::buffer(reinterpret_cast<const char*>(&s_port), sizeof(s_port)));

  // Destination Address
  auto& d_ip = destination_.ip;
  auto& d_port = destination_.port;
  result->push_back(
      asio::buffer(reinterpret_cast<const char*>(&d_ip), sizeof(d_ip)));
  result->push_back(
      asio::buffer(reinterpret_cast<const char*>(&d_port), sizeof(d_port)));
  #endif

  // Trigger
  result->push_back(asio::buffer(
      reinterpret_cast<const char*>(&trigger_), sizeof(trigger_)));

  // Packed Payload Length
  result->push_back(asio::buffer(
      reinterpret_cast<const char*>(&payload_length_), sizeof(payload_length_)));

  // Payload Bytes
  PackedValue* pv = dynamic_cast<PackedValue*>(value_.get());
  result->push_back(asio::buffer(pv->buf(), pv->length()));

  return result;
}

shared_ptr<vector<asio::mutable_buffer>> NetworkMessage::inputBuffers() {
  // Wrap members in boost buffers for an imminent call to async_read
  shared_ptr<vector<asio::mutable_buffer>> result;
  result = make_shared<vector<asio::mutable_buffer>>();

  // Source Address
  #ifdef K3DEBUG
  auto& s_ip = source_.ip;
  auto& s_port = source_.port;
  result->push_back(asio::buffer(reinterpret_cast<char*>(&s_ip), sizeof(s_ip)));
  result->push_back(
      asio::buffer(reinterpret_cast<char*>(&s_port), sizeof(s_port)));

  // Destination Address
  auto& d_ip = destination_.ip;
  auto& d_port = destination_.port;
  result->push_back(asio::buffer(reinterpret_cast<char*>(&d_ip), sizeof(d_ip)));
  result->push_back(
      asio::buffer(reinterpret_cast<char*>(&d_port), sizeof(d_port)));
  #endif

  // Trigger
  result->push_back(
      asio::buffer(reinterpret_cast<char*>(&trigger_), sizeof(trigger_)));

  // Payload Length
  result->push_back(
      asio::buffer(reinterpret_cast<char*>(&payload_length_), sizeof(payload_length_)));

  return result;
}

size_t NetworkMessage::networkHeaderSize() {
  size_t size = 0;
  #ifdef K3DEBUG
  size += sizeof(source_.ip) + sizeof(source_.port) +
         sizeof(destination_.ip) + sizeof(destination_.port);
  #endif
  size += sizeof(trigger_) + sizeof(payload_length_);
  return size;
}

void NetworkMessage::setValue(unique_ptr<PackedValue> v) { value_ = std::move(v); }

}  // namespace K3
