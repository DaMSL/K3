#include <vector>

#include "types/Message.hpp"
#include "core/ProgramContext.hpp"

namespace K3 {

Message::Message(const Address& src, const Address& dst, TriggerID trig,
                 shared_ptr<Value> val)
    : header_(src, dst, trig) {
  value_ = val;
}

Message::Message(const MessageHeader& head, shared_ptr<Value> val)
    : header_(head) {
  value_ = val;
}

Address Message::source() const { return header_.source(); }

Address Message::destination() const { return header_.destination(); }

TriggerID Message::trigger() const { return header_.trigger(); }

shared_ptr<Value> Message::value() const { return value_; }

shared_ptr<vector<asio::const_buffer>> NetworkMessage::outputBuffers() const {
  // Wrap members in boost buffers for an immeninent call to async_write
  shared_ptr<vector<asio::const_buffer>> result;
  result = make_shared<vector<asio::const_buffer>>();

  // Source Address
  auto& s_ip = header_.source_.ip;
  auto& s_port = header_.source_.port;
  result->push_back(
      asio::buffer(reinterpret_cast<const char*>(&s_ip), sizeof(s_ip)));
  result->push_back(
      asio::buffer(reinterpret_cast<const char*>(&s_port), sizeof(s_port)));

  // Destination Address
  auto& d_ip = header_.destination_.ip;
  auto& d_port = header_.destination_.port;
  result->push_back(
      asio::buffer(reinterpret_cast<const char*>(&d_ip), sizeof(d_ip)));
  result->push_back(
      asio::buffer(reinterpret_cast<const char*>(&d_port), sizeof(d_port)));

  // Trigger
  result->push_back(asio::buffer(
      reinterpret_cast<const char*>(&header_.trigger_), sizeof(header_.trigger_)));

  // Packed Payload Length
  result->push_back(asio::buffer(
      reinterpret_cast<const char*>(&payload_length_), sizeof(payload_length_)));

  // Payload Bytes
  shared_ptr<PackedValue> pv = std::dynamic_pointer_cast<PackedValue>(value_);
  result->push_back(asio::buffer(pv->buf(), pv->length()));

  return result;
}

shared_ptr<vector<asio::mutable_buffer>> NetworkMessage::inputBuffers() {
  // Wrap members in boost buffers for an imminent call to async_read
  shared_ptr<vector<asio::mutable_buffer>> result;
  result = make_shared<vector<asio::mutable_buffer>>();

  // Source Address
  auto& s_ip = header_.source_.ip;
  auto& s_port = header_.source_.port;
  result->push_back(asio::buffer(reinterpret_cast<char*>(&s_ip), sizeof(s_ip)));
  result->push_back(
      asio::buffer(reinterpret_cast<char*>(&s_port), sizeof(s_port)));

  // Destination Address
  auto& d_ip = header_.destination_.ip;
  auto& d_port = header_.destination_.port;
  result->push_back(asio::buffer(reinterpret_cast<char*>(&d_ip), sizeof(d_ip)));
  result->push_back(
      asio::buffer(reinterpret_cast<char*>(&d_port), sizeof(d_port)));

  // Trigger
  result->push_back(
      asio::buffer(reinterpret_cast<char*>(&header_.trigger_), sizeof(header_.trigger_)));

  // Payload Length
  result->push_back(
      asio::buffer(reinterpret_cast<char*>(&payload_length_), sizeof(payload_length_)));

  return result;
}

size_t NetworkMessage::networkHeaderSize() {
  return sizeof(header_.source_.ip) + sizeof(header_.source_.port) +
         sizeof(header_.destination_.ip) + sizeof(header_.destination_.port) +
         sizeof(header_.trigger_) + sizeof(payload_length_);
}

void NetworkMessage::setValue(shared_ptr<Value> v) { value_ = v; }

}  // namespace K3
