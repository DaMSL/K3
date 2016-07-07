#ifndef K3_MESSAGE
#define K3_MESSAGE

#include <memory>
#include <vector>

#include "Common.hpp"
#include "types/Value.hpp"

namespace K3 {

// Message container. Holds any type of Value.
class Message {
 public:
  Message() {}
  Message(TriggerID trig, unique_ptr<PackedValue> val) :
    trigger_(trig), value_(std::move(val)) {}

  #ifdef K3MESSAGETRACE
  Address source_;
  Address destination_;
  #endif
  TriggerID trigger_;
  unique_ptr<PackedValue> value_;
};

// Sub-class that can be directly read/written for network IO
class NetworkMessage : public Message {
 public:
  NetworkMessage() : payload_length_(0) {}
  NetworkMessage(TriggerID t, unique_ptr<PackedValue> v)
      : Message(t, std::move(v)), payload_length_(value_->length()) {}
  void init_info(); // initialize the info structure
  void setValue(unique_ptr<PackedValue> v) { value_ = std::move(v); }
  size_t networkHeaderSize() const {
    return
  #ifdef K3MESSAGETRACE
      sizeof(source_.ip) + sizeof(source_.port) +
      sizeof(destination_.ip) + sizeof(destination_.port) +
  #endif
      sizeof(trigger_) + sizeof(payload_length_);
  }

  size_t payload_length_;
};

#ifdef K3MESSAGETRACE
#define ARRAYSIZE 6
#else
#define ARRAYSIZE 2
#endif

class InNetworkMessage : public NetworkMessage {
  public:
  InNetworkMessage() {}
  InNetworkMessage(TriggerID t, unique_ptr<PackedValue> v) : NetworkMessage(t, std::move(v)) {}
  std::array<boost::asio::mutable_buffer, ARRAYSIZE>& inputBuffers();
  private:
  std::array<asio::mutable_buffer, ARRAYSIZE> buffers_;
};

class OutNetworkMessage : public NetworkMessage {
  public:
  OutNetworkMessage(TriggerID t, unique_ptr<PackedValue> v) : NetworkMessage(t, std::move(v)) {}
  std::array<boost::asio::const_buffer, ARRAYSIZE + 1>& outputBuffers();
  private:
  std::array<asio::const_buffer, ARRAYSIZE + 1> buffers_;
};

}  // namespace K3

#endif
