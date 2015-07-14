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
  Message();
  virtual ~Message() { }
  Message(TriggerID, unique_ptr<PackedValue>);

  unique_ptr<PackedValue> value_;
  TriggerID trigger_;
  #ifdef K3DEBUG
  Address source_;
  Address destination_;
  #endif
};

// Sub-class that can be directly read/written for network IO
class NetworkMessage : public Message {
 public:
  NetworkMessage();
  NetworkMessage (NetworkMessage&& other) {
    trigger_ = other.trigger_;
    value_ = std::move(other.value_);
    payload_length_ = other.payload_length_;
    #ifdef K3DEBUG
    source_ = std::move(other.source_);
    destination_ = std::move(other.destination_);
    #endif
  }
  NetworkMessage(TriggerID trig, unique_ptr<PackedValue> v);
  shared_ptr<std::vector<boost::asio::const_buffer>> outputBuffers() const;
  shared_ptr<std::vector<boost::asio::mutable_buffer>> inputBuffers();
  void setValue(unique_ptr<PackedValue> v);
  size_t networkHeaderSize();

  size_t payload_length_;
};

}  // namespace K3

#endif
