#ifndef K3_MESSAGE
#define K3_MESSAGE

#include <memory>
#include <vector>

#include "Common.hpp"
#include "Value.hpp"

namespace K3 {

class Message {
 public:
  Message() { }
  Message(const Address&, const Address&, TriggerID, shared_ptr<Value>);
  Message(const MessageHeader&, shared_ptr<Value>);
  Address source() const;
  Address destination() const;
  TriggerID trigger() const;
  shared_ptr<Value> value() const;

 protected:
  MessageHeader header_;
  shared_ptr<Value> value_;
};

class NetworkMessage : public Message {
 public:
  NetworkMessage() : Message() { }
  NetworkMessage(const MessageHeader& head, shared_ptr<PackedValue> v) : Message(head, v) {
    payload_length_ = v->length();
  }
  shared_ptr<std::vector<boost::asio::const_buffer>> outputBuffers() const;
  shared_ptr<std::vector<boost::asio::mutable_buffer>> inputBuffers();
  void setValue(shared_ptr<Value> v);
  int networkHeaderSize();

  int payload_length_;
};

}  // namespace K3

#endif
