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

// Sub-class that can be directly read/written for network IO
class NetworkMessage : public Message {
 public:
  NetworkMessage();
  NetworkMessage(const MessageHeader& head, shared_ptr<PackedValue> v);
  shared_ptr<std::vector<boost::asio::const_buffer>> outputBuffers() const;
  shared_ptr<std::vector<boost::asio::mutable_buffer>> inputBuffers();
  void setValue(shared_ptr<Value> v);
  size_t networkHeaderSize();

  size_t payload_length_;
};

}  // namespace K3

#endif
