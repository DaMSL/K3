#ifndef K3_MESSAGE
#define K3_MESSAGE

// Messages come from Peers (local or over the network) or from Sources.
// They live in the Queue of a Peer until they are dispatched into a ProgramContext.

#include <memory>

#include "Common.hpp"
#include "Value.hpp"

using std::unique_ptr;
class Message {
 public:
  Message(const Address&, const Address&, TriggerID, unique_ptr<Value>);
  Address source() const;
  Address destination() const;
  TriggerID trigger() const;
  Value* value() const;

 protected:
  Address source_;
  Address destination_;
  TriggerID trigger_;
  unique_ptr<Value> value_;
};

#endif
