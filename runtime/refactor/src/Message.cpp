#include "Message.hpp"

Message::Message(const Address& src, const Address& dst, TriggerID trig, unique_ptr<Value> val) {
  source_ = src;
  destination_ = dst;
  trigger_ = trig;
  value_ = std::move(val);
}

Address Message::source() const {
  return source_;
}

Address Message::destination() const {
  return destination_;
}

TriggerID Message::trigger() const {
  return trigger_;
}

NativeValue* Message::value() const {
  return value_->asNative();
}
