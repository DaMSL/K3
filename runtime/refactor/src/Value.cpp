#include <string>

#include "Codec.hpp"
#include "Common.hpp"
#include "Value.hpp"
#include "ProgramContext.hpp"

void NativeValue::dispatchIntoContext(ProgramContext* pc, TriggerID trig) {
  return pc->dispatch(this, trig);
}

PackedValue::PackedValue(unique_ptr<Buffer> b, CodecFormat format) {
  buffer_ = std::move(b);
  format_ = format;
}

void PackedValue::dispatchIntoContext(ProgramContext* pc, TriggerID trig) {
  return pc->dispatch(this, trig);
}

const char* PackedValue::buf() const {
  if (buffer_) {
    return buffer_->data();
  } else {
    throw std::runtime_error("PackedValue: buffer has been destroyed");
  }
}

size_t PackedValue::length() const {
  if (buffer_) {
    return buffer_->size();
  } else {
    throw std::runtime_error("PackedValue: buffer has been destroyed");
  }
}

CodecFormat PackedValue::format() const {
  return format_;
}

SentinelValue::SentinelValue() { }

void SentinelValue::dispatchIntoContext(ProgramContext* pc, TriggerID trig) {
  return pc->dispatch(this);
}
