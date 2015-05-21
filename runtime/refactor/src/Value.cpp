#include <string>

#include "Codec.hpp"
#include "Common.hpp"
#include "Value.hpp"
#include "ProgramContext.hpp"

void NativeValue::dispatchIntoContext(ProgramContext* pc, TriggerID trig) {
  return pc->dispatch(this, trig);
}

PackedValue::PackedValue(Buffer&& b, CodecFormat format) {
  buffer_ = std::make_unique<Buffer>(std::move(b));
  format_ = format;
}

void PackedValue::dispatchIntoContext(ProgramContext* pc, TriggerID trig) {
  return pc->dispatch(this, trig);
}

const char* PackedValue::buf() const {
  if (buffer_) {
    return buffer_->data();
  } else {
    throw std::runtime_error("PackedValue buf(): buffer pointer null");
  }
}

size_t PackedValue::length() const {
  if (buffer_) {
    return buffer_->size();
  } else {
    throw std::runtime_error("PackedValue length(): buffer pointer null");
  }
}

CodecFormat PackedValue::format() const {
  return format_;
}

SentinelValue::SentinelValue() { }

void SentinelValue::dispatchIntoContext(ProgramContext* pc, TriggerID trig) {
  return pc->dispatch(this);
}
