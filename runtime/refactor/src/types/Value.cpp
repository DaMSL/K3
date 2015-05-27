#include <string>

#include "Common.hpp"
#include "core/ProgramContext.hpp"
#include "serialization/Codec.hpp"
#include "types/Value.hpp"

namespace K3 {

void NativeValue::dispatchIntoContext(ProgramContext* pc, TriggerID trig) {
  return pc->__dispatch(this, trig);
}

PackedValue::PackedValue(CodecFormat format) { format_ = format; }

void PackedValue::dispatchIntoContext(ProgramContext* pc, TriggerID trig) {
  return pc->__dispatch(this, trig);
}

CodecFormat PackedValue::format() const { return format_; }

BufferPackedValue::BufferPackedValue(Buffer&& b, CodecFormat format)
    : PackedValue(format) {
  buffer_ = std::make_unique<Buffer>(std::move(b));
}

const char* BufferPackedValue::buf() const {
  if (buffer_) {
    return buffer_->data();
  } else {
    throw std::runtime_error("BufferPackedValue buf(): buffer pointer null");
  }
}

size_t BufferPackedValue::length() const {
  if (buffer_) {
    return buffer_->size();
  } else {
    throw std::runtime_error("BufferPackedValue length(): buffer pointer null");
  }
}

StringPackedValue::StringPackedValue(string&& b, CodecFormat format)
    : PackedValue(format) {
  string_ = std::make_unique<string>(std::move(b));
}

const char* StringPackedValue::buf() const {
  if (string_) {
    return string_->c_str();
  } else {
    throw std::runtime_error("StringPackedValue buf(): string pointer null");
  }
}

size_t StringPackedValue::length() const {
  if (string_) {
    return string_->length();
  } else {
    throw std::runtime_error("StringPackedValue length(): string pointer null");
  }
}

SentinelValue::SentinelValue() {}

void SentinelValue::dispatchIntoContext(ProgramContext* pc, TriggerID trig) {
  return pc->__dispatch(this);
}

}  // namespace K3
