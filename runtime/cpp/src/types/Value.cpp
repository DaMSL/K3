#include <string>

#include "Common.hpp"
#include "core/ProgramContext.hpp"
#include "serialization/Codec.hpp"
#include "types/Value.hpp"

namespace K3 {

PackedValue::PackedValue(CodecFormat format) { format_ = format; }

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
    : PackedValue(format), string_(std::move(b)) { }

const char* StringPackedValue::buf() const {
  return string_.c_str();
}

size_t StringPackedValue::length() const {
  return string_.length();
}

string StringPackedValue::steal() {
  return std::move(string_);
}

BaseStringRefPackedValue::BaseStringRefPackedValue(const base_string& b, CodecFormat format)
    : PackedValue(format), string_(b) { }

const char* BaseStringRefPackedValue::buf() const {
  return string_.c_str();
}

size_t BaseStringRefPackedValue::length() const {
  return string_.length();
}

YASPackedValue::YASPackedValue(yas::shared_buffer b, CodecFormat format)
    : PackedValue(format) {
  buf_ = b;
}

const char* YASPackedValue::buf() const { return buf_.data.get(); }

size_t YASPackedValue::length() const { return buf_.size; }

}  // namespace K3
