#include "Codec.hpp"
#include "Common.hpp"
#include "Value.hpp"

NativeValue* NativeValue::asNative() {
  return this;
}

PackedValue::PackedValue(unique_ptr<Buffer> b, shared_ptr<Codec> c) {
  buffer_ = std::move(b);
  codec_ = c;
}

NativeValue* PackedValue::asNative() {
  if (!native_) {
    native_ = codec_->unpack(*this);
    buffer_.reset();
  }
  return native_.get();
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

SentinelValue::SentinelValue() { }

NativeValue* SentinelValue::asNative() {
  throw EndOfProgramException();
}
