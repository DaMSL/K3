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
  native_ = codec_->unpack(*this);
  return native_.get();
}

const char* PackedValue::buf() const {
  return buffer_->data();
}

size_t PackedValue::length() const {
  return buffer_->size();
}

SentinelValue::SentinelValue() { }

NativeValue* SentinelValue::asNative() {
  throw EndOfProgramException();
}
