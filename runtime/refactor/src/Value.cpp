#include "Codec.hpp"
#include "Common.hpp"
#include "Value.hpp"

const char* foo(std::vector<char>& v) {
  std::vector<char> v2 = std::move(v);
  return v2.data();
}

void NativeValue::dispatch(ProgramContext& pc, TriggerID t) {
  pc.processNative(t, *this);
  return;
}

void PackedValue::dispatch(ProgramContext& pc, TriggerID t) {
  pc.processNative(t, *codec_->unpack(buf(), length()));
  return;
}

// TODO move buffer in
PackedValue::PackedValue(Buffer b, shared_ptr<Codec> c) {
  buffer_ = b;
  codec_ = c;
}

const char* PackedValue::buf() {
  return buffer_.data();
}

size_t PackedValue::length() {
  return buffer_.size();
}

void SentinelValue::dispatch(ProgramContext& pc, TriggerID t) {
  throw EndOfProgramException();
}
