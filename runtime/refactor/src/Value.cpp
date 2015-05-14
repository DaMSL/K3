#include "Codec.hpp"
#include "Value.hpp"

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
