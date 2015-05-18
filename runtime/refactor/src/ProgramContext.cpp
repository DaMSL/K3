#include <iostream>
#include <string>

#include "Codec.hpp"
#include "Common.hpp"
#include "Value.hpp"
#include "ProgramContext.hpp"

DummyContext::DummyContext() {
  state_ = make_shared<DummyState>();
}

void DummyContext::dispatch(NativeValue* nv, TriggerID t) {
  if (t == 1) {
    int i = *nv->as<int>();
    intTrigger(i);
  } else if (t == 2) {
    std::string s = std::move(*nv->as<std::string>());
    stringTrigger(s);
  } else {
    std::cout << "INVALID TRIGGER ID" << std::endl;
  }
  return;
}

void DummyContext::dispatch(PackedValue* pv, TriggerID t) {
  unique_ptr<NativeValue> nv;
  shared_ptr<Codec> codec;
  if (t == 1) {
    codec = Codec::getCodec<int>(pv->format());
  } else if (t == 2) {
    codec = Codec::getCodec<std::string>(pv->format());
  } else {
    std::cout << "INVALID TRIGGER ID" << std::endl;
  }
  nv = codec->unpack(*pv);
  return dispatch(nv.get(), t);
}

void DummyContext::dispatch(SentinelValue* sv) {
  throw EndOfProgramException();
}

void DummyContext::intTrigger(int i) {
  state_->my_int_ = i;
  return;
}

void DummyContext::stringTrigger(std::string s) {
  state_->my_string_ = s;
  return;
}
