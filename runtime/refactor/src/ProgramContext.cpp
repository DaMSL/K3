#include <iostream>
#include <memory>
#include <string>

#include "Codec.hpp"
#include "Common.hpp"
#include "Engine.hpp"
#include "Message.hpp"
#include "Value.hpp"
#include "ProgramContext.hpp"

DummyContext::DummyContext() {
  state_ = make_shared<DummyState>();
}

// TODO(jbw) test dispatch with a map
// TODO(jbw) ensure we can move out of the as<> function
void DummyContext::dispatch(NativeValue* nv, TriggerID t) {
  if (t == 1) {
    int i = *nv->as<int>();
    intTrigger(i);
  } else if (t == 2) {
    std::string s = std::move(*nv->as<std::string>());
    stringTrigger(s);
  } else {
    throw std::runtime_error("Invalid trigger ID");
  }
  return;
}

// TODO(jbw) test dispatch with a map
void DummyContext::dispatch(PackedValue* pv, TriggerID t) {
  shared_ptr<NativeValue> nv;
  shared_ptr<Codec> codec;
  if (t == 1) {
    codec = Codec::getCodec<int>(pv->format());
  } else if (t == 2) {
    codec = Codec::getCodec<std::string>(pv->format());
  } else {
    throw std::runtime_error("Invalid trigger ID");
  }
  nv = codec->unpack(*pv);
  return dispatch(nv.get(), t);
}

void DummyContext::dispatch(SentinelValue* sv) {
  throw EndOfProgramException();
}

void DummyContext::__patch(const YAML::Node& node) {
  YAML::convert<DummyContext>::decode(node, *this);
}

void DummyContext::__processRole() {
  if (role == "int") {
    MessageHeader h(me, me, 1);
    // TODO(jbw) grab internal format from NetworkManager
    static shared_ptr<Codec> codec = Codec::getCodec<int>(CodecFormat::BoostBinary);
    Engine::getInstance().send(h, make_shared<TNativeValue<int>>(5), codec);
  } else if (role == "string") {
    MessageHeader h(me, me, 2);
    static shared_ptr<Codec> codec = Codec::getCodec<std::string>(CodecFormat::BoostBinary);
    Engine::getInstance().send(h, make_shared<TNativeValue<std::string>>("hi"), codec);
  }
}

void DummyContext::intTrigger(int i) {
  state_->my_int_ = i;
  return;
}

void DummyContext::stringTrigger(std::string s) {
  state_->my_string_ = s;
  return;
}
