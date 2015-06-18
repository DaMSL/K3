#include <iostream>
#include <memory>
#include <string>
#include <map>

#include "Common.hpp"
#include "core/Engine.hpp"
#include "core/ProgramContext.hpp"
#include "serialization/Codec.hpp"
#include "types/Message.hpp"
#include "types/Value.hpp"

namespace K3 {

ProgramContext::ProgramContext(Engine& e) : StandardBuiltins(e), __engine_(e) {}

void ProgramContext::__dispatch(SentinelValue* sv) {
  throw EndOfProgramException();
}

void ProgramContext::__patch(const YAML::Node& node) {
  return;
}

unit_t ProgramContext::initDecls(unit_t) { return unit_t{}; }

unit_t ProgramContext::processRole(unit_t) { return unit_t{}; }

map<string, string> ProgramContext::__prettify() {
  return map<string, string>();
}

map<string, string> ProgramContext::__jsonify() {
  return map<string, string>();
}

string ProgramContext::__triggerName(int trig) {
  auto it = ProgramContext::__trigger_names_.find(trig);
  std::string s = (it != ProgramContext::__trigger_names_.end())
                      ? it->second
                      : "{Undefined Trigger}";
  return s;
}

string ProgramContext::__jsonifyMessage(const Message& m) {
  return "";
}

std::map<TriggerID, string> ProgramContext::__trigger_names_;

// Dummy Context Implementation:

DummyContext::DummyContext(Engine& e) : ProgramContext(e) {
  state_ = make_shared<DummyState>();
}

void DummyContext::__dispatch(NativeValue* nv, TriggerID t, const Address& addr) {
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

void DummyContext::__dispatch(PackedValue* pv, TriggerID t, const Address& addr) {
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
  return __dispatch(nv.get(), t, addr);
}

void DummyContext::__patch(const YAML::Node& node) {
  YAML::convert<DummyContext>::decode(node, *this);
}

unit_t DummyContext::processRole(const unit_t&) {
  if (role == "int") {
    MessageHeader h(me, me, 1);
    static shared_ptr<Codec> codec = Codec::getCodec<int>(__internal_format_);
    __engine_.send(h, make_shared<TNativeValue<int>>(5), codec);
  } else if (role == "string") {
    MessageHeader h(me, me, 2);
    static shared_ptr<Codec> codec =
        Codec::getCodec<std::string>(__internal_format_);
    __engine_.send(h, make_shared<TNativeValue<std::string>>("hi"), codec);
  }

  return unit_t{};
}

void DummyContext::intTrigger(int i) {
  state_->my_int_ = i;
  return;
}

void DummyContext::stringTrigger(std::string s) {
  state_->my_string_ = s;
  return;
}

}  // namespace K3
