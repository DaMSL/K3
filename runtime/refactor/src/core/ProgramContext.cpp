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

//void ProgramContext::__dispatch(SentinelValue* sv) {
//  throw EndOfProgramException();
//}

class SentinelDispatcher : public Dispatcher {
  void operator()() {
    throw EndOfProgramException();
  }
};

unique_ptr<Dispatcher> ProgramContext::__getDispatcher(unique_ptr<SentinelValue>) {
  return make_unique<SentinelDispatcher>(); 
}

void ProgramContext::__patch(const YAML::Node& node) {
  return;
}

unit_t ProgramContext::initDecls(unit_t) { return unit_t{}; }

unit_t ProgramContext::processRole(const unit_t&) { return unit_t{}; }

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
class IntTriggerNativeDispatcher : public Dispatcher {
  public:
    IntTriggerNativeDispatcher(DummyContext& c, unique_ptr<NativeValue> val) : context_(c) {
      value_ = std::move(val);
    }

    void operator()() {
      context_.intTrigger(*value_->as<int>());
    }

  protected:
    DummyContext& context_;
    unique_ptr<NativeValue> value_;
};

class IntTriggerPackedDispatcher : public Dispatcher {
  public:
    IntTriggerPackedDispatcher(DummyContext& c, unique_ptr<PackedValue> val) : context_(c) {
      value_ = std::move(val);
      codec_ = Codec::getCodec<int>(value_->format());
    }

    void operator()() {
      auto native = codec_->unpack(*value_);
      context_.intTrigger(*native->as<int>());
    }

  protected:
    DummyContext& context_;
    unique_ptr<PackedValue> value_;
    shared_ptr<Codec> codec_;
};

class StringTriggerNativeDispatcher : public Dispatcher {
  public:
    StringTriggerNativeDispatcher(DummyContext& c, unique_ptr<NativeValue> val) : context_(c) {
      value_ = std::move(val);
    }

    void operator()() {
      context_.stringTrigger(*value_->as<string>());
    }

  protected:
    DummyContext& context_;
    unique_ptr<NativeValue> value_;
};

class StringTriggerPackedDispatcher : public Dispatcher {
  public:
    StringTriggerPackedDispatcher(DummyContext& c, unique_ptr<PackedValue> val) : context_(c) {

      value_ = std::move(val);
      codec_ = Codec::getCodec<string>(value_->format());
    }

    void operator()() {
      auto native = codec_->unpack(*value_);
      context_.stringTrigger(*native->as<string>());
    }

  protected:
    DummyContext& context_;
    unique_ptr<PackedValue> value_;
    shared_ptr<Codec> codec_;
};

DummyContext::DummyContext(Engine& e) : ProgramContext(e) {
  state_ = make_shared<DummyState>();
}

unique_ptr<Dispatcher> DummyContext::__getDispatcher(unique_ptr<NativeValue> nv, TriggerID t) {
  if (t == 1) {
    return make_unique<IntTriggerNativeDispatcher>(*this, std::move(nv));
  } else if (t == 2) {
    return make_unique<StringTriggerNativeDispatcher>(*this, std::move(nv));
  } else {
    throw std::runtime_error("Invalid trigger ID");
  }
}

unique_ptr<Dispatcher> DummyContext::__getDispatcher(unique_ptr<PackedValue> pv, TriggerID t) {
  if (t == 1) {
    return make_unique<IntTriggerPackedDispatcher>(*this, std::move(pv));
  } else if (t == 2) {
    return make_unique<StringTriggerPackedDispatcher>(*this, std::move(pv));
  } else {
    throw std::runtime_error("Ipvalid trigger ID");
  }
}

//void DummyContext::__dispatch(NativeValue* nv, TriggerID t, const Address& addr) {
//  if (t == 1) {
//    int i = *nv->as<int>();
//    intTrigger(i);
//  } else if (t == 2) {
//    std::string s = std::move(*nv->as<std::string>());
//    stringTrigger(s);
//  } else {
//    throw std::runtime_error("Invalid trigger ID");
//  }
//  return;
//}

//void DummyContext::__dispatch(PackedValue* pv, TriggerID t, const Address& addr) {
//  shared_ptr<NativeValue> nv;
//  shared_ptr<Codec> codec;
//  if (t == 1) {
//    codec = Codec::getCodec<int>(pv->format());
//  } else if (t == 2) {
//    codec = Codec::getCodec<std::string>(pv->format());
//  } else {
//    throw std::runtime_error("Invalid trigger ID");
//  }
//  nv = codec->unpack(*pv);
//  return __dispatch(nv.get(), t, addr);
//}

void DummyContext::__patch(const YAML::Node& node) {
  YAML::convert<DummyContext>::decode(node, *this);
}

unit_t DummyContext::processRole(const unit_t&) {
  if (role == "int") {
    MessageHeader h(me, me, 1);
    static shared_ptr<Codec> codec = Codec::getCodec<int>(__internal_format_);
    __engine_.send(h, make_unique<TNativeValue<int>>(5), codec);
  } else if (role == "string") {
    MessageHeader h(me, me, 2);
    static shared_ptr<Codec> codec =
        Codec::getCodec<std::string>(__internal_format_);
    __engine_.send(h, make_unique<TNativeValue<std::string>>("hi"), codec);
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
