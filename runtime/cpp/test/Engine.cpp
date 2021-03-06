#include <string>
#include <chrono>
#include <thread>
#include <mutex>
#include <vector>
#include <tuple>

#include "gtest/gtest.h"

#include "Common.hpp"
#include "core/Peer.hpp"
#include "core/ProgramContext.hpp"
#include "core/Engine.hpp"
#include "network/Listener.hpp"
#include "network/NetworkManager.hpp"
#include "serialization/Codec.hpp"
#include "collections/Map.hpp"
#include "types/BaseString.hpp"
#include "Hash.hpp"

using std::shared_ptr;
using std::make_shared;
using std::make_unique;
using std::vector;
using std::string;
using std::tuple;
using K3::Engine;
using K3::Options;
using K3::CodecFormat;
using K3::Address;
using K3::make_address;
using K3::Peer;
using K3::unpack;
using K3::pack;
using K3::Listener;
using K3::NetworkManager;
using K3::ProgramContext;
using K3::PackedValue;
using K3::TNativeValue;
using K3::NativeValue;
using K3::StringPackedValue;
using K3::unit_t;
using std::unique_ptr;
using K3::Dispatcher;
using K3::TriggerID;
using K3::EndOfProgramException;

Options getOptions(bool local_sends) {
  std::string config1 =
      "{me: [127.0.0.1, 30000], buddy: [127.0.0.1, 40000], role: main}";
  std::string config2 =
      "{me: [127.0.0.1, 40000], buddy: [127.0.0.1, 30000], role: main}";

  vector<std::string> configs;
  configs.push_back(config1);
  configs.push_back(config2);
  Options o;
  o.peer_strs_ = configs;
  o.log_level_ = 0;
  o.local_sends_enabled_ = local_sends;
  return o;
}

class EngineTest : public ::testing::Test {
 public:
  EngineTest() {
    ProgramContext::__trigger_names_[0] = "null";
    ProgramContext::__trigger_names_[1] = "IntTrigger";
    ProgramContext::__trigger_names_[2] = "StringTrigger";
    ProgramContext::__trigger_names_[3] = "MainTrigger";
    ProgramContext::__trigger_names_[4] = "StopTrigger";

    addr1_ = make_address("127.0.0.1", 30000);
    addr2_ = make_address("127.0.0.1", 40000);
    external_addr_ = make_address("127.0.0.1", 50000);
  }

  ~EngineTest() {}

  Address addr1_;
  Address addr2_;
  Address external_addr_;
};

class DummyContext : public ProgramContext {
 public:
  explicit DummyContext(Engine& e);
  unique_ptr<Dispatcher> __getDispatcher(unique_ptr<NativeValue>,
                                                 TriggerID trig);
  unique_ptr<Dispatcher> __getDispatcher(unique_ptr<PackedValue>,
                                                 TriggerID trig);
  virtual void __patch(const YAML::Node& node);
  unit_t processRole(const unit_t&) const;
  void stopTrigger(unit_t);
  void mainTrigger(unit_t);
  void intTrigger(int i);
  void stringTrigger(std::string s);
  unit_t initDecls(unit_t) { return unit_t{}; }
  static void print(const std::string& s) {
    std::lock_guard<std::mutex> lock(mutex_);
    std::cout << s << std::endl;
  }

  Address me;
  std::string role;
  int my_int_;
  string my_string_;
  static std::mutex mutex_;
};

std::mutex DummyContext::mutex_;

namespace YAML {
template <>
struct convert<DummyContext> {
 public:
  static Node encode(const DummyContext& context) {
    Node _node;
    _node["me"] = convert<K3::Address>::encode(context.me);
    _node["role"] = convert<std::string>::encode(context.role);
    _node["my_int"] = convert<int>::encode(context.my_int_);
    _node["my_string"] = convert<std::string>::encode(context.my_string_);
    return _node;
  }
  static bool decode(const Node& node, DummyContext& context) {
    if (!node.IsMap()) {
      return false;
    }
    if (node["me"]) {
      context.me = node["me"].as<K3::Address>();
    }
    if (node["role"]) {
      context.role = node["role"].as<std::string>();
    }
    if (node["my_int"]) {
      context.my_int_ = node["my_int"].as<int>();
    }
    if (node["my_string"]) {
      context.my_string_ = node["my_string"].as<std::string>();
    }
    return true;
  }
};
}  // namespace YAML

// Dummy Context Implementation:
class MainTriggerNativeDispatcher : public Dispatcher {
 public:
  MainTriggerNativeDispatcher(DummyContext& c, unique_ptr<NativeValue> val)
      : context_(c) {
    value_ = std::move(val);
  }
  void operator()() { context_.mainTrigger(*value_->as<unit_t>()); }

 protected:
  DummyContext& context_;
  unique_ptr<NativeValue> value_;
};

class MainTriggerPackedDispatcher : public Dispatcher {
 public:
  MainTriggerPackedDispatcher(DummyContext& c, unique_ptr<PackedValue> val)
      : context_(c) {
    value_ = std::move(val);
  }

  void operator()() {
    auto native = unpack<unit_t>(std::move(value_));
    context_.mainTrigger(*native);
  }

 protected:
  DummyContext& context_;
  unique_ptr<PackedValue> value_;
};

class StopTriggerNativeDispatcher : public Dispatcher {
 public:
  StopTriggerNativeDispatcher(DummyContext& c, unique_ptr<NativeValue> val)
      : context_(c) {
    value_ = std::move(val);
  }
  void operator()() { context_.stopTrigger(*value_->as<unit_t>()); }

 protected:
  DummyContext& context_;
  unique_ptr<NativeValue> value_;
};

class StopTriggerPackedDispatcher : public Dispatcher {
 public:
  StopTriggerPackedDispatcher(DummyContext& c, unique_ptr<PackedValue> val)
      : context_(c) {
    value_ = std::move(val);
  }

  void operator()() {
    auto native = unpack<unit_t>(std::move(value_));
    context_.stopTrigger(*native);
  }

 protected:
  DummyContext& context_;
  unique_ptr<PackedValue> value_;
};

class IntTriggerNativeDispatcher : public Dispatcher {
 public:
  IntTriggerNativeDispatcher(DummyContext& c, unique_ptr<NativeValue> val)
      : context_(c) {
    value_ = std::move(val);
  }

  void operator()() { context_.intTrigger(*value_->as<int>()); }

 protected:
  DummyContext& context_;
  unique_ptr<NativeValue> value_;
};

class IntTriggerPackedDispatcher : public Dispatcher {
 public:
  IntTriggerPackedDispatcher(DummyContext& c, unique_ptr<PackedValue> val)
      : context_(c) {
    value_ = std::move(val);
  }

  void operator()() {
    auto native = unpack<int>(std::move(value_));
    context_.intTrigger(*native);
  }

 protected:
  DummyContext& context_;
  unique_ptr<PackedValue> value_;
};

class StringTriggerNativeDispatcher : public Dispatcher {
 public:
  StringTriggerNativeDispatcher(DummyContext& c, unique_ptr<NativeValue> val)
      : context_(c) {
    value_ = std::move(val);
  }

  void operator()() { context_.stringTrigger(*value_->as<string>()); }

 protected:
  DummyContext& context_;
  unique_ptr<NativeValue> value_;
};

class StringTriggerPackedDispatcher : public Dispatcher {
 public:
  StringTriggerPackedDispatcher(DummyContext& c, unique_ptr<PackedValue> val)
      : context_(c) {
    value_ = std::move(val);
  }

  void operator()() {
    auto native = unpack<string>(std::move(value_));
    context_.stringTrigger(*native);
  }

 protected:
  DummyContext& context_;
  unique_ptr<PackedValue> value_;
};

DummyContext::DummyContext(Engine& e) : ProgramContext(e) {}

unique_ptr<Dispatcher> DummyContext::__getDispatcher(unique_ptr<NativeValue> nv,
                                                     TriggerID t) {
  if (t == 1) {
    return make_unique<IntTriggerNativeDispatcher>(*this, std::move(nv));
  } else if (t == 2) {
    return make_unique<StringTriggerNativeDispatcher>(*this, std::move(nv));
  } else if (t == 3) {
    return make_unique<MainTriggerNativeDispatcher>(*this, std::move(nv));
  } else if (t == 4) {
    return make_unique<StopTriggerNativeDispatcher>(*this, std::move(nv));
  } else {
    throw std::runtime_error("Invalid trigger ID");
  }
}

unique_ptr<Dispatcher> DummyContext::__getDispatcher(unique_ptr<PackedValue> pv,
                                                     TriggerID t) {
  if (t == 1) {
    return make_unique<IntTriggerPackedDispatcher>(*this, std::move(pv));
  } else if (t == 2) {
    return make_unique<StringTriggerPackedDispatcher>(*this, std::move(pv));
  } else if (t == 3) {
    return make_unique<MainTriggerPackedDispatcher>(*this, std::move(pv));
  } else if (t == 4) {
    return make_unique<StopTriggerPackedDispatcher>(*this, std::move(pv));
  } else {
    throw std::runtime_error("Invalid trigger ID");
  }
}

void DummyContext::__patch(const YAML::Node& node) {
  YAML::convert<DummyContext>::decode(node, *this);
}

unit_t DummyContext::processRole(const unit_t&) const {
  if (role == "main") {
    __engine_.send<unit_t>(me, me, 3, unit_t{});
  }
  return unit_t{};
}

void DummyContext::intTrigger(int i) {
  my_int_ = i;
  return;
}

void DummyContext::stringTrigger(std::string s) {
  my_string_ = s;
  return;
}

void DummyContext::mainTrigger(unit_t) {
  for (int i = 0; i < 100; i++) {
    __engine_.send<int>(me, me, 1, i);
  }
  __engine_.send<unit_t>(me, me, 4, unit_t{});
  return;
}

void DummyContext::stopTrigger(unit_t) { throw EndOfProgramException(); }
TEST(BaseString, one) {
  K3::base_string k = "hi test!";
  std::cout << k << std::endl;
}


TEST(Map, base_string) {
  K3::base_string k = "hello!";
  K3::Map<R_key_value<K3::base_string, int>> m;
  m.insert(R_key_value<K3::base_string, int>{k, 1});

  std::tuple<K3::base_string, K3::base_string> t =
      std::make_tuple("hello", "hello");
  auto f = std::hash<std::tuple<K3::base_string, K3::base_string>>();
  size_t s = f(t);
}

TEST_F(EngineTest, LocalSends) {
  Engine engine_(getOptions(true));
  engine_.run<DummyContext>();
  engine_.join();

  auto& c1 = engine_.getContext(addr1_);
  auto& c2 = engine_.getContext(addr2_);
  auto& dc1 = dynamic_cast<DummyContext&>(c1);
  auto& dc2 = dynamic_cast<DummyContext&>(c2);

  ASSERT_EQ(99, dc1.my_int_);
  ASSERT_EQ(99, dc2.my_int_);
}

TEST_F(EngineTest, NetworkSends) {
  Engine engine_(getOptions(false));
  engine_.run<DummyContext>();
  std::cout << "Running" << std::endl;
  engine_.join();

  auto& dc1 =
      dynamic_cast<DummyContext&>(engine_.getContext(addr1_));
  auto& dc2 =
      dynamic_cast<DummyContext&>(engine_.getContext(addr2_));

  ASSERT_EQ(99, dc1.my_int_);
  ASSERT_EQ(99, dc2.my_int_);
}
