#include <string>
#include <chrono>
#include <thread>
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
using K3::Codec;
using K3::CodecFormat;
using K3::Address;
using K3::make_address;
using K3::Peer;
using K3::Listener;
using K3::NetworkManager;
using K3::ProgramContext;
using K3::PackedValue;
using K3::TNativeValue;
using K3::NativeValue;
using K3::StringPackedValue;
using K3::MessageHeader;
using K3::unit_t;
using std::unique_ptr;
using K3::Dispatcher;
using K3::TriggerID;

class EngineTest : public ::testing::Test {
 public:
  EngineTest() {
    std::string config1 = "{me: [127.0.0.1, 30000]}";
    std::string config2 = "{me: [127.0.0.1, 40000]}";
    vector<std::string> configs;
    configs.push_back(config1);
    configs.push_back(config2);

    addr1_ = make_address("127.0.0.1", 30000);
    addr2_ = make_address("127.0.0.1", 40000);
    external_addr_ = make_address("127.0.0.1", 50000);

    peer_configs_ = K3::Options(configs, 0, "", false, true);
  }

  ~EngineTest() {}

  Engine engine_;

  K3::Options peer_configs_;
  Address addr1_;
  Address addr2_;
  Address external_addr_;
};

// A Dummy Context implementation, for simple tests.
// TODO(jbw) move to separate file
class DummyState {
 public:
  int my_int_ = 0;
  std::string my_string_ = "";
};

class DummyContext : public ProgramContext {
 public:
  explicit DummyContext(Engine& e);
  virtual unique_ptr<Dispatcher> __getDispatcher(unique_ptr<NativeValue>, TriggerID trig);
  virtual unique_ptr<Dispatcher> __getDispatcher(unique_ptr<PackedValue>, TriggerID trig);
  virtual void __patch(const YAML::Node& node);
  virtual unit_t processRole(const unit_t&);
  void intTrigger(int i);
  void stringTrigger(std::string s);
  Address me;
  std::string role;
  shared_ptr<DummyState> state_;
};

namespace YAML {
template <>
struct convert<DummyContext> {
 public:
  static Node encode(const DummyContext& context) {
    Node _node;
    _node["me"] = convert<K3::Address>::encode(context.me);
    _node["role"] = convert<std::string>::encode(context.role);
    _node["my_int"] = convert<int>::encode(context.state_->my_int_);
    _node["my_string"] =
        convert<std::string>::encode(context.state_->my_string_);
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
      context.state_->my_int_ = node["my_int"].as<int>();
    }
    if (node["my_string"]) {
      context.state_->my_string_ = node["my_string"].as<std::string>();
    }
    return true;
  }
};
}  // namespace YAML

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




TEST(Map, base_string) {
  K3::base_string k = "hello!";
  K3::Map<R_key_value<K3::base_string, int>> m;
  m.insert(R_key_value<K3::base_string, int> {k, 1});

  std::tuple<K3::base_string, K3::base_string> t = std::make_tuple("hello", "hello");
  auto f = std::hash<std::tuple<K3::base_string, K3::base_string>>();
  size_t s = f(t);
}

TEST_F(EngineTest, LocalSends) {
  engine_.run<DummyContext>(peer_configs_);

  for (int i = 0; i < 100; i++) {
    shared_ptr<Codec> codec = Codec::getCodec<int>(K3_INTERNAL_FORMAT);
    if (i % 2 == 0) {
      MessageHeader h(addr2_, addr1_, 1);
      engine_.send(h, make_unique<TNativeValue<int>>(i), codec);
    } else {
      MessageHeader h(addr1_, addr2_, 1);
      engine_.send(h, make_unique<TNativeValue<int>>(i), codec);
    }
  }

  engine_.stop();
  engine_.join();

  auto peer1 = engine_.getPeer(addr1_);
  auto peer2 = engine_.getPeer(addr2_);
  auto dc1 = std::dynamic_pointer_cast<DummyContext>(peer1->getContext());
  auto dc2 = std::dynamic_pointer_cast<DummyContext>(peer2->getContext());
  ASSERT_EQ(98, dc1->state_->my_int_);
  ASSERT_EQ(99, dc2->state_->my_int_);
}

TEST_F(EngineTest, NetworkSends) {
  engine_.toggleLocalSends(false);
  engine_.run<DummyContext>(peer_configs_);
  while (!engine_.running()) continue;

  auto peer1 = engine_.getPeer(addr1_);
  auto peer2 = engine_.getPeer(addr2_);
  auto dc1 = std::dynamic_pointer_cast<DummyContext>(peer1->getContext());
  auto dc2 = std::dynamic_pointer_cast<DummyContext>(peer2->getContext());

  for (int i = 0; i < 100; i++) {
    shared_ptr<Codec> codec = Codec::getCodec<int>(K3_INTERNAL_FORMAT);
    if (i % 2 == 0) {
      MessageHeader h(addr2_, addr1_, 1);
      engine_.send(h, make_unique<TNativeValue<int>>(i), codec);
    } else {
      MessageHeader h(addr1_, addr2_, 1);
      engine_.send(h, make_unique<TNativeValue<int>>(i), codec);
    }
  }

  for (int retries = 1000; retries > 0; retries--) {
    if (dc1->state_->my_int_ == 98 && dc2->state_->my_int_ == 99) {
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(1));

    ASSERT_NE(1, retries);
  }

  engine_.stop();
  engine_.join();

  ASSERT_EQ(98, dc1->state_->my_int_);
  ASSERT_EQ(99, dc2->state_->my_int_);
}

TEST_F(EngineTest, ExternalMessages) {
  engine_.run<DummyContext>(peer_configs_);
  while (!engine_.running()) continue;

  auto peer1 = engine_.getPeer(addr1_);
  auto dc1 = std::dynamic_pointer_cast<DummyContext>(peer1->getContext());

  auto& mgr = engine_.getNetworkManager();
  mgr.listenExternal(peer1, external_addr_, 1, CodecFormat::BoostBinary);

  shared_ptr<Codec> codec = Codec::getCodec<int>(CodecFormat::BoostBinary);
  for (int i = 0; i < 100; i++) {
    auto val = make_unique<TNativeValue<int>>(i);
    mgr.sendExternal(external_addr_, codec->pack(*val));
  }

  for (int retries = 1000; retries > 0; retries--) {
    if (dc1->state_->my_int_ == 99) {
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(1));

    ASSERT_NE(1, retries);
  }

  engine_.stop();
  engine_.join();
}
