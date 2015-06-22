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
using K3::DummyContext;
using K3::Value;
using K3::TNativeValue;
using K3::NativeValue;
using K3::StringPackedValue;
using K3::MessageHeader;

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

  auto mgr = engine_.getNetworkManager();
  mgr->listenExternal(peer1, external_addr_, 1, CodecFormat::BoostBinary);

  shared_ptr<Codec> codec = Codec::getCodec<int>(CodecFormat::BoostBinary);
  for (int i = 0; i < 100; i++) {
    auto val = make_unique<TNativeValue<int>>(i);
    mgr->sendExternal(external_addr_, codec->pack(*val));
  }

  for (int retries = 1000; retries > 0; retries--) {
    if (dc1->state_->my_int_ == 99) {
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(1));

    ASSERT_NE(1, retries);
  }

  mgr.reset();
  engine_.stop();
  engine_.join();
}
