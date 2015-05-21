#include <list>
#include <string>
#include <chrono>
#include <thread>

#include "gtest/gtest.h"

#include "Codec.hpp"
#include "Common.hpp"
#include "Engine.hpp"
#include "Listener.hpp"
#include "NetworkManager.hpp"
#include "Peer.hpp"
#include "ProgramContext.hpp"

using std::list;

TEST(Engine, Termination) {
  list<std::string> peer_configs;
  std::string config1 = "{me: [127.0.0.1, 30000]}";
  std::string config2 = "{me: [127.0.0.1, 40000]}";
  peer_configs.push_back(config1);
  peer_configs.push_back(config2);

  Engine& engine = Engine::getInstance();
  auto factory = make_shared<ContextFactory>([] () {
    return make_shared<DummyContext>();
  });

  engine.run(peer_configs, factory);
  engine.stop();
  engine.join();
}

TEST(Engine, LocalSends) {
  list<std::string> peer_configs;
  Address a1 = make_address("127.0.0.1", 30000);
  Address a2 = make_address("127.0.0.1", 40000);
  std::string config1 = "{me: [127.0.0.1, 30000]}";
  std::string config2 = "{me: [127.0.0.1, 40000]}";
  peer_configs.push_back(config1);
  peer_configs.push_back(config2);
  Engine& engine = Engine::getInstance();
  auto factory = make_shared<ContextFactory>([] () {
    return make_shared<DummyContext>();
  });

  engine.run(peer_configs, factory);

  for (int i = 0; i < 100; i++) {
    shared_ptr<Codec> codec = Codec::getCodec<int>(CodecFormat::BoostBinary);
    if (i % 2 == 0) {
      MessageHeader h(a2, a1, 1);
      engine.send(h, make_shared<TNativeValue<int>>(i), codec);
    } else {
      MessageHeader h(a1, a2, 1);
      engine.send(h, make_shared<TNativeValue<int>>(i), codec);
    }
  }

  engine.stop();
  engine.join();

  auto  peer1 = engine.getPeer(a1);
  auto peer2 = engine.getPeer(a2);
  auto dc1 = std::dynamic_pointer_cast<DummyContext>(peer1->getContext());
  auto dc2 = std::dynamic_pointer_cast<DummyContext>(peer2->getContext());
  ASSERT_EQ(98, dc1->state_->my_int_);
  ASSERT_EQ(99, dc2->state_->my_int_);
}

TEST(Engine, ProcessRole) {
  Address a1 = make_address("127.0.0.1", 30000);
  list<std::string> peer_configs;
  std::string config1 = "{me: [127.0.0.1, 30000], role: int}";
  peer_configs.push_back(config1);
  Engine& engine = Engine::getInstance();
  auto factory = make_shared<ContextFactory>([] () {
    return make_shared<DummyContext>();
  });

  engine.run(peer_configs, factory);
  engine.stop();
  engine.join();

  auto peer1 = engine.getPeer(a1);
  auto dc1 = std::dynamic_pointer_cast<DummyContext>(peer1->getContext());
  ASSERT_EQ(5, dc1->state_->my_int_);
}

TEST(NetworkManager, ConnectAccept) {
  Address a1 = make_address("127.0.0.1", 30000);
  list<std::string> peer_configs;
  std::string config1 = "{me: [127.0.0.1, 30000]}";
  peer_configs.push_back(config1);
  Engine& engine = Engine::getInstance();
  auto factory = make_shared<ContextFactory>([] () {
    return make_shared<DummyContext>();
  });

  engine.run(peer_configs, factory);
  NetworkManager::getInstance().connectInternal(a1);
  shared_ptr<Listener> l = NetworkManager::getInstance().getListener(a1);
  for (int retries = 1000; retries > 0; retries--) {
    if (l->numConnections() > 0) {
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
    ASSERT_NE(1, retries);
  }
  engine.stop();
  engine.join();
}

TEST(Engine, NetworkSend) {
  Address a1 = make_address("127.0.0.1", 30000);
  list<std::string> peer_configs;
  std::string config1 = "{me: [127.0.0.1, 30000], role: int}";
  peer_configs.push_back(config1);
  Engine& engine = Engine::getInstance();
  auto factory = make_shared<ContextFactory>([] () {
    return make_shared<DummyContext>();
  });

  engine.toggleLocalSend(false);
  engine.run(peer_configs, factory);

  while (!engine.running()) continue;

  auto peer1 = engine.getPeer(a1);
  auto dc1 = std::dynamic_pointer_cast<DummyContext>(peer1->getContext());

  for (int retries = 1000; retries > 0; retries--) {
    if (dc1->state_->my_int_ ==  5) {
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(1));

    ASSERT_NE(1, retries);
  }

  engine.stop();
  engine.join();
}

TEST(Engine, NetworkSends) {
  list<std::string> peer_configs;
  Address a1 = make_address("127.0.0.1", 30000);
  Address a2 = make_address("127.0.0.1", 40000);
  std::string config1 = "{me: [127.0.0.1, 30000]}";
  std::string config2 = "{me: [127.0.0.1, 40000]}";
  peer_configs.push_back(config1);
  peer_configs.push_back(config2);
  Engine& engine = Engine::getInstance();
  auto factory = make_shared<ContextFactory>([] () {
    return make_shared<DummyContext>();
  });

  engine.toggleLocalSend(false);
  engine.run(peer_configs, factory);
  while (!engine.running()) continue;

  auto  peer1 = engine.getPeer(a1);
  auto peer2 = engine.getPeer(a2);
  auto dc1 = std::dynamic_pointer_cast<DummyContext>(peer1->getContext());
  auto dc2 = std::dynamic_pointer_cast<DummyContext>(peer2->getContext());

  for (int i = 0; i < 100; i++) {
    shared_ptr<Codec> codec = Codec::getCodec<int>(CodecFormat::BoostBinary);
    if (i % 2 == 0) {
      MessageHeader h(a2, a1, 1);
      engine.send(h, make_shared<TNativeValue<int>>(i), codec);
    } else {
      MessageHeader h(a1, a2, 1);
      engine.send(h, make_shared<TNativeValue<int>>(i), codec);
    }
  }

  for (int retries = 1000; retries > 0; retries--) {
    if (dc1->state_->my_int_ ==  98 && dc2->state_->my_int_ == 99) {
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(1));

    ASSERT_NE(1, retries);
  }

  engine.stop();
  engine.join();

  ASSERT_EQ(98, dc1->state_->my_int_);
  ASSERT_EQ(99, dc2->state_->my_int_);
}
