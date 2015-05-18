#include <list>

#include "gtest/gtest.h"

#include "Codec.hpp"
#include "Common.hpp"
#include "Engine.hpp"
#include "ProgramContext.hpp"

using std::list;

TEST(Engine, Termination) {
  list<Address> peer_addrs;
  Address a1 = make_address("127.0.0.1", 30000);
  Address a2 = make_address("127.0.0.1", 40000);
  peer_addrs.push_back(a1);
  peer_addrs.push_back(a2);

  Engine<DummyContext>& engine = Engine<DummyContext>::getInstance();
  engine.initialize(peer_addrs);
  engine.run();
  unique_ptr<Value> v1 = make_unique<SentinelValue>();
  unique_ptr<Value> v2 = make_unique<SentinelValue>();
  engine.send(make_unique<Message>(a1, a2, -1, std::move(v1)));
  engine.send(make_unique<Message>(a2, a1, -1, std::move(v2)));
  engine.join();
  engine.cleanup();
}

TEST(Engine, LocalSends) {
  unique_ptr<Codec> cdec = make_unique<BoostCodec<int>>();
  list<Address> peer_addrs;
  Address a1 = make_address("127.0.0.1", 30000);
  Address a2 = make_address("127.0.0.1", 40000);
  peer_addrs.push_back(a1);
  peer_addrs.push_back(a2);
  Engine<DummyContext>& engine = Engine<DummyContext>::getInstance();
  engine.initialize(peer_addrs);
  engine.run();

  for (int i = 0; i < 100; i++) {
    unique_ptr<Message> m;
    unique_ptr<Value> v;
    if (i % 3 == 0 || i % 4 == 0) {
      v = make_unique<TNativeValue<int>>(i);
    } else {
      unique_ptr<NativeValue> nv = make_unique<TNativeValue<int>>(i);
      v = cdec->pack(*nv);
    }

    if (i % 2 == 0) {
      m = make_unique<Message>(a2, a1, 1, std::move(v));
    } else {
      m = make_unique<Message>(a1, a2, 1, std::move(v));
    }
    engine.send(std::move(m));
  }

  unique_ptr<Value> v1 = make_unique<SentinelValue>();
  unique_ptr<Value> v2 = make_unique<SentinelValue>();
  engine.send(make_unique<Message>(a1, a2, -1, std::move(v1)));
  engine.send(make_unique<Message>(a2, a1, -1, std::move(v2)));
  engine.join();

  Peer<DummyContext>* peer1 = engine.getPeer(a1);
  Peer<DummyContext>* peer2 = engine.getPeer(a2);
  DummyContext* dc1 = dynamic_cast<DummyContext*>(peer1->context());
  DummyContext* dc2 = dynamic_cast<DummyContext*>(peer2->context());
  ASSERT_EQ(98, dc1->state_->my_int_);
  ASSERT_EQ(99, dc2->state_->my_int_);
}
