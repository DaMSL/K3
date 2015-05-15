// Test that a peer can dispatch messages
// and run its event loop until a sentinel is dispatched

#include <memory>

#include "gtest/gtest.h"

#include "Codec.hpp"
#include "Common.hpp"
#include "Peer.hpp"
#include "ProgramContext.hpp"

using std::unique_ptr;
using std::make_unique;

// Run the peers event loop using only native values
TEST(Peer, SimpleLoop) {
  Address me = make_address("127.0.0.1", 30000);
  shared_ptr<DummyState> s1 = make_shared<DummyState>();
  Peer peer(make_unique<DummyContext>(s1));
  peer.run();

  for (int i = 0; i < 100; i++) {
    unique_ptr<Value> v = make_unique<TNativeValue<int>>(i);
    peer.enqueue(make_unique<Message>(me, me, 1, std::move(v)));
  }

  unique_ptr<Value> v = make_unique<SentinelValue>();
  peer.enqueue(make_unique<Message>(me, me, -1, std::move(v)));

  peer.join();
  ASSERT_EQ(99, s1->my_int_);
}

// Run the peers event loop using a combination of packed and native values
TEST(Peer, NativeAndPackedLoop) {
  unique_ptr<Codec> cdec = make_unique<BoostCodec<int>>();
  Address me = make_address("127.0.0.1", 30000);
  shared_ptr<DummyState> s1 = make_shared<DummyState>();
  Peer peer(make_unique<DummyContext>(s1));
  peer.run();

  for (int i = 0; i < 100; i++) {
    unique_ptr<Value> v;
    if (i % 2 == 0) {
      v = make_unique<TNativeValue<int>>(i);
    } else {
      unique_ptr<NativeValue> nv = make_unique<TNativeValue<int>>(i);
      v = cdec->pack(*nv);
    }
    peer.enqueue(make_unique<Message>(me, me, 1, std::move(v)));
  }

  unique_ptr<Value> v = make_unique<SentinelValue>();
  peer.enqueue(make_unique<Message>(me, me, -1, std::move(v)));

  peer.join();
  ASSERT_EQ(99, s1->my_int_);
}

// Run two peers on seperate threads
TEST(Peer, MultiThreaded) {
  unique_ptr<Codec> cdec = make_unique<BoostCodec<int>>();
  Address me = make_address("127.0.0.1", 30000);
  shared_ptr<DummyState> s1 = make_shared<DummyState>();
  shared_ptr<DummyState> s2 = make_shared<DummyState>();
  Peer peer1(make_unique<DummyContext>(s1));
  Peer peer2(make_unique<DummyContext>(s2));
  peer1.run();
  peer2.run();

  for (int i = 0; i < 100; i++) {
    unique_ptr<Value> v;
    if (i % 3 == 0) {
      v = make_unique<TNativeValue<int>>(i);
    } else {
      unique_ptr<NativeValue> nv = make_unique<TNativeValue<int>>(i);
      v = cdec->pack(*nv);
    }

    if (i % 2 == 0) {
      peer1.enqueue(make_unique<Message>(me, me, 1, std::move(v)));
    } else {
      peer2.enqueue(make_unique<Message>(me, me, 1, std::move(v)));
    }
  }

  unique_ptr<Value> v = make_unique<SentinelValue>();
  peer1.enqueue(make_unique<Message>(me, me, -1, std::move(v)));
  unique_ptr<Value> v2 = make_unique<SentinelValue>();
  peer2.enqueue(make_unique<Message>(me, me, -1, std::move(v2)));

  peer1.join();
  peer2.join();
  ASSERT_EQ(98, s1->my_int_);
  ASSERT_EQ(99, s2->my_int_);
}
