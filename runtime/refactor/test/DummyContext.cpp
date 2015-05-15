#include <memory>

#include "gtest/gtest.h"

#include "Codec.hpp"
#include "Message.hpp"
#include "ProgramContext.hpp"
#include "Value.hpp"

using std::unique_ptr;
using std::make_unique;
using std::shared_ptr;
using std::make_shared;
TEST(DummyContext, DispatchNative) {
  shared_ptr<DummyState> s1 = make_shared<DummyState>();
  DummyContext context(s1);
  Address me = make_address("127.0.0.1", 30000);

  int i = 100;
  unique_ptr<Value> v = make_unique<TNativeValue<int>>(i);
  Message m1 = Message(me, me, 1, std::move(v));

  context.dispatch(m1);
  ASSERT_EQ(i, context.state_->my_int_);

  std::string s = "Hello!";
  unique_ptr<NativeValue> v2 = make_unique<TNativeValue<std::string>>(s);
  Message m2 = Message(me, me, 2, std::move(v2));

  context.dispatch(m2);
  ASSERT_EQ(s, context.state_->my_string_);
}

TEST(DummyContext, DispatchPacked) {
  unique_ptr<Codec> cdec = make_unique<BoostCodec<int>>();
  shared_ptr<DummyState> s1 = make_shared<DummyState>();
  DummyContext context(s1);
  Address me = make_address("127.0.0.1", 30000);

  int i = 100;
  unique_ptr<NativeValue> v = make_unique<TNativeValue<int>>(i);
  unique_ptr<Value> b = cdec->pack(*v);
  
  Message m = Message(me, me, 1, std::move(b));

  context.dispatch(m);
  ASSERT_EQ(i, context.state_->my_int_);
}
