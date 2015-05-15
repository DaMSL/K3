// Test the DummyContext's ability to dispatch both Packed and Native values

#include <memory>
#include <string>

#include "gtest/gtest.h"

#include "Codec.hpp"
#include "Message.hpp"
#include "ProgramContext.hpp"
#include "Value.hpp"

using std::unique_ptr;
using std::make_unique;
using std::shared_ptr;
using std::make_shared;

// Dispatch Messages containing NativeValues
// Ensure that the state maintained by the DummyContext is updated properly
TEST(DummyContext, DispatchNative) {
  shared_ptr<DummyState> s1 = make_shared<DummyState>();
  DummyContext context(s1);
  Address me = make_address("127.0.0.1", 30000);

  int i = 100;
  unique_ptr<Value> v = make_unique<TNativeValue<int>>(i);
  auto m1 = make_unique<Message>(me, me, 1, std::move(v));

  context.dispatch(std::move(m1));
  ASSERT_EQ(i, context.state_->my_int_);

  std::string s = "Hello!";
  unique_ptr<NativeValue> v2 = make_unique<TNativeValue<std::string>>(s);
  auto m2 = make_unique<Message>(me, me, 2, std::move(v2));

  context.dispatch(std::move(m2));
  ASSERT_EQ(s, context.state_->my_string_);
}

// Dispatch Messages containing PackedValues
// Ensure that the state maintained by the DummyContext is updated properly
TEST(DummyContext, DispatchPacked) {
  unique_ptr<Codec> cdec = make_unique<BoostCodec<int>>();
  shared_ptr<DummyState> s1 = make_shared<DummyState>();
  DummyContext context(s1);
  Address me = make_address("127.0.0.1", 30000);

  int i = 100;
  unique_ptr<NativeValue> v = make_unique<TNativeValue<int>>(i);
  unique_ptr<Value> b = cdec->pack(*v);

  auto m = make_unique<Message>(me, me, 1, std::move(b));

  context.dispatch(std::move(m));
  ASSERT_EQ(i, context.state_->my_int_);

  unique_ptr<Codec> cdec2 = make_unique<BoostCodec<std::string>>();
  std::string s = "Hello!";
  unique_ptr<NativeValue> v2 = make_unique<TNativeValue<std::string>>(s);
  unique_ptr<Value> b2 = cdec2->pack(*v2);
  auto m2 = make_unique<Message>(me, me, 2, std::move(b2));

  context.dispatch(std::move(m2));
  ASSERT_EQ(s, context.state_->my_string_);
}
