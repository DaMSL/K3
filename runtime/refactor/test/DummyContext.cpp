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

// Dispatch NativeValues
// Ensure that the state maintained by the DummyContext is updated properly
TEST(DummyContext, DispatchNative) {
  DummyContext context;
  Address me = make_address("127.0.0.1", 30000);

  int i = 100;
  unique_ptr<NativeValue> v = make_unique<TNativeValue<int>>(i);

  context.dispatch(v.get(), 1);
  ASSERT_EQ(i, context.state_->my_int_);

  std::string s = "Hello!";
  unique_ptr<NativeValue> v2 = make_unique<TNativeValue<std::string>>(s);

  context.dispatch(v2.get(), 2);
  ASSERT_EQ(s, context.state_->my_string_);
}

// Dispatch PackedValues
// Ensure that the state maintained by the DummyContext is updated properly
TEST(DummyContext, DispatchPacked) {
  unique_ptr<Codec> cdec = make_unique<BoostCodec<int>>();
  DummyContext context;
  Address me = make_address("127.0.0.1", 30000);

  int i = 100;
  unique_ptr<NativeValue> v = make_unique<TNativeValue<int>>(i);
  unique_ptr<PackedValue> b = cdec->pack(*v);

  context.dispatch(b.get(), 1);

  unique_ptr<Codec> cdec2 = make_unique<BoostCodec<std::string>>();
  std::string s = "Hello!";
  unique_ptr<NativeValue> v2 = make_unique<TNativeValue<std::string>>(s);
  unique_ptr<PackedValue> b2 = cdec2->pack(*v2);

  context.dispatch(b2.get(), 2);
  ASSERT_EQ(s, context.state_->my_string_);
}

// Dispatch Messages containing SentinelValue
// Ensure that an EndOfProgramExpcetion is thrown
TEST(DummyContext, DispatchSentinel) {
  DummyContext context;
  unique_ptr<SentinelValue> v = make_unique<SentinelValue>();
  try {
    context.dispatch(v.get());
    // An exception should have been thrown
    ASSERT_TRUE(false);
  } catch (EndOfProgramException e) {
    ASSERT_TRUE(true);
  }
}
