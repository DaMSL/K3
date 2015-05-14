#include <memory>

#include "gtest/gtest.h"

#include "Codec.hpp"
#include "Value.hpp"

using std::unique_ptr;
using std::make_unique;
TEST(DummyContext, DispatchNative) {
  DummyContext context;

  int i = 100;
  unique_ptr<NativeValue> v = make_unique<TNativeValue<int>>(i);
  v->dispatch(context, 1);
  ASSERT_EQ(i, context.my_int_);

  std::string s = "Hello!";
  unique_ptr<NativeValue> v2 = make_unique<TNativeValue<std::string>>(s);
  v2->dispatch(context, 2);
  ASSERT_EQ(s, context.my_string_);
}

TEST(DummyContext, DispatchPacked) {
  unique_ptr<Codec> cdec = make_unique<BoostCodec<int>>();
  DummyContext context;

  int i = 100;
  unique_ptr<NativeValue> v = make_unique<TNativeValue<int>>(i);
  Buffer b = cdec->pack(*v);

  PackedValue pv(b, make_unique<BoostCodec<int>>());

  pv.dispatch(context, 1);
  ASSERT_EQ(i, context.my_int_);
}
