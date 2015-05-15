#include <memory>

#include "gtest/gtest.h"

#include "Codec.hpp"
#include "Value.hpp"

using std::unique_ptr;
using std::make_unique;

TEST(Codec, BoostCodecRoundTrips) {
  unique_ptr<Codec> cdec = make_unique<BoostCodec<int>>();
  int i = 100;
  unique_ptr<NativeValue> v = make_unique<TNativeValue<int>>(i);
  unique_ptr<PackedValue> b = cdec->pack(*v);
  v = cdec->unpack(*b);
  ASSERT_EQ(i, *v->as<int>());
  
  unique_ptr<Codec> cdec2 = make_unique<BoostCodec<std::string>>();
  std::string s = "HELLO!";
  unique_ptr<NativeValue> v2 = make_unique<TNativeValue<std::string>>(s);
  unique_ptr<PackedValue> b2 = cdec2->pack(*v2);
  v2 = cdec2->unpack(*b2);
  ASSERT_EQ(s, *v2->as<std::string>());
}
