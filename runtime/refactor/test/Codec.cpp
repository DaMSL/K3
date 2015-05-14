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
  Buffer b = cdec->pack(*v);
  v = cdec->unpack(b.data(), b.size());
  ASSERT_EQ(i, *v->as<int>());
  
  unique_ptr<Codec> cdec2 = make_unique<BoostCodec<std::string>>();
  std::string s = "HELLO!";
  unique_ptr<NativeValue> v2 = make_unique<TNativeValue<std::string>>(s);
  Buffer b2 = cdec2->pack(*v2);
  v2 = cdec2->unpack(b2.data(), b2.size());
  ASSERT_EQ(s, *v2->as<std::string>());
}
