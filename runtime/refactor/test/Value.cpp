// Test basic abilities to convert between native c++ values and our Value representations

#include <memory>
#include <string>
#include <vector>

#include "gtest/gtest.h"

#include "Codec.hpp"
#include "Value.hpp"

using std::unique_ptr;
using std::make_unique;

// Test ability to convert between C++ values and our NativeValue class
TEST(Value, NativeRoundTrips) {
  // Round trip a Native value with several native types
  unique_ptr<NativeValue> ptr;

  // Int
  int in_int = 100;
  ptr = make_unique<TNativeValue<int>>(in_int);
  int out_int = *ptr->as<int>();
  ASSERT_EQ(in_int, out_int);

  // Double
  double in_d = 100.1;
  ptr = make_unique<TNativeValue<double>>(in_d);
  double out_d = *ptr->as<double>();
  ASSERT_EQ(in_d, out_d);

  // String
  std::string in_s = "HELLO";
  ptr = make_unique<TNativeValue<std::string>>(in_s);
  std::string out_s = *ptr->as<std::string>();
  ASSERT_EQ(in_s, out_s);
}

// Test ability to convert between NativeValue and PackedValue
TEST(Value, PackedRoundTrips) {
  // Int -> NativeValue -> PackedValue -> NativeValue -> Int
  int i = 100;
  unique_ptr<NativeValue> v = make_unique<TNativeValue<int>>(i);

  unique_ptr<Codec> cdec = make_unique<BoostCodec<int>>();
  unique_ptr<PackedValue> b = cdec->pack(*v);

  v = cdec->unpack(*b);
  ASSERT_EQ(i, *v->as<int>());

  // String -> NativeValue -> PackedValue -> NativeValue -> String
  std::string s = "HELLO!";
  unique_ptr<NativeValue> v2 = make_unique<TNativeValue<std::string>>(s);

  unique_ptr<Codec> cdec2 = make_unique<BoostCodec<std::string>>();
  unique_ptr<PackedValue> b2 = cdec2->pack(*v2);

  v2 = cdec2->unpack(*b2);
  ASSERT_EQ(s, *v2->as<std::string>());
}