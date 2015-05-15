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

// Ensure both Native and Packed values can be placed in a vector
// and converted back to C++ values
TEST(Value, HomogenousValues) {
  std::vector<unique_ptr<Value>> vals;

  // Add a packed int to the vector
  int i = 100;
  unique_ptr<NativeValue> v = make_unique<TNativeValue<int>>(i);

  unique_ptr<Codec> cdec = make_unique<BoostCodec<int>>();
  unique_ptr<PackedValue> b = cdec->pack(*v);

  unique_ptr<Value> val1 = std::move(b);
  vals.push_back(std::move(val1));

  // Add a native string to the vector
  std::string s = "HELLO!";
  unique_ptr<Value> v2 = make_unique<TNativeValue<std::string>>(s);
  vals.push_back(std::move(v2));

  // Check the int inside the vector
  int i2 = *vals[0]->asNative()->as<int>();
  ASSERT_EQ(i, i2);

  // Check the string inside the vector
  std::string s2 = *vals[1]->asNative()->as<std::string>();
  ASSERT_EQ(s, s2);
}
