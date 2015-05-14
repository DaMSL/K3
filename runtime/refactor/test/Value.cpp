#include <memory>

#include "gtest/gtest.h"

#include "Value.hpp"

using std::unique_ptr;
using std::make_unique;
TEST(NativeValue, RoundTrips) {
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
