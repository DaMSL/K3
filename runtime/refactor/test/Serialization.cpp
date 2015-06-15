#include <tuple>
#include <unordered_map>

#include "gtest/gtest.h"

#include "Common.hpp"
#include "serialization/Codec.hpp"
#include "serialization/YAS.hpp"
#include "types/BaseString.hpp"
#include "Flat.hpp"

using std::shared_ptr;
using std::make_shared;
using std::vector;
using std::string;
using std::tuple;
using K3::Codec;
using K3::CodecFormat;
using K3::Address;
using K3::make_address;
using K3::Value;
using K3::TNativeValue;
using K3::NativeValue;
using K3::StringPackedValue;
using K3::MessageHeader;


TEST(BOOST, unorderedmap) {
  std::unordered_map<int, int> m;
  m[1] = 1;
  m[2] = 2;
  m[3] = 3;

  auto cdec =
      Codec::getCodec<std::unordered_map<int, int>>(CodecFormat::BoostBinary);
  auto packed = cdec->pack(
      *make_shared<TNativeValue<std::unordered_map<int, int>>>(std::move(m)));
  auto unpacked = cdec->unpack(*packed);
  auto m2 = *unpacked->as<std::unordered_map<int, int>>();
  ASSERT_EQ(1, m2[1]);
  ASSERT_EQ(2, m2[2]);
  ASSERT_EQ(3, m2[3]);
}

TEST(BOOST, unorderedset) {
  std::unordered_set<int> s;
  s.insert(1);
  s.insert(2);
  s.insert(3);

  auto cdec =
      Codec::getCodec<std::unordered_set<int>>(CodecFormat::BoostBinary);
  auto packed = cdec->pack(
      *make_shared<TNativeValue<std::unordered_set<int>>>(std::move(s)));
  auto unpacked = cdec->unpack(*packed);
  auto s2 = *unpacked->as<std::unordered_set<int>>();

  ASSERT_EQ(3, s2.size());
  ASSERT_TRUE(s2.find(1) != s2.end());
  ASSERT_TRUE(s2.find(2) != s2.end());
  ASSERT_TRUE(s2.find(3) != s2.end());
}

TEST(YAS, One) {
  auto cdec = Codec::getCodec<int>(CodecFormat::YASBinary);
  auto packed = cdec->pack(*make_shared<TNativeValue<int>>(5));
  auto unpacked = cdec->unpack(*packed);
  ASSERT_EQ(*unpacked->as<int>(), 5);

  auto cdec2 = Codec::getCodec<shared_ptr<int>>(CodecFormat::YASBinary);
  auto packed2 = cdec2->pack(
      *make_shared<TNativeValue<shared_ptr<int>>>(make_shared<int>(5)));
  auto unpacked2 = cdec2->unpack(*packed2);
  ASSERT_EQ(*(*unpacked2->as<shared_ptr<int>>()), 5);
}

TEST(YAS, Address) {
  auto a = K3::make_address("127.0.0.1", 30000);
  auto cdec = Codec::getCodec<K3::Address>(CodecFormat::YASBinary);
}

TEST(Flat, Primitives) {
  ASSERT_EQ(true, K3::is_flat<int>::value);
  ASSERT_EQ(true, K3::is_flat<double>::value);
  ASSERT_EQ(true, K3::is_flat<std::string>::value);
  ASSERT_EQ(true, K3::is_flat<K3::base_string>::value);
}

TEST(Flat, Composites) {
  bool b = K3::is_flat<std::tuple<int, int, std::string>>::value;
  ASSERT_EQ(true, b);
}

TEST(CSV, StringPackedValue) {
  auto codec = Codec::getCodec<tuple<int, string>>(CodecFormat::CSV);
  string s = "1,one";

  shared_ptr<K3::PackedValue> packed =
      make_shared<StringPackedValue>(std::move(s), CodecFormat::CSV);
  auto result = codec->unpack(*packed);

  ASSERT_EQ(1, std::get<0>(*result->as<tuple<int, string>>()));
  ASSERT_EQ("one", std::get<1>(*result->as<tuple<int, string>>()));
}

TEST(CSV, Tuple) {
  auto codec = Codec::getCodec<tuple<int, string>>(CodecFormat::CSV);

  tuple<int, string> s = std::make_tuple(1, "one");
  auto val = make_shared<TNativeValue<tuple<int, string>>>(s);

  auto packed = codec->pack(*val);
  string str = string(packed->buf(), packed->length());
  std::cout << str << std::endl;
  auto result = codec->unpack(*packed);

  ASSERT_EQ(std::get<0>(s), std::get<0>(*result->as<tuple<int, string>>()));
  ASSERT_EQ(std::get<1>(s), std::get<1>(*result->as<tuple<int, string>>()));
}

TEST(CSV, String) {
  auto codec = Codec::getCodec<string>(CodecFormat::CSV);

  string s = "Hello. this is a test.";
  auto val = make_shared<TNativeValue<string>>(s);

  auto packed = codec->pack(*val);
  auto result = codec->unpack(*packed);

  ASSERT_EQ(s, *result->as<string>());
}

TEST(PSV, Tuple) {
  auto codec = Codec::getCodec<tuple<int, string>>(CodecFormat::PSV);

  tuple<int, string> s = std::make_tuple(1, "one");
  auto val = make_shared<TNativeValue<tuple<int, string>>>(s);

  auto packed = codec->pack(*val);
  string str = string(packed->buf(), packed->length());
  std::cout << str << std::endl;
  auto result = codec->unpack(*packed);

  ASSERT_EQ(std::get<0>(s), std::get<0>(*result->as<tuple<int, string>>()));
  ASSERT_EQ(std::get<1>(s), std::get<1>(*result->as<tuple<int, string>>()));
}
