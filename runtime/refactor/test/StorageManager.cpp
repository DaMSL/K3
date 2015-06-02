//#include <list>
#include <string>
// #include <chrono>
// #include <thread>

#include "gtest/gtest.h"

#include "Common.hpp"
#include "serialization/Codec.hpp"
#include "core/Engine.hpp"
#include "core/Peer.hpp"
#include "core/ProgramContext.hpp"
#include "storage/StorageManager.hpp"
#include "spdlog/spdlog.h"

using namespace K3;
using std::tuple;

TEST(Storage, BinaryFile) {
  Address a1 = make_address("127.0.0.1", 30000);

  // Write a few ints to a sink
  StorageManager storage = StorageManager();

  storage.openFile(a1, "sink1", "vals1.txt",  
      StorageFormat::Binary, CodecFormat::BoostBinary, IOMode::Write);

  storage.openFile(a1, "sink2", "vals2.txt",  
      StorageFormat::Binary, CodecFormat::BoostBinary, IOMode::Write);


  shared_ptr<NativeValue> nv1 = make_shared<TNativeValue<int>>(1);
  shared_ptr<NativeValue> nv2 = make_shared<TNativeValue<int>>(2);
  shared_ptr<NativeValue> nv3 = make_shared<TNativeValue<int>>(3);
  shared_ptr<NativeValue> nv4 = make_shared<TNativeValue<std::string>>("Foo");
  shared_ptr<NativeValue> nv5 = make_shared<TNativeValue<std::string>>("FooBar");
  shared_ptr<NativeValue> nv6 = make_shared<TNativeValue<std::string>>("The quick Brown Fox Jumps Over the Lazy Dog/nQuietly!");


  shared_ptr<Codec> codec_int = Codec::getCodec<int>(CodecFormat::BoostBinary);
  shared_ptr<Codec> codec_str = Codec::getCodec<std::string>(CodecFormat::BoostBinary);
  storage.doWrite(a1, "sink1", codec_int->pack(*nv1));
  storage.doWrite(a1, "sink1", codec_int->pack(*nv2));
  storage.doWrite(a1, "sink2", codec_int->pack(*nv3));
  storage.doWrite(a1, "sink1", codec_str->pack(*nv4));
  storage.doWrite(a1, "sink1", codec_str->pack(*nv5));
  storage.doWrite(a1, "sink2", codec_str->pack(*nv6));

  vector<shared_ptr<PackedValue>> pv_vec;
  for (int i = 0; i < 10; i++) {
    shared_ptr<NativeValue> nv_i = make_shared<TNativeValue<int>>(i*10);
    pv_vec.push_back(codec_int->pack(*nv_i));
  }
  storage.doBlockWrite(a1, "sink2", pv_vec);

  storage.closeFile(a1, "sink1");
  storage.closeFile(a1, "sink2");

  // Read them back in from a source
  storage.openFile(a1, "source1", "vals1.txt", StorageFormat::Binary, CodecFormat::BoostBinary, IOMode::Read);
  storage.openFile(a1, "source2", "vals2.txt", StorageFormat::Binary, CodecFormat::BoostBinary, IOMode::Read);

  shared_ptr<PackedValue> pv1 = storage.doRead(a1, "source1");
  shared_ptr<PackedValue> pv2 = storage.doRead(a1, "source1");
  shared_ptr<PackedValue> pv3 = storage.doRead(a1, "source2");
  shared_ptr<PackedValue> pv4 = storage.doRead(a1, "source1");
  shared_ptr<PackedValue> pv5 = storage.doRead(a1, "source1");
  shared_ptr<PackedValue> pv6 = storage.doRead(a1, "source2");

  pv_vec = storage.doBlockRead(a1, "source2", 10);

  storage.closeFile(a1, "source1");
  storage.closeFile(a1, "source2");
// Use codec to convert back to native


  nv1 = codec_int->unpack(*pv1);
  nv2 = codec_int->unpack(*pv2);
  nv3 = codec_int->unpack(*pv3);
  nv4 = codec_str->unpack(*pv4);
  nv5 = codec_str->unpack(*pv5);
  nv6 = codec_str->unpack(*pv6);

  int i1 = *nv1->as<int>();
  int i2 = *nv2->as<int>();
  int i3 = *nv3->as<int>();

  std::string s1 = *nv4->as<std::string>();
  std::string s2 = *nv5->as<std::string>();
  std::string s3 = *nv6->as<std::string>();

  ASSERT_EQ (i1, 1); 
  ASSERT_EQ (i2, 2); 
  ASSERT_EQ (i3, 3); 
  ASSERT_EQ (s1, "Foo"); 
  ASSERT_EQ (s2, "FooBar"); 
  ASSERT_EQ (s3, "The quick Brown Fox Jumps Over the Lazy Dog/nQuietly!"); 

  for (int i = 0; i < 10; i++) {
    shared_ptr<NativeValue> nv_x = codec_int->unpack(*(pv_vec[i]));
    int x = *nv_x->as<int>();
    ASSERT_EQ (i*10, x);
  }

}


TEST(Storage, TextFile) {
  Address a1 = make_address("127.0.0.1", 30000);

  StorageManager storage = StorageManager();

  // Write a 2 rows to a sink
  storage.openFile(a1, "sink", "table.csv",  
      StorageFormat::Text, CodecFormat::CSV, IOMode::Write);

  auto codec = Codec::getCodec<tuple<int, string>>(CodecFormat::CSV);

  tuple<int, string> row1 = std::make_tuple(1, "one");
  tuple<int, string> row2 = std::make_tuple(2, "two");

  auto val1 = make_shared<TNativeValue<tuple<int, string>>>(row1);
  auto val2 = make_shared<TNativeValue<tuple<int, string>>>(row2);

  auto packed1 = codec->pack(*val1);
  auto packed2 = codec->pack(*val2);

  storage.doWrite(a1, "sink", codec->pack(*val1));
  storage.doWrite(a1, "sink", codec->pack(*val2));

  storage.closeFile(a1, "sink");


  // Read them back in from a source
  storage.openFile(a1, "source", "table.csv", 
      StorageFormat::Text, CodecFormat::CSV, IOMode::Read);

  shared_ptr<PackedValue> pv1 = storage.doRead(a1, "source");
  shared_ptr<PackedValue> pv2 = storage.doRead(a1, "source");

  storage.closeFile(a1, "source");

  string str1 = string(pv1->buf(), pv1->length());
  string str2 = string(pv2->buf(), pv2->length());

  std::cout << str1 << std::endl;
  std::cout << str2 << std::endl;

  auto result1 = codec->unpack(*pv1);
  auto result2 = codec->unpack(*pv2);  

  ASSERT_EQ(std::get<0>(row1), std::get<0>(*result1->as<tuple<int, string>>()));
  ASSERT_EQ(std::get<1>(row1), std::get<1>(*result1->as<tuple<int, string>>()));
  ASSERT_EQ(std::get<0>(row2), std::get<0>(*result2->as<tuple<int, string>>()));
  ASSERT_EQ(std::get<1>(row2), std::get<1>(*result2->as<tuple<int, string>>()));

}
