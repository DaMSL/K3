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
