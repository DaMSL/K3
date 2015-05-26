//#include <list>
#include <string>
// #include <chrono>
// #include <thread>

#include "gtest/gtest.h"

#include "Codec.hpp"
#include "Common.hpp"
#include "Engine.hpp"
#include "StorageManager.hpp"
#include "Peer.hpp"
#include "ProgramContext.hpp"



TEST(Storage, BinaryFile) {
  std::cout << "STORAGE.BIN Test" << std::endl;
  Address a1 = make_address("127.0.0.1", 30000);

  // Write a few ints to a sink
  std::cout << "getInstance" << std::endl;
  StorageManager& storage = StorageManager::getInstance();

  std::cout << "openFile (Write)" << std::endl;
  storage.openFile(a1, "sink1", "vals1.txt",  
      StorageFormat::Binary, CodecFormat::BoostBinary, IOMode::Write);

  storage.openFile(a1, "sink2", "vals2.txt",  
      StorageFormat::Binary, CodecFormat::BoostBinary, IOMode::Write);


  std::cout << "gen values" << std::endl;
  shared_ptr<NativeValue> nv1 = make_shared<TNativeValue<int>>(1);
  shared_ptr<NativeValue> nv2 = make_shared<TNativeValue<int>>(2);
  shared_ptr<NativeValue> nv3 = make_shared<TNativeValue<int>>(3);
  shared_ptr<NativeValue> nv4 = make_shared<TNativeValue<std::string>>("Foo");
  shared_ptr<NativeValue> nv5 = make_shared<TNativeValue<std::string>>("FooBar");
  shared_ptr<NativeValue> nv6 = make_shared<TNativeValue<std::string>>("The quick Brown Fox Jumps Over the Lazy Dog/nQuietly!");


  std::cout << "doWrite" << std::endl;
  shared_ptr<Codec> codec_int = Codec::getCodec<int>(CodecFormat::BoostBinary);
  shared_ptr<Codec> codec_str = Codec::getCodec<std::string>(CodecFormat::BoostBinary);
  storage.doWrite(a1, "sink1", codec_int->pack(*nv1));
  storage.doWrite(a1, "sink1", codec_int->pack(*nv2));
  storage.doWrite(a1, "sink2", codec_int->pack(*nv3));
  storage.doWrite(a1, "sink1", codec_str->pack(*nv4));
  storage.doWrite(a1, "sink1", codec_str->pack(*nv5));
  storage.doWrite(a1, "sink2", codec_str->pack(*nv6));

  std::cout << "closeFile" << std::endl;
  storage.closeFile(a1, "sink1");
  storage.closeFile(a1, "sink2");

  // Read them back in from a source
  std::cout << "openFile (Read)" << std::endl;
  storage.openFile(a1, "source1", "vals1.txt", StorageFormat::Binary, CodecFormat::BoostBinary, IOMode::Read);
  storage.openFile(a1, "source2", "vals2.txt", StorageFormat::Binary, CodecFormat::BoostBinary, IOMode::Read);

  std::cout << "doRead" << std::endl;
  shared_ptr<PackedValue> pv1 = storage.doRead(a1, "source1");
  shared_ptr<PackedValue> pv2 = storage.doRead(a1, "source1");
  shared_ptr<PackedValue> pv3 = storage.doRead(a1, "source2");
  shared_ptr<PackedValue> pv4 = storage.doRead(a1, "source1");
  shared_ptr<PackedValue> pv5 = storage.doRead(a1, "source1");
  shared_ptr<PackedValue> pv6 = storage.doRead(a1, "source2");


  std::cout << "closeFile" << std::endl;
  storage.closeFile(a1, "source1");
  storage.closeFile(a1, "source2");
// Use codec to convert back to native


  std::cout << "unpack" << std::endl;
  nv1 = codec_int->unpack(*pv1);
  nv2 = codec_int->unpack(*pv2);
  nv3 = codec_int->unpack(*pv3);
  nv4 = codec_str->unpack(*pv4);
  nv5 = codec_str->unpack(*pv5);
  nv6 = codec_str->unpack(*pv6);

  std::cout << "get ints" << std::endl;
  int i1 = *nv1->as<int>();
  int i2 = *nv2->as<int>();
  int i3 = *nv3->as<int>();

  std::cout << "get strings" << std::endl;
  std::string s1 = *nv4->as<std::string>();
  std::string s2 = *nv5->as<std::string>();
  std::string s3 = *nv6->as<std::string>();

  std::cout << "assert" << std::endl;
  ASSERT_EQ (i1, 1); 
  ASSERT_EQ (i2, 2); 
  ASSERT_EQ (i3, 3); 
  ASSERT_EQ (s1, "Foo"); 
  ASSERT_EQ (s2, "FooBar"); 
  ASSERT_EQ (s3, "The quick Brown Fox Jumps Over the Lazy Dog/nQuietly!"); 

}
