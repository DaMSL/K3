#include "gtest/gtest.h"

TEST(Engine, Termination) {
  Address a1 = make_address("127.0.0.1", 30000);

  // Write a few ints to a sink
  StorageManager& storage = StorageManager::getInstance();
  storage.openFile(a1, "sink1", "ints.txt",  StorageFormat::Binary, CodecFormat::BoostBinary, IOMode::Write);

  shared_ptr<NativeValue> nv1 = make_shared<TNativeValue<int>>(1);
  shared_ptr<NativeValue> nv2 = make_shared<TNativeValue<int>>(2);
  shared_ptr<NativeValue> nv3 = make_shared<TNativeValue<int>>(3);


  shared_ptr<Codec> codec = Codec::getCodec<int>(CodecFormat::BoostBinary);
  storage.doWrite(a1, "sink1", codec->pack(*nv1));
  storage.doWrite(a1, "sink1", codec->pack(*nv2));
  storage.doWrite(a1, "sink1", codec->pack(*nv3));

  storage.closeFile(a1, "sink1");

  // Read them back in from a source
  storage.openFile(a1, "source1", "ints.txt", StorageFormat::Binary, CodecFormat::BoostBinary, IOMode::Read);

  shared_ptr<PackedValue> pv1 = storage.doRead(a1, "source1");
  shared_ptr<PackedValue> pv2 = storage.doRead(a1, "source1");
  shared_ptr<PackedValue> pv3 = storage.doRead(a1, "source1");
  storage.closeFile(a1, "source1");

// Use codec to convert back to native

  nv1 = codec->unpack(*pv1);
  nv2 = codec->unpack(*pv2);
  nv3 = codec->unpack(*pv3);

  int i1 = *nv1->as<int>();
  int i2 = *nv2->as<int>();
  int i3 = *nv3->as<int>();

}
