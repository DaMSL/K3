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


  string s1 = "Foo";
  string s2 = "FooBar";
  string s3 = "The quick Brown Fox Jumps Over the Lazy Dog/nQuietly!";
  storage.doWrite<int>(a1, "sink1", 1, CodecFormat::BoostBinary);
  storage.doWrite<int>(a1, "sink1", 2, CodecFormat::BoostBinary);
  storage.doWrite<int>(a1, "sink2", 3, CodecFormat::BoostBinary);
  storage.doWrite<string>(a1, "sink1", s1 , CodecFormat::BoostBinary);
  storage.doWrite<string>(a1, "sink1", s2, CodecFormat::BoostBinary);
  storage.doWrite<string>(a1, "sink2", s3, CodecFormat::BoostBinary);

  vector<int> int_vec;
  for (int i = 0; i < 10; i++) {
    int_vec.push_back(i*10);
  }
  storage.doBlockWrite<int>(a1, "sink2", int_vec, CodecFormat::BoostBinary);

  storage.closeFile(a1, "sink1");
  storage.closeFile(a1, "sink2");

  // Read them back in from a source
  storage.openFile(a1, "source1", "vals1.txt", StorageFormat::Binary, CodecFormat::BoostBinary, IOMode::Read);
  storage.openFile(a1, "source2", "vals2.txt", StorageFormat::Binary, CodecFormat::BoostBinary, IOMode::Read);

  int i1 = storage.doRead<int>(a1, "source1");
  int i2 = storage.doRead<int>(a1, "source1");
  int i3 = storage.doRead<int>(a1, "source2");
  s1 = storage.doRead<string>(a1, "source1");
  s2 = storage.doRead<string>(a1, "source1");
  s3 = storage.doRead<string>(a1, "source2");


  auto int_vec2 = storage.doBlockRead<int>(a1, "source2", 10);

  storage.closeFile(a1, "source1");
  storage.closeFile(a1, "source2");

  ASSERT_EQ (i1, 1);
  ASSERT_EQ (i2, 2);
  ASSERT_EQ (i3, 3);
  ASSERT_EQ (s1, "Foo");
  ASSERT_EQ (s2, "FooBar");
  ASSERT_EQ (s3, "The quick Brown Fox Jumps Over the Lazy Dog/nQuietly!");

  for (int i = 0; i < 10; i++) {
    int x = int_vec2[i];
    ASSERT_EQ (i*10, x);
  }
}


TEST(Storage, TextFile) {
  Address a1 = make_address("127.0.0.1", 30000);

  StorageManager storage = StorageManager();

  // Write a 2 rows to a sink
  storage.openFile(a1, "sink", "table.csv",
      StorageFormat::Text, CodecFormat::CSV, IOMode::Write);

  tuple<int, string> row1 = std::make_tuple(1, "one");
  tuple<int, string> row2 = std::make_tuple(2, "two");

  storage.doWrite<tuple<int, string>>(a1, "sink", row1, CodecFormat::CSV);
  storage.doWrite<tuple<int, string>>(a1, "sink", row2, CodecFormat::CSV);

  storage.closeFile(a1, "sink");

  // Read them back in from a source
  storage.openFile(a1, "source", "table.csv",
      StorageFormat::Text, CodecFormat::CSV, IOMode::Read);

  tuple<int, string> result1 = storage.doRead<tuple<int, string>>(a1, "source");
  tuple<int, string> result2 = storage.doRead<tuple<int, string>>(a1, "source");

  storage.closeFile(a1, "source");

  ASSERT_EQ(std::get<0>(row1), std::get<0>(result1));
  ASSERT_EQ(std::get<1>(row1), std::get<1>(result1));
  ASSERT_EQ(std::get<0>(row2), std::get<0>(result2));
  ASSERT_EQ(std::get<1>(row2), std::get<1>(result2));
}
