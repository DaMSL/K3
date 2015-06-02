#ifndef K3_STORAGEMANAGER
#define K3_STORAGEMANAGER

#include "Common.hpp"
#include "FileHandle.hpp"
#include "spdlog/spdlog.h"

namespace K3 {

enum class StorageFormat { Binary, Text };
enum class IOMode { Read, Write };

using std::pair;
using std::make_shared;

class StorageManager {
 public:
  // Core
  StorageManager() {
      files_ = make_shared<ConcurrentMap<pair<Address, Identifier>, shared_ptr<FileHandle>>> ();
      logger_ = spdlog::get("engine");
  }
  void openFile(Address peer, Identifier id, std::string path, 
                      StorageFormat fmt, CodecFormat codec, IOMode io);
  void closeFile(Address peer, Identifier id);

  // Reading
  bool hasRead(Address peer, Identifier id);
  shared_ptr<PackedValue> doRead(Address peer, Identifier id);
  vector<shared_ptr<PackedValue>> doBlockRead(Address peer, Identifier id, int max_blocksize);

  // Writing
  bool hasWrite(Address peer, Identifier id);
  void doWrite(Address peer, Identifier id, shared_ptr<PackedValue> val);
  void doBlockWrite(Address peer, Identifier id, vector<shared_ptr<PackedValue>> vals);


 private:
  shared_ptr<ConcurrentMap<pair<Address, Identifier>, shared_ptr<FileHandle>>> files_;

  // logger
  shared_ptr<spdlog::logger> logger_;

};

}  // namespace K3

#endif