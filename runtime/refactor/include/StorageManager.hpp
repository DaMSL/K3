#ifndef K3_STORAGEMANAGER
#define K3_STORAGEMANAGER

#include "Common.hpp"
#include "FileHandle.hpp"


enum class StorageFormat { Binary, Text };
enum class IOMode { Read, Write };

using std::pair;
using std::make_shared;

class StorageManager {
 public:
  // Core
  static StorageManager& getInstance();
  void openFile(Address peer, Identifier id, std::string path, 
                      StorageFormat fmt, CodecFormat codec, IOMode io);
  void closeFile(Address peer, Identifier id);

  // Reading
  bool hasRead(Address peer, Identifier id);
  shared_ptr<PackedValue> doRead(Address peer, Identifier id);
  // vector<shared_ptr<PackedValue>> doBlockRead(Address peer, Identifier id, int max_blocksize);

  // Writing
  bool hasWrite(Address peer, Identifier id);
  void doWrite(Address peer, Identifier id, shared_ptr<PackedValue> val);
  // void doBlockWrite(Address peer, Identifier id, vector<shared_ptr<PackedValue>> vals);

 private:
  // Singleton class
  StorageManager() {
      files_ = make_shared<ConcurrentMap<pair<Address, Identifier>, shared_ptr<FileHandle>>> ();
  }
  StorageManager(const StorageManager&) = delete;
  void operator=(const StorageManager&) = delete;

  shared_ptr<ConcurrentMap<pair<Address, Identifier>, shared_ptr<FileHandle>>> files_;
};

// Perhaps create a FileHandle class with its own doWrite/doRead methods
// And a different subclass for StorageFormat::Binary (that reads/writes an int as a header indicating the size in bytes of a value) and StorageFormat::Text (that seperates values with a newline). Either way, read in the bytes corresponding to a single value to construct a PackedValue
#endif