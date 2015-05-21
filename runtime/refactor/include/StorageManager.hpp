#ifndef K3_NETWORKMANAGER
#define K3_NETWORKMANAGER

#include "Common.k3"

enum class StorageFormat { Binary, Text };
enum class IOMode { Read, Write };

class StorageManager {
 public:
  // Core
  static StorageManager& getInstance();
  void openFile(Address peer, Identifier id, std::string path, StorageFormat, CodecFormat, IOMode);
  void closeFile(Address peer, Identifier id);

  // Reading
  bool hasRead(Address peer, Identifier id);
  shared_ptr<PackedValue> doRead(Address peer, Identifier id,);
  vector<shared_ptr<PackedValue>> doReadBlock(Address peer, Identifier id, int max_blocksize);

  // Writing
  bool hasWrite(Address peer, Identifier id);
  void doWrite(Address peer, Identifier id, shared_ptr<PackedValue> val);

 protected:
  // ConcurrentMap<(Address, Identifier), shared_ptr<FileHandle>> ???
}

// Perhaps create a FileHandle class with its own doWrite/doRead methods
// And a different subclass for StorageFormat::Binary (that reads/writes an int as a header indicating the size in bytes of a value) and StorageFormat::Text (that seperates values with a newline). Either way, read in the bytes corresponding to a single value to construct a PackedValue
