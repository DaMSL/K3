#include "storage/StorageManager.hpp"

namespace K3 {
using std::pair; 


void StorageManager::openFile (Address peer, Identifier id, std::string path, 
                      StorageFormat fmt, CodecFormat codec, IOMode io) {
  pair<Address, Identifier> key = std::make_pair (peer, id);

  logger_->info("Opening file `{}`, id={}", path, id);

  try {
    switch (io) {
      case IOMode::Read:
        if (fmt == StorageFormat::Binary) {
          files_->insert (key, make_shared<SourceFileHandle> (path, codec));
        }
        else {
          files_->insert (key, make_shared<SourceTextHandle> (path, codec));
        }
        break;
      case IOMode::Write:
        if (fmt == StorageFormat::Binary) {
          files_->insert (key, make_shared<SinkFileHandle> (path));
        }
        else {
          files_->insert (key, make_shared<SinkTextHandle> (path));
        }
        break;
    }
  }
  catch (std::exception e) {
    logger_->error ("ERROR! Failed to open file `{}`", path);
    throw std::runtime_error ("File I/O Error. Program is Halting.");
  }
}

void StorageManager::closeFile(Address peer, Identifier id) {
  files_->lookup(make_pair(peer, id))->close();
}

bool StorageManager::hasRead(Address peer, Identifier id)  {
  return files_->lookup(make_pair(peer, id))->hasRead();
}

bool StorageManager::hasWrite(Address peer, Identifier id)  {
  auto file = files_->lookup(make_pair(peer, id));
  return file->hasWrite();
}

}  // namespace K3
