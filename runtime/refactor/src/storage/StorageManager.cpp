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


shared_ptr<PackedValue> StorageManager::doRead(Address peer, Identifier id)  {
  try {
    auto val = files_->lookup(make_pair(peer, id))->doRead();
    return val;
  }
  catch (std::ios_base::failure e) {
    logger_->error ("ERROR Reading from {}", id);
    throw std::runtime_error ("File I/O Error. Program is Halting.");
  }
}

vector<shared_ptr<PackedValue>> StorageManager::doBlockRead(
      Address peer, Identifier id, int max_blocksize)  {
  vector<shared_ptr<PackedValue>> vals;
  try {
    auto file = files_->lookup(make_pair(peer, id));
    for (int i = 0; i < max_blocksize; i++) {
      vals.push_back(file->doRead());
    }
  }
  catch (std::exception e)  {
    logger_->error ("ERROR Reading from {}", id);
    throw std::runtime_error ("File I/O Error. Program is Halting.");
  }
    return vals;

}

bool StorageManager::hasWrite(Address peer, Identifier id)  {
  auto file = files_->lookup(make_pair(peer, id));
  return file->hasWrite();
}

void StorageManager::doWrite(Address peer, Identifier id, 
              shared_ptr<PackedValue> val)  {
  try {
    auto file = files_->lookup(make_pair(peer, id));
    file->doWrite(val);
  }
  catch (std::ios_base::failure e) {
    logger_->error ("ERROR Writing to {}", id);
    throw std::runtime_error ("File I/O Error. Program is Halting.");
  }

}

void StorageManager::doBlockWrite(Address peer, Identifier id, 
      vector<shared_ptr<PackedValue>> vals) {
  try  {
    auto file = files_->lookup(make_pair(peer, id));
    for (vector<shared_ptr<PackedValue>>::iterator itr = vals.begin(); 
          itr != vals.end(); ++itr) {
      file->doWrite(*itr);
    }
  }
  catch (std::exception e)  {
    logger_->error ("ERROR Writing to {}", id);
    throw std::runtime_error ("File I/O Error. Program is Halting.");
  }
}

}  // namespace K3
