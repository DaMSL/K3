#include "StorageManager.hpp"


StorageManager& StorageManager::getInstance() {
  static StorageManager instance;
  return instance;
}

using std::pair; 

void StorageManager::openFile (Address peer, Identifier id, std::string path, 
                      StorageFormat fmt, CodecFormat codec, IOMode io) {
  pair<Address, Identifier> key = std::make_pair (peer, id);

  // shared_ptr<FileHandle> fh;

  switch (io) {
    case IOMode::Read:
      files_->insert (key, make_shared<SourceFileHandle> (path, codec));
      break;
    case IOMode::Write:
      files_->insert (key, make_shared<SinkFileHandle> (path));
      break;
  }

}


void StorageManager::closeFile(Address peer, Identifier id) {
  files_->lookup(make_pair(peer, id))->close();
//  free fh;
}



bool StorageManager::hasRead(Address peer, Identifier id)  {
  return files_->lookup(make_pair(peer, id))->hasRead();
}


shared_ptr<PackedValue> StorageManager::doRead(Address peer, Identifier id)  {
  auto val = files_->lookup(make_pair(peer, id))->doRead();
  return val;
}

//vector<shared_ptr<PackedValue>> StorageManager::doBlockRead(Address peer, Identifier id, int max_blocksize);

bool StorageManager::hasWrite(Address peer, Identifier id)  {
  auto file = files_->lookup(make_pair(peer, id));
  return file->hasWrite();
}

void StorageManager::doWrite(Address peer, Identifier id, 
              shared_ptr<PackedValue> val)  {
  //TODO: Optimize w/mutli-index or additional peer -> id mapping lookup for pairs
  auto file = files_->lookup(make_pair(peer, id));
  file->doWrite(val);
}

//  void doBlockWrite(Address peer, Identifier id, vector<shared_ptr<PackedValue>> vals);



