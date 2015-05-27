#include "storage/StorageManager.hpp"

namespace K3 {
using std::pair; 

// Set the logger
void StorageManager::setLogger (std::string logger_id)  {
  logger = spdlog::get(logger_id);
  if (!logger)  {
    logger = spdlog::stdout_logger_mt(logger_id);
  }
}

void StorageManager::openFile (Address peer, Identifier id, std::string path, 
                      StorageFormat fmt, CodecFormat codec, IOMode io) {
  pair<Address, Identifier> key = std::make_pair (peer, id);

  logger->info("Opening file");

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
  logger->info("Closing file");
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

vector<shared_ptr<PackedValue>> StorageManager::doBlockRead(Address peer, Identifier id, int max_blocksize)  {
  vector<shared_ptr<PackedValue>> vals;
  auto file = files_->lookup(make_pair(peer, id));
  for (int i = 0; i < max_blocksize; i++) {
    vals.push_back(file->doRead());
  }
  return vals;
}

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

void StorageManager::doBlockWrite(Address peer, Identifier id, vector<shared_ptr<PackedValue>> vals) {
  auto file = files_->lookup(make_pair(peer, id));
  for (vector<shared_ptr<PackedValue>>::iterator itr = vals.begin(); itr != vals.end(); ++itr) {
    file->doWrite(*itr);
  }
}

}  // namespace K3
