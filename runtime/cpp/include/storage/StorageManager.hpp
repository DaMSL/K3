#ifndef K3_STORAGEMANAGER
#define K3_STORAGEMANAGER

#include "Common.hpp"
#include "serialization/Codec.hpp"
#include "FileHandle.hpp"
#include "spdlog/spdlog.h"

namespace K3 {

using std::pair;
using std::make_shared;

class StorageManager {
 public:
  // Core
  StorageManager() {
      files_ = make_shared<ConcurrentMap<pair<Address, Identifier>, shared_ptr<FileHandle>>> ();
      logger_ = spdlog::get("engine");
  }

  ~StorageManager() {
      files_->apply([](auto& filemap){
        for (auto elem : filemap) { elem.second->close(); }
      });
  }

  void openFile(Address peer, Identifier id, std::string path,
                StorageFormat fmt, CodecFormat codec, IOMode io);

  void closeFile(Address peer, Identifier id);

  // Reading
  bool hasRead(Address peer, Identifier id);

  template <class T>
  T doRead(Address peer, Identifier id) {
    try {
      auto handle = files_->lookup(make_pair(peer, id));
      auto pval = handle->doRead();
      return std::move(*unpack<T>(std::move(pval)));
    }
    catch (std::ios_base::failure e) {
      logger_->error ("ERROR: Reading from {}", id);
      throw e;
      //throw std::runtime_error ("File I/O Error. Program is Halting.");
    }
  }

  template <class T>
  vector<T> doBlockRead(Address peer, Identifier id, int max_blocksize) {
    vector<T> vals;
    try {
      auto file = files_->lookup(make_pair(peer, id));
      for (int i = 0; i < max_blocksize; i++) {
        if (file->hasRead()) {
          auto val = file->doRead();
          auto t = unpack<T>(std::move(val));
          vals.push_back(std::move(*t));
        } else {
          break;
        }
      }
    }
    catch (std::exception e)  {
      logger_->error ("ERROR: Reading from {}", id);
      throw e;
    }
    return std::move(vals);
  }

  // Writing
  bool hasWrite(Address peer, Identifier id);

  template <class T>
  void doWrite(Address peer, Identifier id, const T& val) {
    try {
      auto file = files_->lookup(make_pair(peer, id));
      file->doWrite(val);
    }
    catch (std::ios_base::failure e) {
      logger_->error ("ERROR Writing to {}: {}", id, e.what());
      throw std::runtime_error ("File I/O Error. Program is Halting.");
    }
  }

  template <class T>
  void doBlockWrite(Address peer, Identifier id, const vector<T>& vals, CodecFormat fmt) {
      // TODO(jbw) avoid copies when creating the native-value wrappers
      try  {
        auto file = files_->lookup(make_pair(peer, id));
        for (const auto& elem : vals) {
          file->doWrite<T>(elem);
        }
      }
      catch (std::exception e)  {
        logger_->error ("ERROR Writing to {}", id);
        throw std::runtime_error ("File I/O Error. Program is Halting.");
      }
  }

 private:
  shared_ptr<ConcurrentMap<pair<Address, Identifier>, shared_ptr<FileHandle>>> files_;

  // logger
  shared_ptr<spdlog::logger> logger_;
};

}  // namespace K3

#endif
