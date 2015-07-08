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
  void openFile(Address peer, Identifier id, std::string path, 
                      StorageFormat fmt, CodecFormat codec, IOMode io);
  void closeFile(Address peer, Identifier id);

  // Reading
  bool hasRead(Address peer, Identifier id);
  template <class T>
  T doRead(Address peer, Identifier id) {
    try {
      auto val = files_->lookup(make_pair(peer, id))->doRead();
      auto cdec = Codec::getCodec<T>(val->format());
      auto native = cdec->unpack(*val);
      T t = std::move(*native->template as<T>());
      return t;
    }
    catch (std::ios_base::failure e) {
      logger_->error ("ERROR Reading from {}", id);
      throw std::runtime_error ("File I/O Error. Program is Halting.");
    }
  }
 
  template <class T>
  vector<T> doBlockRead(Address peer, Identifier id, int max_blocksize) {
    vector<T> vals;
    try {
      auto file = files_->lookup(make_pair(peer, id));
      for (int i = 0; i < max_blocksize; i++) {
        auto val = file->doRead(); 
        auto cdec = Codec::getCodec<T>(val->format());
        auto native = cdec->unpack(*val);
        T t = std::move(*native->template as<T>());
        vals.push_back(std::move(t));
      }
    }
    catch (std::exception e)  {
      logger_->error ("ERROR Reading from {}", id);
      throw std::runtime_error ("File I/O Error. Program is Halting.");
    }
    return vals;
  }

  // Writing
  bool hasWrite(Address peer, Identifier id);

  template <class T>
  void doWrite(Address peer, Identifier id, const T& val, CodecFormat fmt) {
    try {
      // TODO(jbw) avoid copies when creating the native-value wrappers
      auto file = files_->lookup(make_pair(peer, id));
      TNativeValue<T> nv(val);
      auto pv = Codec::getCodec<T>(fmt)->pack(nv);
      file->doWrite(*pv);
    }
    catch (std::ios_base::failure e) {
      logger_->error ("ERROR Writing to {}", id);
      throw std::runtime_error ("File I/O Error. Program is Halting.");
    }
  }
  
  template <class T> 
  void doBlockWrite(Address peer, Identifier id, const vector<T>& vals, CodecFormat fmt) {
      // TODO(jbw) avoid copies when creating the native-value wrappers
      try  {
        auto file = files_->lookup(make_pair(peer, id));
        for (const auto& elem : vals) {
          TNativeValue<T> nv(elem);
          auto pv = Codec::getCodec<T>(fmt)->pack(nv);
          file->doWrite(*pv);
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
