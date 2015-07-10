#ifndef K3_FILEBUILTINS
#define K3_FILEBUILTINS

#include <climits>
#include <boost/thread/mutex.hpp>
#include <boost/thread/thread.hpp>

#include "types/BaseString.hpp"
#include "Common.hpp"
#include "core/Engine.hpp"

namespace K3 {

class FileBuiltins {
 public:
  FileBuiltins(Engine& engine) : __engine_(engine) { }

  unit_t openFile(const Address& peer, const string_impl& id, const string_impl& path, const string_impl& fmt, bool txt, const string_impl& mode) {
    CodecFormat c_format = Codec::getFormat(fmt);
    IOMode io_mode = getIOMode(mode);
    StorageFormat s_format = txt ? StorageFormat::Text : StorageFormat::Binary;
    __engine_.getStorageManager().openFile(peer, id, path, s_format, c_format, io_mode);
    return unit_t{};
  }

  unit_t close(const Address& peer, const string_impl& id) {
    __engine_.getStorageManager().closeFile(peer, id);
    return unit_t{};
  }

  bool hasRead(const Address& peer, const string_impl& id) {
    return __engine_.getStorageManager().hasRead(peer, id);
  }

  template <class T>
  T doRead(const Address& peer, const string_impl& id) {
    return __engine_.getStorageManager().doRead<T>(peer, id);
  }

  bool hasWrite(const Address& peer, const string_impl& id) {
    return __engine_.getStorageManager().hasWrite(peer, id);
  }

  template <class T>
  unit_t doWrite(const Address& peer, const string_impl& id, const T& t, const string_impl& cfmt) {
    CodecFormat c_format = Codec::getFormat(cfmt);
    __engine_.getStorageManager().doWrite<T>(peer, id, t, c_format);
    return unit_t{};
  }
 
  // TODO(jbw) remove this after code generation is fixed to include format
  template <class T>
  unit_t doWrite(const Address& peer, const string_impl& id, const T& t) {
    __engine_.getStorageManager().doWrite<T>(peer, id, t, CodecFormat::CSV);
    return unit_t{};
  }

  // TODO(jbw) block versions

 protected:
  Engine& __engine_;
};

}  // namespace K3

#endif
