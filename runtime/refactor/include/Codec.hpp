#ifndef K3_CODEC
#define K3_CODEC

#include <memory>

#include "Value.hpp"
#include "Serialization.hpp"

using std::unique_ptr;
using std::make_unique;

class Codec {
 public:
  virtual Buffer pack(const NativeValue&) = 0;
  virtual unique_ptr<NativeValue> unpack(const char* buf, int length) = 0;
};

template <class T>
class BoostCodec : public Codec {
 public:
  virtual Buffer pack(const NativeValue& v) {
    return Serialization::pack<T>(*(v.asConst<T>()));
  }
  virtual unique_ptr<NativeValue> unpack(const char* buf, int length) {
    return make_unique<TNativeValue<T>>(Serialization::unpack<T>(buf, length));
  }
};

#endif
