#ifndef K3_CODEC
#define K3_CODEC

// Codec is an interface for conversion between Packed and Native Values.

#include <memory>

#include "Value.hpp"
#include "Serialization.hpp"

using std::unique_ptr;
using std::make_unique;
using std::make_shared;

class Codec {
 public:
  virtual unique_ptr<PackedValue> pack(const NativeValue&) = 0;
  virtual unique_ptr<NativeValue> unpack(const PackedValue& pv) = 0;
};

template <class T>
class BoostCodec : public Codec {
 public:
  virtual unique_ptr<PackedValue> pack(const NativeValue& nv) {
    auto buf = make_unique<Buffer>(Serialization::pack<T>(*(nv.asConst<T>())));
    return make_unique<PackedValue>(std::move(buf), make_shared<BoostCodec<T>>(*this));
  }

  virtual unique_ptr<NativeValue> unpack(const PackedValue& pv) {
    auto t = Serialization::unpack<T>(pv.buf(), pv.length());
    return make_unique<TNativeValue<T>>(t);
  }
};

#endif
