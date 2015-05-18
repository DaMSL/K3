#ifndef K3_CODEC
#define K3_CODEC

// Codec is an interface for conversion between Packed and Native Values.

#include <memory>
#include <string>

#include "Common.hpp"
#include "Value.hpp"
#include "Serialization.hpp"

using std::unique_ptr;
using std::make_unique;
using std::shared_ptr;
using std::make_shared;

class Codec {
 public:
  virtual unique_ptr<PackedValue> pack(const NativeValue&) = 0;
  virtual unique_ptr<NativeValue> unpack(const PackedValue& pv) = 0;
  virtual CodecFormat format() = 0;

  // TODO(jbw) switch to map lookup
  template <typename T>
  static shared_ptr<Codec> getCodec(CodecFormat format);
};

template <class T>
class BoostCodec : public Codec {
 public:
  virtual unique_ptr<PackedValue> pack(const NativeValue& nv) {
    auto buf = make_unique<Buffer>(Serialization::pack<T>(*(nv.asConst<T>())));
    return make_unique<PackedValue>(std::move(buf), format());
  }

  virtual unique_ptr<NativeValue> unpack(const PackedValue& pv) {
    auto t = Serialization::unpack<T>(pv.buf(), pv.length());
    return make_unique<TNativeValue<T>>(t);
  }

  virtual CodecFormat format() { return format_; }

 protected:
  CodecFormat format_ = CodecFormat::BoostBinary;
};

template <typename T>
shared_ptr<Codec> Codec::getCodec(CodecFormat format) {
  if (format == CodecFormat::BoostBinary) {
    return make_shared<BoostCodec<T>>();
  } else {
    throw std::runtime_error("Unrecognized codec format");
  }
}

#endif
