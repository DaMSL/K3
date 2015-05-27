#ifndef K3_CODEC
#define K3_CODEC

// Codec is an interface for conversion between Packed and Native Values.

#include <memory>
#include <string>

#include "Common.hpp"
#include "types/Value.hpp"
#include "serialization/Serialization.hpp"

namespace K3 {

class Codec {
 public:
  virtual shared_ptr<PackedValue> pack(const NativeValue&) = 0;
  virtual shared_ptr<NativeValue> unpack(const PackedValue& pv) = 0;
  virtual CodecFormat format() = 0;

  // TODO(jbw) switch to map lookup
  template <typename T>
  static shared_ptr<Codec> getCodec(CodecFormat format);
};

template <class T>
class BoostCodec : public Codec {
 public:
  virtual shared_ptr<PackedValue> pack(const NativeValue& nv) {
    auto buf = Serialization::pack<T>(*(nv.asConst<T>()));
    return make_shared<PackedValue>(std::move(buf), format());
  }

  virtual shared_ptr<NativeValue> unpack(const PackedValue& pv) {
    auto t = Serialization::unpack<T>(pv.buf(), pv.length());
    return make_shared<TNativeValue<T>>(t);
  }

  virtual CodecFormat format() { return format_; }

 protected:
  CodecFormat format_ = CodecFormat::BoostBinary;
};

template <class T>
class CSVCodec : public Codec {
 public:
  virtual shared_ptr<PackedValue> pack(const NativeValue& nv) {
    auto buf = Serialization::pack_csv<T>(*(nv.asConst<T>()));
    return make_shared<PackedValue>(std::move(buf), format());
  }

  virtual shared_ptr<NativeValue> unpack(const PackedValue& pv) {
    auto t = Serialization::unpack_csv<T>(pv.buf(), pv.length());
    return make_shared<TNativeValue<T>>(t);
  }

  virtual CodecFormat format() { return format_; }

 protected:
  CodecFormat format_ = CodecFormat::CSV;
};

template <typename T>
shared_ptr<Codec> Codec::getCodec(CodecFormat format) {
  if (format == CodecFormat::BoostBinary) {
    return make_shared<BoostCodec<T>>();
  } else if (format == CodecFormat::CSV) {
    return make_shared<CSVCodec<T>>();
  } else {
    throw std::runtime_error("Unrecognized codec format");
  }
}

}  // namespace K3

#endif
