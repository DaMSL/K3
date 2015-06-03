#ifndef K3_CODEC
#define K3_CODEC

// Codec is an interface for conversion between Packed and Native Values.

#include <memory>
#include <string>

#include <yas/mem_streams.hpp>
#include <yas/binary_iarchive.hpp>
#include <yas/binary_oarchive.hpp>
#include <yas/text_iarchive.hpp>
#include <yas/text_oarchive.hpp>
#include <yas/serializers/std_types_serializers.hpp>
#include <yas/serializers/boost_types_serializers.hpp>

#include "Common.hpp"
#include "Flat.hpp"
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
    return make_shared<BufferPackedValue>(std::move(buf), format());
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
class YASCodec : public Codec {
 public:
  virtual shared_ptr<PackedValue> pack(const NativeValue& nv) {
    yas::mem_ostream os;
    yas::binary_oarchive<yas::mem_ostream> oa(os);
    oa & *nv.asConst<T>();
    return make_shared<YASPackedValue>(os.get_shared_buffer(), format_);
  }

  virtual shared_ptr<NativeValue> unpack(const PackedValue& pv) {
    yas::mem_istream is(pv.buf(), pv.length());
    yas::binary_iarchive<yas::mem_istream> ia(is);
    T t;
    ia & t;
    return make_shared<TNativeValue<T>>(std::move(t));
  }

  virtual CodecFormat format() { return format_; }

 protected:
  CodecFormat format_ = CodecFormat::YASBinary;
};

template <class T>
class CSVCodec : public Codec {
 public:
  virtual shared_ptr<PackedValue> pack(const NativeValue& nv) {
    auto buf = Serialization::pack_csv<T>(*(nv.asConst<T>()));
    return make_shared<BufferPackedValue>(std::move(buf), format());
  }

  virtual shared_ptr<NativeValue> unpack(const PackedValue& pv) {
    auto t = Serialization::unpack_csv<T>(pv.buf(), pv.length());
    return make_shared<TNativeValue<T>>(t);
  }

  virtual CodecFormat format() { return format_; }

 protected:
  CodecFormat format_ = CodecFormat::CSV;
};

template <class T>
std::enable_if_t<is_flat<T>::value, shared_ptr<Codec>> makeCSVCodec() {
  return make_shared<CSVCodec<T>>();
}

template <class T>
std::enable_if_t<!is_flat<T>::value, shared_ptr<Codec>> makeCSVCodec() {
  throw std::runtime_error("Invalid csv type");
}

template <typename T>
shared_ptr<Codec> Codec::getCodec(CodecFormat format) {
  switch (format) {
    case CodecFormat::YASBinary:
      return make_shared<YASCodec<T>>();
    case CodecFormat::BoostBinary:
      return make_shared<BoostCodec<T>>();
    case CodecFormat::CSV:
      return makeCSVCodec<T>();
    default:
      throw std::runtime_error("Unrecognized codec format");
  }
}

}  // namespace K3

#endif
