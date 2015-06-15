#ifndef K3_CODEC
#define K3_CODEC

// Codec is an interface for conversion between Packed and Native Values.

#include <memory>
#include <string>

#include "yas/mem_streams.hpp"
#include "yas/binary_iarchive.hpp"
#include "yas/binary_oarchive.hpp"
#include "yas/text_iarchive.hpp"
#include "yas/text_oarchive.hpp"
#include "yas/serializers/std_types_serializers.hpp"
#include "yas/serializers/boost_types_serializers.hpp"

#include "boost/archive/binary_oarchive.hpp"
#include "boost/archive/binary_iarchive.hpp"
#include "boost/archive/text_oarchive.hpp"
#include "boost/archive/text_iarchive.hpp"
#include "boost/serialization/split_free.hpp"
#include "boost/iostreams/stream_buffer.hpp"
#include "boost/iostreams/stream.hpp"
#include "boost/iostreams/device/back_inserter.hpp"
#include "boost/serialization/vector.hpp"
#include "boost/serialization/set.hpp"
#include "boost/serialization/list.hpp"
#include "boost/serialization/base_object.hpp"
#include "boost/serialization/nvp.hpp"

#include "csvpp/csv.h"
#include "csvpp/string.h"
#include "csvpp/array.h"
#include "csvpp/deque.h"
#include "csvpp/list.h"
#include "csvpp/set.h"
#include "csvpp/vector.h"

#include "Common.hpp"
#include "Flat.hpp"
#include "types/Value.hpp"
#include "serialization/Boost.hpp"
#include "serialization/YAS.hpp"

namespace K3 {

// Codec Interface
class Codec {
 public:
  virtual shared_ptr<PackedValue> pack(const NativeValue&) = 0;
  virtual shared_ptr<NativeValue> unpack(const PackedValue& pv) = 0;
  virtual CodecFormat format() = 0;

  template <typename T>
  static shared_ptr<Codec> getCodec(CodecFormat format);
  static CodecFormat getFormat(const string& s);
};

// Boost Codec
// TODO(jbw) Add a text archive based codec?
namespace io = boost::iostreams;
typedef io::stream<io::back_insert_device<Buffer>> OByteStream;
template <class T>
class BoostCodec : public Codec {
 public:
  virtual shared_ptr<PackedValue> pack(const NativeValue& nv) {
    Buffer buf;
    OByteStream output_stream(buf);
    boost::archive::binary_oarchive oa(output_stream);
    oa << *nv.asConst<T>();
    output_stream.flush();
    return make_shared<BufferPackedValue>(std::move(buf), format());
  }

  virtual shared_ptr<NativeValue> unpack(const PackedValue& pv) {
    io::basic_array_source<char> source(pv.buf(), pv.length());
    io::stream<io::basic_array_source<char>> input_stream(source);
    boost::archive::binary_iarchive ia(input_stream);

    T t;
    ia >> t;
    return make_shared<TNativeValue<T>>(std::move(t));
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
    oa&* nv.asConst<T>();
    return make_shared<YASPackedValue>(os.get_shared_buffer(), format_);
  }

  virtual shared_ptr<NativeValue> unpack(const PackedValue& pv) {
    yas::mem_istream is(pv.buf(), pv.length());
    yas::binary_iarchive<yas::mem_istream> ia(is);
    T t;
    ia& t;
    return make_shared<TNativeValue<T>>(std::move(t));
  }

  virtual CodecFormat format() { return format_; }

 protected:
  CodecFormat format_ = CodecFormat::YASBinary;
};

template <class T, char sep = ','>
class CSVCodec : public Codec {
 public:
  virtual shared_ptr<PackedValue> pack(const NativeValue& nv) {
    Buffer buf;
    OByteStream output_stream(buf);
    csv::writer oa(output_stream, sep);
    oa << *nv.asConst<T>();
    output_stream.flush();
    return make_shared<BufferPackedValue>(std::move(buf), format());
  }

  virtual shared_ptr<NativeValue> unpack(const PackedValue& pv) {
    io::basic_array_source<char> source(pv.buf(), pv.length());
    io::stream<io::basic_array_source<char>> input_stream(source);
    csv::parser ia(input_stream, sep);

    T t;
    ia >> t;
    return make_shared<TNativeValue<T>>(std::move(t));
  }

  virtual CodecFormat format() { return format_; }

 protected:
  CodecFormat format_ = CodecFormat::CSV;
};

template <class T, char sep = ','>
std::enable_if_t<is_flat<T>::value, shared_ptr<Codec>> makeCSVCodec() {
  return make_shared<CSVCodec<T, sep>>();
}

template <class T, char sep = ','>
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
    case CodecFormat::PSV:
      return makeCSVCodec<T, '|'>();
    default:
      throw std::runtime_error("Unrecognized codec format");
  }
}

}  // namespace K3

#endif
