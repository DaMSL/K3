#ifndef K3_CODEC
#define K3_CODEC

#include <csvpp/csv.h>
#include <csvpp/string.h>
#include <csvpp/array.h>
#include <csvpp/deque.h>
#include <csvpp/list.h>
#include <csvpp/set.h>
#include <csvpp/vector.h>

#include <memory>
#include <string>

#include <yas/mem_streams.hpp>
#include <yas/binary_iarchive.hpp>
#include <yas/binary_oarchive.hpp>
#include <yas/text_iarchive.hpp>
#include <yas/text_oarchive.hpp>
#include <yas/serializers/std_types_serializers.hpp>
#include <yas/serializers/boost_types_serializers.hpp>

#include <boost/archive/binary_oarchive.hpp>
#include <boost/archive/binary_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/serialization/split_free.hpp>
#include <boost/iostreams/stream_buffer.hpp>
#include <boost/iostreams/stream.hpp>
#include <boost/iostreams/device/back_inserter.hpp>
#include <boost/serialization/vector.hpp>
#include <boost/serialization/set.hpp>
#include <boost/serialization/list.hpp>
#include <boost/serialization/base_object.hpp>
#include <boost/serialization/nvp.hpp>

#include "Common.hpp"
#include "Flat.hpp"
#include "types/Value.hpp"
#include "serialization/Boost.hpp"
#include "serialization/YAS.hpp"

namespace K3 {

  namespace Codec {
    CodecFormat getFormat(const string& s);
  }
  namespace io = boost::iostreams;
  typedef io::stream<io::back_insert_device<Buffer>> OByteStream;
namespace boost_ser {

  template <class T>
  unique_ptr<PackedValue> pack(const T& t, CodecFormat format) {
    Buffer buf;
    OByteStream output_stream(buf);
    boost::archive::binary_oarchive oa(output_stream);
    oa << t;
    output_stream.flush();
    return make_unique<BufferPackedValue>(std::move(buf), format);
  }

  template <class T>
  unique_ptr<T> unpack(const PackedValue& pv) {
    io::basic_array_source<char> source(pv.buf(), pv.length());
    io::stream<io::basic_array_source<char>> input_stream(source);
    boost::archive::binary_iarchive ia(input_stream);
    T t;
    ia >> t;
    return make_unique<T>(std::move(t));
  }
}

namespace yas_ser {
  template <class T>
  unique_ptr<PackedValue> pack(const T& t, CodecFormat format) {
    ::yas::mem_ostream os;
    ::yas::binary_oarchive<::yas::mem_ostream> oa(os);
    oa& t;
    return make_unique<YASPackedValue>(os.get_shared_buffer(), format);
  }

  template <class T>
  unique_ptr<T> unpack(const PackedValue& pv) {
    ::yas::mem_istream is(pv.buf(), pv.length());
    ::yas::binary_iarchive<::yas::mem_istream> ia(is);
    T t;
    ia& t;
    return make_unique<T>(std::move(t));
  }
}

namespace csvpp_ser {
  template <class T, char sep>
  std::enable_if_t<is_flat<T>::value, unique_ptr<PackedValue>> pack(const T& t, CodecFormat format) {
    Buffer buf;
    OByteStream output_stream(buf);
    csv::writer oa(output_stream, sep);
    oa << t;
    output_stream.flush();
    return make_unique<BufferPackedValue>(std::move(buf), format);
  }

  template <class T, char sep>
  std::enable_if_t<!is_flat<T>::value, unique_ptr<PackedValue>> pack(const T& t, CodecFormat format) {
    throw std::runtime_error("CSV pack error: value is not flat");
  }

  template <class T, char sep>
  std::enable_if_t<is_flat<T>::value, unique_ptr<T>> unpack(const PackedValue& pv) {
    io::basic_array_source<char> source(pv.buf(), pv.length());
    io::stream<io::basic_array_source<char>> input_stream(source);
    csv::parser ia(input_stream, sep);

    T t;
    ia >> t;
    return make_unique<T>(std::move(t));
  }

  template <class T, char sep>
  std::enable_if_t<!is_flat<T>::value, unique_ptr<T>> unpack(const PackedValue& pv) {
    throw std::runtime_error("CSV unpack error: value is not flat");
  }
}

template <class T>
unique_ptr<PackedValue> pack(const T& t, CodecFormat format) {
  switch (format) {
    case CodecFormat::YASBinary:
      return yas_ser::pack<T>(t, format);
    case CodecFormat::BoostBinary:
      return boost_ser::pack<T>(t, format);
    case CodecFormat::CSV:
      return csvpp_ser::pack<T, ','>(t, format);
    case CodecFormat::PSV:
      return csvpp_ser::pack<T, '|'>(t, format);
    default:
      throw std::runtime_error("Unrecognized codec format");
  }
}

template <class T>
unique_ptr<T> unpack(unique_ptr<PackedValue> t) {
  switch (t->format()) {
    case CodecFormat::YASBinary:
      return yas_ser::unpack<T>(*t);
    case CodecFormat::BoostBinary:
      return boost_ser::unpack<T>(*t);
    case CodecFormat::CSV:
      return csvpp_ser::unpack<T, ','>(*t);
    case CodecFormat::PSV:
      return csvpp_ser::unpack<T, '|'>(*t);
    default:
      throw std::runtime_error("Unrecognized codec format");
  }
}

template <class T>
unique_ptr<T> unpack(const PackedValue& t) {
  switch (t.format()) {
    case CodecFormat::YASBinary:
      return yas_ser::unpack<T>(t);
    case CodecFormat::BoostBinary:
      return boost_ser::unpack<T>(t);
    case CodecFormat::CSV:
      return csvpp_ser::unpack<T, ','>(t);
    case CodecFormat::PSV:
      return csvpp_ser::unpack<T, '|'>(t);
    default:
      throw std::runtime_error("Unrecognized codec format");
  }
}

unique_ptr<string> steal(unique_ptr<PackedValue> t);

template <>
unique_ptr<string> unpack(unique_ptr<PackedValue> t);

}  // namespace K3

#endif
