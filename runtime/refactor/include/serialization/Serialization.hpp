#ifndef K3_SERIALIZATION
#define K3_SERIALIZATION

// Serialization between Native C++ types and Binary data

#include <vector>
#include <iostream>
#include <tuple>

#include "boost/archive/binary_oarchive.hpp"
#include "boost/archive/binary_iarchive.hpp"
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

namespace K3 {
namespace Serialization {

namespace io = boost::iostreams;
typedef io::stream<io::back_insert_device<Buffer> > OByteStream;

// Boost Binary serialization
template <class T>
Buffer pack(const T& t) {
  Buffer buf;
  OByteStream output_stream(buf);
  boost::archive::binary_oarchive oa(output_stream);
  oa << t;
  output_stream.flush();
  return buf;
}

template <class T>
T unpack(const char* buf, size_t len) {
  io::basic_array_source<char> source(buf, len);
  io::stream<io::basic_array_source<char> > input_stream(source);
  boost::archive::binary_iarchive ia(input_stream);

  T t;
  ia >> t;
  return t;
}

template <class T>
T unpack(const Buffer& buf) {
  return unpack<T>(&buf[0], buf.size());
}

// Csvpp serialization
template <typename T>
Buffer pack_csv(const T& t, char sep = ',') {
  // TODO(jbw) nvp?
  Buffer buf;
  OByteStream output_stream(buf);
  csv::writer oa(output_stream, sep);
  oa << t;
  output_stream.flush();
  return buf;
}

template <typename T>
T unpack_csv(const char* buf, size_t len, char sep = ',') {
  io::basic_array_source<char> source(buf, len);
  io::stream<io::basic_array_source<char> > input_stream(source);
  csv::parser ia(input_stream, sep);

  T t;
  ia >> t;
  return t;
}

template <class T>
T unpack_csv(const Buffer& buf) {
  return unpack_csv<T>(&buf[0], buf.size());
}

}  // namespace Serialization
}  // namespace K3

namespace boost {
namespace serialization {

template <uint N>
struct tuple_serializer {
  template <class archive, class... args>
  static void serialize(archive& a, std::tuple<args...>& t,
                        const unsigned int version) {
    a& std::get<N - 1>(t);
    tuple_serializer<N - 1>::serialize(a, t, version);
  }
};

template <>
struct tuple_serializer<0> {
  template <class archive, class... args>
  static void serialize(archive&, std::tuple<args...>&, const unsigned int) {}
};

template <class archive, class... args>
void serialize(archive& a, std::tuple<args...>& t, const unsigned int version) {
  tuple_serializer<sizeof...(args)>::serialize(a, t, version);
}

}  // namespace serialization
}  // namespace boost

#endif
