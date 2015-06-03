#ifndef K3_SERIALIZATION
#define K3_SERIALIZATION

// Serialization between Native C++ types and Binary data

#include <vector>
#include <iostream>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <sstream>

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

#include <yas/mem_streams.hpp>
#include <yas/binary_iarchive.hpp>
#include <yas/binary_oarchive.hpp>
#include <yas/text_iarchive.hpp>
#include <yas/text_oarchive.hpp>
#include <yas/serializers/std_types_serializers.hpp>
#include <yas/serializers/boost_types_serializers.hpp>

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

// Boost Text serialization
template <class T>
Buffer pack_text(const T& t) {
  Buffer buf;
  OByteStream output_stream(buf);
  boost::archive::text_oarchive oa(output_stream);
  oa << t;
  output_stream.flush();
  return buf;
}

template <class T>
T unpack_text(const char* buf, size_t len) {
  io::basic_array_source<char> source(buf, len);
  io::stream<io::basic_array_source<char> > input_stream(source);
  boost::archive::text_iarchive ia(input_stream);

  T t;
  ia >> t;
  return t;
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

// Elide object class information for std::pair and std::tuple serialization
template <class _T0, class _T1>
class implementation_level<std::pair<_T0, _T1>> {
  public:
    typedef  mpl::integral_c_tag tag;
    typedef  mpl::int_<object_serializable> type;
    BOOST_STATIC_CONSTANT(int, value = implementation_level::type::value);
};

template <class... _T0>
class implementation_level<std::tuple<_T0...>> {
  public:
    typedef  mpl::integral_c_tag tag;
    typedef  mpl::int_<object_serializable> type;
    BOOST_STATIC_CONSTANT(int, value = implementation_level::type::value);
};

// Elide object class information for standard collection types.
template <class _T0>
class implementation_level<std::vector<_T0>> {
  public:
    typedef  mpl::integral_c_tag tag;
    typedef  mpl::int_<object_serializable> type;
    BOOST_STATIC_CONSTANT(int, value = implementation_level::type::value);
};

template <class _T0>
class implementation_level<std::list<_T0>> {
  public:
    typedef  mpl::integral_c_tag tag;
    typedef  mpl::int_<object_serializable> type;
    BOOST_STATIC_CONSTANT(int, value = implementation_level::type::value);
};


template <class _T0>
class implementation_level<std::multiset<_T0>> {
  public:
    typedef  mpl::integral_c_tag tag;
    typedef  mpl::int_<object_serializable> type;
    BOOST_STATIC_CONSTANT(int, value = implementation_level::type::value);
};

template <uint N>
struct tuple_serializer {
  template <class archive, class... args>
  static void serialize(archive& a, std::tuple<args...>& t,
                        const unsigned int version) {
    tuple_serializer<N - 1>::serialize(a, t, version);
    a& std::get<N - 1>(t);
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

template <class archive, class T>
void save(archive& ar, const std::shared_ptr<T>& sp, unsigned int) {
  bool b = sp ? true : false;
  ar & b;
  if (b) {
    ar & *sp;
  }
}

template <class archive, class T>
void load(archive& ar, std::shared_ptr<T>& sp, unsigned int) {
  bool b;
  ar & b;
  if (b) {
    T t;
    ar & t;
    sp = std::make_shared<T>(std::move(t));
  }
}

template <class archive, class K, class V>
void save(archive& ar, const std::unordered_map<K, V> m, unsigned int) {
  int size = m.size();
  ar & size;
  for (const auto& it : m) {
    ar & it.first;
    ar & it.second;
  }
}

template <class archive, class K, class V>
void load(archive& ar, std::unordered_map<K, V>& m, unsigned int) {
  int size; 
  ar & size;
  m.reserve(size);

  for (int i=0; i < size; i++) {
    K k;
    ar & k;
    V v;
    ar & v;
    m[std::move(k)] = std::move(v);
  }
}

template <class archive, class V>
void save(archive& ar, const std::unordered_set<V> s, unsigned int) {
  int size = s.size();
  ar & size;
  for (const auto& it : s) {
    ar & it;
  }
}

template <class archive, class V>
void load(archive& ar, std::unordered_set<V>& s, unsigned int) {
  int size; 
  ar & size;
  s.reserve(size);

  for (int i=0; i < size; i++) {
    V v;
    ar & v;
    s.insert(std::move(v));
  }
}

}  // namespace serialization
}  // namespace boost


namespace boost {
namespace serialization {

template <class Archive, class T>
inline void serialize(Archive& ar, std::shared_ptr<T>& t,
                      const unsigned int file_version) {
  split_free(ar, t, file_version);
}

template <class Archive, class K, class V>
inline void serialize(Archive& ar, std::unordered_map<K, V>& m,
                      const unsigned int file_version) {
  split_free(ar, m, file_version);
}

template <class Archive, class V>
inline void serialize(Archive& ar, std::unordered_set<V>& s,
                      const unsigned int file_version) {
  split_free(ar, s, file_version);
}

}
}


////////////////////////////////////
//
// Yas serialization for K3 types.
//
namespace yas { namespace detail {

template<typename archive_type, typename T>
void serialize(archive_type& ar, const std::shared_ptr<T>& sp) {
  bool b = sp ? 1 : 0;
  ar & b;
  if (b) {
    ar & *sp;
  }
}

template<typename archive_type, typename T>
void serialize(archive_type& ar, std::shared_ptr<T>& sp) {
  bool b;
  ar & b;
  if (b) {
    T t;
    ar & t;
    sp = std::make_shared<T>(std::move(t));
  }
}

template<typename archive_type, typename T>
void serialize(archive_type& ar, const boost::serialization::nvp<T>& nvp) {
  ar & nvp.const_value();
}

template<typename archive_type, typename T>
void serialize(archive_type& ar, boost::serialization::nvp<T>& nvp) {
  ar & nvp.value();
}

} // namespace detail
} // namespace yas

#endif
