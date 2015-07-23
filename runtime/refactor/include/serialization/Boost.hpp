#ifndef K3_BOOST
#define K3_BOOST

#include <utility>
#include <set>
#include <vector>
#include <list>
#include <tuple>

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

namespace boost {
namespace serialization {

// Elide object class information for std::pair and std::tuple serialization
template <class _T0, class _T1>
class implementation_level<std::pair<_T0, _T1>> {
 public:
  typedef mpl::integral_c_tag tag;
  typedef mpl::int_<object_serializable> type;
  BOOST_STATIC_CONSTANT(int, value = implementation_level::type::value);
};

template <class... _T0>
class implementation_level<std::tuple<_T0...>> {
 public:
  typedef mpl::integral_c_tag tag;
  typedef mpl::int_<object_serializable> type;
  BOOST_STATIC_CONSTANT(int, value = implementation_level::type::value);
};

// Elide object class information for standard collection types.
template <class _T0>
class implementation_level<std::vector<_T0>> {
 public:
  typedef mpl::integral_c_tag tag;
  typedef mpl::int_<object_serializable> type;
  BOOST_STATIC_CONSTANT(int, value = implementation_level::type::value);
};

template <class _T0>
class implementation_level<std::list<_T0>> {
 public:
  typedef mpl::integral_c_tag tag;
  typedef mpl::int_<object_serializable> type;
  BOOST_STATIC_CONSTANT(int, value = implementation_level::type::value);
};

template <class _T0>
class implementation_level<std::multiset<_T0>> {
 public:
  typedef mpl::integral_c_tag tag;
  typedef mpl::int_<object_serializable> type;
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
  ar& b;
  if (b) {
    ar&* sp;
  }
}

template <class archive, class T>
void load(archive& ar, std::shared_ptr<T>& sp, unsigned int) {
  bool b;
  ar& b;
  if (b) {
    T t;
    ar& t;
    sp = std::make_shared<T>(std::move(t));
  }
}

template <class archive, class K, class V>
void save(archive& ar, const std::unordered_map<K, V> m, unsigned int) {
  int size = m.size();
  ar& size;
  for (const auto& it : m) {
    ar& it.first;
    ar& it.second;
  }
}

template <class archive, class K, class V>
void load(archive& ar, std::unordered_map<K, V>& m, unsigned int) {
  int size;
  ar& size;
  m.reserve(size);

  for (int i = 0; i < size; i++) {
    K k;
    ar& k;
    V v;
    ar& v;
    m[std::move(k)] = std::move(v);
  }
}

template <class archive, class V>
void save(archive& ar, const std::unordered_set<V> s, unsigned int) {
  int size = s.size();
  ar& size;
  for (const auto& it : s) {
    ar& it;
  }
}

template <class archive, class V>
void load(archive& ar, std::unordered_set<V>& s, unsigned int) {
  int size;
  ar& size;
  s.reserve(size);

  for (int i = 0; i < size; i++) {
    V v;
    ar& v;
    s.insert(std::move(v));
  }
}

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

}  // namespace serialization
}  // namespace boost

#endif
