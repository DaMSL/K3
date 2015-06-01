#ifndef K3_HASH
#define K3_HASH

#include <tuple>

#include "boost/functional/hash.hpp"

#include "Common.hpp"

namespace std {
// Address
template <>
struct hash<K3::Address> {
  size_t operator()(const K3::Address& addr) const {
    size_t seed = 0;
    boost::hash_combine(seed, addr.ip);
    boost::hash_combine(seed, addr.port);
    return seed;
  }
};

// String
template <>
struct hash<K3::base_string> {
  size_t operator()(const K3::base_string& s) const {
    std::size_t seed = 0;
    for (int i =0; i < s.length(); i++) {
      boost::hash_combine(seed, s.c_str()[i]);
    }
    return seed;
  }
};

// Tuple
template <size_t n, typename... T>
typename std::enable_if<(n >= sizeof...(T))>::type hash_tuple(
    size_t&, const std::tuple<T...>&) {}

template <size_t n, typename... T>
typename std::enable_if<(n < sizeof...(T))>::type hash_tuple(
    size_t & seed, const std::tuple<T...>& tup) {
  boost::hash_combine(seed, get<n>(tup));
  hash_tuple<n + 1>(seed, tup);
}

template <typename... T>
struct hash<tuple<T...>> {
  size_t operator()(const tuple<T...>& tup) {
    size_t seed = 0;
    hash_tuple<0>(seed, tup);
    return seed;
  }
};

// Collections
template <class K3Collection>
size_t hash_collection(K3Collection const& b) {
  const auto& c = b.getConstContainer();
  return boost::hash_range(c.begin(), c.end());
}

template <class Elem>
struct hash<K3::Collection<Elem>> {
  size_t operator()(K3::Collection<Elem> const& b) {
    return hash_collection(b);
  }
};

template <class Elem>
struct hash<K3::Map<Elem>> {
  size_t operator()(K3::Map<Elem> const& b) { return hash_collection(b); }
};

template <class Elem>
struct hash<K3::Set<Elem>> {
  size_t operator()(K3::Set<Elem> const& b) { return hash_collection(b); }
};

template <class Elem>
struct hash<K3::Seq<Elem>> {
  size_t operator()(K3::Seq<Elem> const& b) { return hash_collection(b); }
};

template <class Elem>
struct hash<K3::Sorted<Elem>> {
  size_t operator()(K3::Sorted<Elem> const& b) { return hash_collection(b); }
};

}  // namespace std

#endif
