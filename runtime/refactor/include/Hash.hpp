#ifndef K3_HASH
#define K3_HASH

#include <tuple>

#include "boost/functional/hash.hpp"

#include "Common.hpp"


namespace std {
// Unit
template <>
struct hash<K3::unit_t> {
  size_t operator()(const K3::unit_t&) const {
    return 0; 
  }
};

  
// Address
template <>
struct hash<K3::Address> {
  size_t operator()(const K3::Address& addr) const {
    size_t seed = 0;
    hash_combine(seed, addr.ip);
    hash_combine(seed, addr.port);
    return seed;
  }
};

// String
template <>
struct hash<K3::base_string> {
  size_t operator()(const K3::base_string& s) const {
    std::size_t seed = 0;
    for (int i = 0; i < s.length(); i++) {
      hash_combine(seed, s.c_str()[i]);
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
    size_t& seed, const std::tuple<T...>& tup) {
  hash_combine(seed, get<n>(tup));
  hash_tuple<n + 1>(seed, tup);
}

template <typename... T>
struct hash<tuple<T...>> {
  size_t operator()(const tuple<T...>& tup) const {
    size_t seed = 0;
    hash_tuple<0>(seed, tup);
    return seed;
  }
};

}  // namespace std

#endif
