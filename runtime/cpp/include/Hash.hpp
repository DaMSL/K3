#ifndef K3_HASH
#define K3_HASH

#include <tuple>

#include "boost/functional/hash.hpp"

#include "types/BaseString.hpp"
#include "Common.hpp"

namespace std {
// Unit
template <>
struct hash<K3::unit_t> {
  size_t operator()(const K3::unit_t&) const { return 0; }
};

// Address
template <>
struct hash<K3::Address> {
  size_t operator()(const K3::Address& b) const {
    const unsigned int fnv_prime = 0x811C9DC5;
    unsigned int hash = 0;
    const char* p = (const char*)&(b.ip);
    for (std::size_t i = 0; i < sizeof(unsigned long); i++) {
      hash *= fnv_prime;
      hash ^= p[i];
    }
    p = (const char*)&(b.port);
    for (std::size_t i = 0; i < sizeof(unsigned short); i++) {
      hash *= fnv_prime;
      hash ^= p[i];
    }
    return hash;
  }
};

// String
template <>
struct hash<K3::base_string> {
  size_t operator()(const K3::base_string& s) const {
    std::size_t seed = 0;
    std::size_t len = s.length();
    auto s_c = s.c_str();
    for (int i = 0; i < len; i++) {
      hash_combine(seed, s_c[i]);
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

// Boost-hash compatibility:
namespace K3 {
template <class T>
std::size_t hash_value(T const& b) {
  std::hash<T> hasher;
  return hasher(b);
}
}
#endif
