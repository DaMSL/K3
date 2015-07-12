#ifndef K3_HASH_COLLECTIONS
#define K3_HASH_COLLECTIONS

#include "Hash.hpp"
#include "Common.hpp"

namespace std {

// Collections
template <class K3Collection>
size_t hash_collection(K3Collection const& b) {
  size_t seed = 0;
  for (auto& elem : b) {
    hash_combine(seed, elem);
  }
  return seed;
}

template <class Elem>
struct hash<K3::Collection<Elem>> {
  size_t operator()(K3::Collection<Elem> const& b) const {
    return hash_collection(b);
  }
};

template <class Elem>
struct hash<K3::Map<Elem>> {
  size_t operator()(K3::Map<Elem> const& b) const { return hash_collection(b); }
};

template <class Elem>
struct hash<K3::Set<Elem>> {
  size_t operator()(K3::Set<Elem> const& b) const { return hash_collection(b); }
};

template <class Elem>
struct hash<K3::Seq<Elem>> {
  size_t operator()(K3::Seq<Elem> const& b) const { return hash_collection(b); }
};

template <class Elem>
struct hash<K3::Sorted<Elem>> {
  size_t operator()(K3::Sorted<Elem> const& b) const {
    return hash_collection(b);
  }
};

template <class Elem>
struct hash<K3::VMap<Elem>> {
  size_t operator()(K3::VMap<Elem> const& b) const {
    const auto& c = b.getConstContainer();
    size_t seed = 0;
    for (const auto& e : b) {
      hash_combine(seed, e.first);
      hash_combine(seed, e.second);
    }
    return seed;
  }
};

template <class Elem>
struct hash<K3::SortedMap<Elem>> {
  size_t operator()(K3::SortedMap<Elem> const& b) const {
    return hash_collection(b);
  }
};

template <typename... Args>
struct hash<K3::MultiIndexBag<Args...>> {
  size_t operator()(K3::MultiIndexBag<Args...> const& b) {
    return hash_collection(b);
  }
};

template <typename... Args>
struct hash<K3::MultiIndexMap<Args...>> {
  size_t operator()(K3::MultiIndexMap<Args...> const& b) {
    return hash_collection(b);
  }
};

template <typename... Args>
struct hash<K3::MultiIndexVMap<Args...>> {
  size_t operator()(K3::MultiIndexVMap<Args...> const& b) {
    return hash_collection(b);
  }
};

}  // namespace std

#endif
