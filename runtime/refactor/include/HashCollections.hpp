#ifndef K3_HASH_COLLECTIONS
#define K3_HASH_COLLECTIONS

#include "boost/functional/hash.hpp"

#include "Common.hpp"

namespace std {

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
