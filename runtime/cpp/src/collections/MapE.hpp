#ifndef K3_MAPE
#define K3_MAPE

#include <iterator>
#include <map>

#include <functional>
#include <stdexcept>

#include <boost/serialization/array.hpp>
#include <boost/serialization/binary_object.hpp>
#include <boost/serialization/string.hpp>
#include <csvpp/csv.h>

#include "collections/Collection.hpp"
#include "collections/Map.hpp"

namespace K3 {
template <class R>
class MapE {
  using Key = typename R::KeyType;
  using Value = typename R::ValueType;
  using Container = std::unordered_map<Key, R>;

 public:
  // Default Constructor
  MapE() : container() {}
  MapE(const std::unordered_map<Key, R>& con) : container(con) {}
  MapE(std::unordered_map<Key, R>&& con) : container(std::move(con)) {}

  // Construct from (container) iterators
  template <typename Iterator>
  MapE(Iterator begin, Iterator end)
      : container(begin, end) {}

  template <class Pair>
  R elemToRecord(const Pair& e) const {
    return e.second;
  }

  template <class I>
  class map_iterator : public std::iterator<std::forward_iterator_tag, R> {
    using container = std::unordered_map<Key, R>;
    using reference =
        typename std::iterator<std::forward_iterator_tag, R>::reference;

   public:
    template <class _I>
    map_iterator(_I&& _i)
        : i(std::forward<_I>(_i)) {}

    map_iterator& operator++() {
      ++i;
      return *this;
    }

    map_iterator operator++(int) {
      map_iterator t = *this;
      *this ++;
      return t;
    }

    auto operator -> () const { return &(i->second); }

    auto& operator*() const { return i->second; }

    bool operator==(const map_iterator& other) const { return i == other.i; }

    bool operator!=(const map_iterator& other) const { return i != other.i; }

   private:
    I i;
  };

  using iterator = map_iterator<typename std::unordered_map<Key, R>::iterator>;
  using const_iterator =
      map_iterator<typename std::unordered_map<Key, R>::const_iterator>;

  iterator begin() { return iterator(container.begin()); }

  iterator end() { return iterator(container.end()); }

  const_iterator begin() const { return const_iterator(container.cbegin()); }

  const_iterator end() const { return const_iterator(container.cend()); }

  // Functionality
  int size(unit_t) const { return container.size(); }

  unit_t clear(const unit_t&) {
    container.clear();
    return unit_t();
  }

  template <typename F, typename G>
  auto peek(F f, G g) const {
    auto it = container.begin();
    if (it == container.end()) {
      return f(unit_t{});
    } else {
      return g(it->second);
    }
  }

  // Map retrieval.
  // For a MapE, these methods expect a key argument instead of a key-value
  // struct.

  template <typename K>
  bool member(const K& k) const {
    return container.find(k.key) != container.end();
  }

  template <typename K, class F, class G>
  RT<G,R> lookup(K const& k, F f, G g) const {
    auto it = container.find(k.key);
    if (it == container.end()) {
      return f(unit_t{});
    } else {
      return g(it->second);
    }
  }

  template <class Q>
  unit_t insert(Q&& q) {
    container[q.key] = std::forward<Q>(q);
    return unit_t();
  }

  template <typename K, typename V>
  unit_t update(const K& k, V&& val) {
    auto it = container.find(k.key);
    if (it != container.end()) {
      it->second.value = std::move(val.value);
    }
    return unit_t();
  }

  template <typename K>
  unit_t erase(const K& k) {
    container.erase(k.key);
    return unit_t();
  }

  template <typename K, typename F, typename G>
  RT<G,R> erase_with(const K& k, F f, G g) {
    auto existing = container.find(k.key);
    if (existing == std::end(container)) {
      return f(unit_t{});
    } else {
      auto t = g(std::move(existing->second));
      container.erase(k.key);
      return t;
    }
  }

  template <class F>
  unit_t insert_with(const R& rec, F f) {
    auto existing = container.find(rec.key);
    if (existing == std::end(container)) {
      container.insert(existing, std::make_pair(rec.key, rec));
    } else {
      existing->second = f(std::move(existing->second), rec);
    }

    return unit_t{};
  }

  template <typename K, typename F, typename G>
  unit_t upsert_with(const K& k, F f, G g) {
    auto existing = container.find(k.key);
    if (existing == std::end(container)) {
      container.insert(existing, std::make_pair(k.key, f(unit_t{})));
    } else {
      existing->second = g(std::move(existing->second));
    }

    return unit_t{};
  }

  ////////////////////////////////////////////////////////
  // Bulk transformations.

  MapE combine(const MapE& other) const {
    // copy this DS
    MapE result = MapE(*this);
    // copy other DS
    for (const auto& p : other.container) {
      result.container[p.first] = p.second;
    }
    return result;
  }

  tuple<MapE, MapE> split(const unit_t&) const {
    // Find midpoint
    const size_t size = container.size();
    const size_t half = size / 2;
    // Setup iterators
    auto beg = container.begin();
    auto mid = container.begin();
    std::advance(mid, half);
    auto end = container.end();
    // Construct DS from iterators
    return std::make_tuple(MapE(beg, mid), MapE(mid, end));
  }

  template <typename Fun>
  unit_t iterate(Fun f) const {
    for (const auto& p : container) {
      f(p.second);
    }
    return unit_t();
  }

  template <typename Fun>
  auto map(Fun f) const -> K3::MapE<RT<Fun, R>> {
    K3::MapE<RT<Fun, R>> result;
    for (const auto& p : container) {
      result.insert(f(p.second));
    }
    return result;
  }

  template <typename Fun>
  auto map_generic(Fun f) const -> K3::Map<RT<Fun, R>> {
    K3::Map<RT<Fun, R>> result;
    for (const auto& p : container) {
      result.insert(f(p.second));
    }
    return result;
  }

  template <typename Fun>
  MapE<R> filter(Fun predicate) const {
    MapE<R> result;
    for (const auto& p : container) {
      if (predicate(p.second)) {
        result.insert(p.second);
      }
    }
    return result;
  }

  template <typename Fun, typename Acc>
  Acc fold(Fun f, Acc acc) const {
    for (const auto& p : container) {
      acc = f(std::move(acc), p.second);
    }
    return acc;
  }

  template <typename F1, typename F2, typename Z>
  MapE<R_key_value<RT<F1, R>, Z>>
  group_by(F1 grouper, F2 folder, const Z& init) const
  {
    // Create a map to hold partial results
    typedef RT<F1, R> K;
    std::unordered_map<K, Z> accs;

    for (const auto& it : container) {
      K key = grouper(it.second);
      if (accs.find(key) == accs.end()) {
        accs[key] = init;
      }
      accs[key] = folder(std::move(accs[key]), it.second);
    }

    MapE<R_key_value<K, Z>> result;
    for (auto&& it : accs) {
      result.insert(std::move(
          R_key_value<K, Z>{std::move(it.first), std::move(it.second)}));
    }
    return result;
  }

  template <typename F1, typename F2, typename Z>
  Map<R_key_value<RT<F1, R>, Z>>
  group_by_generic(F1 grouper, F2 folder, const Z& init) const
  {
    // Create a map to hold partial results
    typedef RT<F1, R> K;
    std::unordered_map<K, Z> accs;

    for (const auto& it : container) {
      K key = grouper(it.second);
      if (accs.find(key) == accs.end()) {
        accs[key] = init;
      }
      accs[key] = folder(std::move(accs[key]), it.second);
    }

    Map<R_key_value<K, Z>> result;
    for (auto&& it : accs) {
      result.insert(std::move(
          R_key_value<K, Z>{std::move(it.first), std::move(it.second)}));
    }
    return result;
  }

  template <class Fun>
  auto ext(Fun expand) const -> MapE<typename RT<Fun, R>::ElemType> {
    typedef typename RT<Fun, R>::ElemType T;
    MapE<T> result;
    for (const auto& it : container) {
      for (auto&& it2 : expand(it.second).container) {
        result.insert(std::move(it2.second));
      }
    }

    return result;
  }

  template <class Fun>
  auto ext_generic(Fun expand) const -> Map<typename RT<Fun, R>::ElemType> {
    typedef typename RT<Fun, R>::ElemType T;
    Map<T> result;
    for (const auto& it : container) {
      for (auto&& it2 : expand(it.second).container) {
        result.insert(std::move(it2.second));
      }
    }

    return result;
  }

  // Mosaic-specific functionality.

  template<class Other, class OtherKeyFun, class Folder, class Acc>
  Acc equijoinkf_kv(const Collection<Other>& other, OtherKeyFun keyf, Folder f, Acc acc) const
  {
    // Probe and accumulate.
    for (const auto& otherelem : other.getConstContainer()) {
      RT<OtherKeyFun, Other> key(keyf(otherelem));
      auto it = container.find(key);
      if ( it != container.end() ) {
        acc = f(std::move(acc), it->second, otherelem);
      }
    }
    return acc;
  }

  // Comparators.
  bool operator==(const MapE& other) const {
    return container == other.container;
  }

  bool operator!=(const MapE& other) const {
    return container != other.container;
  }

  bool operator<(const MapE& other) const {
    return container < other.container;
  }

  bool operator>(const MapE& other) const {
    return container > other.container;
  }

  Container& getContainer() { return container; }

  const Container& getConstContainer() const { return container; }

  template <class Archive>
  void serialize(Archive& ar) {
    ar& container;
  }

  template <class Archive>
  void serialize(Archive& ar, const unsigned int) {
    ar& boost::serialization::make_nvp("__K3MapE", container);
  }

 protected:
  Container container;

 private:
  friend class boost::serialization::access;
};  // class MapE

}  // namespace K3

namespace YAML {
template <class R>
struct convert<K3::MapE<R>> {
  static Node encode(const K3::MapE<R>& c) {
    Node node;
    auto container = c.getConstContainer();
    if (container.size() > 0) {
      for (auto i : container) {
        node.push_back(convert<R>::encode(i.second));
      }
    } else {
      node = YAML::Load("[]");
    }
    return node;
  }

  static bool decode(const Node& node, K3::MapE<R>& c) {
    for (auto& i : node) {
      c.insert(i.as<R>());
    }
    return true;
  }
};
}

namespace JSON {
template <class E>
struct convert<K3::MapE<E>> {
  template <class Allocator>
  static Value encode(const K3::MapE<E>& c, Allocator& al) {
    Value v;
    v.SetObject();
    v.AddMember("type", Value("MapE"), al);
    Value inner;
    inner.SetArray();
    for (const auto& e : c.getConstContainer()) {
      inner.PushBack(convert<E>::encode(e.second, al), al);
    }
    v.AddMember("value", inner.Move(), al);
    return v;
  }
};
}

#endif
