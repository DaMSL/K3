#ifndef K3_MAPCE
#define K3_MAPCE

#include <iterator>
#include <map>

#include <functional>
#include <stdexcept>

#include <boost/serialization/array.hpp>
#include <boost/serialization/binary_object.hpp>
#include <boost/serialization/string.hpp>
#include <csvpp/csv.h>

#include "collections/Map.hpp"

namespace K3 {
template <class R>
class MapCE {

 public:
  using Key   = typename R::KeyType;
  using Value = typename R::ValueType;
  using Container = std::unordered_map<Key, Value>;
  using Elem  = typename Container::value_type;

  // Default Constructor
  MapCE() : container() {}
  MapCE(const std::unordered_map<Key, Value>& con) : container(con) {}
  MapCE(std::unordered_map<Key, Value>&& con) : container(std::move(con)) {}

  // Construct from (container) iterators
  template <typename Iterator>
  MapCE(Iterator begin, Iterator end)
      : container(begin, end) {}

  template <class I>
  class map_iterator : public std::iterator<std::forward_iterator_tag, Value> {
    using container = std::unordered_map<Key, Value>;
    using reference = typename std::iterator<std::forward_iterator_tag, Value>::reference;

   public:
    template <class _I>
    map_iterator(_I&& _i) : i(std::forward<_I>(_i)) {}

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

  using iterator = map_iterator<typename std::unordered_map<Key, Value>::iterator>;
  using const_iterator = map_iterator<typename std::unordered_map<Key, Value>::const_iterator>;

  iterator begin() { return iterator(container.begin()); }
  iterator end() { return iterator(container.end()); }

  const_iterator begin() const { return const_iterator(container.cbegin()); }
  const_iterator end() const { return const_iterator(container.cend()); }

  R elemToRecord(const Elem& e) const { return R(e.first, e.second); }

  // Functionality
  int size(unit_t) const { return container.size(); }

  template <typename F, typename G>
  auto peek(F f, G g) const {
    auto it = container.begin();
    if (it == container.end()) {
      return f(unit_t{});
    } else {
      return g(it->first, it->second);
    }
  }

  // Map retrieval.

  template <typename K>
  bool member(const K& k) const {
    return container.find(k) != container.end();
  }

  template <typename K, class F, class G>
  RT<G,Value> lookup(const K& k, F f, G g) const {
    auto it = container.find(k);
    if (it == container.end()) {
      return f(unit_t{});
    } else {
      return g(it->second);
    }
  }

  template <class Q>
  unit_t insert(Q&& q) {
    container[q.key] = std::forward<Q>(q.value);
    return unit_t();
  }

  template <typename K, typename V>
  unit_t update(const K& k, V&& val) {
    auto it = container.find(k);
    if (it != container.end()) {
      container[k] = std::move(val);
    }
    return unit_t();
  }

  template <typename K>
  unit_t erase(const K& k) {
    container.erase(k);
    return unit_t();
  }

  template <typename K, typename V, class F>
  unit_t insert_with(const K& k, V&& val, F f) {
    auto existing = container.find(k);
    if (existing == std::end(container)) {
      container[k] = std::move(val);
    } else {
      container[k] = f(std::move(existing->second), std::move(val));
    }
    return unit_t{};
  }


  template <typename K, typename V, class F>
  unit_t insert_with(const K& k, const V& val, F f) {
    auto existing = container.find(k);
    if (existing == std::end(container)) {
      container[k] = val;
    } else {
      container[k] = f(std::move(existing->second), val);
    }
    return unit_t{};
  }

  template <typename K, typename F, typename G>
  unit_t upsert_with(const K& k, F f, G g) {
    auto existing = container.find(k);
    if (existing == std::end(container)) {
      container[k] = f(unit_t{});
    } else {
      container[k] = g(std::move(existing->second));
    }
    return unit_t{};
  }

  ////////////////////////////////////////////////////////
  // Bulk transformations.

  MapCE combine(const MapCE& other) const {
    // copy this DS
    MapCE result = MapCE(*this);
    // copy other DS
    for (const auto& p : other.container) {
      result.container[p.first] = p.second;
    }
    return result;
  }

  tuple<MapCE, MapCE> split(const unit_t&) const {
    // Find midpoint
    const size_t size = container.size();
    const size_t half = size / 2;
    // Setup iterators
    auto beg = container.begin();
    auto mid = container.begin();
    std::advance(mid, half);
    auto end = container.end();
    // Construct DS from iterators
    return std::make_tuple(MapCE(beg, mid), MapCE(mid, end));
  }

  template <typename Fun>
  unit_t iterate(Fun f) const {
    for (const auto& p : container) {
      f(p.second);
    }
    return unit_t();
  }

  template <typename Fun>
  auto map(Fun f) const -> K3::MapCE<RT<RT<Fun, Key>, Value>> {
    K3::MapCE<RT<RT<Fun, Key>, Value>> result;
    for (const auto& p : container) {
      result.insert(f(p.first, p.second));
    }
    return result;
  }

  template <typename Fun>
  auto map_generic(Fun f) const -> K3::Map<RT<RT<Fun, Key>, Value>> {
    K3::MapCE<RT<RT<Fun, Key>, Value>> result;
    for (const auto& p : container) {
      result.insert(f(p.first, p.second));
    }
    return result;
  }

  template <typename Fun>
  MapCE<R> filter(Fun predicate) const {
    MapCE<R> result;
    for (const auto& p : container) {
      if (predicate(p.first, p.second)) {
        result.insert(p.first, p.second);
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
  MapCE<R_key_value<RT<RT<F1, Key>, Value>, Z>>
  group_by(F1 grouper, F2 folder, const Z& init) const
  {
    // Create a map to hold partial results
    typedef RT<RT<F1, Key>, Value> K;
    MapCE<R_key_value<K, Z>> result;

    for (const auto& it : container) {
      K key = grouper(it.first);
      if (result.find(key) == result.end()) {
        result[key] = init;
      }
      result[key] = folder(std::move(result[key]), it.first, it.second);
    }
    return result;
  }

  template <typename F1, typename F2, typename Z>
  Map<R_key_value<RT<RT<F1, Key>, Value>, Z>>
  group_by_generic(F1 grouper, F2 folder, const Z& init) const
  {
    // Create a map to hold partial results
    typedef RT<RT<F1, Key>, Value> K;
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
        R_key_value<K, Z>{ std::move(it.first), std::move(it.second) }));
    }
    return result;
  }

  template <class Fun>
  auto ext(Fun expand) const -> MapCE<typename RT<Fun, R>::ElemType> {
    typedef typename RT<Fun, R>::ElemType T;
    MapCE<T> result;
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

  bool operator==(const MapCE& other) const {
    return container == other.container;
  }

  bool operator!=(const MapCE& other) const {
    return container != other.container;
  }

  bool operator<(const MapCE& other) const {
    return container < other.container;
  }

  bool operator>(const MapCE& other) const {
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
    ar& boost::serialization::make_nvp("__K3MapCE", container);
  }

 protected:
  Container container;

 private:
  friend class boost::serialization::access;
};  // class MapCE

}  // namespace K3

namespace YAML {
template <class R>
struct convert<K3::MapCE<R>> {
  static Node encode(const K3::MapCE<R>& c) {
    Node node;
    auto container = c.getConstContainer();
    if (container.size() > 0) {
      for (auto i : container) {
        node.push_back(convert<R>::encode(c.elemToRecord(i)));
      }
    } else {
      node = YAML::Load("[]");
    }
    return node;
  }

  static bool decode(const Node& node, K3::MapCE<R>& c) {
    for (auto& i : node) {
      auto p = i.as<typename K3::MapCE<R>::Elem>();
      c.insert(c.elemToRecord(p));
    }
    return true;
  }
};
}

namespace JSON {
template <class E>
struct convert<K3::MapCE<E>> {
  template <class Allocator>
  static Value encode(const K3::MapCE<E>& c, Allocator& al) {
    Value v;
    v.SetObject();
    v.AddMember("type", Value("MapCE"), al);
    Value inner;
    inner.SetArray();
    for (const auto& e : c.getConstContainer()) {
      inner.PushBack(convert<E>::encode(c.elemToRecord(e), al), al);
    }
    v.AddMember("value", inner.Move(), al);
    return v;
  }
};
}

#endif
