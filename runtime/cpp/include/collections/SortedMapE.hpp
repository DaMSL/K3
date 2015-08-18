#ifndef K3_SORTEDMAPE
#define K3_SORTEDMAPE

#include <iterator>
#include <map>

#include <functional>
#include <stdexcept>

#include <boost/serialization/array.hpp>
#include <boost/serialization/binary_object.hpp>
#include <boost/serialization/string.hpp>
#include <csvpp/csv.h>

namespace K3 {
template<class R>
class SortedMapE {
  using Key = typename R::KeyType;
  using Value = typename R::ValueType;
  using Container = std::map<Key, R>;

 public:
  // Default Constructor
  SortedMapE(): container() {}
  SortedMapE(const std::map<Key,R>& con): container(con) {}
  SortedMapE(std::map<Key, R>&& con): container(std::move(con)) {}

  // Construct from (container) iterators
  template<typename Iterator>
  SortedMapE(Iterator begin, Iterator end): container(begin,end) {}

  template <class Pair>
  R elemToRecord(const Pair& e) const { return e.second; }

  template <class I>
  class map_iterator: public std::iterator<std::forward_iterator_tag, R> {
    using container = std::map<Key, R>;
    using reference = typename std::iterator<std::forward_iterator_tag, R>::reference;
   public:
    template <class _I>
    map_iterator(_I&& _i): i(std::forward<_I>(_i)) {}

    map_iterator& operator ++() {
      ++i;
      return *this;
    }

    map_iterator operator ++(int) {
      map_iterator t = *this;
      *this++;
      return t;
    }

    auto operator->() const {
      return &(i->second);
    }

    auto& operator*() const {
      return i->second;
    }

    bool operator ==(const map_iterator& other) const {
      return i == other.i;
    }

    bool operator !=(const map_iterator& other) const {
      return i != other.i;
    }

   private:
    I i;
  };

  using iterator = map_iterator<typename std::map<Key, R>::iterator>;
  using const_iterator = map_iterator<typename std::map<Key, R>::const_iterator>;

  iterator begin() {
    return iterator(container.begin());
  }

  iterator end() {
    return iterator(container.end());
  }

  const_iterator begin() const {
    return const_iterator(container.cbegin());
  }

  const_iterator end() const {
    return const_iterator(container.cend());
  }

  // Functionality
  int size(unit_t) const { return container.size(); }

  template<typename F, typename G>
  auto peek(F f, G g) const {
    auto it = container.begin();
    if (it == container.end()) {
      return f(unit_t {});
    } else {
      return g(it->second);
    }
  }

  template<typename K>
  bool member(const K& k) const {
    return container.find(k.key) != container.end();
  }

  template <typename K, class F, class G>
  RT<G, R> lookup(K const& k, F f, G g) const {
    auto it = container.find(k.key);
    if (it == container.end()) {
      return f(unit_t {});
    } else {
      return g(it->second);
    }
  }

  template <class Q>
  unit_t insert(Q&& q) {
    container[q.key] = std::forward<Q>(q);
    return unit_t();
  }

  template<typename K, typename V>
  unit_t update(const K& k, V&& val) {
    auto it = container.find(k.key);
    if (it != container.end()) {
      container[k.key].value = std::move(val.value);
    }
    return unit_t();
  }

  template<typename K>
  unit_t erase(const K& k) {
    container.erase(k.key);
    return unit_t();
  }

  template <class F>
  unit_t insert_with(const R& rec, F f) {
    auto existing = container.find(rec.key);
    if (existing == std::end(container)) {
      container[rec.key] = rec;
    } else {
      container[rec.key] = f(std::move(existing->second), rec);
    }

    return unit_t {};
  }

  template <typename K, typename F, typename G>
  unit_t upsert_with(const K& k, F f, G g) {
    auto existing = container.find(k.key);
    if (existing == std::end(container)) {
      container[k.key] = f(unit_t {});
    } else {
      container[k.key] = g(std::move(existing->second));
    }
    return unit_t {};
  }

  //////////////////////////////////////////////////////////
  // Sort-order methods.
  // For a SortedMapE, these methods expect a key argument instead of a key-value struct.

  // Extremal key operations.
  template<typename F, typename G>
  auto min(F f, G g) const {
    auto it = container.begin();
    if (it == container.end()) {
      return f(unit_t {});
    } else {
      return g(it->second);
    }
  }

  template<typename F, typename G>
  auto max(F f, G g) const {
    auto it = container.rbegin();
    if (it == container.rend()) {
      return f(unit_t {});
    } else {
      return g(it->second);
    }
  }

  template <class K, class F, class G>
  auto lower_bound(const K& k, F f, G g) const {
    const auto& x = Super::getConstContainer();
    auto it = x.lower_bound(k.key);
    if (it == x.end()) {
      return f(unit_t{});
    } else {
      return g(*it);
    }
  }

  template <class K, class F, class G>
  auto upper_bound(const K& k, F f, G g) const {
    const auto& x = Super::getConstContainer();
    auto it = x.upper_bound(k.key);
    if (it == x.end()) {
      return f(unit_t{});
    } else {
      return g(*it);
    }
  }

  template<typename K, typename F, typename G>
  auto lookup_lt(const K& k, F f, G g) const {
    auto it = container.lower_bound(k.key);
    if (it != container.begin()) {
      --it;
      return g(it->second);
    } else {
      return f(unit_t {});
    }
  }

  template<typename K, typename F, typename G>
  auto lookup_gt(const K& k, F f, G g) const {
    auto it = container.upper_bound(k.key);
    if (it == container.end()) {
      return f(unit_t {});
    } else {
      return g(it->second);
    }
  }

  template<typename K, typename F, typename G>
  auto lookup_geq(const K& k, F f, G g) const {
    auto it = container.lower_bound(k.key);
    if (it == container.end()) {
      return f(unit_t {});
    } else {
      return g(it->second);
    }
  }

  template<typename K, typename F, typename G>
  auto lookup_leq(const K& k, F f, G g) const {
    auto it = container.upper_bound(k.key);
    if (it != container.begin()) {
      --it;
      return g(it->second);
    } else {
      return f(unit_t {});
    }
  }

  template<typename K>
  SortedMapE<R> filter_lt(const K& k) const {
    const auto& x = getConstContainer();
    auto it = x.lower_bound(k.key);
    if (it != x.begin()) --it;
    return SortedMapE<R>(x.begin(), it);
  }

  template<typename K>
  SortedMapE<R> filter_gt(const K& k) const {
    const auto& x = getConstContainer();
    auto it = x.upper_bound(k.key);
    return SortedMapE<R>(it, x.end());
  }

  template<typename K>
  SortedMapE<R> filter_geq(const K& k) const {
    const auto& x = getConstContainer();
    auto it = x.lower_bound(k.key);
    return SortedMapE<R>(it, x.end());
  }

  template<typename K>
  SortedMapE<R> filter_leq(const K& k) const {
    const auto& x = getConstContainer();
    auto it = x.upper_bound(k.key);
    if (it != x.begin()) --it;
    return SortedMapE<R>(x.begin(), it);
  }

  template<typename K>
  SortedMapE<R> between(const K& a, const K& b) const {
    const auto& x = getConstContainer();
    auto it = x.lower_bound(a.key);
    auto end = x.upper_bound(b.key);
    if ( it != x.end() ){
      return SortedMapE<R>(it, end);
    } else {
      return SortedMapE<R>();
    }
  }

  // Range-based modification, exclusive of the given key.

  template<typename K>
  unit_t erase_before(const K& k) {
    auto it = container.lower_bound(k.key);
    if (it != container.end()) {
        container.erase(container.cbegin(), it);
    }
    return unit_t();
  }

  template<typename K>
  unit_t erase_after(const K& k) {
    auto it = container.upper_bound(k.key);
    if (it != container.end()) {
        container.erase(it, container.cend());
    }
    return unit_t();
  }

  //////////////////////////////////////////////////////////////
  // Bulk transformations.

  SortedMapE combine(const SortedMapE& other) const {
    // copy this DS
    SortedMapE result = SortedMapE(*this);
    // copy other DS
    for (const auto& p: other.container) {
      result.container[p.first] = p.second;
    }
    return result;
  }

  tuple<SortedMapE, SortedMapE> split(const unit_t&) const {
    // Find midpoint
    const size_t size = container.size();
    const size_t half = size / 2;
    // Setup iterators
    auto beg = container.begin();
    auto mid = container.begin();
    std::advance(mid, half);
    auto end = container.end();
    // Construct DS from iterators
    return std::make_tuple(SortedMapE(beg, mid), SortedMapE(mid, end));
  }

  template <typename Fun>
  unit_t iterate(Fun f) const {
    for (const auto& p : container) {
      f(p.second);
    }
    return unit_t();
  }

  template<typename Fun>
  auto map_generic(Fun f) const -> SortedMap< RT<Fun, R> > {
    SortedMap< RT<Fun,R> > result;
    for (const auto& p : container) {
      result.insert( f(p.second) );
    }
    return result;
  }

  template <typename Fun>
  SortedMapE<R> filter(Fun predicate) const {
    SortedMapE<R> result;
    for (const auto& p : container) {
      if (predicate(p.second)) {
        result.insert(p.second);
      }
    }
    return result;
  }

  template<typename Fun, typename Acc>
  Acc fold(Fun f, Acc acc) const {
    for (const auto& p : container) {
      acc = f(std::move(acc), p.second);
    }
    return acc;
  }

  template<typename F1, typename F2, typename Z>
  SortedMap< R_key_value< RT<F1, R>,Z >>
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

    SortedMap<R_key_value<K,Z>> result;
    for (auto&& it : accs) {
      result.insert(std::move(R_key_value<K, Z> {std::move(it.first), std::move(it.second)}));
    }
    return result;
  }

  template <class Fun>
  auto ext_generic(Fun expand) const -> SortedMap < typename RT<Fun, R>::ElemType >  {
    typedef typename RT<Fun, R>::ElemType T;
    SortedMap<T> result;
    for (const auto& it : container) {
      for (auto&& it2 : expand(it.second).container) {
        result.insert(std::move(it2.second));
      }
    }

    return result;
  }

  bool operator==(const SortedMapE& other) const {
    return container == other.container;
  }

  bool operator!=(const SortedMapE& other) const {
    return container != other.container;
  }

  bool operator<(const SortedMapE& other) const {
    return container < other.container;
  }

  bool operator>(const SortedMapE& other) const {
    return container > other.container;
  }

  Container& getContainer() { return container; }

  const Container& getConstContainer() const { return container; }

  template<class Archive>
  void serialize(Archive &ar) { ar & container; }

  template<class Archive>
  void serialize(Archive &ar, const unsigned int) {
    ar & boost::serialization::make_nvp("__K3SortedMapE", container);
  }

 protected:
  std::map<Key,R> container;

 private:
  friend class boost::serialization::access;
}; // class SortedMapE

}

namespace YAML {
template <class R>
struct convert<K3::SortedMapE<R>> {
  static Node encode(const K3::SortedMapE<R>& c) {
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

  static bool decode(const Node& node, K3::SortedMapE<R>& c) {
    for (auto& i : node) {
      c.insert(i.as<R>());
    }
    return true;
  }
};
}

namespace JSON {
template <class E>
struct convert<K3::SortedMapE<E>> {
  template <class Allocator>
  static Value encode(const K3::SortedMapE<E>& c, Allocator& al) {
    Value v;
    v.SetObject();
    v.AddMember("type", Value("SortedMapE"), al);
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
