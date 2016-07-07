#ifndef K3_ORDEREDMAP
#define K3_ORDEREDMAP

#include <iterator>
#include <map>

namespace K3 {

template<class R>
class SortedMap {
  using Key = typename R::KeyType;
  using Container = std::map<Key, R>;

 public:
  // Default Constructor
  SortedMap(): container() {}
  SortedMap(const std::map<Key,R>& con): container(con) {}
  SortedMap(std::map<Key, R>&& con): container(std::move(con)) {}

  // Construct from (container) iterators
  template<typename Iterator>
  SortedMap(Iterator begin, Iterator end): container(begin,end) {}

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

  unit_t clear(const unit_t&) {
    container.clear();
    return unit_t();
  }

  template<typename F, typename G>
  auto peek(F f, G g) const {
    auto it = container.begin();
    if (it == container.end()) {
      return f(unit_t {});
    } else {
      return g(it->second);
    }
  }

  bool member(const R& r) const {
    return container.find(r.key) != container.end();
  }

  template <class F, class G>
  auto lookup(R const& r, F f, G g) const {
    auto it = container.find(r.key);
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

  unit_t update(const R& rec1, const R& rec2) {
    auto it = container.find(rec1.key);
    if (it != container.end()) {
        container.erase(it);
        container[rec2.key] = rec2;
    }
    return unit_t();
  }

  unit_t erase(const R& rec) {
    auto it = container.find(rec.key);
    if (it != container.end()) {
        container.erase(it);
    }
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

  template <class F, class G>
  unit_t upsert_with(const R& rec, F f, G g) {
    auto existing = container.find(rec.key);
    if (existing == std::end(container)) {
      container[rec.key] = f(unit_t {});
    } else {
      container[rec.key] = g(std::move(existing->second));
    }

    return unit_t {};
  }


  //////////////////////////////////////////////////////////
  // Sort-order methods.

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

  template <class F, class G>
  auto lower_bound(const R& e, F f, G g) const {
    const auto& x = container;
    auto it = x.lower_bound(e);
    if (it == x.end()) {
      return f(unit_t{});
    } else {
      return g(*it);
    }
  }

  template <class F, class G>
  auto upper_bound(const R& e, F f, G g) const {
    const auto& x = container;
    auto it = x.upper_bound(e);
    if (it == x.end()) {
      return f(unit_t{});
    } else {
      return g(*it);
    }
  }

  template<typename F, typename G>
  auto lookup_lt(const R& k, F f, G g) const {
    auto it = container.lower_bound(k.key);
    if (it != container.begin()) {
      --it;
      return g(it->second);
    } else {
      return f(unit_t {});
    }
  }

  template<typename F, typename G>
  auto lookup_gt(const R& k, F f, G g) const {
    auto it = container.upper_bound(k.key);
    if (it == container.end()) {
      return f(unit_t {});
    } else {
      return g(it->second);
    }
  }

  template<typename F, typename G>
  auto lookup_geq(const R& k, F f, G g) const {
    auto it = container.lower_bound(k.key);
    if (it == container.end()) {
      return f(unit_t {});
    } else {
      return g(it->second);
    }
  }

  template<typename F, typename G>
  auto lookup_leq(const R& k, F f, G g) const {
    auto it = container.upper_bound(k.key);
    if (it != container.begin()) {
      --it;
      return g(it->second);
    } else {
      return f(unit_t {});
    }
  }

  SortedMap<R> filter_lt(const R& k) const {
    const auto& x = getConstContainer();
    auto it = x.lower_bound(k.key);
    return SortedMap<R>(x.begin(), it);
  }

  SortedMap<R> filter_gt(const R& k) const {
    const auto& x = getConstContainer();
    auto it = x.upper_bound(k.key);
    return SortedMap<R>(it, x.end());
  }

  SortedMap<R> filter_geq(const R& k) const {
    const auto& x = getConstContainer();
    auto it = x.lower_bound(k.key);
    return SortedMap<R>(it, x.end());
  }

  SortedMap<R> filter_leq(const R& k) const {
    const auto& x = getConstContainer();
    auto it = x.upper_bound(k.key);
    return SortedMap<R>(x.begin(), it);
  }

  SortedMap<R> between(const R& a, const R& b) const {
    const auto& x = getConstContainer();
    auto it = x.lower_bound(a.key);
    auto end = x.upper_bound(b.key);
    if ( it != x.end() ){
      return SortedMap<R>(it, end);
    } else {
      return SortedMap<R>();
    }
  }

  // Range-based modification, exclusive of the given key.

  unit_t erase_before(const R& rec) {
    auto it = container.lower_bound(rec.key);
    if (it != container.end()) {
        container.erase(container.cbegin(), it);
    }
    return unit_t();
  }

  unit_t erase_after(const R& rec) {
    auto it = container.upper_bound(rec.key);
    if (it != container.end()) {
        container.erase(it, container.cend());
    }
    return unit_t();
  }


  //////////////////////////////////////////////////////////////
  // Bulk transformations.

  SortedMap combine(const SortedMap& other) const {
    // copy this DS
    SortedMap result = SortedMap(*this);
    // copy other DS
    for (const auto& p: other.container) {
      result.container[p.first] = p.second;
    }
    return result;
  }

  tuple<SortedMap, SortedMap> split(const unit_t&) const {
    // Find midpoint
    const size_t size = container.size();
    const size_t half = size / 2;
    // Setup iterators
    auto beg = container.begin();
    auto mid = container.begin();
    std::advance(mid, half);
    auto end = container.end();
    // Construct DS from iterators
    return std::make_tuple(SortedMap(beg, mid), SortedMap(mid, end));
  }

  template <typename Fun>
  unit_t iterate(Fun f) const {
    for (const auto& p : container) {
      f(p.second);
    }
    return unit_t();
  }

  template<typename Fun>
  auto map(Fun f) const -> SortedMap< RT<Fun, R> > {
    SortedMap< RT<Fun,R> > result;
    for (const auto& p : container) {
      result.insert( f(p.second) );
    }
    return result;
  }

  template <typename Fun>
  SortedMap<R> filter(Fun predicate) const {
    SortedMap<R> result;
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
  SortedMap<R_key_value<RT<F1, R>, Z>>
  group_by(F1 grouper, F2 folder, const Z& init) const
  {
    // Create a map to hold partial results
    typedef RT<F1, R> K;
    std::map<K, Z> accs;

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

  template <class F1, class F2, class Z>
  SortedMap<R_key_value<RT<F1, R>, Z>>
  group_by_contiguous(F1 grouper, F2 folder, const Z& zero, const int& size) const
  {
    auto table = std::vector<Z>(size, zero);
    for (const auto& elem : container) {
      auto key = grouper(elem);
      table[key] = folder(std::move(table[key]), elem);
    }
    // Build the R_key_value records and insert them into result
    SortedMap<R_key_value<RT<F1, R>, Z>> result;
    for (auto i = 0; i < table.size(); ++i) {
      // move out of the map as we iterate
      result.insert(R_key_value<int, Z>{i, std::move(table[i])});
    }
    return result;
  }

  template <class Fun>
  auto ext(Fun expand) const -> SortedMap < typename RT<Fun, R>::ElemType >  {
    typedef typename RT<Fun, R>::ElemType T;
    SortedMap<T> result;
    for (const auto& it : container) {
      for (auto&& it2 : expand(it.second).container) {
        result.insert(std::move(it2.second));
      }
    }

    return result;
  }

  bool operator==(const SortedMap& other) const {
    return container == other.container;
  }

  bool operator!=(const SortedMap& other) const {
    return container != other.container;
  }

  bool operator<(const SortedMap& other) const {
    return container < other.container;
  }

  bool operator>(const SortedMap& other) const {
    return container > other.container;
  }

  Container& getContainer() { return container; }

  const Container& getConstContainer() const { return container; }

  template<class Archive>
  void serialize(Archive &ar) { ar & container; }

  template<class Archive>
  void serialize(Archive &ar, const unsigned int) {
    ar & boost::serialization::make_nvp("__K3SortedMap", container);
  }

 protected:
  std::map<Key,R> container;

 private:
  friend class boost::serialization::access;
}; // class SortedMap
}  // namespace K3

namespace boost {
namespace serialization {
template <class _T0>
class implementation_level<K3::SortedMap<_T0>> {
 public:
  typedef mpl::integral_c_tag tag;
  typedef mpl::int_<object_serializable> type;
  BOOST_STATIC_CONSTANT(int, value = implementation_level::type::value);
};
}  // namespace serialization
}  // namespace boost

namespace JSON {
using namespace rapidjson;
template <class E>
struct convert<K3::SortedMap<E>> {
  template <class Allocator>
  static Value encode(const K3::SortedMap<E>& c, Allocator& al) {
    Value v;
    v.SetObject();
    v.AddMember("type", Value("SortedMap"), al);
    Value inner;
    inner.SetArray();
    for (const auto& e : c.getConstContainer()) {
      inner.PushBack(convert<E>::encode(e.second, al), al);
    }
    v.AddMember("value", inner.Move(), al);
    return v;
  }
};
}  // namespace JSON

namespace YAML {
template <class R>
struct convert<K3::SortedMap<R>> {
  static Node encode(const K3::SortedMap<R>& c) {
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

  static bool decode(const Node& node, K3::SortedMap<R>& c) {
    for (auto& i : node) {
      c.insert(i.as<R>());
    }
    return true;
  }
};

}  // namespace YAML

#endif
