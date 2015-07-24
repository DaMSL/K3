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

  shared_ptr<R> peek(const unit_t&) const {
    shared_ptr<R> res(nullptr);
    auto it = container.begin();
    if (it != container.end()) {
      res = std::make_shared<R>(it->second);
    }
    return res;
  }

  template<typename F, typename G>
  auto peek_with(F f, G g) const {
    auto it = container.begin();
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

  template <class F>
  unit_t insert_with(const R& rec, F f) {
    auto existing = container.find(rec.key);
    if (existing == std::end(container)) {
      container[rec.key] = rec;
    } else {
      container[rec.key] = f(std::move(existing->second))(rec);
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

  unit_t erase(const R& rec) {
    auto it = container.find(rec.key);
    if (it != container.end()) {
        container.erase(it);
    }
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

  int size(unit_t) const { return container.size(); }

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
      acc = f(std::move(acc))(p.second);
    }
    return acc;
  }

  template<typename F1, typename F2, typename Z>
  SortedMap< R_key_value< RT<F1, R>,Z >> groupBy(F1 grouper, F2 folder, const Z& init) const {
    // Create a map to hold partial results
    typedef RT<F1, R> K;
    std::map<K, Z> accs;

    for (const auto& it : container) {
      K key = grouper(it.second);
      if (accs.find(key) == accs.end()) {
        accs[key] = init;
      }
      accs[key] = folder(std::move(accs[key]))(it.second);
    }

    // TODO more efficient implementation?
    SortedMap<R_key_value<K,Z>> result;
    for (auto&& it : accs) {
      result.insert(std::move(R_key_value<K, Z> {std::move(it.first), std::move(it.second)}));
    }
    return result;
  }

  template <class Fun>
  auto ext(Fun expand) const -> SortedMap < typename RT<Fun, R>::ElemType >  {
    typedef typename RT<Fun, R>::ElemType T;
    SortedMap<T> result;
    for (const auto& it : container) {
      for (auto& it2 : expand(it.second).container) {
        result.insert(it2.second);
      }
    }

    return result;
  }

  // lookup ignores the value of the argument
  shared_ptr<R> lookup(const R& r) const {
      auto it = container.find(r.key);
      if (it != container.end()) {
        return std::make_shared<R>(it->second);
      } else {
        return nullptr;
      }
  }

  bool member(const R& r) const {
    return container.find(r.key) != container.end();
  }

  template <class F>
  unit_t lookup_with(R const& r, F f) const {
    auto it = container.find(r.key);
    if (it != container.end()) {
      return f(it->second);
    }

    return unit_t {};
  }

  template <class F, class G>
  auto lookup_with2(R const& r, F f, G g) const {
    auto it = container.find(r.key);
    if (it == container.end()) {
      return f(unit_t {});
    } else {
      return g(it->second);
    }
  }

  template <class F>
  auto lookup_with3(R const& r, F f) const {
    auto it = container.find(r.key);
    if (it != container.end()) {
      return f(it->second);
    }
    throw std::runtime_error("No match on Map.lookup_with3");
  }

  template <class F, class G>
  auto lookup_with4(R const& r, F f, G g) const {
    auto it = container.find(r.key);
    if (it == container.end()) {
      return f(unit_t {});
    } else {
      return g(it->second);
    }
  }

  // Extremal key operations.

  shared_ptr<R> min(const unit_t&) const {
    shared_ptr<R> res(nullptr);
    auto it = container.begin();
    if (it != container.end()) {
      res = std::make_shared<R>(it->second);
    }
    return res;
  }

  shared_ptr<R> max(const unit_t&) const {
    shared_ptr<R> res(nullptr);
    auto it = container.rbegin();
    if (it != container.rend()) {
      res = std::make_shared<R>(it->second);
    }
    return res;
  }

  template<typename F, typename G>
  auto min_with(F f, G g) const {
    auto it = container.begin();
    if (it == container.end()) {
      return f(unit_t {});
    } else {
      return g(it->second);
    }
  }

  template<typename F, typename G>
  auto max_with(F f, G g) const {
    auto it = container.rbegin();
    if (it == container.rend()) {
      return f(unit_t {});
    } else {
      return g(it->second);
    }
  }

  std::shared_ptr<R> lower_bound(const R& rec) const {
    const auto& x = getConstContainer();
    auto it = x.lower_bound(rec.key);
    std::shared_ptr<R> result(nullptr);
    if (it != x.end()) {
      result = std::make_shared<R>(it->second);
    }
    return result;
  }

  std::shared_ptr<R> upper_bound(const R& rec) const {
    const auto& x = getConstContainer();
    auto it = x.upper_bound(rec.key);
    std::shared_ptr<R> result(nullptr);
    if (it != x.end()) {
      result = std::make_shared<R>(it->second);
    }
    return result;
  }

  template<typename F, typename G>
  auto upper_bound_with(const R& rec, F f, G g) const {
    auto it = container.upper_bound(rec.key);
    if (it == container.end()) {
      return f(unit_t {});
    } else {
      return g(it->second);
    }
  }

  template<typename F, typename G>
  auto lower_bound_with(const R& rec, F f, G g) const {
    auto it = container.lower_bound(rec.key);
    if (it == container.end()) {
      return f(unit_t {});
    } else {
      return g(it->second);
    }
  }

  SortedMap<R> filter_lt(const R& rec) const {
    const auto& x = getConstContainer();
    auto it = x.lower_bound(rec.key);
    return SortedMap<R>(x.begin(), it);
  }

  SortedMap<R> filter_gt(const R& rec) const {
    const auto& x = getConstContainer();
    auto it = x.upper_bound(rec.key);
    return SortedMap<R>(it, x.end());
  }

  SortedMap<R> filter_geq(const R& rec) const {
    const auto& x = getConstContainer();
    auto it = x.lower_bound(rec.key);
    return SortedMap<R>(it, x.end());
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

  unit_t erase_prefix(const R& rec) {
    auto it = container.lower_bound(rec.key);
    if (it != container.end()) {
        container.erase(container.cbegin(), it);
    }
    return unit_t();
  }

  unit_t erase_suffix(const R& rec) {
    auto it = container.upper_bound(rec.key);
    if (it != container.end()) {
        container.erase(it, container.cend());
    }
    return unit_t();
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
