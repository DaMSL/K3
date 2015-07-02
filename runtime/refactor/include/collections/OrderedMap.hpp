#ifndef K3_ORDEREDMAP
#define K3_ORDEREDMAP

#include <iterator>
#include <map>

namespace K3 {

template <class R>
class OrderedMap {
  using Key = typename R::KeyType;

 public:
  // Default Constructor
  OrderedMap() : container() {}
  OrderedMap(const std::map<Key, R>& con) : container(con) {}
  OrderedMap(std::map<Key, R>&& con) : container(std::move(con)) {}

  // Construct from (container) iterators
  template <typename Iterator>
  OrderedMap(Iterator begin, Iterator end)
      : container(begin, end) {}

  template <class Pair>
  R elemToRecord(const Pair& e) const {
    return e.second;
  }

  template <class I>
  class map_iterator : public std::iterator<std::forward_iterator_tag, R> {
    using container = std::map<Key, R>;
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

  using iterator = map_iterator<typename std::map<Key, R>::iterator>;
  using const_iterator =
      map_iterator<typename std::map<Key, R>::const_iterator>;

  iterator begin() { return iterator(container.begin()); }

  iterator end() { return iterator(container.end()); }

  const_iterator begin() const { return const_iterator(container.cbegin()); }

  const_iterator end() const { return const_iterator(container.cend()); }

  // DS Operations:
  // Maybe return the first element in the DS
  shared_ptr<R> peek(const unit_t&) const {
    shared_ptr<R> res(nullptr);
    auto it = container.begin();
    if (it != container.end()) {
      res = std::make_shared<R>(it->second);
    }
    return res;
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

    return unit_t{};
  }

  template <class F, class G>
  unit_t upsert_with(const R& rec, F f, G g) {
    auto existing = container.find(rec.key);
    if (existing == std::end(container)) {
      container[rec.key] = f(unit_t{});
    } else {
      container[rec.key] = g(std::move(existing->second));
    }

    return unit_t{};
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

  OrderedMap combine(const OrderedMap& other) const {
    // copy this DS
    OrderedMap result = OrderedMap(*this);
    // copy other DS
    for (const auto& p : other.container) {
      result.container[p.first] = p.second;
    }
    return result;
  }

  tuple<OrderedMap, OrderedMap> split(const unit_t&) const {
    // Find midpoint
    const size_t size = container.size();
    const size_t half = size / 2;
    // Setup iterators
    auto beg = container.begin();
    auto mid = container.begin();
    std::advance(mid, half);
    auto end = container.end();
    // Construct DS from iterators
    return std::make_tuple(OrderedMap(beg, mid), OrderedMap(mid, end));
  }

  template <typename Fun>
  unit_t iterate(Fun f) const {
    for (const auto& p : container) {
      f(p.second);
    }
    return unit_t();
  }

  template <typename Fun>
  auto map(Fun f) const -> OrderedMap<RT<Fun, R>> {
    OrderedMap<RT<Fun, R>> result;
    for (const auto& p : container) {
      result.insert(f(p.second));
    }
    return result;
  }

  template <typename Fun>
  OrderedMap<R> filter(Fun predicate) const {
    OrderedMap<R> result;
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
      acc = f(std::move(acc))(p.second);
    }
    return acc;
  }

  template <typename F1, typename F2, typename Z>
  OrderedMap<R_key_value<RT<F1, R>, Z>> groupBy(F1 grouper, F2 folder,
                                                const Z& init) const {
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
    OrderedMap<R_key_value<K, Z>> result;
    for (auto&& it : accs) {
      result.insert(std::move(
          R_key_value<K, Z>{std::move(it.first), std::move(it.second)}));
    }
    return result;
  }

  template <class Fun>
  auto ext(Fun expand) const -> OrderedMap<typename RT<Fun, R>::ElemType> {
    typedef typename RT<Fun, R>::ElemType T;
    OrderedMap<T> result;
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

    return unit_t{};
  }

  template <class F, class G>
  auto lookup_with2(R const& r, F f, G g) const {
    auto it = container.find(r.key);
    if (it == container.end()) {
      return f(unit_t{});
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

  template <class F>
  unit_t min_with(F f) const {
    auto it = container.begin();
    if (it != container.end()) {
      return f(it->second);
    }

    return unit_t{};
  }

  template <class F>
  unit_t max_with(F f) const {
    auto it = container.rbegin();
    if (it != container.rend()) {
      return f(it->second);
    }

    return unit_t{};
  }

  // Range-based modification, exclusive of the given key.

  unit_t erase_prefix(const R& rec) {
    auto it = container.find(rec.key);
    if (it != container.end()) {
      container.erase(container.cbegin(), it);
    }
    return unit_t();
  }

  unit_t erase_suffix(const R& rec) {
    auto it = container.find(rec.key);
    if (it != container.end()) {
      container.erase(it, container.cend());
    }
    return unit_t();
  }

  bool operator==(const OrderedMap& other) const {
    return container == other.container;
  }

  bool operator!=(const OrderedMap& other) const {
    return container != other.container;
  }

  bool operator<(const OrderedMap& other) const {
    return container < other.container;
  }

  bool operator>(const OrderedMap& other) const {
    return container > other.container;
  }

  std::map<Key, R>& getContainer() { return container; }

  const std::map<Key, R>& getConstContainer() const { return container; }

  template <class Archive>
  void serialize(Archive& ar) {
    ar& container;
  }

  template <class Archive>
  void serialize(Archive& ar, const unsigned int) {
    ar& boost::serialization::make_nvp("__K3OrderedMap", container);
  }

 protected:
  std::map<Key, R> container;

 private:
  friend class boost::serialization::access;
};  // class OrderedMap
}  // namespace K3

namespace boost {
namespace serialization {
template <class _T0>
class implementation_level<K3::OrderedMap<_T0>> {
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
struct convert<K3::OrderedMap<E>> {
  template <class Allocator>
  static Value encode(const K3::OrderedMap<E>& c, Allocator& al) {
    Value v;
    v.SetObject();
    v.AddMember("type", Value("OrderedMap"), al);
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
struct convert<K3::OrderedMap<R>> {
  static Node encode(const K3::OrderedMap<R>& c) {
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

  static bool decode(const Node& node, K3::OrderedMap<R>& c) {
    for (auto& i : node) {
      c.insert(i.as<R>());
    }
    return true;
  }
};

}  // namespace YAML

#endif
