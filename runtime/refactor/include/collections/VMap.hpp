#ifndef K3_VMAP
#define K3_VMAP

#include <iterator>
#include <map>
#include <unordered_map>

namespace K3 {

template <class R>
class VMap {
 public:
  using Version = int;

 protected:
  using Key = typename R::KeyType;

  // The VMap stores versions in decreasing order.
  // The VMap stores versions in decreasing order.
  template <typename Elem>
  using VContainer = map<Version, Elem, std::greater<Version>>;
  using Container = unordered_map<Key, VContainer<R>>;

  typedef typename Container::const_iterator const_iterator_type;
  typedef typename Container::iterator iterator_type;

 public:
  VMap() : container() {}
  VMap(const Container& con) : container(con) {}
  VMap(Container&& con) : container(std::move(con)) {}

  // Construct from (container) iterators
  template <typename Iterator>
  VMap(Iterator begin, Iterator end)
      : container(begin, end) {}

  // Returns the latest version of the element.
  template <class Pair>
  R elemToRecord(const Pair& e) const {
    R result = (e.second.begin() == e.second.end()) ? std::move(R())
                                                    : e.second.begin()->second;
    return result;
  }

  template <class I>
  class map_iterator : public std::iterator<std::forward_iterator_tag, R> {
    using container = Container;
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

    auto operator -> () const { return &(*(i->second.begin())); }

    auto& operator*() const { return *(i->second.begin()); }

    bool operator==(const map_iterator& other) const { return i == other.i; }

    bool operator!=(const map_iterator& other) const { return i != other.i; }

   private:
    I i;
  };

  using iterator = map_iterator<typename Container::iterator>;
  using const_iterator = map_iterator<typename Container::const_iterator>;

  iterator begin() { return iterator(container.begin()); }

  iterator end() { return iterator(container.end()); }

  const_iterator begin() const { return const_iterator(container.cbegin()); }

  const_iterator end() const { return const_iterator(container.cend()); }

  //////////////////////////////
  // VMap methods.

  int size(const Version& v) const {
    int r = 0;
    for (const auto& vmap : container) {
      r += (vmap.second.lower_bound(v) == vmap.second.end()) ? 0 : 1;
    }
    return r;
  }

  int total_size(unit_t) const {
    int r = 0;
    for (const auto& vmap : container) {
      r += vmap.second.size();
    }
    return r;
  }

  ////////////////////////////
  // Exact version methods.

  shared_ptr<R> peek(const Version& v) const {
    shared_ptr<R> res(nullptr);
    for (const auto& elem : container) {
      auto vit = elem.second.find(v);
      if (vit != elem.second.end()) {
        res = std::make_shared<R>(vit->second);
        break;
      }
    }
    return res;
  }

  template <class Q>
  unit_t insert(const Version& v, Q&& q) {
    container[q.key][v] = std::forward<Q>(q);
    return unit_t();
  }

  template <class F>
  unit_t insert_with(const Version& v, const R& rec, F f) {
    auto existing = container.find(rec.key);
    auto vexisting = existing == std::end(container)
                         ? std::end(container[rec.key])
                         : container[rec.key].find(v);

    if (vexisting == std::end(container[rec.key])) {
      container[rec.key][v] = rec;
    } else {
      container[rec.key][v] = f(std::move(vexisting->second))(rec);
    }

    return unit_t{};
  }

  template <class F, class G>
  unit_t upsert_with(const Version& v, const R& rec, F f, G g) {
    auto existing = container.find(rec.key);
    auto vexisting = existing == std::end(container)
                         ? std::end(container[rec.key])
                         : container[rec.key].find(v);

    if (vexisting == std::end(container[rec.key])) {
      container[rec.key][v] = f(unit_t{});
    } else {
      container[rec.key][v] = g(std::move(vexisting->second));
    }

    return unit_t{};
  }

  unit_t erase(const Version& v, const R& rec) {
    auto it = container.find(rec.key);
    if (it != container.end()) {
      auto vit = it->second.find(v);
      if (vit != it->second.end()) {
        it->second.erase(vit);
        if (it->second.empty()) {
          container.erase(it);
        }
      }
    }
    return unit_t();
  }

  unit_t update(const Version& v, const R& rec1, const R& rec2) {
    auto it = container.find(rec1.key);
    if (it != container.end()) {
      auto vit = it->second.find(v);
      if (vit != it->second.end()) {
        it->second.erase(vit);
        container[rec2.key][v] = rec2;
      }
    }
    return unit_t();
  }

  bool member(const Version& v, const R& r) const {
    auto it = container.find(r.key);
    return it != container.end() && it->second.find(v) != it->second.end();
  }

  shared_ptr<R> lookup(const Version& v, const R& r) const {
    auto it = container.find(r.key);
    if (it != container.end()) {
      auto vit = it->second.find(v);
      if (vit != it->second.end()) {
        return std::make_shared<R>(vit->second);
      }
    }
    return nullptr;
  }

  template <class F>
  unit_t lookup_with(const Version& v, R const& r, F f) const {
    auto it = container.find(r.key);
    if (it != container.end()) {
      auto vit = it->second.find(v);
      if (vit != it->second.end()) {
        return f(vit->second);
      }
    }
    return unit_t{};
  }

  template <class F, class G>
  auto lookup_with2(const Version& v, R const& r, F f, G g) const {
    auto it = container.find(r.key);
    if (it == container.end()) {
      return f(unit_t{});
    } else {
      auto vit = it->second.find(v);
      if (vit == it->second.end()) {
        return f(unit_t{});
      } else {
        return g(vit->second);
      }
    }
  }

  template <class F>
  auto lookup_with3(const Version& v, R const& r, F f) const {
    auto it = container.find(r.key);
    if (it != container.end()) {
      auto vit = it->second.find(v);
      if (vit != it->second.end()) {
        return f(vit->second);
      }
    }
    throw std::runtime_error("No match on Map.lookup_with3");
  }

  //////////////////////////////////////////////////
  // Frontier-based map retrieval.
  // These methods apply to the nearest version that is strictly less
  // than the version specified as an argument.

  shared_ptr<R> lookup_before(const Version& v, const R& r) const {
    auto it = container.find(r.key);
    if (it != container.end()) {
      auto vit = it->second.upper_bound(v);
      if (vit != it->second.end()) {
        return std::make_shared<R>(vit->second);
      }
    }
    return nullptr;
  }

  template <class F>
  unit_t lookup_with_before(const Version& v, R const& r, F f) const {
    auto it = container.find(r.key);
    if (it != container.end()) {
      auto vit = it->second.upper_bound(v);
      if (vit != it->second.end()) {
        return f(vit->second);
      }
    }
    return unit_t{};
  }

  template <class F, class G>
  auto lookup_with2_before(const Version& v, R const& r, F f, G g) const {
    auto it = container.find(r.key);
    if (it == container.end()) {
      return f(unit_t{});
    } else {
      auto vit = it->second.upper_bound(v);
      if (vit == it->second.end()) {
        return f(unit_t{});
      } else {
        return g(vit->second);
      }
    }
  }

  template <class F>
  auto lookup_with3_before(const Version& v, R const& r, F f) const {
    auto it = container.find(r.key);
    if (it != container.end()) {
      auto vit = it->second.upper_bound(v);
      if (vit != it->second.end()) {
        return f(vit->second);
      }
    }
    throw std::runtime_error("No match on Map.lookup_with3_before");
  }

  // Non-inclusive erase less than version.
  unit_t erase_prefix(const Version& v, const R& rec) {
    auto it = container.find(rec.key);
    if (it != container.end()) {
      auto vlteq = it->second.lower_bound(v);
      auto vless = it->second.upper_bound(v);
      auto vend = it->second.end();
      if (vless != it->second.end()) {
        it->second.erase((vlteq == vless) ? ++vless : vless, vend);
        if (it->second.empty()) {
          container.erase(it);
        }
      }
    }
    return unit_t();
  }

  // Inclusive update greater than a given version.
  template <class F>
  unit_t update_suffix(const Version& v, const R& rec, F f) {
    auto it = container.find(rec.key);
    if (it != container.end()) {
      auto vstart = it->second.begin();
      auto vlteq = it->second.lower_bound(v);
      for (; vstart != vlteq; vstart++) {
        container[rec.key][vstart->first] =
            f(vstart->first)(std::move(vstart->second));
      }
    }
    return unit_t();
  }

  //////////////////////////////////
  // Most-recent version methods.

  shared_ptr<R> peek_now(const unit_t&) const {
    shared_ptr<R> res(nullptr);
    for (const auto& p : container) {
      auto vit = p.second.begin();
      if (vit != p.second.end()) {
        res = std::make_shared<R>(vit->second);
        break;
      }
    }
    return res;
  }

  /////////////////
  // Transformers.
  //
  // For transformers that apply to a version, these methods apply
  // to the nearest version that is strictly less than the argument.

  VMap combine(const VMap& other) const {
    // copy this DS
    VMap result = VMap(*this);
    // copy other DS
    for (const auto& p : other.container) {
      for (const auto& vp : p.second) {
        result.container[p.first][vp.first] = vp.second;
      }
    }
    return result;
  }

  tuple<VMap, VMap> split(const unit_t&) const {
    // Find midpoint
    const size_t size = container.size();
    const size_t half = size / 2;
    // Setup iterators
    auto beg = container.begin();
    auto mid = container.begin();
    std::advance(mid, half);
    auto end = container.end();
    // Construct DS from iterators
    return std::make_tuple(VMap(beg, mid), VMap(mid, end));
  }

  template <typename Fun>
  unit_t iterate(const Version& v, Fun f) const {
    for (const auto& p : container) {
      auto it = p.second.upper_bound(v);
      if (it != p.second.end()) {
        f(it->second);
      }
    }
    return unit_t();
  }

  template <typename Fun>
  auto map(const Version& v, Fun f) const -> VMap<RT<Fun, R>> {
    VMap<RT<Fun, R>> result;
    for (const auto& p : container) {
      auto it = p.second.upper_bound(v);
      if (it != p.second.end()) {
        result.insert(it->first, f(it->second));
      }
    }
    return result;
  }

  template <typename Fun>
  VMap<R> filter(const Version& v, Fun predicate) const {
    VMap<R> result;
    for (const auto& p : container) {
      auto it = p.second.upper_bound(v);
      if (it != p.second.end() && predicate(it->second)) {
        result.insert(it->first, it->second);
      }
    }
    return result;
  }

  template <typename Fun, typename Acc>
  Acc fold(const Version& v, Fun f, Acc acc) const {
    for (const auto& p : container) {
      auto it = p.second.upper_bound(v);
      if (it != p.second.end()) {
        acc = f(std::move(acc))(it->second);
      }
    }
    return acc;
  }

  template <typename F1, typename F2, typename Z>
  VMap<R_key_value<RT<F1, R>, Z>> groupBy(const Version& v, F1 grouper,
                                          F2 folder, const Z& init) const {
    // Create a map to hold partial results
    using K = RT<F1, R>;
    unordered_map<K, VContainer<Z>> accs;

    for (const auto& it : container) {
      auto vit = it->second.upper_bound(v);
      if (vit != it->second.end()) {
        K key = grouper(vit->second);
        if (accs.find(key) == accs.end()) {
          accs[key][vit->first] = init;
        }
        accs[key][vit->first] =
            folder(std::move(accs[key][vit->first]))(vit->second);
      }
    }

    VMap<R_key_value<K, Z>> result;
    for (auto& it : accs) {
      for (auto&& vit : it.second) {
        result.insert(
            std::move(vit.first),
            std::move(R_key_value<K, Z>{it.first, std::move(vit.second)}));
      }
    }

    return result;
  }

  template <class Fun>
  auto ext(const Version& v,
           Fun expand) const -> VMap<typename RT<Fun, R>::ElemType> {
    typedef typename RT<Fun, R>::ElemType T;
    VMap<T> result;
    for (const auto& it : container) {
      auto vit = it->second.upper_bound(v);
      if (vit != it->second.end()) {
        for (auto& it2 : expand(vit->second).container) {
          result.insert(vit->first, it2.second);
        }
      }
    }

    return result;
  }

  template <typename Fun, typename Acc>
  Acc fold_all(Fun f, Acc acc) const {
    for (const auto& p : container) {
      for (const auto& velem : p.second) {
        acc = f(std::move(acc))(velem.first)(velem.second);
      }
    }
    return acc;
  }

  //////////////////
  // Comparators.
  bool operator==(const VMap& other) const {
    return container == other.container;
  }

  bool operator!=(const VMap& other) const {
    return container != other.container;
  }

  bool operator<(const VMap& other) const {
    return container < other.container;
  }

  bool operator>(const VMap& other) const {
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
    ar& boost::serialization::make_nvp("__K3VMap", container);
  }

 protected:
  Container container;

 private:
  friend class boost::serialization::access;

};  // class VMap
}  // namespace K3

namespace boost {
namespace serialization {
template <class _T0>
class implementation_level<K3::VMap<_T0>> {
 public:
  typedef mpl::integral_c_tag tag;
  typedef mpl::int_<object_serializable> type;
  BOOST_STATIC_CONSTANT(int, value = implementation_level::type::value);
};
}  // namespace serialization
}  // namespace boost

namespace YAML {
template <class R>
struct convert<K3::VMap<R>> {
  static Node encode(const K3::VMap<R>& c) {
    Node node;
    auto container = c.getConstContainer();
    if (container.size() > 0) {
      for (auto i : container) {
        for (auto j : i.second) {
          node.push_back(
              convert<typename K3::VMap<R>::Version>::encode(j.first));
          node.push_back(convert<R>::encode(j.second));
        }
      }
    } else {
      node = YAML::Load("[]");
    }
    return node;
  }

  static bool decode(const Node& node, K3::VMap<R>& c) {
    bool asVersion = true;
    typename K3::VMap<R>::Version v;
    for (auto& i : node) {
      if (asVersion) {
        v = i.as<typename K3::VMap<R>::Version>();
      } else {
        c.insert(v, i.as<R>());
      }
      asVersion = !asVersion;
    }
    return true;
  }
};
}  // namespace YAML

namespace JSON {
using namespace rapidjson;
template <class E>
struct convert<K3::VMap<E>> {
  template <class Allocator>
  static Value encode(const K3::VMap<E>& c, Allocator& al) {
    Value v;
    v.SetObject();
    v.AddMember("type", Value("VMap"), al);
    Value inner;
    inner.SetArray();
    for (const auto& e : c.getConstContainer()) {
      for (const auto& ve : e.second) {
        inner.PushBack(
            convert<typename K3::VMap<E>::Version>::encode(ve.first, al), al);
        inner.PushBack(convert<E>::encode(ve.second, al), al);
      }
    }
    v.AddMember("value", inner.Move(), al);
    return v;
  }
};
}  // namespace JSON
#endif
