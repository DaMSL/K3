#ifndef K3_MAP
#define K3_MAP

#include <unordered_map>
#include <tuple>

#include "STLDataspace.hpp"
#include "Hash.hpp"
#include "serialization/Json.hpp"
#include "serialization/Yaml.hpp"

namespace K3 {

template <class R>
class Map {
  using Key = typename R::KeyType;
  using Container = std::unordered_map<Key, R>;

 protected:
  Container container;

 public:
  // Default Constructor
  Map() : container() {}
  Map(const Container& con) : container(con) {}
  Map(Container&& con) : container(std::move(con)) {}
  template <typename Iterator>
  Map(Iterator begin, Iterator end)
      : container(begin, end) {}

  template <class I>
  class map_iterator : public std::iterator<std::forward_iterator_tag, R> {
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
      *this++;
      return t;
    }

    auto operator -> () const { return &(i->second); }

    auto& operator*() const { return i->second; }

    bool operator==(const map_iterator& other) const { return i == other.i; }

    bool operator!=(const map_iterator& other) const { return i != other.i; }

   private:
    I i;
  };

  // Functionality
  int size(const unit_t&) const {
    return container.size();
  }

  unit_t clear(const unit_t&) {
    container.clear();
    return unit_t();
  }

  template <class F, class G>
  auto peek(F f, G g) const {
    auto it = container.begin();
    if (it == container.end()) {
      return f(unit_t{});
    } else {
      return g(*it);
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

    return unit_t{};
  }

  template <class F, class G>
  unit_t upsert_with(const R& rec, F f, G g) {
    auto existing = container.find(rec.key);
    if (existing == std::end(container)) {
      container[rec.key] = f(unit_t{});
    } else {
      existing->second = g(std::move(existing->second));
    }

    return unit_t{};
  }

  //////////////////////////////////////////////////////////////
  // Bulk transformations.

  Map combine(const Map& other) const {
    // copy this DS
    Map result = Map(*this);
    // copy other DS
    for (const auto& p : other.container) {
      result.container[p.first] = p.second;
    }
    return result;
  }

  tuple<Map, Map> split(const unit_t&) const {
    // Find midpoint
    const size_t size = container.size();
    const size_t half = size / 2;
    // Setup iterators
    auto beg = container.begin();
    auto mid = container.begin();
    std::advance(mid, half);
    auto end = container.end();
    // Construct DS from iterators
    return std::make_tuple(Map(beg, mid), Map(mid, end));
  }

  template <typename Fun>
  unit_t iterate(Fun f) const {
    for (const auto& p : container) {
      f(p.second);
    }
    return unit_t();
  }

  template <typename Fun>
  auto map(Fun f) const -> Map<RT<Fun, R>> {
    Map<RT<Fun, R>> result;
    for (const auto& p : container) {
      result.insert(f(p.second));
    }
    return result;
  }

  template <typename Fun>
  Map<R> filter(Fun predicate) const {
    Map<R> result;
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
  Map<R_key_value<RT<F1, R>, Z>> group_by(F1 grouper, F2 folder, const Z& init) const
  {
    // Create a std::unordered_map to hold partial results
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

  template <class F1, class F2, class Z>
  Map<R_key_value<RT<F1, R>, Z>>
  group_by_contiguous(F1 grouper, F2 folder, const Z& zero, const int& size) const
  {
    auto table = std::vector<Z>(size, zero);
    for (const auto& elem : container) {
      auto key = grouper(elem);
      table[key] = folder(std::move(table[key]), elem);
    }

    Map<R_key_value<RT<F1, R>, Z>> result;
    for (auto i = 0; i < table.size(); ++i) {
      // move out of the map as we iterate
      result.insert(R_key_value<int, Z>{i, std::move(table[i])});
    }
    return result;
  }

  template <class Fun>
  auto ext(Fun expand) const -> Map<typename RT<Fun, R>::ElemType> {
    typedef typename RT<Fun, R>::ElemType T;
    Map<T> result;
    for (const auto& it : container) {
      for (auto&& it2 : expand(it.second).container) {
        result.insert(std::move(it2.second));
      }
    }
    return result;
  }

  // Iterators.
  using iterator = map_iterator<typename Container::iterator>;
  using const_iterator = map_iterator<typename Container::const_iterator>;

  iterator begin() { return iterator(container.begin()); }
  iterator end() { return iterator(container.end()); }
  const_iterator begin() const { return const_iterator(container.cbegin()); }
  const_iterator end() const { return const_iterator(container.cend()); }

  bool operator==(const Map& other) const {
    return container == other.container;
  }

  bool operator!=(const Map& other) const {
    return container != other.container;
  }

  bool operator<(const Map& other) const { return container < other.container; }

  bool operator>(const Map& other) const { return container > other.container; }

  Container& getContainer() { return container; }

  const Container& getConstContainer() const { return container; }

  template <class Archive>
  void serialize(Archive& ar) {
    ar& container;
  }

  template <class Archive>
  void serialize(Archive& ar, const unsigned int) {
    ar& boost::serialization::make_nvp("__K3Map", container);
  }

 private:
  friend class boost::serialization::access;
};  // class Map

}  // namespace K3

namespace YAML {
template <class R>
struct convert<K3::Map<R>> {
  static Node encode(const K3::Map<R>& c) {
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

  static bool decode(const Node& node, K3::Map<R>& c) {
    for (auto& i : node) {
      c.insert(i.as<R>());
    }

    return true;
  }
};

}  // namespace YAML

namespace JSON {
using namespace rapidjson;
template <class E>
struct convert<K3::Map<E>> {
  template <class Allocator>
  static Value encode(const K3::Map<E>& c, Allocator& al) {
    Value v;
    v.SetObject();
    v.AddMember("type", Value("Map"), al);
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

#endif
