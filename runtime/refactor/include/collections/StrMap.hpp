#ifndef K3_STRMAP
#define K3_STRMAP

#include <tuple>
#include <functional>
#include <stdexcept>

#if HAS_LIBDYNAMIC
extern "C" {
#include <dynamic/mapi.h>
#include <dynamic/map_str.h>
}

#include "boost/serialization/array.hpp"
#include "boost/serialization/binary_object.hpp"
#include "boost/serialization/string.hpp"
#include "csvpp/csv.h"

#include "Common.hpp"

namespace K3 {
namespace Libdynamic {

template <class R>
class StrMap {
  using Key = typename R::KeyType;
  using CloneFn = void (*)(void*, void*, size_t);

 public:
  using Container = shared_ptr<map_str>;

  StrMap() : container() {}

  StrMap(const Container& con) {
    init_map_str(true, sizeof(R));
    map_str* m = con.get();
    for (auto o = map_str_begin(m); o < map_str_end(m);
         o = map_str_next(m, o)) {
      insert(static_cast<R*>(map_str_get(m, o)));
    }
  }

  StrMap(Container&& con) : container(std::move(con)) {}

  template <typename Iterator>
  StrMap(Iterator begin, Iterator end) {
    init_map_str(true, sizeof(R));
    for (auto it = begin; it != end; ++it) {
      insert(*it);
    }
  }

  template <class V, class I>
  class map_iterator : public std::iterator<std::forward_iterator_tag, R> {
    using reference =
        typename std::iterator<std::forward_iterator_tag, R>::reference;

   public:
    template <class _I>
    map_iterator(map_str* _m, _I&& _i)
        : m(_m), i(std::forward<_I>(_i)) {}

    map_iterator& operator++() {
      i = map_str_next(m, i);
      return *this;
    }

    map_iterator operator++(int) {
      map_iterator t = *this;
      *this++;
      return t;
    }

    auto operator -> () const { return static_cast<V*>(map_str_get(m, i)); }

    auto& operator*() const { return *static_cast<V*>(map_str_get(m, i)); }

    bool operator==(const map_iterator& other) const { return i == other.i; }

    bool operator!=(const map_iterator& other) const { return i != other.i; }

   private:
    map_str* m;
    I i;
  };

  using iterator = map_iterator<R, size_t>;
  using const_iterator = map_iterator<const R, size_t>;

  iterator begin() {
    map_str* m = get_map_str();
    return iterator(m, map_str_begin(m));
  }

  iterator end() {
    map_str* m = get_map_str();
    return iterator(m, map_str_end(m));
  }

  const_iterator begin() const {
    map_str* m = get_map_str();
    return const_iterator(m, map_str_begin(m));
  }

  const_iterator end() const {
    map_str* m = get_map_str();
    return const_iterator(m, map_str_end(m));
  }

  int size(unit_t) const { return map_str_size(get_map_str()); }

  R elemToRecord(const R& e) const { return e; }

  // DS Operations:
  // Maybe return the first element in the DS
  shared_ptr<R> peek(const unit_t&) const {
    shared_ptr<R> res(nullptr);
    map_str* m = get_map_str();
    auto it = map_str_begin(m);
    if (it < map_str_end(m)) {
      res = std::make_shared<R>(*map_str_get(m, it));
    }
    return res;
  }

  size_t insert_aux(const R& q) {
    map_str* m = get_map_str();
    auto pos = map_str_insert(m, q.key.begin(),
                              const_cast<void*>(static_cast<const void*>(&q)));

    R* v = static_cast<R*>(map_str_get(m, pos));
    map_str_stabilize_key(m, pos, v->key.begin());

    return pos;
  }

  unit_t insert(const R& q) {
    insert_aux(q);
    return unit_t{};
  }

  template <class F>
  unit_t insert_with(const R& rec, F f) {
    map_str* m = get_map_str();
    if (m->size == 0) {
      insert(rec);
    } else {
      auto existing = map_str_find(m, rec.key.begin());
      if (existing == map_str_end(m)) {
        insert(rec);
      } else {
        auto* v = static_cast<R*>(map_str_get(m, existing));
        *v = f(std::move(*v))(rec);
        map_str_stabilize_key(m, existing, v->key.begin());
      }
    }
    return unit_t{};
  }

  template <class F, class G>
  unit_t upsert_with(const R& rec, F f, G g) {
    map_str* m = get_map_str();
    if (m->size == 0) {
      auto new_pos = insert_aux(rec);
      auto* placement = static_cast<R*>(map_str_get(m, new_pos));
      *placement = f(unit_t{});
      map_str_stabilize_key(m, new_pos, placement->key.begin());
    } else {
      auto existing = map_str_find(m, rec.key.begin());
      if (existing == map_str_end(m)) {
        auto new_pos = insert_aux(rec);
        auto* placement = static_cast<R*>(map_str_get(m, new_pos));
        *placement = f(unit_t{});
        map_str_stabilize_key(m, new_pos, placement->key.begin());
      } else {
        auto* v = static_cast<R*>(map_str_get(m, existing));
        *v = g(std::move(*v));
        map_str_stabilize_key(m, existing, v->key.begin());
      }
    }
    return unit_t{};
  }

  unit_t erase(const R& rec) {
    map_str* m = get_map_str();
    if (m->size > 0) {
      auto existing = map_str_find(m, rec.key.begin());
      if (existing != map_str_end(m)) {
        R* v = static_cast<R*>(map_str_get(m, existing));
        map_str_erase(m, rec.key.begin());
        v->~R();
      }
    }
    return unit_t();
  }

  unit_t update(const R& rec1, R& rec2) {
    map_str* m = get_map_str();
    if (m->size > 0) {
      auto existing = map_str_find(m, rec1.key.begin());
      if (existing != map_str_end(m)) {
        R* v = static_cast<R*>(map_str_get(m, existing));
        map_str_erase(m, rec1.key.begin());
        v->~R();
        insert(rec2);
      }
    }
    return unit_t();
  }

  template <typename Fun, typename Acc>
  Acc fold(Fun f, Acc acc) const {
    map_str* m = get_map_str();
    for (auto o = map_str_begin(m); o < map_str_end(m);
         o = map_str_next(m, o)) {
      acc = f(std::move(acc))(*map_str_get(m, o));
    }
    return acc;
  }

  template <typename Fun>
  auto map(Fun f) const -> StrMap<RT<Fun, R>> {
    map_str* m = get_map_str();
    StrMap<RT<Fun, R>> result;
    for (auto o = map_str_begin(m); o < map_str_end(m);
         o = map_str_next(m, o)) {
      result.insert(f(*map_str_get(m, o)));
    }
    return result;
  }

  template <typename Fun>
  unit_t iterate(Fun f) const {
    map_str* m = get_map_str();
    for (auto o = map_str_begin(m); o < map_str_end(m);
         o = map_str_next(m, o)) {
      f(*map_str_get(m, o));
    }
    return unit_t();
  }

  template <typename Fun>
  StrMap<R> filter(Fun predicate) const {
    map_str* m = get_map_str();
    StrMap<RT<Fun, R>> result;
    for (auto o = map_str_begin(m); o < map_str_end(m);
         o = map_str_next(m, o)) {
      if (predicate(*map_str_get(m, o))) {
        result.insert(*map_str_get(m, o));
      }
    }
    return result;
  }

  tuple<StrMap, StrMap> split(const unit_t&) const {
    map_str* m = get_map_str();

    // Find midpoint
    const size_t size = map_str_size(m);
    const size_t half = size / 2;

    // Setup iterators
    auto mid = map_str_begin(m);
    for (size_t i = 0; i < half; ++i) {
      mid = map_str_next(m, mid);
    }

    // Construct DS from iterators
    return std::make_tuple(StrMap(begin(), iterator(m, mid)),
                           StrMap(iterator(m, mid), end()));
  }

  StrMap combine(const StrMap& other) const {
    // copy this DS
    StrMap result = StrMap(*this);
    // copy other DS
    map_str* m = other.get_map_str();
    for (auto o = map_str_begin(m); o < map_str_end(m);
         o = map_str_next(m, o)) {
      result.insert(*map_str_get(m, o));
    }
    return result;
  }

  template <typename F1, typename F2, typename Z>
  StrMap<R_key_value<RT<F1, R>, Z>> groupBy(F1 grouper, F2 folder,
                                            const Z& init) const {
    // Create a map to hold partial results
    typedef RT<F1, R> K;
    StrMap<R_key_value<K, Z>> result;

    map_str* m = get_map_str();
    map_str* n = result.get_map_str();

    for (auto o = map_str_begin(m); o < map_str_end(m);
         o = map_str_next(m, o)) {
      R* v = static_cast<R*>(map_str_get(m, o));
      K key = grouper(*v);
      auto existing_acc = map_str_find(n, key.begin());
      if (existing_acc == map_str_end(n)) {
        R_key_value<K, Z> elem(key,
                               std::move(folder(init)(*map_str_get(m, o))));
        insert(elem);
      } else {
        R* accv = static_cast<R*>(map_str_get(n, existing_acc));
        R_key_value<K, Z> elem(key,
                               std::move(folder(std::move(accv->value))(*v)));
        map_str_erase(n, key);
        accv->~R_key_value<K, Z>();
        insert(elem);
        delete accv;
      }
    }
    return result;
  }

  template <class Fun>
  auto ext(Fun expand) const -> StrMap<typename RT<Fun, R>::ElemType> {
    typedef typename RT<Fun, R>::ElemType T;
    StrMap<T> result;
    map_str* m = get_map_str();
    auto end = map_str_end(m);
    for (auto it = map_str_begin(m); it < end; it = map_str_next(m, it)) {
      map_str* n = expand(*map_str_get(m, it));
      auto end2 = map_str_end(n);
      for (auto it2 = map_str_begin(n); it2 < end2;
           it2 = map_str_next(n, it2)) {
        result.insert(*map_str_get(n, it2));
      }
    }
    return result;
  }

  // lookup ignores the value of the argument
  shared_ptr<R> lookup(const R& r) const {
    map_str* m = get_map_str();
    if (m->size == 0) {
      return nullptr;
    } else {
      auto existing = map_str_find(m, r.key.begin());
      if (existing == map_str_end(m)) {
        return nullptr;
      } else {
        return std::make_shared<R>(*static_cast<R*>(map_str_get(m, existing)));
      }
    }
  }

  bool member(const R& r) const {
    map_str* m = get_map_str();
    return m->size == 0 ? false
                        : (map_str_find(m, r.key.begin()) != map_str_end(m));
  }

  template <class F>
  unit_t lookup_with(R const& r, F f) const {
    map_str* m = get_map_str();
    if (m->size > 0) {
      auto existing = map_str_find(m, r.key.begin());
      if (existing != map_str_end(m)) {
        return f(*map_str_get(m, existing));
      }
    }
    return unit_t{};
  }

  template <class F, class G>
  auto lookup_with2(R const& r, F f, G g) const {
    map_str* m = get_map_str();
    if (m->size == 0) {
      return f(unit_t{});
    } else {
      auto existing = map_str_find(m, r.key.begin());
      if (existing == map_str_end(m)) {
        return f(unit_t{});
      } else {
        return g(*map_str_get(m, existing));
      }
    }
  }

  template <class F>
  auto lookup_with3(R const& r, F f) const {
    map_str* m = get_map_str();
    if (m->size > 0) {
      auto existing = map_str_find(m, r.key.begin());
      if (existing != map_str_end(m)) {
        return f(*map_str_get(m, existing));
      }
    }
    throw std::runtime_error("No match on StrMap.lookup_with3");
  }

  bool operator==(const StrMap& other) const {
    return get_map_str() == other.get_map_str() ||
           (size() == other.size() &&
            std::is_permutation(begin(), end(), other.begin(), other.end()));
  }

  bool operator!=(const StrMap& other) const {
    return !(this->operator==(other));
  }

  bool operator<(const StrMap& other) const {
    return std::lexicographical_compare(begin(), end(), other.begin(),
                                        other.end());
  }

  bool operator>(const StrMap& other) const {
    return !(this->operator<(other) || this->operator==(other));
  }

  StrMap& getContainer() { return *const_cast<StrMap*>(this); }

  // Return a constant reference to the container
  const StrMap& getConstContainer() const { return *this; }

  template <class archive>
  void serialize(archive& ar) const {
    map_str* m = get_map_str();

    ar & m->value_size;
    ar & m->size;
    ar & m->capacity;
    ar & m->deleted;
    ar & m->max_load_factor;

    for (auto o = map_str_begin(m); o < map_str_end(m);
         o = map_str_next(m, o)) {
      R* v = static_cast<R*>(map_str_get(m, o));
      ar&* v;
    }
  }

  template <class archive>
  void serialize(archive& ar) {
    size_t value_size;
    size_t container_size;
    size_t capacity;
    size_t deleted;
    double mlf;

    ar& value_size;
    ar& container_size;
    ar& capacity;
    ar& deleted;
    ar& mlf;

    if (container) {
      map_str_clear(get_map_str());
      container.reset();
    }
    init_map_str(true, value_size);

    if (container) {
      map_str* n = get_map_str();
      map_str_max_load_factor(n, mlf);
      map_str_rehash(n, capacity);

      for (auto i = 0; i < container_size; ++i) {
        R r;
        ar& r;
        insert(r);
      }
    } else {
      throw std::runtime_error(
          "Failed to instantiate StrMap for deserialization");
    }
  }

  BOOST_SERIALIZATION_SPLIT_MEMBER()

 protected:
  shared_ptr<map_str> container;

 private:
  friend class boost::serialization::access;

  static inline void cloneElem(void* dest, void* src, size_t sz) {
    new (dest) R(*static_cast<R*>(src));
  }

  map_str* get_map_str() const {
    if (!container) {
      init_map_str(true, sizeof(R));
    }
    return container.get();
  }

  void init_map_str(bool alloc, size_t sz) {
    if (alloc) {
      container =
          Container(map_str_new(sz), [](map_str* m) { map_str_free(m); });
    }
    map_str_clone(get_map_str(), (CloneFn)&StrMap<R>::cloneElem);
  }

  void init_map_str(bool alloc, size_t sz) const {
    if (alloc) {
      const_cast<shared_ptr<map_str>&>(container) =
          Container(map_str_new(sz), [](map_str* m) { map_str_free(m); });
    }
    map_str_clone(get_map_str(), (CloneFn)&StrMap<R>::cloneElem);
  }

  template <class archive>
  void save(archive& ar, const unsigned int) const {
    size_t value_size = container->value_size;
    size_t container_size = container->size;
    size_t capacity = container->capacity;
    size_t deleted = container->deleted;
    double mlf = container->max_load_factor;

    ar << boost::serialization::make_nvp("value_size", value_size);
    ar << boost::serialization::make_nvp("size", container_size);
    ar << boost::serialization::make_nvp("capacity", capacity);
    ar << boost::serialization::make_nvp("deleted", deleted);
    ar << boost::serialization::make_nvp("mlf", mlf);

    map_str* m = get_map_str();
    for (auto o = map_str_begin(m); o < map_str_end(m);
         o = map_str_next(m, o)) {
      R* v = static_cast<R*>(map_str_get(m, o));
      ar << boost::serialization::make_nvp("item", *v);
    }
  }

  template <class archive>
  void load(archive& ar, const unsigned int) {
    size_t value_size;
    size_t container_size;
    size_t capacity;
    size_t deleted;
    double mlf;

    ar >> boost::serialization::make_nvp("value_size", value_size);
    ar >> boost::serialization::make_nvp("size", container_size);
    ar >> boost::serialization::make_nvp("capacity", capacity);
    ar >> boost::serialization::make_nvp("deleted", deleted);
    ar >> boost::serialization::make_nvp("mlf", mlf);

    if (container) {
      map_str_clear(get_map_str());
      container.reset();
    }
    init_map_str(true, value_size);

    if (container) {
      map_str* n = get_map_str();
      map_str_max_load_factor(n, mlf);
      map_str_rehash(n, capacity);

      for (auto i = 0; i < container_size; ++i) {
        R r;
        ar >> boost::serialization::make_nvp("item", r);
        insert(r);
      }
    } else {
      throw std::runtime_error(
          "Failed to instantiate StrMap for deserialization");
    }
  }
};

}  // namespace Libdynamic
}  // namespace K3

namespace YAML {
  template <class R>
  struct convert<K3::Libdynamic::StrMap<R>> {
    static Node encode(const K3::Libdynamic::StrMap<R>& c) {
      Node node;
      if (c.size(K3::unit_t {}) > 0) {
        for (auto& i: c) { node.push_back(convert<R>::encode(i)); }
      }
      else { node = YAML::Load("[]"); }
      return node;
    }

    static bool decode(const Node& node, K3::Libdynamic::StrMap<R>& c) {
      for (auto& i: node) { c.insert(i.as<R>()); }
      return true;
    }
  };
}  // namespace YAML


#endif

// Map selection.
#if (USE_CUSTOM_HASHMAPS && HAS_LIBDYNAMIC)

namespace K3 {
template <class R>
using StrMap = Libdynamic::StrMap<R>;
}  // namespace K3

#else

namespace K3 {
static_assert(false, "NO STRMAP");
template <class R>
using StrMap = Map<R>;
}  // namespace K3

#endif

#endif
