#ifndef K3_INTMAP
#define K3_INTMAP

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
#include "serialization/Json.hpp"
#include "serialization/Yaml.hpp"

namespace K3 {
namespace Libdynamic {

template <class R>
class IntMap {
  using Key = typename R::KeyType;
  using CloneFn = void (*)(void*, void*, size_t);
  using Container = shared_ptr<mapi>;

 public:
  IntMap() : container() {}

  IntMap(const Container& con) {
    mapi* m = con.get();
    if (m) {
      init_mapi(true, sizeof(R));
      mapi* n = get_mapi();
      for (auto o = mapi_begin(m); o < mapi_end(m); o = mapi_next(m, o)) {
        mapi_insert(n, o);
      }
    }
  }

  IntMap(Container&& con) : container(std::move(con)) {}

  template <typename Iterator>
  IntMap(Iterator begin, Iterator end) {
    init_mapi(true, sizeof(R));
    mapi* m = get_mapi();
    for (auto it = begin; it != end; ++it) {
      mapi_insert(m, static_cast<void*>(&(*it)));
    }
  }

  template <class I>
  class map_iterator : public std::iterator<std::forward_iterator_tag, R> {
    using reference =
        typename std::iterator<std::forward_iterator_tag, R>::reference;

   public:
    template <class _I>
    map_iterator(mapi* _m, _I&& _i)
        : m(_m), i(static_cast<I>(std::forward<_I>(_i))) {}

    map_iterator& operator++() {
      i = static_cast<I>(
          mapi_next(m, const_cast<void*>(static_cast<const void*>(i))));
      return *this;
    }

    map_iterator operator++(int) {
      map_iterator t = *this;
      *this++;
      return t;
    }

    auto operator -> () const { return i; }

    auto& operator*() const { return *i; }

    bool operator==(const map_iterator& other) const { return i == other.i; }

    bool operator!=(const map_iterator& other) const { return i != other.i; }

   private:
    mapi* m;
    I i;
  };

  using iterator = map_iterator<R*>;
  using const_iterator = map_iterator<const R*>;

  iterator begin() {
    mapi* m = get_mapi();
    return iterator(m, mapi_begin(m));
  }

  iterator end() {
    mapi* m = get_mapi();
    return iterator(m, mapi_end(m));
  }

  const_iterator begin() const {
    mapi* m = get_mapi();
    return const_iterator(m, mapi_begin(m));
  }

  const_iterator end() const {
    mapi* m = get_mapi();
    return const_iterator(m, mapi_end(m));
  }

  R elemToRecord(const R& e) const { return e; }

  // Functionality
  int size(unit_t) const { return mapi_size(get_mapi()); }

  template<typename F, typename G>
  auto peek(F f, G g) const {
    auto it = mapi_begin(get_mapi());
    if (it < mapi_end(get_mapi()) ) {
      return g(*static_cast<R*>(it));
    } else {
      return f(unit_t {});
    }
  }

  bool member(const R& r) const {
    mapi* m = get_mapi();
    return m->size == 0 ? false : (mapi_find(m, r.key) != nullptr);
  }

  template <class F, class G>
  auto lookup(R const& r, F f, G g) const {
    mapi* m = get_mapi();
    if (m->size == 0) {
      return f(unit_t{});
    } else {
      auto existing = mapi_find(m, r.key);
      if (existing == nullptr) {
        return f(unit_t{});
      } else {
        return g(*static_cast<R*>(existing));
      }
    }
  }

  template <class F, class G>
  auto lookup_key(int key, F f, G g) const {
    mapi* m = get_mapi();
    if (m->size == 0) {
      return f(unit_t{});
    } else {
      auto existing = mapi_find(m, key);
      if (existing == nullptr) {
        return f(unit_t{});
      } else {
        return g(*static_cast<R*>(existing));
      }
    }
  }

  // TODO(yanif): Fix insert semantics to replace value if key exists.
  unit_t insert(const R& q) {
    mapi_insert(get_mapi(), const_cast<void*>(static_cast<const void*>(&q)));
    return unit_t();
  }

  unit_t insert(R&& q) {
    mapi_clone(get_mapi(), (CloneFn)&IntMap<R>::moveElem);
    mapi_insert(get_mapi(), static_cast<void*>(&q));
    mapi_clone(get_mapi(), (CloneFn)&IntMap<R>::cloneElem);
    return unit_t();
  }

  unit_t update(const R& rec1, R& rec2) {
    mapi* m = get_mapi();
    if (m->size > 0) {
      auto existing = mapi_find(m, rec1.key);
      if (existing != nullptr) {
        mapi_erase(m, rec1.key);
        mapi_insert(m, &rec2);
      }
    }
    return unit_t();
  }

  unit_t erase(const R& rec) {
    mapi* m = get_mapi();
    if (m->size > 0) {
      auto existing = mapi_find(m, rec.key);
      if (existing != nullptr) {
        mapi_erase(m, rec.key);
      }
    }
    return unit_t();
  }

  template <class F>
  unit_t insert_with(const R& rec, F f) {
    mapi* m = get_mapi();
    if (m->size == 0) {
      mapi_insert(m, const_cast<void*>(static_cast<const void*>(&rec)));
    } else {
      auto* existing = static_cast<R*>(mapi_find(m, rec.key));
      if (existing == nullptr) {
        mapi_insert(m, const_cast<void*>(static_cast<const void*>(&rec)));
      } else {
        *existing = f(std::move(*existing), rec);
      }
    }
    return unit_t{};
  }

  template <class F, class G>
  unit_t upsert_with(const R& rec, F f, G g) {
    mapi* m = get_mapi();
    if (m->size == 0) {
      auto* placement = static_cast<R*>(
          mapi_insert(m, const_cast<void*>(static_cast<const void*>(&rec))));
      *placement = f(unit_t{});
    } else {
      auto* existing = static_cast<R*>(mapi_find(m, rec.key));
      if (existing == nullptr) {
        auto* placement = static_cast<R*>(
            mapi_insert(m, const_cast<void*>(static_cast<const void*>(&rec))));
        *placement = f(unit_t{});
      } else {
        *existing = g(std::move(*existing));
      }
    }
    return unit_t{};
  }

  template <class F, class G>
  unit_t upsert_with_key(int key, F f, G g) {
    mapi* m = get_mapi();
    if (m->size == 0) {
      insert(f(unit_t{}));
    } else {
      auto* existing = static_cast<R*>(mapi_find(m, key));
      if (existing == nullptr) {
        insert(f(unit_t{}));
      } else {
        *existing = g(std::move(*existing));
      }
    }
    return unit_t{};
  }


  //////////////////////////////////////////////////////////////
  // Bulk transformations.

  IntMap combine(const IntMap& other) const {
    // copy this DS
    IntMap result = IntMap(*this);
    // copy other DS
    mapi* m = other.get_mapi();
    for (auto o = mapi_begin(m); o < mapi_end(m); o = mapi_next(m, o)) {
      result.insert(*o);
    }
    return result;
  }

  tuple<IntMap, IntMap> split(const unit_t&) const {
    mapi* m = get_mapi();

    // Find midpoint
    const size_t size = mapi_size(m);
    const size_t half = size / 2;

    // Setup iterators
    auto mid = mapi_begin(m);
    for (size_t i = 0; i < half; ++i) {
      mid = mapi_next(m, mid);
    }

    // Construct DS from iterators
    return std::make_tuple(IntMap(begin(), iterator(m, mid)),
                           IntMap(iterator(m, mid), end()));
  }

  template <typename Fun>
  unit_t iterate(Fun f) const {
    mapi* m = get_mapi();
    for (auto o = mapi_begin(m); o < mapi_end(m); o = mapi_next(m, o)) {
      f(*static_cast<R*>(o));
    }
    return unit_t();
  }

  template <typename Fun>
  auto map(Fun f) const -> IntMap<RT<Fun, R>> {
    mapi* m = get_mapi();
    IntMap<RT<Fun, R>> result;
    for (auto o = mapi_begin(m); o < mapi_end(m); o = mapi_next(m, o)) {
      result.insert(f(*o));
    }
    return result;
  }

  template <typename Fun>
  auto map_generic(Fun f) const -> Map<RT<Fun, R>> {
    mapi* m = get_mapi();
    Map<RT<Fun, R>> result;
    for (auto o = mapi_begin(m); o < mapi_end(m); o = mapi_next(m, o)) {
      result.insert(f(*o));
    }
    return result;
  }

  template <typename Fun>
  IntMap<R> filter(Fun predicate) const {
    mapi* m = get_mapi();
    IntMap<R> result;
    for (auto o = mapi_begin(m); o < mapi_end(m); o = mapi_next(m, o)) {
      if (predicate(*o)) {
        result.insert(*o);
      }
    }
    return result;
  }

  template <typename Fun, typename Acc>
  Acc fold(Fun f, Acc acc) const {
    mapi* m = get_mapi();
    for (auto o = mapi_begin(m); o < mapi_end(m); o = mapi_next(m, o)) {
      acc = f(std::move(acc), *static_cast<R*>(o));
    }
    return acc;
  }

  template <typename F1, typename F2, typename Z>
  IntMap<R_key_value<RT<F1, R>, Z>> group_by(F1 grouper, F2 folder, const Z& init) const
  {
    // Create a map to hold partial results
    typedef RT<F1, R> K;
    IntMap<R_key_value<K, Z>> result;

    mapi* m = get_mapi();
    mapi* n = result.get_mapi();

    for (auto o = mapi_begin(m); o < mapi_end(m); o = mapi_next(m, o)) {
      R* elem = static_cast<R*>(o);
      K key = grouper(*elem);
      auto existing_acc = mapi_find(n, key);
      if (existing_acc == nullptr) {
        R_key_value<K, Z> elem(key, std::move(folder(init, *elem)));
        mapi_insert(n, &elem);
      } else {
        R_key_value<K, Z> elem(
            key, std::move(folder(std::move(existing_acc->value), *elem)));
        mapi_erase(n, key);
        mapi_insert(n, &elem);
      }
    }
    return result;
  }

  template <typename F1, typename F2, typename Z>
  Map<R_key_value<RT<F1, R>, Z>> group_by_generic(F1 grouper, F2 folder, const Z& init) const
  {
    // Create a std::unordered_map to hold partial results
    typedef RT<F1, R> K;
    std::unordered_map<K, Z> accs;

    mapi* m = get_mapi();
    for (auto o = mapi_begin(m); o < mapi_end(m); o = mapi_next(m, o)) {
      R* elem = static_cast<R*>(o);
      K key = grouper(*elem);
      if (accs.find(key) == accs.end()) {
        accs[key] = init;
      }
      accs[key] = folder(std::move(accs[key]), *elem);
    }

    Map<R_key_value<K, Z>> result;
    for (auto&& it : accs) {
      result.insert(std::move(
          R_key_value<K, Z>{std::move(it.first), std::move(it.second)}));
    }
    return result;
  }

  template <typename F1, typename F2, typename Z>
  IntMap<R_key_value<RT<F1, R>, Z>>
  group_by_contiguous(F1 grouper, F2 folder, const Z& init, const int& size) const
  {
    typedef RT<F1, R> K;
    IntMap<R_key_value<K, Z>> result;

    mapi* m = get_mapi();
    mapi* n = result.get_mapi();
    auto table = std::vector<Z>(size, init);

    for (auto o = mapi_begin(m); o < mapi_end(m); o = mapi_next(m, o)) {
      auto key = grouper(*o);
      table[key] = folder(std::move(table[key]), *o);
    }

    for (auto i = 0; i < table.size(); ++i) {
      // move out of the array as we iterate
      R_key_value<K, Z> elem(i, std::move(table[i]));
      mapi_insert(n, &elem);
    }
    return result;
  }

  template <class Fun>
  auto ext(Fun expand) const -> IntMap<typename RT<Fun, R>::ElemType> {
    typedef typename RT<Fun, R>::ElemType T;
    IntMap<T> result;
    mapi* m = get_mapi();
    auto end = mapi_end(m);
    for (auto it = mapi_begin(m); it < end; it = mapi_next(m, it)) {
      mapi* n = expand(*it);
      auto end2 = mapi_end(n);
      for (auto it2 = mapi_begin(n); it2 < end2; it2 = mapi_next(n, it2)) {
        result.insert(*it2);
      }
    }
    return result;
  }

  template <class Fun>
  auto ext_generic(Fun expand) const -> Map<typename RT<Fun, R>::ElemType> {
    typedef typename RT<Fun, R>::ElemType T;
    Map<T> result;
    mapi* m = get_mapi();
    auto end = mapi_end(m);
    for (auto it = mapi_begin(m); it < end; it = mapi_next(m, it)) {
      for (T&& elem : expand(*it).container) {
        result.insert(std::move(elem));
      }
    }
    return result;
  }

  bool operator==(const IntMap& other) const {
    return get_mapi() == other.get_mapi() ||
           (size(unit_t{}) == other.size(unit_t{}) &&
            std::is_permutation(begin(), end(), other.begin(), other.end()));
  }

  bool operator!=(const IntMap& other) const {
    return !(this->operator==(other));
  }

  bool operator<(const IntMap& other) const {
    return std::lexicographical_compare(begin(), end(), other.begin(),
                                        other.end());
  }

  bool operator>(const IntMap& other) const {
    return !(this->operator<(other) || this->operator==(other));
  }

  IntMap& getContainer() { return *const_cast<IntMap*>(this); }

  // Return a constant reference to the container
  const IntMap& getConstContainer() const { return *this; }

  template <class archive>
  void serialize(archive& ar) const {
    mapi* m = get_mapi();

    ar.write(reinterpret_cast<const char*>(&m->object_size), sizeof(m->object_size));
    ar & m->empty_key;
    ar.write(reinterpret_cast<const char*>(&m->size), sizeof(m->size));
    ar.write(reinterpret_cast<const char*>(&m->capacity), sizeof(m->capacity));

    for (auto o = mapi_begin(m); o < mapi_end(m); o = mapi_next(m, o)) {
      ar&* static_cast<R*>(o);
    }
  }

  template <class archive>
  void serialize(archive& ar) {
    size_t object_size;
    uint32_t empty_key;
    size_t container_size;
    size_t capacity;

    ar.read(reinterpret_cast<char*>(&object_size), sizeof(object_size));
    ar & empty_key;
    ar.read(reinterpret_cast<char*>(&container_size), sizeof(container_size));
    ar.read(reinterpret_cast<char*>(&capacity), sizeof(capacity));

    if (container) {
      mapi_clear(get_mapi());
      container.reset();
    }
    init_mapi(true, object_size);

    if (container) {
      mapi* n = get_mapi();
      mapi_empty_key(n, empty_key);
      mapi_rehash(n, capacity);
      mapi_clone(get_mapi(), (CloneFn)&IntMap<R>::moveElem);

      for (auto i = 0; i < container_size; ++i) {
        R r;
        ar& r;
        mapi_insert(n, &r);
      }

      mapi_clone(get_mapi(), (CloneFn)&IntMap<R>::cloneElem);
    } else {
      throw std::runtime_error(
          "Failed to instantiate IntMap for deserialization");
    }
  }

  BOOST_SERIALIZATION_SPLIT_MEMBER()

  static inline void cloneElem(void* dest, void* src, size_t sz) {
    new (dest) R(*static_cast<R*>(src));
  }

  static inline void moveElem(void* dest, void* src, size_t sz) {
    new (dest) R(std::move(*static_cast<R*>(src)));
  }

 protected:
  shared_ptr<mapi> container;

 private:
  friend class boost::serialization::access;

  mapi* get_mapi() const {
    if (!container) {
      init_mapi(true, sizeof(R));
    }
    return container.get();
  }

  void init_mapi(bool alloc, size_t sz) {
    if (alloc) {
      container = Container(mapi_new(sz), [](mapi* m) { mapi_free(m); });
    }
    mapi_clone(get_mapi(), (CloneFn)&IntMap<R>::cloneElem);
  }

  void init_mapi(bool alloc, size_t sz) const {
    if (alloc) {
      const_cast<shared_ptr<mapi>&>(container) =
          Container(mapi_new(sz), [](mapi* m) { mapi_free(m); });
    }
    mapi_clone(get_mapi(), (CloneFn)&IntMap<R>::cloneElem);
  }

  template <class archive>
  void save(archive& ar, const unsigned int) const {
    ar << boost::serialization::make_nvp("object_size", container->object_size);
    ar << boost::serialization::make_nvp("empty_key", container->empty_key);
    ar << boost::serialization::make_nvp("size", container->size);
    ar << boost::serialization::make_nvp("capacity", container->capacity);

    mapi* m = get_mapi();
    for (auto o = mapi_begin(m); o < mapi_end(m); o = mapi_next(m, o)) {
      ar << boost::serialization::make_nvp("item", *static_cast<R*>(o));
    }
  }

  template <class archive>
  void load(archive& ar, const unsigned int) {
    size_t object_size;
    uint32_t empty_key;
    size_t container_size;
    size_t capacity;

    ar >> boost::serialization::make_nvp("object_size", object_size);
    ar >> boost::serialization::make_nvp("empty_key", empty_key);
    ar >> boost::serialization::make_nvp("size", container_size);
    ar >> boost::serialization::make_nvp("capacity", capacity);

    if (container) {
      mapi_clear(get_mapi());
      container.reset();
    }
    init_mapi(true, object_size);

    if (container) {
      mapi* n = get_mapi();
      mapi_empty_key(n, empty_key);
      mapi_rehash(n, capacity);

      for (auto i = 0; i < container_size; ++i) {
        R r;
        ar >> boost::serialization::make_nvp("item", r);
        mapi_insert(n, &r);
      }
    } else {
      throw std::runtime_error(
          "Failed to instantiate IntMap for deserialization");
    }
  }
};

}  // namespace Libdynamic
}  // namespace K3

namespace YAML {
template <class R>
struct convert<K3::Libdynamic::IntMap<R>> {
  static Node encode(const K3::Libdynamic::IntMap<R>& c) {
    Node node;
    if (c.size(K3::unit_t{}) > 0) {
      for (auto& i : c) {
        node.push_back(convert<R>::encode(i));
      }
    } else {
      node = YAML::Load("[]");
    }
    return node;
  }

  static bool decode(const Node& node, K3::Libdynamic::IntMap<R>& c) {
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
struct convert<K3::Libdynamic::IntMap<E>> {
  template <class Allocator>
  static Value encode(const K3::Libdynamic::IntMap<E>& c, Allocator& al) {
    Value v;
    v.SetObject();
    v.AddMember("type", Value("IntMap"), al);
    Value inner;
    inner.SetArray();
    for (const auto& e : c) {
      inner.PushBack(convert<E>::encode(e, al), al);
    }
    v.AddMember("value", inner.Move(), al);
    return v;
  }
};
}  // namespace JSON

#endif

// Map selection.
#if USE_CUSTOM_HASHMAPS && HAS_LIBDYNAMIC

namespace K3 {
template <class R>
using IntMap = Libdynamic::IntMap<R>;
}

#else

namespace K3 {
template <class R>
using IntMap = Map<R>;
}

#endif
#endif
