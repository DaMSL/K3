// Custom map implementations for K3
#ifndef __K3_RUNTIME_MAP_DATASPACE__
#define __K3_RUNTIME_MAP_DATASPACE__

#include <iterator>
#include <map>
#include <unordered_map>

#include <functional>
#include <stdexcept>

#include <boost/serialization/array.hpp>
#include <boost/serialization/binary_object.hpp>
#include <boost/serialization/string.hpp>
#include <csvpp/csv.h>

#include "K3Config.hpp"

#if HAS_LIBDYNAMIC

extern "C" {
#include <dynamic/mapi.h>
#include <dynamic/map_str.h>
}

namespace K3 { namespace Libdynamic {

template<class R>
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

  template<typename Iterator>
  IntMap(Iterator begin, Iterator end) {
    init_mapi(true, sizeof(R));
    mapi* m = get_mapi();
    for ( auto it = begin; it != end; ++it) {
      mapi_insert(m, static_cast<void*>(&(*it)));
    }
  }

  template <class I>
  class map_iterator: public std::iterator<std::forward_iterator_tag, R> {
    using reference = typename std::iterator<std::forward_iterator_tag, R>::reference;
   public:
    template <class _I>
    map_iterator(mapi* _m, _I&& _i): m(_m), i(static_cast<I>(std::forward<_I>(_i))) {}

    map_iterator& operator ++() {
      i = static_cast<I>(mapi_next(m, const_cast<void*>(static_cast<const void*>(i))));
      return *this;
    }

    map_iterator operator ++(int) {
      map_iterator t = *this;
      *this++;
      return t;
    }

    auto operator->() const {
      return i;
    }

    auto& operator*() const {
      return *i;
    }

    bool operator ==(const map_iterator& other) const {
      return i == other.i;
    }

    bool operator !=(const map_iterator& other) const {
      return i != other.i;
    }

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

  int size(unit_t) const { return mapi_size(get_mapi()); }

  R elemToRecord(const R& e) const { return e; }

  // Maybe return the first element in the DS
  shared_ptr<R> peek(const unit_t&) const {
    shared_ptr<R> res(nullptr);
    auto it = mapi_begin(get_mapi());
    if (it < mapi_end(get_mapi()) ) {
      res = std::make_shared<R>(*static_cast<R*>(it));
    }
    return res;
  }

  template<typename F, typename G>
  auto peek_with(F f, G g) const {
    auto it = mapi_begin(get_mapi());
    if (it < mapi_end(get_mapi()) ) {
      return g(*static_cast<R*>(it));
    } else {
      return f(unit_t {});
    }
  }

  // TODO: Fix insert semantics to replace value if key exists.
  unit_t insert(const R& q) {
    mapi_insert(get_mapi(), const_cast<void*>(static_cast<const void*>(&q)));
    return unit_t();
  }

  unit_t insert(R&& q) {
    R tmp(std::move(q));
    mapi_insert(get_mapi(), static_cast<void*>(&tmp));
    return unit_t();
  }

  template <class F>
  unit_t insert_with(const R& rec, F f) {
    mapi* m = get_mapi();
    if ( m->size == 0 ) {
      mapi_insert(m, const_cast<void*>(static_cast<const void*>(&rec)));
    } else {
      auto* existing = static_cast<R*>(mapi_find(m, rec.key));
      if (existing == nullptr) {
        mapi_insert(m, const_cast<void*>(static_cast<const void*>(&rec)));
      } else {
        *existing = f(std::move(*existing))(rec);
      }
    }
    return unit_t {};
  }

  template <class F, class G>
  unit_t upsert_with(const R& rec, F f, G g) {
    mapi* m = get_mapi();
    if ( m->size == 0 ) {
      auto* placement = static_cast<R*>(mapi_insert(m, const_cast<void*>(static_cast<const void*>(&rec))));
      *placement = f(unit_t {});
    } else {
      auto* existing = static_cast<R*>(mapi_find(m, rec.key));
      if (existing == nullptr) {
        auto* placement = static_cast<R*>(mapi_insert(m, const_cast<void*>(static_cast<const void*>(&rec))));
        *placement = f(unit_t {});
      } else {
        *existing = g(std::move(*existing));
      }
    }
    return unit_t {};
  }

  unit_t erase(const R& rec) {
    mapi* m = get_mapi();
    if ( m->size > 0 ) {
      auto existing = mapi_find(m, rec.key);
      if (existing != nullptr) {
        mapi_erase(m, rec.key);
      }
    }
    return unit_t();
  }

  unit_t update(const R& rec1, R& rec2) {
    mapi* m = get_mapi();
    if ( m->size > 0 ) {
      auto existing = mapi_find(m, rec1.key);
      if (existing != nullptr) {
        mapi_erase(m, rec1.key);
        mapi_insert(m, &rec2);
      }
    }
    return unit_t();
  }

  template<typename Fun, typename Acc>
  Acc fold(Fun f, Acc acc) const {
    mapi* m = get_mapi();
    for (auto o = mapi_begin(m); o < mapi_end(m); o = mapi_next(m, o)) {
      acc = f(std::move(acc))(*o);
    }
    return acc;
  }

  template<typename Fun>
  auto map(Fun f) const -> IntMap< RT<Fun, R> > {
    mapi* m = get_mapi();
    IntMap< RT<Fun,R> > result;
    for (auto o = mapi_begin(m); o < mapi_end(m); o = mapi_next(m, o)) {
      result.insert( f(*o) );
    }
    return result;
  }

  template <typename Fun>
  unit_t iterate(Fun f) const {
    mapi* m = get_mapi();
    for (auto o = mapi_begin(m); o < mapi_end(m); o = mapi_next(m, o)) {
      f(*o);
    }
    return unit_t();
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

  tuple<IntMap, IntMap> split(const unit_t&) const {
    mapi* m = get_mapi();

    // Find midpoint
    const size_t size = mapi_size(m);
    const size_t half = size / 2;

    // Setup iterators
    auto mid = mapi_begin(m);
    for ( size_t i = 0; i < half ; ++i ) { mid = mapi_next(m, mid); }

    // Construct DS from iterators
    return std::make_tuple(IntMap(begin(), iterator(m, mid)), IntMap(iterator(m, mid), end()));
  }

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

  template<typename F1, typename F2, typename Z>
  IntMap< R_key_value< RT<F1, R>,Z >> groupBy(F1 grouper, F2 folder, const Z& init) const {
    // Create a map to hold partial results
    typedef RT<F1, R> K;
    IntMap<R_key_value<K, Z>> result;

    mapi* m = get_mapi();
    mapi* n = result.get_mapi();

    for (auto o = mapi_begin(m); o < mapi_end(m); o = mapi_next(m, o)) {
      K key = grouper(*o);
      auto existing_acc = mapi_find(n, key);
      if (existing_acc == nullptr) {
        R_key_value<K,Z> elem(key, std::move(folder(init)(*o)));
        mapi_insert(n, &elem);
      } else {
        R_key_value<K,Z> elem(key, std::move(folder(std::move(existing_acc->value))(*o)));
        mapi_erase(n, key);
        mapi_insert(n, &elem);
      }
    }
    return result;
  }

  template <class Fun>
  auto ext(Fun expand) const -> IntMap < typename RT<Fun, R>::ElemType >  {
    typedef typename RT<Fun, R>::ElemType T;
    IntMap<T> result;
    mapi* m = get_mapi();
    auto end = mapi_end(m);
    for ( auto it = mapi_begin(m); it < end; it = mapi_next(m, it) ) {
      mapi* n = expand(*it);
      auto end2 = mapi_end(n);
      for ( auto it2 = mapi_begin(n); it2 < end2; it2 = mapi_next(n, it2)  ) {
        result.insert(*it2);
      }
    }
    return result;
  }

  // Accumulate over a sampled ds.
  // This number of items accessed depends on the iterator implementation, via std::advance.
  template<typename Fun, typename Acc>
  Acc sample(Fun f, Acc acc, size_t sampleSize) const {
    mapi* m  = get_mapi();
    auto it  = mapi_begin(m);
    auto end = mapi_end(m);

    DSSampler seqSampler(m->size, sampleSize);

    tuple<bool, size_t> next_skip = seqSampler.next();
    while ( it < end && std::get<0>(next_skip) ) {
      for (auto i = 0; i < std::get<1>(next_skip); ++i) { it = mapi_next(m, it); }
      if ( it < end ) {
        acc = f(std::move(acc))(*it);
        next_skip = seqSampler.next();
      }
    }

    return acc;
  }

  // lookup ignores the value of the argument
  shared_ptr<R> lookup(const R& r) const {
    mapi* m = get_mapi();
    if ( m->size == 0 ) {
      return nullptr;
    } else {
      auto existing = mapi_find(m, r.key);
      if (existing == nullptr) {
        return nullptr;
      } else {
        return std::make_shared<R>(*static_cast<R*>(existing));
      }
    }
  }

  bool member(const R& r) const {
    mapi* m = get_mapi();
    return m->size == 0? false : ( mapi_find(m, r.key) != nullptr );
  }

  template <class F, class G>
  unit_t key_update(int key, F f, G g) {
    mapi* m = get_mapi();
    if ( m->size == 0 ) {
      auto rec = f(unit_t {});
      auto* placement = static_cast<R*>(mapi_insert(m, const_cast<void*>(static_cast<const void*>(&rec))));
    } else {
      auto* existing = static_cast<R*>(mapi_find(m, key));
      if (existing == nullptr) {
        auto rec = f(unit_t {});
        auto* placement = static_cast<R*>(mapi_insert(m, const_cast<void*>(static_cast<const void*>(&rec))));
      } else {
        *existing = g(std::move(*existing));
      }
    }
    return unit_t {};
  }

  template <class F>
  unit_t key_lookup_with(int key, F f) const {
    mapi* m = get_mapi();
    if ( m->size > 0 ) {
      auto existing = mapi_find(m, key);
      if (existing != nullptr) {
        return f(*static_cast<R*>(existing));
      }
    }
    return unit_t {};
  }

  template <class F>
  unit_t lookup_with(R const& r, F f) const {
    mapi* m = get_mapi();
    if ( m->size > 0 ) {
      auto existing = mapi_find(m, r.key);
      if (existing != nullptr) {
        return f(*static_cast<R*>(existing));
      }
    }
    return unit_t {};
  }

  template <class F, class G>
  auto lookup_with2(R const& r, F f, G g) const {
    mapi* m = get_mapi();
    if ( m->size == 0 ) {
      return f(unit_t {});
    } else {
      auto existing = mapi_find(m, r.key);
      if (existing == nullptr) {
        return f(unit_t {});
      } else {
        return g(*static_cast<R*>(existing));
      }
    }
  }

  template <class F>
  auto lookup_with3(R const& r, F f) const {
    mapi* m = get_mapi();
    if ( m->size > 0 ) {
      auto existing = mapi_find(m, r.key);
      if (existing != nullptr) {
        return f(*static_cast<R*>(existing));
      }
    }
    throw std::runtime_error("No match on IntMap.lookup_with3");
  }

  template <class F, class G>
  auto lookup_with4(R const& r, F f, G g) const {
    mapi* m = get_mapi();
    if ( m->size == 0 ) {
      return f(unit_t {});
    } else {
      auto existing = mapi_find(m, r.key);
      if (existing == nullptr) {
        return f(unit_t {});
      } else {
        return g(*static_cast<R*>(existing));
      }
    }
  }

  bool operator==(const IntMap& other) const {
    return get_mapi() == other.get_mapi()
            || ( size() == other.size()
                  && std::is_permutation(begin(), end(), other.begin(), other.end()) ) ;
  }

  bool operator!=(const IntMap& other) const {
    return !(this->operator==(other));
  }

  bool operator<(const IntMap& other) const {
    return std::lexicographical_compare(begin(), end(), other.begin(), other.end());
  }

  bool operator>(const IntMap& other) const {
    return !(this->operator<(other) || this->operator==(other));
  }

  IntMap& getContainer() { return *const_cast<IntMap*>(this); }

  // Return a constant reference to the container
  const IntMap& getConstContainer() const { return *this; }


  template<class archive>
  void serialize(archive &ar) const {
    mapi* m = get_mapi();

    ar & m->object_size;
    ar & m->empty_key;
    ar & m->size;
    ar & m->capacity;

    for (auto o = mapi_begin(m); o < mapi_end(m); o = mapi_next(m, o)) {
      ar & *static_cast<R*>(o);
    }
  }

  template<class archive>
  void serialize(archive &ar) {
    size_t object_size;
    uint32_t empty_key;
    size_t container_size;
    size_t capacity;

    ar & object_size;
    ar & empty_key;
    ar & container_size;
    ar & capacity;

    if ( container ) { mapi_clear(get_mapi()); container.reset(); }
    init_mapi(true, object_size);

    if ( container ) {
      mapi* n = get_mapi();
      mapi_empty_key(n, empty_key);
      mapi_rehash(n, capacity);

      for ( auto i = 0; i < container_size; ++i ) {
        R r;
        ar & r;
        mapi_insert(n, &r);
      }
    } else {
      throw std::runtime_error("Failed to instantiate IntMap for deserialization");
    }
  }

  BOOST_SERIALIZATION_SPLIT_MEMBER()

  static inline void cloneElem(void* dest, void* src, size_t sz) {
    new(dest) R(*static_cast<R*>(src));
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
    if ( alloc ) {
      container = Container(mapi_new(sz), [](mapi* m){ mapi_free(m); });
    }
    mapi_clone(get_mapi(), (CloneFn) &IntMap<R>::cloneElem);
  }

  void init_mapi(bool alloc, size_t sz) const {
    if ( alloc ) {
      const_cast<shared_ptr<mapi>&>(container) =
        Container(mapi_new(sz), [](mapi* m){ mapi_free(m); });
    }
    mapi_clone(get_mapi(), (CloneFn) &IntMap<R>::cloneElem);
  }

  template<class archive>
  void save(archive &ar, const unsigned int) const {
    ar << boost::serialization::make_nvp("object_size", container->object_size);
    ar << boost::serialization::make_nvp("empty_key", container->empty_key);
    ar << boost::serialization::make_nvp("size", container->size);
    ar << boost::serialization::make_nvp("capacity", container->capacity);

    mapi* m = get_mapi();
    for (auto o = mapi_begin(m); o < mapi_end(m); o = mapi_next(m, o)) {
      ar << boost::serialization::make_nvp("item", *static_cast<R*>(o));
    }
  }

  template<class archive>
  void load(archive &ar, const unsigned int) {
    size_t object_size;
    uint32_t empty_key;
    size_t container_size;
    size_t capacity;

    ar >> boost::serialization::make_nvp("object_size", object_size);
    ar >> boost::serialization::make_nvp("empty_key", empty_key);
    ar >> boost::serialization::make_nvp("size", container_size);
    ar >> boost::serialization::make_nvp("capacity", capacity);

    if ( container ) { mapi_clear(get_mapi()); container.reset(); }
    init_mapi(true, object_size);

    if ( container ) {
      mapi* n = get_mapi();
      mapi_empty_key(n, empty_key);
      mapi_rehash(n, capacity);

      for ( auto i = 0; i < container_size; ++i ) {
        R r;
        ar >> boost::serialization::make_nvp("item", r);
        mapi_insert(n, &r);
      }
    } else {
      throw std::runtime_error("Failed to instantiate IntMap for deserialization");
    }
  }

};

//
// TODO: map_str appears to rely on external management of its keys, since it
// only stores keys' pointers, while memcpying the values.
// Thus we need to ensure stable keys are added to our StrMap. Possible implementations:
// a. have the StrMap directly manage keys. This will duplicate keys.
// b. 'stabilize' the keys. Since we store the R_key_value as the value part of a map_str,
//    we could stabilize the pointer after addition to the map_str by updating its pointer
//    to the memcpy'd value. We should do this in a fork of libdynamic.
//
template<class R>
class StrMap {
  using Key = typename R::KeyType;
  using CloneFn = void (*)(void*, void*, size_t);

 public:
  using Container = shared_ptr<map_str>;

  StrMap(): container() {}

  StrMap(const Container& con) {
    init_map_str(true, sizeof(R));
    map_str* m = con.get();
    for (auto o = map_str_begin(m); o < map_str_end(m); o = map_str_next(m, o)) {
      insert(static_cast<R*>(map_str_get(m,o)));
    }
  }

  StrMap(Container&& con) : container(std::move(con)) {}

  template<typename Iterator>
  StrMap(Iterator begin, Iterator end) {
    init_map_str(true, sizeof(R));
    for ( auto it = begin; it != end; ++it) {
      insert(*it);
    }
  }

  template <class V, class I>
  class map_iterator: public std::iterator<std::forward_iterator_tag, R> {
    using reference = typename std::iterator<std::forward_iterator_tag, R>::reference;
   public:
    template <class _I>
    map_iterator(map_str* _m, _I&& _i): m(_m), i(std::forward<_I>(_i)) {}

    map_iterator& operator ++() {
      i = map_str_next(m, i);
      return *this;
    }

    map_iterator operator ++(int) {
      map_iterator t = *this;
      *this++;
      return t;
    }

    auto operator->() const {
      return static_cast<V*>(map_str_get(m, i));
    }

    auto& operator*() const {
      return *static_cast<V*>(map_str_get(m, i));
    }

    bool operator ==(const map_iterator& other) const {
      return i == other.i;
    }

    bool operator !=(const map_iterator& other) const {
      return i != other.i;
    }

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

  shared_ptr<R> peek(const unit_t&) const {
    shared_ptr<R> res(nullptr);
    map_str* m = get_map_str();
    auto it = map_str_begin(m);
    if (it < map_str_end(m) ) {
      res = std::make_shared<R>(*map_str_get(m, it));
    }
    return res;
  }

  template<typename F, typename G>
  auto peek_with(F f, G g) const {
    map_str* m = get_map_str();
    auto it = map_str_begin(m);
    if (it < map_str_end(m) ) {
      return g(*map_str_get(m, it));
    } else {
      return f(unit_t {});
    }
  }

  size_t insert_aux(const R& q) {
    map_str* m = get_map_str();
    auto pos = map_str_insert(m, q.key.begin(), const_cast<void*>(static_cast<const void*>(&q)));

    R* v = static_cast<R*>(map_str_get(m, pos));
    map_str_stabilize_key(m, pos, v->key.begin());

    return pos;
  }

  unit_t insert(const R& q) {
    insert_aux(q);
    return unit_t {};
  }

  template <class F>
  unit_t insert_with(const R& rec, F f) {
    map_str* m = get_map_str();
    if ( m->size == 0 ) {
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
    return unit_t {};
  }

  template <class F, class G>
  unit_t upsert_with(const R& rec, F f, G g) {
    map_str* m = get_map_str();
    if ( m->size == 0 ) {
      auto new_pos = insert_aux(rec);
      auto* placement = static_cast<R*>(map_str_get(m, new_pos));
      *placement = f(unit_t {});
      map_str_stabilize_key(m, new_pos, placement->key.begin());
    } else {
      auto existing = map_str_find(m, rec.key.begin());
      if (existing == map_str_end(m)) {
        auto new_pos = insert_aux(rec);
        auto* placement = static_cast<R*>(map_str_get(m, new_pos));
        *placement = f(unit_t {});
        map_str_stabilize_key(m, new_pos, placement->key.begin());
      } else {
        auto* v = static_cast<R*>(map_str_get(m,existing));
        *v = g(std::move(*v));
        map_str_stabilize_key(m, existing, v->key.begin());
      }
    }
    return unit_t {};
  }

  unit_t erase(const R& rec) {
    map_str* m = get_map_str();
    if ( m->size > 0 ) {
      auto existing = map_str_find(m, rec.key.begin());
      if (existing != map_str_end(m)) {
        R* v = static_cast<R*>(map_str_get(m,existing));
        map_str_erase(m, rec.key.begin());
        v->~R();
      }
    }
    return unit_t();
  }

  unit_t update(const R& rec1, R& rec2) {
    map_str* m = get_map_str();
    if ( m->size > 0 ) {
      auto existing = map_str_find(m, rec1.key.begin());
      if (existing != map_str_end(m)) {
        R* v = static_cast<R*>(map_str_get(m,existing));
        map_str_erase(m, rec1.key.begin());
        v->~R();
        insert(rec2);
      }
    }
    return unit_t();
  }

  template<typename Fun, typename Acc>
  Acc fold(Fun f, Acc acc) const {
    map_str* m = get_map_str();
    for (auto o = map_str_begin(m); o < map_str_end(m); o = map_str_next(m, o)) {
      acc = f(std::move(acc))(*map_str_get(m,o));
    }
    return acc;
  }

  template<typename Fun>
  auto map(Fun f) const -> StrMap< RT<Fun, R> > {
    map_str* m = get_map_str();
    StrMap< RT<Fun,R> > result;
    for (auto o = map_str_begin(m); o < map_str_end(m); o = map_str_next(m, o)) {
      result.insert( f(*map_str_get(m,o)) );
    }
    return result;
  }

  template <typename Fun>
  unit_t iterate(Fun f) const {
    map_str* m = get_map_str();
    for (auto o = map_str_begin(m); o < map_str_end(m); o = map_str_next(m, o)) {
      f(*map_str_get(m,o));
    }
    return unit_t();
  }

  template <typename Fun>
  StrMap<R> filter(Fun predicate) const {
    map_str* m = get_map_str();
    StrMap< RT<Fun,R> > result;
    for (auto o = map_str_begin(m); o < map_str_end(m); o = map_str_next(m, o)) {
      if (predicate(*map_str_get(m,o))) {
        result.insert(*map_str_get(m,o));
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
    for ( size_t i = 0; i < half ; ++i ) { mid = map_str_next(m, mid); }

    // Construct DS from iterators
    return std::make_tuple(StrMap(begin(), iterator(m, mid)), StrMap(iterator(m, mid), end()));
  }

  StrMap combine(const StrMap& other) const {
    // copy this DS
    StrMap result = StrMap(*this);
    // copy other DS
    map_str* m = other.get_map_str();
    for (auto o = map_str_begin(m); o < map_str_end(m); o = map_str_next(m, o)) {
      result.insert( *map_str_get(m,o) );
    }
    return result;
  }

  template<typename F1, typename F2, typename Z>
  StrMap< R_key_value< RT<F1, R>,Z >> groupBy(F1 grouper, F2 folder, const Z& init) const {
    // Create a map to hold partial results
    typedef RT<F1, R> K;
    StrMap<R_key_value<K, Z>> result;

    map_str* m = get_map_str();
    map_str* n = result.get_map_str();

    for (auto o = map_str_begin(m); o < map_str_end(m); o = map_str_next(m, o)) {
      R* v = static_cast<R*>(map_str_get(m,o));
      K key = grouper(*v);
      auto existing_acc = map_str_find(n, key.begin());
      if (existing_acc == map_str_end(n)) {
        R_key_value<K,Z> elem(key, std::move(folder(init)(*map_str_get(m,o))));
        insert(elem);
      } else {
        R* accv = static_cast<R*>(map_str_get(n, existing_acc));
        R_key_value<K,Z> elem(key, std::move(folder(std::move(accv->value))(*v)));
        map_str_erase(n, key);
        accv->~R_key_value<K,Z>();
        insert(elem);
        delete accv;
      }
    }
    return result;
  }

  template <class Fun>
  auto ext(Fun expand) const -> StrMap < typename RT<Fun, R>::ElemType >  {
    typedef typename RT<Fun, R>::ElemType T;
    StrMap<T> result;
    map_str* m = get_map_str();
    auto end = map_str_end(m);
    for ( auto it = map_str_begin(m); it < end; it = map_str_next(m, it) ) {
      map_str* n = expand(*map_str_get(m,it));
      auto end2 = map_str_end(n);
      for ( auto it2 = map_str_begin(n); it2 < end2; it2 = map_str_next(n, it2)  ) {
        result.insert(*map_str_get(n,it2));
      }
    }
    return result;
  }

  // Accumulate over a sampled ds.
  // This number of items accessed depends on the iterator implementation, via std::advance.
  template<typename Fun, typename Acc>
  Acc sample(Fun f, Acc acc, size_t sampleSize) const {
    map_str* m  = get_map_str();
    auto it  = map_str_begin(m);
    auto end = map_str_end(m);

    DSSampler seqSampler(m->size, sampleSize);

    tuple<bool, size_t> next_skip = seqSampler.next();
    while ( it < end && std::get<0>(next_skip) ) {
      for (auto i = 0; i < std::get<1>(next_skip); ++i) { it = map_str_next(m, it); }
      if ( it < end ) {
        acc = f(std::move(acc))(*map_str_get(m,it));
        next_skip = seqSampler.next();
      }
    }

    return acc;
  }

  // lookup ignores the value of the argument
  shared_ptr<R> lookup(const R& r) const {
    map_str* m = get_map_str();
    if ( m->size == 0 ) {
      return nullptr;
    } else {
      auto existing = map_str_find(m, r.key.begin());
      if (existing == map_str_end(m)) {
        return nullptr;
      } else {
        return std::make_shared<R>(*static_cast<R*>(map_str_get(m,existing)));
      }
    }
  }

  bool member(const R& r) const {
    map_str* m = get_map_str();
    return m->size == 0? false : ( map_str_find(m, r.key.begin()) != map_str_end(m) );
  }

  template <class F>
  unit_t lookup_with(R const& r, F f) const {
    map_str* m = get_map_str();
    if ( m->size > 0 ) {
      auto existing = map_str_find(m, r.key.begin());
      if (existing != map_str_end(m)) {
        return f(*map_str_get(m,existing));
      }
    }
    return unit_t {};
  }

  template <class F, class G>
  auto lookup_with2(R const& r, F f, G g) const {
    map_str* m = get_map_str();
    if ( m->size == 0 ) {
      return f(unit_t {});
    } else {
      auto existing = map_str_find(m, r.key.begin());
      if (existing == map_str_end(m)) {
        return f(unit_t {});
      } else {
        return g(*map_str_get(m,existing));
      }
    }
  }

  template <class F>
  auto lookup_with3(R const& r, F f) const {
    map_str* m = get_map_str();
    if ( m->size > 0 ) {
      auto existing = map_str_find(m, r.key.begin());
      if (existing != map_str_end(m)) {
        return f(*map_str_get(m,existing));
      }
    }
    throw std::runtime_error("No match on StrMap.lookup_with3");
  }

  template <class F, class G>
  auto lookup_with4(R const& r, F f, G g) const {
    map_str* m = get_map_str();
    if ( m->size == 0 ) {
      return f(unit_t {});
    } else {
      auto existing = map_str_find(m, r.key.begin());
      if (existing == map_str_end(m)) {
        return f(unit_t {});
      } else {
        return g(*map_str_get(m,existing));
      }
    }
  }

  bool operator==(const StrMap& other) const {
    return get_map_str() == other.get_map_str()
            || ( size() == other.size()
                  && std::is_permutation(begin(), end(), other.begin(), other.end()) ) ;
  }

  bool operator!=(const StrMap& other) const {
    return !(this->operator==(other));
  }

  bool operator<(const StrMap& other) const {
    return std::lexicographical_compare(begin(), end(), other.begin(), other.end());
  }

  bool operator>(const StrMap& other) const {
    return !(this->operator<(other) || this->operator==(other));
  }

  StrMap& getContainer() { return *const_cast<StrMap*>(this); }

  // Return a constant reference to the container
  const StrMap& getConstContainer() const { return *this; }

  template<class archive>
  void serialize(archive &ar) const {
    map_str* m = get_map_str();

    ar & m->value_size;
    ar & m->size;
    ar & m->capacity;
    ar & m->deleted;
    ar & m->max_load_factor;

    for (auto o = map_str_begin(m); o < map_str_end(m); o = map_str_next(m, o)) {
      R* v = static_cast<R*>(map_str_get(m,o));
      ar & *v;
    }
  }

  template<class archive>
  void serialize(archive &ar) {
    size_t value_size;
    size_t container_size;
    size_t capacity;
    size_t deleted;
    double mlf;

    ar & value_size;
    ar & container_size;
    ar & capacity;
    ar & deleted;
    ar & mlf;

    if ( container ) { map_str_clear(get_map_str()); container.reset(); }
    init_map_str(true, value_size);

    if ( container ) {
      map_str* n = get_map_str();
      map_str_max_load_factor(n, mlf);
      map_str_rehash(n, capacity);

      for ( auto i = 0; i < container_size; ++i ) {
        R r;
        ar & r;
        insert(r);
      }
    } else {
      cout << "Here" << endl;
      throw std::runtime_error("Failed to instantiate StrMap for deserialization");
    }
  }

  BOOST_SERIALIZATION_SPLIT_MEMBER()

protected:
  shared_ptr<map_str> container;

private:
  friend class boost::serialization::access;

  static inline void cloneElem(void* dest, void* src, size_t sz) {
    new(dest) R(*static_cast<R*>(src));
  }

  map_str* get_map_str() const {
    if (!container) {
      init_map_str(true, sizeof(R));
    }
    return container.get();
  }

  void init_map_str(bool alloc, size_t sz) {
    if ( alloc ) {
      container = Container(map_str_new(sz), [](map_str* m){ map_str_free(m); });
    }
    map_str_clone(get_map_str(), (CloneFn) &StrMap<R>::cloneElem);
  }

  void init_map_str(bool alloc, size_t sz) const {
    if ( alloc ) {
      const_cast<shared_ptr<map_str>&>(container) =
        Container(map_str_new(sz), [](map_str* m){ map_str_free(m); });
    }
    map_str_clone(get_map_str(), (CloneFn) &StrMap<R>::cloneElem);
  }

  template<class archive>
  void save(archive &ar, const unsigned int) const {
    size_t value_size     = container->value_size;
    size_t container_size = container->size;
    size_t capacity       = container->capacity;
    size_t deleted        = container->deleted;
    double mlf            = container->max_load_factor;

    ar << boost::serialization::make_nvp("value_size", value_size);
    ar << boost::serialization::make_nvp("size", container_size);
    ar << boost::serialization::make_nvp("capacity", capacity);
    ar << boost::serialization::make_nvp("deleted", deleted);
    ar << boost::serialization::make_nvp("mlf", mlf);

    map_str* m = get_map_str();
    for (auto o = map_str_begin(m); o < map_str_end(m); o = map_str_next(m, o)) {
      R* v = static_cast<R*>(map_str_get(m,o));
      ar << boost::serialization::make_nvp("item", *v);
    }
  }

  template<class archive>
  void load(archive &ar, const unsigned int) {
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

    if ( container ) { map_str_clear(get_map_str()); container.reset(); }
    init_map_str(true, value_size);

    if ( container ) {
      map_str* n = get_map_str();
      map_str_max_load_factor(n, mlf);
      map_str_rehash(n, capacity);

      for ( auto i = 0; i < container_size; ++i ) {
        R r;
        ar >> boost::serialization::make_nvp("item", r);
        insert(r);
      }
    } else {
      cout << "Here" << endl;
      throw std::runtime_error("Failed to instantiate StrMap for deserialization");
    }
  }

};

} // end Libdynamic

} // end K3

namespace boost {
  namespace serialization {
    template <class _T0>
    class implementation_level<K3::Libdynamic::IntMap<_T0>> {
      public:
        typedef  mpl::integral_c_tag tag;
        typedef  mpl::int_<object_serializable> type;
        BOOST_STATIC_CONSTANT(int, value = implementation_level::type::value);
    };

    template <class _T0>
    class implementation_level<K3::Libdynamic::StrMap<_T0>> {
      public:
        typedef  mpl::integral_c_tag tag;
        typedef  mpl::int_<object_serializable> type;
        BOOST_STATIC_CONSTANT(int, value = implementation_level::type::value);
    };
  }
}

template <class Elem>
std::size_t hash_value(K3::Libdynamic::IntMap<Elem> const& b) {
  return boost::hash_range(b.begin(), b.end());
}

template <class Elem>
std::size_t hash_value(K3::Libdynamic::StrMap<Elem> const& b) {
  return boost::hash_range(b.begin(), b.end());
}

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
     for (const auto& e : c) { inner.PushBack(convert<E>::encode(e, al), al); }
     v.AddMember("value", inner.Move(), al);
     return v;
    }
  };

  template <class E>
  struct convert<K3::Libdynamic::StrMap<E>> {
    template <class Allocator>
    static Value encode(const K3::Libdynamic::StrMap<E>& c, Allocator& al) {
     Value v;
     v.SetObject();
     v.AddMember("type", Value("StrMap"), al);
     Value inner;
     inner.SetArray();
     for (const auto& e : c) { inner.PushBack(convert<E>::encode(e, al), al); }
     v.AddMember("value", inner.Move(), al);
     return v;
    }
  };

}

namespace YAML {
  template <class R>
  struct convert<K3::Libdynamic::IntMap<R>> {
    static Node encode(const K3::Libdynamic::IntMap<R>& c) {
      Node node;
      if (c.size(unit_t {}) > 0) {
        for (auto& i: c) { node.push_back(convert<R>::encode(i)); }
      }
      else { node = YAML::Load("[]"); }
      return node;
    }

    static bool decode(const Node& node, K3::Libdynamic::IntMap<R>& c) {
      for (auto& i: node) { c.insert(i.as<R>()); }
      return true;
    }
  };

  template <class R>
  struct convert<K3::Libdynamic::StrMap<R>> {
    static Node encode(const K3::Libdynamic::StrMap<R>& c) {
      Node node;
      if (c.size(unit_t {}) > 0) {
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
}

#endif

// Map selection.
#if USE_CUSTOM_HASHMAPS && HAS_LIBDYNAMIC

namespace K3 {
  template<class R> using IntMap = Libdynamic::IntMap<R>;
  template<class R> using StrMap = Libdynamic::StrMap<R>;
}

#else

namespace K3 {
  template<class R> using IntMap = Map<R>;
  template<class R> using StrMap = Map<R>;
}

#endif

namespace K3 {

template<class R>
class VMap {
 public:
  using Version = int;

 protected:
  using Key = typename R::KeyType;

  // The VMap stores versions in decreasing order.
  template<typename Elem> using VContainer = map<Version, Elem, std::greater<Version>>;
  using Container = unordered_map<Key, VContainer<R>>;

  typedef typename Container::const_iterator const_iterator_type;
  typedef typename Container::iterator iterator_type;

 public:
  VMap() : container() {}
  VMap(const Container& con) : container(con) {}
  VMap(Container&& con) : container(std::move(con)) {}

  // Construct from (container) iterators
  template<typename Iterator>
  VMap(Iterator begin, Iterator end) : container(begin,end) {}

  // Returns the latest version of the element.
  template <class Pair>
  R elemToRecord(const Pair& e) const {
    R result = (e.second.begin() == e.second.end())? std::move(R()) : e.second.begin()->second;
    return result;
  }

  template <class I>
  class map_iterator: public std::iterator<std::forward_iterator_tag, R> {
    using container = Container;
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
      return &(*(i->second.begin()));
    }

    auto& operator*() const {
      return *(i->second.begin());
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

  using iterator = map_iterator<typename Container::iterator>;
  using const_iterator = map_iterator<typename Container::const_iterator>;

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
      if ( vit != elem.second.end() ) {
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
    auto vexisting = existing == std::end(container)?
      std::end(container[rec.key]) : container[rec.key].find(v);

    if (vexisting == std::end(container[rec.key])) {
      container[rec.key][v] = rec;
    } else {
      container[rec.key][v] = f(std::move(vexisting->second))(rec);
    }

    return unit_t {};
  }

  template <class F, class G>
  unit_t upsert_with(const Version& v, const R& rec, F f, G g) {
    auto existing = container.find(rec.key);
    auto vexisting = existing == std::end(container)?
      std::end(container[rec.key]) : container[rec.key].find(v);

    if (vexisting == std::end(container[rec.key])) {
      container[rec.key][v] = f(unit_t {});
    } else {
      container[rec.key][v] = g(std::move(vexisting->second));
    }

    return unit_t {};
  }

  unit_t erase(const Version& v, const R& rec) {
    auto it = container.find(rec.key);
    if (it != container.end()) {
      auto vit = it->second.find(v);
      if ( vit != it->second.end() ) {
        it->second.erase(vit);
        if ( it->second.empty() ) {
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
      if ( vit != it->second.end() ) {
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
      if ( vit != it->second.end() ) {
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
      if ( vit != it->second.end() ) {
        return f(vit->second);
      }
    }
    return unit_t {};
  }

  template <class F, class G>
  auto lookup_with2(const Version& v, R const& r, F f, G g) const {
    auto it = container.find(r.key);
    if (it == container.end()) {
      return f(unit_t {});
    } else {
      auto vit = it->second.find(v);
      if ( vit == it->second.end() ) {
        return f(unit_t {});
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
      if ( vit != it->second.end() ) {
        return f(vit->second);
      }
    }
    throw std::runtime_error("No match on Map.lookup_with3");
  }

  template <class F, class G>
  auto lookup_with4(const Version& v, R const& r, F f, G g) const {
    auto it = container.find(r.key);
    if (it == container.end()) {
      return f(unit_t {});
    } else {
      auto vit = it->second.find(v);
      if ( vit == it->second.end() ) {
        return f(unit_t {});
      } else {
        return g(vit->second);
      }
    }
  }

  //////////////////////////////////////////////////
  // Frontier-based map retrieval.
  // These methods apply to the nearest version that is strictly less
  // than the version specified as an argument.

  shared_ptr<R> lookup_before(const Version& v, const R& r) const {
    auto it = container.find(r.key);
    if (it != container.end()) {
      auto vit = it->second.upper_bound(v);
      if ( vit != it->second.end() ) {
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
      if ( vit != it->second.end() ) {
        return f(vit->second);
      }
    }
    return unit_t {};
  }

  template <class F, class G>
  auto lookup_with2_before(const Version& v, R const& r, F f, G g) const {
    auto it = container.find(r.key);
    if (it == container.end()) {
      return f(unit_t {});
    } else {
      auto vit = it->second.upper_bound(v);
      if ( vit == it->second.end() ) {
        return f(unit_t {});
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
      if ( vit != it->second.end() ) {
        return f(vit->second);
      }
    }
    throw std::runtime_error("No match on Map.lookup_with3_before");
  }

  template <class F, class G>
  auto lookup_with4_before(const Version& v, R const& r, F f, G g) const {
    auto it = container.find(r.key);
    if (it == container.end()) {
      return f(unit_t {});
    } else {
      auto vit = it->second.upper_bound(v);
      if ( vit == it->second.end() ) {
        return f(unit_t {});
      } else {
        return g(vit->second);
      }
    }
  }

  // Non-inclusive erase less than version.
  unit_t erase_prefix(const Version& v, const R& rec) {
    auto it = container.find(rec.key);
    if (it != container.end()) {
      auto vlteq = it->second.lower_bound(v);
      auto vless = it->second.upper_bound(v);
      auto vend  = it->second.end();
      if ( vless != vend ) {
        it->second.erase((vlteq == vless)? ++vless : vless, vend);
        if ( it->second.empty() ) {
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
      auto vlteq  = it->second.lower_bound(v);
      for (; vstart != vlteq; vstart++) {
        container[rec.key][vstart->first] = f(vstart->first)(std::move(vstart->second));
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
      if ( vit != p.second.end() ) {
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
      if ( it != p.second.end() ) {
        f(it->second);
      }
    }
    return unit_t();
  }

  template<typename Fun>
  auto map(const Version& v, Fun f) const -> VMap< RT<Fun, R> > {
    VMap< RT<Fun,R> > result;
    for (const auto& p : container) {
      auto it = p.second.upper_bound(v);
      if ( it != p.second.end() ) {
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
      if ( it != p.second.end() && predicate(it->second) ) {
        result.insert(it->first, it->second);
      }
    }
    return result;
  }

  template<typename Fun, typename Acc>
  Acc fold(const Version& v, Fun f, Acc acc) const {
    for (const auto& p : container) {
      auto it = p.second.upper_bound(v);
      if ( it != p.second.end() ) {
        acc = f(std::move(acc))(it->second);
      }
    }
    return acc;
  }

  template<typename F1, typename F2, typename Z>
  VMap<R_key_value<RT<F1, R>, Z>> groupBy(const Version& v, F1 grouper, F2 folder, const Z& init) const {
    // Create a map to hold partial results
    using K = RT<F1, R>;
    unordered_map<K, VContainer<Z>> accs;

    for (const auto& it : container) {
      auto vit = it->second.upper_bound(v);
      if ( vit != it->second.end() ) {
        K key = grouper(vit->second);
        if (accs.find(key) == accs.end()) {
          accs[key][vit->first] = init;
        }
        accs[key][vit->first] = folder(std::move(accs[key][vit->first]))(vit->second);
      }
    }

    VMap<R_key_value<K,Z>> result;
    for (auto& it : accs) {
      for (auto&& vit : it.second) {
        result.insert(std::move(vit.first), std::move(R_key_value<K, Z> {it.first, std::move(vit.second)}));
      }
    }

    return result;
  }

  template <class Fun>
  auto ext(const Version& v, Fun expand) const -> VMap < typename RT<Fun, R>::ElemType >  {
    typedef typename RT<Fun, R>::ElemType T;
    VMap<T> result;
    for (const auto& it : container) {
      auto vit = it->second.upper_bound(v);
      if ( vit != it->second.end() ) {
        for (auto& it2 : expand(vit->second).container) {
          result.insert(vit->first, it2.second);
        }
      }
    }

    return result;
  }


  //////////////////////////
  // Multi-version methods.

  template<typename Fun, typename Acc>
  Acc fold_all(Fun f, Acc acc) const {
    for (const auto& p : container) {
      for (const auto& velem : p.second) {
        acc = f(std::move(acc))(velem.first)(velem.second);
      }
    }
    return acc;
  }

  // Non-inclusive erase less than version.
  unit_t erase_prefix_all(const Version& v) {
    auto end = container.end();
    for (auto it = container.begin(); it != end;) {
      auto vlteq = it->second.lower_bound(v);
      auto vless = it->second.upper_bound(v);
      auto vend  = it->second.end();
      if ( vless != vend ) {
        it->second.erase((vlteq == vless)? ++vless : vless, vend);
        if ( it->second.empty() ) {
          it = container.erase(it);
        } else {
          ++it;
        }
      } else {
        ++it;
      }
    }
    return unit_t();
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

  template<class Archive>
  void serialize(Archive &ar) { ar & container; }

  template<class Archive>
  void serialize(Archive &ar, const unsigned int) {
    ar & boost::serialization::make_nvp("__K3VMap", container);
  }

 protected:
  Container container;

 private:
  friend class boost::serialization::access;

}; // class VMap


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

  // Accumulate over a sampled ds.
  // This number of items accessed depends on the iterator implementation, via std::advance.
  template<typename Fun, typename Acc>
  Acc sample(Fun f, Acc acc, size_t sampleSize) const {
    auto it = container.begin();
    DSSampler seqSampler(container.size(), sampleSize);

    tuple<bool, size_t> next_skip = seqSampler.next();
    while ( it != container.end() && std::get<0>(next_skip) ) {
      std::advance(it, std::get<1>(next_skip));
      if ( it != container.end() ) {
        acc = f(std::move(acc))(*it);
        next_skip = seqSampler.next();
      }
    }

    return acc;
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
  auto map(Fun f) const -> SortedMapE< RT<Fun, R> > {
    SortedMapE< RT<Fun,R> > result;
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
      acc = f(std::move(acc))(p.second);
    }
    return acc;
  }

  template<typename F1, typename F2, typename Z>
  SortedMapE< R_key_value< RT<F1, R>,Z >> groupBy(F1 grouper, F2 folder, const Z& init) const {
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
    SortedMapE<R_key_value<K,Z>> result;
    for (auto&& it : accs) {
      result.insert(std::move(R_key_value<K, Z> {std::move(it.first), std::move(it.second)}));
    }
    return result;
  }

  template <class Fun>
  auto ext(Fun expand) const -> SortedMapE < typename RT<Fun, R>::ElemType >  {
    typedef typename RT<Fun, R>::ElemType T;
    SortedMapE<T> result;
    for (const auto& it : container) {
      for (auto& it2 : expand(it.second).container) {
        result.insert(it2.second);
      }
    }

    return result;
  }

  // Accumulate over a sampled ds.
  // This number of items accessed depends on the iterator implementation, via std::advance.
  template<typename Fun, typename Acc>
  Acc sample(Fun f, Acc acc, size_t sampleSize) const {
    auto it = container.begin();
    DSSampler seqSampler(container.size(), sampleSize);

    tuple<bool, size_t> next_skip = seqSampler.next();
    while ( it != container.end() && std::get<0>(next_skip) ) {
      std::advance(it, std::get<1>(next_skip));
      if ( it != container.end() ) {
        acc = f(std::move(acc))(*it);
        next_skip = seqSampler.next();
      }
    }

    return acc;
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

  SortedMapE<R> filter_lt(const R& rec) const {
    const auto& x = getConstContainer();
    auto it = x.lower_bound(rec.key);
    return SortedMapE<R>(x.begin(), it);
  }

  SortedMapE<R> filter_gt(const R& rec) const {
    const auto& x = getConstContainer();
    auto it = x.upper_bound(rec.key);
    return SortedMapE<R>(it, x.end());
  }

  SortedMapE<R> filter_geq(const R& rec) const {
    const auto& x = getConstContainer();
    auto it = x.lower_bound(rec.key);
    return SortedMapE<R>(it, x.end());
  }

  SortedMapE<R> between(const R& a, const R& b) const {
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



template<class R>
class MapE {
  using Key   = typename R::KeyType;
  using Value = typename R::ValueType;
  using Container = unordered_map<Key,R>;

 public:
  // Default Constructor
  MapE(): container() {}
  MapE(const unordered_map<Key,R>& con): container(con) {}
  MapE(unordered_map<Key, R>&& con): container(std::move(con)) {}

  // Construct from (container) iterators
  template<typename Iterator>
  MapE(Iterator begin, Iterator end): container(begin,end) {}

  template <class Pair>
  R elemToRecord(const Pair& e) const { return e.second; }

  template <class I>
  class map_iterator: public std::iterator<std::forward_iterator_tag, R> {
    using container = unordered_map<Key, R>;
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

  using iterator = map_iterator<typename unordered_map<Key, R>::iterator>;
  using const_iterator = map_iterator<typename unordered_map<Key, R>::const_iterator>;

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

  template<typename K>
  unit_t erase(const K& k) {
    container.erase(k.key);
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

  int size(unit_t) const { return container.size(); }

  MapE combine(const MapE& other) const {
    // copy this DS
    MapE result = MapE(*this);
    // copy other DS
    for (const auto& p: other.container) {
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

  template<typename Fun>
  auto map(Fun f) const -> MapE< RT<Fun, R> > {
    MapE< RT<Fun,R> > result;
    for (const auto& p : container) {
      result.insert( f(p.second) );
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

  template<typename Fun, typename Acc>
  Acc fold(Fun f, Acc acc) const {
    for (const auto& p : container) {
      acc = f(std::move(acc))(p.second);
    }
    return acc;
  }

  template<typename F1, typename F2, typename Z>
  MapE< R_key_value< RT<F1, R>,Z >> groupBy(F1 grouper, F2 folder, const Z& init) const {
    // Create a map to hold partial results
    typedef RT<F1, R> K;
    unordered_map<K, Z> accs;

    for (const auto& it : container) {
      K key = grouper(it.second);
      if (accs.find(key) == accs.end()) {
        accs[key] = init;
      }
      accs[key] = folder(std::move(accs[key]))(it.second);
    }

    // TODO more efficient implementation?
    MapE<R_key_value<K,Z>> result;
    for (auto&& it : accs) {
      result.insert(std::move(R_key_value<K, Z> {std::move(it.first), std::move(it.second)}));
    }
    return result;
  }

  template <class Fun>
  auto ext(Fun expand) const -> MapE < typename RT<Fun, R>::ElemType >  {
    typedef typename RT<Fun, R>::ElemType T;
    MapE<T> result;
    for (const auto& it : container) {
      for (auto& it2 : expand(it.second).container) {
        result.insert(it2.second);
      }
    }

    return result;
  }

  // Accumulate over a sampled ds.
  // This number of items accessed depends on the iterator implementation, via std::advance.
  template<typename Fun, typename Acc>
  Acc sample(Fun f, Acc acc, size_t sampleSize) const {
    auto it = container.begin();
    DSSampler seqSampler(container.size(), sampleSize);

    tuple<bool, size_t> next_skip = seqSampler.next();
    while ( it != container.end() && std::get<0>(next_skip) ) {
      std::advance(it, std::get<1>(next_skip));
      if ( it != container.end() ) {
        acc = f(std::move(acc))(*it);
        next_skip = seqSampler.next();
      }
    }

    return acc;
  }

  // Map retrieval.
  // For a MapE, these methods expect a key argument instead of a key-value struct.

  template<typename K>
  bool member(const K& k) const {
    return container.find(k.key) != container.end();
  }

  template<typename K>
  shared_ptr<R> lookup(const K& k) const {
      auto it = container.find(k.key);
      if (it != container.end()) {
        return std::make_shared<R>(it->second);
      } else {
        return nullptr;
      }
  }

  template <typename K, class F>
  unit_t lookup_with(K const& k, F f) const {
    auto it = container.find(k.key);
    if (it != container.end()) {
      return f(it->second);
    }

    return unit_t {};
  }

  template <typename K, class F, class G>
  auto lookup_with2(K const& k, F f, G g) const {
    auto it = container.find(k.key);
    if (it == container.end()) {
      return f(unit_t {});
    } else {
      return g(it->second);
    }
  }

  template <typename K, class F>
  auto lookup_with3(K const& k, F f) const {
    auto it = container.find(k.key);
    if (it != container.end()) {
      return f(it->second);
    }
    throw std::runtime_error("No match on Map.lookup_with3");
  }

  template <typename K, class F, class G>
  auto lookup_with4(K const& k, F f, G g) const {
    auto it = container.find(k.key);
    if (it == container.end()) {
      return f(unit_t {});
    } else {
      return g(it->second);
    }
  }

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

  template<class Archive>
  void serialize(Archive &ar) { ar & container; }

  template<class Archive>
  void serialize(Archive &ar, const unsigned int) {
    ar & boost::serialization::make_nvp("__K3MapE", container);
  }

 protected:
  Container container;

 private:
  friend class boost::serialization::access;
}; // class MapE


} // Namespace K3

namespace boost {
  namespace serialization {
    template <class _T0>
    class implementation_level<K3::VMap<_T0>> {
      public:
        typedef  mpl::integral_c_tag tag;
        typedef  mpl::int_<object_serializable> type;
        BOOST_STATIC_CONSTANT(int, value = implementation_level::type::value);
    };

    template <class _T0>
    class implementation_level<K3::SortedMap<_T0>> {
      public:
        typedef  mpl::integral_c_tag tag;
        typedef  mpl::int_<object_serializable> type;
        BOOST_STATIC_CONSTANT(int, value = implementation_level::type::value);
    };

    template <class _T0>
    class implementation_level<K3::MapE<_T0>> {
      public:
        typedef  mpl::integral_c_tag tag;
        typedef  mpl::int_<object_serializable> type;
        BOOST_STATIC_CONSTANT(int, value = implementation_level::type::value);
    };
  }
}

template <class Elem>
std::size_t hash_value(K3::VMap<Elem> const& b) {
  const auto& c = b.getConstContainer();
  return boost::hash_range(c.begin(), c.end());
}

template <class Elem>
std::size_t hash_value(K3::SortedMap<Elem> const& b) {
  return hash_collection(b);
}

template <class Elem>
std::size_t hash_value(K3::MapE<Elem> const& b) {
  return hash_collection(b);
}

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
       inner.PushBack(convert<typename K3::VMap<E>::Version>::encode(ve.first, al), al);
       inner.PushBack(convert<E>::encode(ve.second, al), al);
      }
     }
     v.AddMember("value", inner.Move(), al);
     return v;
    }
  };

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

namespace YAML {
  template <class R>
  struct convert<K3::VMap<R>> {
    static Node encode(const K3::VMap<R>& c) {
      Node node;
      auto container = c.getConstContainer();
      if (container.size() > 0) {
        for (auto i: container) {
          for (auto j : i.second) {
            node.push_back(convert<typename K3::VMap<R>::Version>::encode(j.first));
            node.push_back(convert<R>::encode(j.second));
          }
        }
      }
      else {
        node = YAML::Load("[]");
      }
      return node;
    }

    static bool decode(const Node& node, K3::VMap<R>& c) {
      bool asVersion = true;
      typename K3::VMap<R>::Version v;
      for (auto& i: node) {
        if ( asVersion ) { v = i.as<typename K3::VMap<R>::Version>(); }
        else { c.insert(v, i.as<R>()); }
        asVersion = !asVersion;
      }
      return true;
    }
  };

  template <class R>
  struct convert<K3::SortedMap<R>> {
    static Node encode(const K3::SortedMap<R>& c) {
      Node node;
      auto container = c.getConstContainer();
      if (container.size() > 0) {
        for (auto i: container) {
          node.push_back(convert<R>::encode(i.second));
        }
      } else {
        node = YAML::Load("[]");
      }
      return node;
    }

    static bool decode(const Node& node, K3::SortedMap<R>& c) {
      for (auto& i: node) { c.insert(i.as<R>()); }
      return true;
    }
  };

  template <class R>
  struct convert<K3::MapE<R>> {
    static Node encode(const K3::MapE<R>& c) {
      Node node;
      auto container = c.getConstContainer();
      if (container.size() > 0) {
        for (auto i: container) {
          node.push_back(convert<R>::encode(i.second));
        }
      } else {
        node = YAML::Load("[]");
      }
      return node;
    }

    static bool decode(const Node& node, K3::MapE<R>& c) {
      for (auto& i: node) { c.insert(i.as<R>()); }
      return true;
    }
  };
}
#endif
