// Custom map implementations for K3
#ifndef __K3_RUNTIME_MAP_DATASPACE__
#define __K3_RUNTIME_MAP_DATASPACE__

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

public:
  using Container = shared_ptr<mapi>;

  // Default Constructor
  IntMap() { init_mapi(true, sizeof(R)); }

  IntMap(const Container& con) {
    init_mapi(true, sizeof(R));
    mapi* m = con.get();
    mapi* n = container.get();
    for (auto o = mapi_begin(m); o < mapi_end(m); o = mapi_next(m, o)) {
      mapi_insert(n, o);
    }
  }

  IntMap(Container&& con) : container(std::move(con)) {
    init_mapi(false, sizeof(R));
  }

  template<typename Iterator>
  IntMap(Iterator begin, Iterator end): container(begin,end) {
    init_mapi(true, sizeof(R));
    mapi* m = container.get();
    for ( auto it = begin; it != end; ++it) {
      mapi_insert(m, &(*it));
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
    mapi* m = container.get();
    return iterator(m, mapi_begin(m));
  }

  iterator end() {
    mapi* m = container.get();
    return iterator(m, mapi_end(m));
  }

  const_iterator begin() const {
    mapi* m = container.get();
    return const_iterator(m, mapi_begin(m));
  }

  const_iterator end() const {
    mapi* m = container.get();
    return const_iterator(m, mapi_end(m));
  }

  int size(unit_t) const { return mapi_size(container.get()); }

  // DS Operations:
  // Maybe return the first element in the DS
  shared_ptr<R> peek(const unit_t&) const {
    shared_ptr<R> res(nullptr);
    auto it = mapi_begin(container.get());
    if (it < mapi_end(container.get()) ) {
      res = std::make_shared<R>(*it);
    }
    return res;
  }

  unit_t insert(const R& q) {
    mapi_insert(container.get(), &q);
    return unit_t();
  }

  unit_t insert(R&& q) {
    R tmp(std::move(q));
    mapi_insert(container.get(), &tmp);
    return unit_t();
  }

  template <class F>
  unit_t insert_with(R& rec, F f) {
    mapi* m = container.get();
    auto existing = mapi_find(m, rec.key);
    if (existing == nullptr) {
      mapi_insert(m, &rec);
    } else {
      auto nrec = f(std::move(*existing))(rec);
      mapi_erase(m, rec.key);
      mapi_insert(m, &nrec);
    }
    return unit_t {};
  }

  template <class F, class G>
  unit_t upsert_with(const R& rec, F f, G g) {
    mapi* m = container.get();
    auto existing = mapi_find(m, rec.key);
    if (existing == nullptr) {
      auto nrec = f(unit_t {});
      mapi_insert(m, &nrec);
    } else {
      auto nrec = g(std::move(*existing));
      mapi_erase(m, rec.key);
      mapi_insert(m, &nrec);
    }

    return unit_t {};
  }

  unit_t erase(const R& rec) {
    mapi* m = container.get();
    auto existing = mapi_find(m, rec.key);
    if (existing != nullptr) {
      mapi_erase(m, rec.key);
    }
    return unit_t();
  }

  unit_t update(const R& rec1, R& rec2) {
    mapi* m = container.get();
    auto existing = mapi_find(m, rec1.key);
    if (existing != nullptr) {
      mapi_erase(m, rec1.key);
      mapi_insert(m, &rec2);
    }
    return unit_t();
  }

  template<typename Fun, typename Acc>
  Acc fold(Fun f, Acc acc) const {
    mapi* m = container.get();
    for (auto o = mapi_begin(m); o < mapi_end(m); o = mapi_next(m, o)) {
      acc = f(std::move(acc))(*o);
    }
    return acc;
  }

  template<typename Fun>
  auto map(Fun f) const -> IntMap< RT<Fun, R> > {
    mapi* m = container.get();
    IntMap< RT<Fun,R> > result;
    for (auto o = mapi_begin(m); o < mapi_end(m); o = mapi_next(m, o)) {
      result.insert( f(*o) );
    }
    return result;
  }

  template <typename Fun>
  unit_t iterate(Fun f) const {
    mapi* m = container.get();
    for (auto o = mapi_begin(m); o < mapi_end(m); o = mapi_next(m, o)) {
      f(*o);
    }
    return unit_t();
  }

  template <typename Fun>
  IntMap<R> filter(Fun predicate) const {
    mapi* m = container.get();
    IntMap<R> result;
    for (auto o = mapi_begin(m); o < mapi_end(m); o = mapi_next(m, o)) {
      if (predicate(*o)) {
        result.insert(*o);
      }
    }
    return result;
  }

  tuple<IntMap, IntMap> split(const unit_t&) const {
    mapi* m = container.get();

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
    mapi* m = other.container.get();
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

    mapi* m = container.get();
    mapi* n = result.container.get();

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
    mapi* m = container.get();
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
    mapi* m  = container.get();
    auto it  = mapi_begin(m);
    auto end = mapi_end(m);

    DSSampler seqSampler(m->size(), sampleSize);

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
    mapi* m = container.get();
    auto existing = mapi_find(m, r.key);
    if (existing == nullptr) {
      return nullptr;
    } else {
      return std::make_shared<R>(*existing);
    }
  }

  bool member(const R& r) const {
    return mapi_find(container.get(), r.key) != nullptr;
  }

  template <class F>
  unit_t lookup_with(R const& r, F f) const {
    mapi* m = container.get();
    auto existing = mapi_find(m, r.key);
    if (existing != nullptr) {
      return f(*existing);
    }
    return unit_t {};
  }

  template <class F, class G>
  auto lookup_with2(R const& r, F f, G g) const {
    mapi* m = container.get();
    auto existing = mapi_find(m, r.key);
    if (existing == nullptr) {
      return f(unit_t {});
    } else {
      return g(*existing);
    }
  }

  bool operator==(const IntMap& other) const {
    return container.get() == other.container.get()
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

  Container& getContainer() { return container; }

  // Return a constant reference to the container
  const Container& getConstContainer() const { return container; }

protected:
  shared_ptr<mapi> container;

private:
  friend class boost::serialization::access;

  static inline void cloneElem(void* dest, void* src, size_t sz) {
    new(dest) R(*static_cast<R*>(src));
  }

  void init_mapi(bool alloc, size_t sz) {
    if ( alloc ) { container = Container(mapi_new(sz)); }
    mapi_clone(container.get(), (CloneFn) &IntMap<R>::cloneElem);
  }

  template<class archive>
  void save(archive &ar, const unsigned int) const {
    ar << boost::serialization::make_nvp("object_size", container->object_size);
    ar << boost::serialization::make_nvp("empty_key", container->empty_key);
    ar << boost::serialization::make_nvp("size", container->size);
    ar << boost::serialization::make_nvp("capacity", container->capacity);

    mapi* m = container.get();
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

    if ( container ) { mapi_clear(container.get()); container.reset(); }
    init_mapi(true, object_size);

    if ( container ) {
      mapi* n = container.get();
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

  BOOST_SERIALIZATION_SPLIT_MEMBER()
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

  // Default Constructor
  StrMap() { init_map_str(true, sizeof(R)); }

  StrMap(const Container& con) {
    init_map_str(true, sizeof(R));
    map_str* m = con.get();
    for (auto o = map_str_begin(m); o < map_str_end(m); o = map_str_next(m, o)) {
      insert(static_cast<R*>(map_str_get(m,o)));
    }
  }

  StrMap(Container&& con) : container(std::move(con)) {
    init_map_str(false, sizeof(R));
  }

  template<typename Iterator>
  StrMap(Iterator begin, Iterator end): container(begin,end) {
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
    map_str* m = container.get();
    return iterator(m, map_str_begin(m));
  }

  iterator end() {
    map_str* m = container.get();
    return iterator(m, map_str_end(m));
  }

  const_iterator begin() const {
    map_str* m = container.get();
    return const_iterator(m, map_str_begin(m));
  }

  const_iterator end() const {
    map_str* m = container.get();
    return const_iterator(m, map_str_end(m));
  }

  int size(unit_t) const { return map_str_size(container.get()); }

  // DS Operations:
  // Maybe return the first element in the DS
  shared_ptr<R> peek(const unit_t&) const {
    shared_ptr<R> res(nullptr);
    map_str* m = container.get();
    auto it = map_str_begin(m);
    if (it < map_str_end(m) ) {
      res = std::make_shared<R>(*map_str_get(m, it));
    }
    return res;
  }

  unit_t insert(const R& q) {
    map_str* m = container.get();
    auto pos = map_str_insert(m, q.key.begin(), const_cast<void*>(static_cast<const void*>(&q)));

    R* v = static_cast<R*>(map_str_get(m, pos));
    //v->key.divorce(q.key.begin());
    map_str_stabilize_key(m, pos, v->key.begin());

    return unit_t();
  }

  unit_t insert(R&& q) {
    R tmp(std::move(q));

    map_str* m = container.get();
    auto pos = map_str_insert(m, tmp.key.begin(), &tmp);

    R* v = static_cast<R*>(map_str_get(m, pos));
    //v->key.divorce(tmp.key.begin());
    map_str_stabilize_key(m, pos, v->key.begin());

    return unit_t();
  }

  template <class F>
  unit_t insert_with(R& rec, F f) {
    map_str* m = container.get();
    auto existing = map_str_find(m, rec.key.begin());
    if (existing == map_str_end(m)) {
      insert(rec);
    } else {
      R* v = static_cast<R*>(map_str_get(m,existing));
      auto nrec = f(std::move(*v))(rec);
      map_str_erase(m, rec.key.begin());
      v->~R();
      insert(nrec);
    }
    return unit_t {};
  }

  template <class F, class G>
  unit_t upsert_with(const R& rec, F f, G g) {
    map_str* m = container.get();
    auto existing = map_str_find(m, rec.key.begin());
    if (existing == map_str_end(m)) {
      auto nrec = f(unit_t {});
      insert(nrec);
    } else {
      R* v = static_cast<R*>(map_str_get(m,existing));
      auto nrec = g(std::move(*v));
      map_str_erase(m, rec.key.begin());
      v->~R();
      insert(nrec);
    }

    return unit_t {};
  }

  unit_t erase(const R& rec) {
    map_str* m = container.get();
    auto existing = map_str_find(m, rec.key.begin());
    if (existing != map_str_end(m)) {
      R* v = static_cast<R*>(map_str_get(m,existing));
      map_str_erase(m, rec.key.begin());
      v->~R();
    }
    return unit_t();
  }

  unit_t update(const R& rec1, R& rec2) {
    map_str* m = container.get();
    auto existing = map_str_find(m, rec1.key.begin());
    if (existing != map_str_end(m)) {
      R* v = static_cast<R*>(map_str_get(m,existing));
      map_str_erase(m, rec1.key.begin());
      v->~R();
      insert(rec2);
    }
    return unit_t();
  }

  template<typename Fun, typename Acc>
  Acc fold(Fun f, Acc acc) const {
    map_str* m = container.get();
    for (auto o = map_str_begin(m); o < map_str_end(m); o = map_str_next(m, o)) {
      acc = f(std::move(acc))(*map_str_get(m,o));
    }
    return acc;
  }

  template<typename Fun>
  auto map(Fun f) const -> StrMap< RT<Fun, R> > {
    map_str* m = container.get();
    StrMap< RT<Fun,R> > result;
    for (auto o = map_str_begin(m); o < map_str_end(m); o = map_str_next(m, o)) {
      result.insert( f(*map_str_get(m,o)) );
    }
    return result;
  }

  template <typename Fun>
  unit_t iterate(Fun f) const {
    map_str* m = container.get();
    for (auto o = map_str_begin(m); o < map_str_end(m); o = map_str_next(m, o)) {
      f(*map_str_get(m,o));
    }
    return unit_t();
  }

  template <typename Fun>
  StrMap<R> filter(Fun predicate) const {
    map_str* m = container.get();
    StrMap< RT<Fun,R> > result;
    for (auto o = map_str_begin(m); o < map_str_end(m); o = map_str_next(m, o)) {
      if (predicate(*map_str_get(m,o))) {
        result.insert(*map_str_get(m,o));
      }
    }
    return result;
  }

  tuple<StrMap, StrMap> split(const unit_t&) const {
    map_str* m = container.get();

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
    map_str* m = other.container.get();
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

    map_str* m = container.get();
    map_str* n = result.container.get();

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
    map_str* m = container.get();
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
    map_str* m  = container.get();
    auto it  = map_str_begin(m);
    auto end = map_str_end(m);

    DSSampler seqSampler(m->size(), sampleSize);

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
    map_str* m = container.get();
    auto existing = map_str_find(m, r.key.begin());
    if (existing == map_str_end(m)) {
      return nullptr;
    } else {
      return std::make_shared<R>(*map_str_get(m,existing));
    }
  }

  bool member(const R& r) const {
    map_str* m = container.get();
    return map_str_find(m, r.key.begin()) != map_str_end(m);
  }

  template <class F>
  unit_t lookup_with(R const& r, F f) const {
    map_str* m = container.get();
    auto existing = map_str_find(m, r.key.begin());
    if (existing != map_str_end(m)) {
      return f(*map_str_get(m,existing));
    }
    return unit_t {};
  }

  template <class F, class G>
  auto lookup_with2(R const& r, F f, G g) const {
    map_str* m = container.get();
    auto existing = map_str_find(m, r.key.begin());
    if (existing == map_str_end(m)) {
      return f(unit_t {});
    } else {
      return g(*map_str_get(m,existing));
    }
  }

  bool operator==(const StrMap& other) const {
    return container.get() == other.container.get()
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

  Container& getContainer() { return container; }

  // Return a constant reference to the container
  const Container& getConstContainer() const { return container; }

protected:
  shared_ptr<map_str> container;

private:
  friend class boost::serialization::access;

  static inline void cloneElem(void* dest, void* src, size_t sz) {
    new(dest) R(*static_cast<R*>(src));
  }

  void init_map_str(bool alloc, size_t sz) {
    if ( alloc ) { container = Container(map_str_new(sz)); }
    map_str_clone(container.get(), (CloneFn) &StrMap<R>::cloneElem);
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

    map_str* m = container.get();
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

    if ( container ) { map_str_clear(container.get()); container.reset(); }
    init_map_str(true, value_size);

    if ( container ) {
      map_str* n = container.get();
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

  BOOST_SERIALIZATION_SPLIT_MEMBER()

};

} } // end Libdynamic, K3

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
      if (c.size() > 0) {
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
      if (c.size() > 0) {
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

#endif