#ifndef __K3_RUNTIME_MULTIINDEX_DATASPACE__
#define __K3_RUNTIME_MULTIINDEX_DATASPACE__

#include <math.h>
#include <random>

#include <boost/serialization/base_object.hpp>
#include <boost/multi_index_container.hpp>
#include <boost/multi_index/tag.hpp>
#include <boost/multi_index/sequenced_index.hpp>
#include <boost/lambda/lambda.hpp>

#include <Common.hpp>
#include <BaseTypes.hpp>
#include <dataspace/Dataspace.hpp>

#include <serialization/json.hpp>
#include <rapidjson/document.h>
#include <rapidjson/writer.h>
#include <rapidjson/stringbuffer.h>
#include <yaml-cpp/yaml.h>

#include <yas/mem_streams.hpp>
#include <yas/binary_iarchive.hpp>
#include <yas/binary_oarchive.hpp>
#include <yas/serializers/std_types_serializers.hpp>
#include <yas/serializers/boost_types_serializers.hpp>

namespace K3 {

// Utility to give the return type of a Function F expecting an Element E as an argument:
template <class F, class E> using RT = decltype(std::declval<F>()(std::declval<E>()));

// Key extraction utilities.
template<class KeyExtractor1, class KeyExtractor2>
struct subkey
{
public:
  typedef typename KeyExtractor1::result_type result_type;

  subkey(const KeyExtractor1& key1_ = KeyExtractor1(),
         const KeyExtractor2& key2_ = KeyExtractor2())
    : key1(key1_), key2(key2_)
  {}

  template<typename Arg>
  result_type operator()(Arg& arg) const
  {
    return key1(key2(arg));
  }

private:
  KeyExtractor1 key1;
  KeyExtractor2 key2;
};


// Multi-index implementations.
template <template <typename, typename...> class Derived, typename Elem, typename BaseIndex, typename... Indexes>
class MultiIndexDS {
  public:

  typedef boost::multi_index_container<
    Elem,
    boost::multi_index::indexed_by<BaseIndex, Indexes...>
  > Container;

  Container container;

  // Default
  MultiIndexDS(): container() {}
  MultiIndexDS(const Container& con): container(con) {}
  MultiIndexDS(Container&& con): container(std::move(con)) {}

  // Construct from (container) iterators
  template<typename Iterator>
  MultiIndexDS(Iterator begin, Iterator end): container(begin,end) {}

  Elem elemToRecord(const Elem& e) const { return e; }

  // Maybe return the first element in the ds
  shared_ptr<Elem> peek(unit_t) const {
    shared_ptr<Elem> res(nullptr);
    auto it = container.begin();
    if (it != container.end()) {
      res = std::make_shared<Elem>(*it);
    }
    return res;
  }

  template<typename F, typename G>
  auto peek_with(F f, G g) const {
    auto it = container.begin();
    if (it == container.end()) {
      return f(unit_t {});
    } else {
      return g(*it);
    }
  }

   // Insert by move
  unit_t insert(Elem &&e) {
    container.insert(container.end(), std::move(e));
    return unit_t();
  }

  // Insert by copy
  unit_t insert(const Elem& e) {
    // Create a copy, then delegate to a insert-by-move
    return insert(Elem(e));
  }

  // If v is found in the container, proxy a call to erase on the container.
  // Behavior depends on the container's erase implementation
  unit_t erase(const Elem& v) {
    auto it = std::find(container.begin(), container.end(), v);
    if (it != container.end()) {
      container.erase(it);
    }
    return unit_t();
  }

  // Update by move
  // Find v in the container. Insert (by move) v2 in its position. Erase v.
  unit_t update(const Elem& v, Elem&& v2) {
    auto it = std::find(container.begin(), container.end(), v);
    if (it != container.end()) {
      *it = std::forward<Elem>(v2);
    }
    return unit_t();
  }

  // Return the number of elements in this ds
  int size(const unit_t&) const {
    return container.size();
  }

  // Return a new DS with data from this and other
  Derived<Elem, Indexes...> combine(const MultiIndexDS& other) const {
    // copy this DS
    Derived<Elem, Indexes...> result;
    result = MultiIndexDS(*this);
    // copy other DS
    for (const Elem &e : other.container) {
      result.insert(e);
    }
    return result;
  }

  // Split the ds at its midpoint
  tuple<Derived<Elem, Indexes...>, Derived<Elem, Indexes...>> split(const unit_t&) const {
    // Find midpoint
    const size_t size = container.size();
    const size_t half = size / 2;
    // Setup iterators
    auto beg = container.begin();
    auto mid = container.begin();
    std::advance(mid, half);
    auto end = container.end();
    // Construct from iterators
    return std::make_tuple(Derived<Elem, Indexes...>(MultiIndexDS(beg,mid)),
                           Derived<Elem, Indexes...>(MultiIndexDS(mid,end)));
  }

  // Apply a function to each element of this ds
  template<typename Fun>
  unit_t iterate(Fun f) const {
    for (const Elem& e : container) {
      f(e);
    }
    return unit_t();
  }

  // Produce a new ds by mapping a function over this ds
  template<typename Fun>
  auto map(Fun f) const -> Derived<R_elem<RT<Fun, Elem>>> const {
    Derived<R_elem<RT<Fun, Elem>>> result;
    for (const Elem &e : container) {
      result.insert( R_elem<RT<Fun, Elem>>{ f(e) } ); // Copies e (f is pass by value), then move inserts
    }
    return result;
  }

  // Create a new DS consisting of elements from this ds that satisfy the predicate
  template<typename Fun>
  Derived<Elem, Indexes...> filter(Fun predicate) const {
    Derived<Elem, Indexes...> result;
    for (const Elem &e : container) {
      if (predicate(e)) {
        result.insert(e);
      }
    }
    return result;
  }

  // Fold a function over this ds
  template<typename Fun, typename Acc>
  Acc fold(Fun f, const Acc& init_acc) const {
    Acc acc = init_acc;
    for (const Elem &e : container) { acc = f(std::move(acc))(e); }
    return acc;
  }

  // Group By
  template<typename F1, typename F2, typename Z>
  Derived<R_key_value<RT<F1, Elem>,Z>> groupBy(F1 grouper, F2 folder, const Z& init) const {
    // Create a map to hold partial results
    typedef RT<F1, Elem> K;
    unordered_map<K, Z> accs;

    for (const auto& elem : container) {
       K key = grouper(elem);
       if (accs.find(key) == accs.end()) {
         accs[key] = init;
       }
       accs[key] = folder(std::move(accs[key]))(elem);
    }

    // Build the R_key_value records and insert them into resul
    Derived<R_key_value<RT<F1, Elem>,Z>> result;
    for(const auto& it : accs) {
      result.insert(R_key_value<K, Z>{std::move(it.first), std::move(it.second)});
    }

    return result;
  }

  template <class Fun>
  auto ext(Fun expand) -> Derived<typename RT<Fun, Elem>::ElemType> const {
    typedef typename RT<Fun, Elem>::ElemType T;
    Derived<T> result;
    for (const Elem& elem : container) {
      for (T& elem2 : expand(elem).container) {
        result.insert(std::move(elem2));
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

  ////////////////////////
  // Index operations.

  template <class Index, class Key>
  shared_ptr<Elem> lookup_by_index(const Index& index, Key key) const {
    const auto& it = index.find(key);
    shared_ptr<Elem> result;
    if (it != index.end()) {
      result = make_shared<Elem>(*it);
    }
    return result;
  }

  template <class Index, class Key, typename F, typename G>
  auto lookup_with_by_index(const Index& index, Key key, F f, G g) const {
    const auto& it = index.find(key);
    if (it == index.end()) {
      return f(unit_t {});
    } else {
      return g(*it);
    }
  }

  template <class Index, class Key>
  Derived<Elem, Indexes...> slice_by_index(const Index& index, Key key) const {
    Derived<Elem, Indexes...> result;
    std::pair<typename Index::iterator, typename Index::iterator> p = index.equal_range(key);
    for (typename Index::iterator it = p.first; it != p.second; it++) {
      result.insert(*it);
    }
    return result;
  }

  template <class Index, class Key>
  Derived<Elem, Indexes...> range_by_index(const Index& index, Key a, Key b) const {
    Derived<Elem, Indexes...> result;
    std::pair<typename Index::iterator, typename Index::iterator> p =
      index.range(a <= boost::lambda::_1, b >= boost::lambda::_1);
    for (typename Index::iterator it = p.first; it != p.second; it++) {
      result.insert(*it);
    }
    return result;
  }

  template <class Index, class Key, typename Fun, typename Acc>
  Acc fold_slice_by_index(const Index& index, Key key, Fun f, Acc acc) const
  {
    std::pair<typename Index::iterator, typename Index::iterator> p = index.equal_range(key);
    for (typename Index::iterator it = p.first; it != p.second; it++) {
      acc = f(std::move(acc))(*it);
    }
    return acc;
  }

  template <class Index, class Key, typename Fun, typename Acc>
  Acc fold_range_by_index(const Index& index, Key a, Key b, Fun f, Acc acc) const
  {
    std::pair<typename Index::iterator, typename Index::iterator> p =
      index.range(a <= boost::lambda::_1, b >= boost::lambda::_1);
    for (typename Index::iterator it = p.first; it != p.second; it++) {
      acc = f(std::move(acc))(*it);
    }
    return acc;
  }


  /////////////////
  // Comparators.

  bool operator==(const MultiIndexDS& other) const {
    return container == other.container;
  }

  bool operator!=(const MultiIndexDS& other) const {
    return container != other.container;
  }

  bool operator<(const MultiIndexDS& other) const {
    return container < other.container;
  }

  bool operator>(const MultiIndexDS& other) const {
    return container > other.container;
  }

  Container& getContainer() { return container; }

  // Return a constant reference to the container
  const Container& getConstContainer() const {return container;}

  template<class Archive>
  void serialize(Archive &ar) const {
    ar.write(container.size());
    for (const auto& it : container) {
      ar & it;
    }
  }

  template<class Archive>
  void serialize(Archive &ar) {
    size_t sz = 0;
    ar.read(sz);
    while ( sz > 0 ) {
      Elem e;
      ar & e;
      insert(std::move(e));
      sz--;
    }
  }

  template<class Archive>
  void serialize(Archive &ar, const unsigned int) {
    ar & boost::serialization::make_nvp("__K3MultiIndexDS", container);
  }

 private:
  friend class boost::serialization::access;

};


template <typename Elem, typename... Indexes>
class MultiIndexBag : public MultiIndexDS<K3::MultiIndexBag, Elem, boost::multi_index::sequenced<>, Indexes...> {
  using Super = MultiIndexDS<K3::MultiIndexBag, Elem, boost::multi_index::sequenced<>, Indexes...>;

  public:
  MultiIndexBag(): Super() {}
  MultiIndexBag(const Super& c): Super(c) { }
  MultiIndexBag(Super&& c): Super(std::move(c)) { }

  template<class Archive>
  void serialize(Archive &ar) {
    ar & yas::base_object<Super>(*this);
  }

  template<class Archive>
  void serialize(Archive &ar, const unsigned int) {
    ar &  boost::serialization::make_nvp("__K3MultiIndexBag",
            boost::serialization::base_object<Super>(*this));
  }

 private:
  friend class boost::serialization::access;
};


template<typename R, typename Key>
using HashUniqueIndex = boost::multi_index::hashed_unique<boost::multi_index::member<R,Key,&R::key>>;

template <typename R, typename... Indexes>
class MultiIndexMap : public MultiIndexDS<K3::MultiIndexMap, R, HashUniqueIndex<R, typename R::KeyType>, Indexes...> {
  using Key   = typename R::KeyType;
  using Super = MultiIndexDS<K3::MultiIndexMap, R, HashUniqueIndex<R, Key>, Indexes...>;

  public:
  MultiIndexMap(): Super() {}
  MultiIndexMap(const Super& c): Super(c) { }
  MultiIndexMap(Super&& c): Super(std::move(c)) { }

  // Override MultiIndexDS::insert to omit the end insertion hint.
  unit_t insert(R &&rec) {
    auto& c = Super::getContainer();
    auto inserted = c.emplace(std::move(rec));
    if (!inserted.second) {
      c.replace(inserted.first, std::move(rec));
    }
    return unit_t();
  }

  // Insert by copy
  unit_t insert(const R& rec) {
    // Create a copy, then delegate to a insert-by-move
    return insert(R(rec));
  }

  template <class F>
  unit_t insert_with(const R& rec, F f) {
    auto& c = Super::getContainer();
    auto existing = c.find(rec.key);
    if (existing == std::end(c)) {
      c.insert(rec);
    } else {
      c.replace(existing, f(std::move(existing->second))(rec));
    }
    return unit_t {};
  }

  template <class F, class G>
  unit_t upsert_with(const R& rec, F f, G g) {
    auto& c = Super::getContainer();
    auto existing = c.find(rec.key);
    if (existing == std::end(c)) {
      c.insert(f(unit_t {}));
    } else {
      c.replace(existing, g(std::move(existing->second)));
    }
    return unit_t {};
  }

  // Override MultiIndexDS::erase to use the base hash index.
  unit_t erase(const R& rec) {
    auto& c = Super::getContainer();
    auto it = c.find(rec.key);
    if (it != c.end()) {
      c.erase(it);
    }
    return unit_t();
  }

  // Override MultiIndexDS::update to use the base hash index.
  unit_t update(const R& rec1, const R& rec2) {
    auto& c = Super::getContainer();
    auto it = c.find(rec1.key);
    if (it != c.end()) {
      c.replace(it, rec2);
    }
    return unit_t();
  }

  template<typename Result, typename F, typename G>
  Result update_with(const R& rec, const Result& r, F f, G g) {
    auto& c = Super::getContainer();
    auto it = c.find(rec.key);
    if (it == c.end()) {
      return r;
    } else {
      Result r2(g(*it));
      *it = f(std::move(*it));
      return r2;
    }
  }

  bool member(const R& r) const {
    auto& c = Super::getConstContainer();
    return c.find(r.key) != c.end();
  }

  shared_ptr<R> lookup(const R& r) const {
    auto& c = Super::getConstContainer();
    auto it = c.find(r.key);
    if (it != c.end()) {
      return std::make_shared<R>(it->second);
    } else {
      return nullptr;
    }
  }

  template <class F>
  unit_t lookup_with(R const& r, F f) const {
    auto& c = Super::getConstContainer();
    auto it = c.find(r.key);
    if (it != c.end()) {
      return f(it->second);
    }
    return unit_t {};
  }

  template <class F, class G>
  auto lookup_with2(R const& r, F f, G g) const {
    auto& c = Super::getConstContainer();
    auto it = c.find(r.key);
    if (it == c.end()) {
      return f(unit_t {});
    } else {
      return g(it->second);
    }
  }

  template <class F>
  auto lookup_with3(R const& r, F f) const {
    auto& c = Super::getConstContainer();
    auto it = c.find(r.key);
    if (it != c.end()) {
      return f(it->second);
    }
    throw std::runtime_error("No match on Map.lookup_with3");
  }

  template <class F, class G>
  auto lookup_with4(R const& r, F f, G g) const {
    auto& c = Super::getConstContainer();
    auto it = c.find(r.key);
    if (it == c.end()) {
      return f(unit_t {});
    } else {
      return g(it->second);
    }
  }

  template<class Archive>
  void serialize(Archive &ar) {
    ar & yas::base_object<Super>(*this);
  }

  template<class Archive>
  void serialize(Archive &ar, const unsigned int) {
    ar &  boost::serialization::make_nvp("__K3MultiIndexMap",
            boost::serialization::base_object<Super>(*this));
  }

 private:
  friend class boost::serialization::access;
};


template<typename R, typename Version> using VElem
  = std::tuple<typename R::KeyType, std::map<Version, R, std::greater<Version>>>;

// MultiIndexVMap tags and extractors.
struct vmapkey {};

template<typename R, typename Version>
struct VKeyExtractor
{
public:
  typedef typename R::KeyType result_type;

  VKeyExtractor() {}

  result_type operator()(const VElem<R, Version>& arg) const {
    return std::get<0>(arg);
  }
};

template <typename R, typename... Indexes>
class MultiIndexVMap
{
 public:
  using Version = int;

 protected:
  using Key     = typename R::KeyType;
  using Elem    = VElem<R, Version>;

  template<typename E>
  using VContainer = std::map<Version, E, std::greater<Version>>;
  using VMap = VContainer<R>;

  using HashUniqueVKeyIndex =
    boost::multi_index::hashed_unique<
      boost::multi_index::tag<vmapkey>, VKeyExtractor<R, Version>>;

  typedef boost::multi_index_container<
    Elem,
    boost::multi_index::indexed_by<
      HashUniqueVKeyIndex,
      Indexes...>
  > Container;

  typedef typename Container::iterator iterator;

  Container container;

 public:
  MultiIndexVMap(): container() {}
  MultiIndexVMap(const Container& con): container(con) {}
  MultiIndexVMap(Container&& con): container(std::move(con)) {}

  // Construct from (container) iterators
  template<typename Iterator>
  MultiIndexVMap(Iterator begin, Iterator end): container(begin,end) {}

  R elemToRecord(const Elem& e) const {
    auto& vmap = std::get<1>(e);
    return (vmap.begin() == vmap.end())? std::move(R()) : vmap.begin()->second;
  }

  //////////////////////////////
  // VMap methods.

  int size(const Version& v) const {
    int r = 0;
    for (const auto& elem : container.get<vmapkey>()) {
      auto& vmap = std::get<1>(elem);
      r += (vmap.lower_bound(v) == vmap.end()) ? 0 : 1;
    }
    return r;
  }

  int total_size(unit_t) const {
    int r = 0;
    for (const auto& elem : container) {
      r += std::get<1>(elem).size();
    }
    return r;
  }

  ////////////////////////////
  // Exact version methods.

  shared_ptr<R> peek(const Version& v) const {
    shared_ptr<R> res(nullptr);
    for (const auto& elem : container) {
      const auto& vmap = std::get<1>(elem);
      auto vit = vmap.find(v);
      if ( vit != vmap.end() ) {
        res = std::make_shared<R>(vit->second);
        break;
      }
    }
    return res;
  }

  template <class Q>
  unit_t insert(const Version& v, Q&& q) {
    auto existing = container.find(q.key);
    if ( existing == container.end() ) {
      Key k = q.key;
      VMap vmap; vmap[v] = std::forward<Q>(q);
      container.emplace(std::move(std::make_tuple(std::move(k), std::move(vmap))));
    } else {
      container.modify(existing, [&](auto& elem){
        auto& vmap = std::get<1>(elem);
        vmap[v] = std::forward<Q>(q);
      });
    }
    return unit_t();
  }

  template <class F>
  unit_t insert_with(const Version& v, const R& rec, F f) {
    auto existing = container.find(rec.key);
    if ( existing == container.end() ) {
      Key k = rec.key;
      VMap vmap; vmap[v] = rec;
      container.emplace(std::move(std::make_tuple(std::move(k), std::move(vmap))));
    } else {
      container.modify(existing, [&](auto& elem){
        auto& vmap = std::get<1>(elem);
        auto vexisting = vmap.find(v);
        if ( vexisting == vmap.end() ) {
          vmap[v] = rec;
        } else {
          vmap[v] = f(std::move(vexisting->second))(rec);
        }
      });
    }
    return unit_t {};
  }

  template <class F, class G>
  unit_t upsert_with(const Version& v, const R& rec, F f, G g) {
    auto existing = container.find(rec.key);
    if ( existing == container.end() ) {
      Key k = rec.key;
      VMap vmap; vmap[v] = f(unit_t {});
      container.emplace(std::move(std::make_tuple(std::move(k), std::move(vmap))));
    } else {
      container.modify(existing, [&](auto& elem){
        auto& vmap = std::get<1>(elem);
        auto vexisting = vmap.find(v);
        if ( vexisting == vmap.end() ) {
          vmap[v] = f(unit_t {});
        } else {
          vmap[v] = g(std::move(vexisting->second));
        }
      });
    }
    return unit_t {};
  }

  unit_t erase(const Version& v, const R& rec) {
    auto it = container.find(rec.key);
    if (it != container.end()) {
      auto& vmap = std::get<1>(*it);
      auto vit = vmap.find(v);
      if ( vit != vmap.end() ) {
        vmap.erase(vit);
        if ( vmap.empty() ) {
          container.erase(it);
        }
      }
    }
    return unit_t();
  }

  unit_t update(const Version& v, const R& rec1, const R& rec2) {
    auto it = container.find(rec1.key);
    if (it != container.end()) {
      auto& vmap = std::get<1>(*it);
      auto vit = vmap.find(v);
      if ( vit != vmap.end() ) {
        vmap.erase(vit);
        insert(v, rec2);
      }
    }
    return unit_t();
  }

  template<typename Result, typename F, typename G>
  auto update_with(const Version& v, const R& rec, const Result& r, F f, G g) {
    auto it = container.find(rec.key);
    if (it == container.end()) {
      return r;
    } else {
      auto& vmap = std::get<1>(*it);
      auto vit = vmap.find(v);
      if ( vit != vmap.end() ) {
        Result r2 = g(vit->second);
        R rec2 = f(std::move(vit->second));
        container.modify(it, [&](auto& elem){ std::get<1>(elem).erase(vit); });
        insert(v, std::move(rec2));
        return r2;
      } else {
        return r;
      }
    }
  }

  bool member(const Version& v, const R& r) const {
    auto it = container.find(r.key);
    return it != container.end() && std::get<1>(*it).find(v) != std::get<1>(*it).end();
  }

  shared_ptr<R> lookup(const Version& v, const R& r) const {
    auto it = container.find(r.key);
    if (it != container.end()) {
      auto& vmap = std::get<1>(*it);
      auto vit = vmap.find(v);
      if ( vit != vmap.end() ) {
        return std::make_shared<R>(vit->second);
      }
    }
    return nullptr;
  }

  template <class F>
  unit_t lookup_with(const Version& v, R const& r, F f) const {
    auto it = container.find(r.key);
    if (it != container.end()) {
      auto& vmap = std::get<1>(*it);
      auto vit = vmap.find(v);
      if ( vit != vmap.end() ) {
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
      auto& vmap = std::get<1>(*it);
      auto vit = vmap.find(v);
      if ( vit == vmap.end() ) {
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
      auto& vmap = std::get<1>(*it);
      auto vit = vmap.find(v);
      if ( vit != vmap.end() ) {
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
      auto& vmap = std::get<1>(*it);
      auto vit = vmap.find(v);
      if ( vit == vmap.end() ) {
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
      auto& vmap = std::get<1>(*it);
      auto vit = vmap.upper_bound(v);
      if ( vit != vmap.end() ) {
        return std::make_shared<R>(vit->second);
      }
    }
    return nullptr;
  }

  template <class F>
  unit_t lookup_with_before(const Version& v, R const& r, F f) const {
    auto it = container.find(r.key);
    if (it != container.end()) {
      auto& vmap = std::get<1>(*it);
      auto vit = vmap.upper_bound(v);
      if ( vit != vmap.end() ) {
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
      auto& vmap = std::get<1>(*it);
      auto vit = vmap.upper_bound(v);
      if ( vit == vmap.end() ) {
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
      auto& vmap = std::get<1>(*it);
      auto vit = vmap.upper_bound(v);
      if ( vit != vmap.end() ) {
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
      auto& vmap = std::get<1>(*it);
      auto vit = vmap.upper_bound(v);
      if ( vit == vmap.end() ) {
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
      bool erase_elem = false;
      container.modify(it, [&](auto& elem){
        auto& vmap = std::get<1>(elem);
        auto vlteq = vmap.lower_bound(v);
        auto vless = vmap.upper_bound(v);
        auto vend  = vmap.end();
        if ( vless != vmap.end() ) {
          vmap.erase((vlteq == vless)? ++vless : vless, vend);
          erase_elem = vmap.empty();
        }
      });
      if ( erase_elem ) { container.erase(it); }
    }
    return unit_t();
  }

  // Inclusive update greater than a given version.
  template <class F>
  unit_t update_suffix(const Version& v, const R& rec, F f) {
    auto it = container.find(rec.key);
    if (it != container.end()) {
      container.modify(it, [&](auto& elem){
        auto& vmap = std::get<1>(elem);
        auto vstart = vmap.begin();
        auto vlteq  = vmap.lower_bound(v);
        for (; vstart != vlteq; vstart++) {
          vmap[vstart->first] = f(vstart->first)(std::move(vstart->second));
        }
      });
    }
    return unit_t();
  }


  //////////////////////////////////
  // Most-recent version methods.

  shared_ptr<R> peek_now(const unit_t&) const {
    shared_ptr<R> res(nullptr);
    for (const auto& elem : container) {
      auto& vmap = std::get<1>(elem);
      auto vit = vmap.begin();
      if ( vit != vmap.end() ) {
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

  MultiIndexVMap combine(const MultiIndexVMap& other) const {
    // copy this DS
    MultiIndexVMap result = MultiIndexVMap(*this);
    // copy other DS
    for (const auto& elem : other.container) {
      for (const auto& velem : std::get<1>(elem)) {
        result.insert(velem.first, velem.second);
      }
    }
    return result;
  }

  tuple<MultiIndexVMap, MultiIndexVMap> split(const unit_t&) const {
    // Find midpoint
    const size_t size = container.size();
    const size_t half = size / 2;
    // Setup iterators
    auto beg = container.begin();
    auto mid = container.begin();
    std::advance(mid, half);
    auto end = container.end();
    // Construct DS from iterators
    return std::make_tuple(MultiIndexVMap(beg, mid), MultiIndexVMap(mid, end));
  }

  template <typename Fun>
  unit_t iterate(const Version& v, Fun f) const {
    for (const auto& elem : container) {
      auto& vmap = std::get<1>(elem);
      auto it = vmap.upper_bound(v);
      if ( it != vmap.end() ) {
        f(it->second);
      }
    }
    return unit_t();
  }

  template<typename Fun>
  auto map(const Version& v, Fun f) const -> MultiIndexVMap< RT<Fun, R> > {
    MultiIndexVMap< RT<Fun,R> > result;
    for (const auto& elem : container) {
      auto& vmap = std::get<1>(elem);
      auto it = vmap.upper_bound(v);
      if ( it != vmap.end() ) {
        result.insert(it->first, f(it->second));
      }
    }
    return result;
  }

  template <typename Fun>
  MultiIndexVMap<R, Indexes...> filter(const Version& v, Fun predicate) const {
    MultiIndexVMap<R, Indexes...> result;
    for (const auto& elem : container) {
      auto& vmap = std::get<1>(elem);
      auto it = vmap.upper_bound(v);
      if ( it != vmap.end() && predicate(it->second) ) {
        result.insert(it->first, it->second);
      }
    }
    return result;
  }

  template<typename Fun, typename Acc>
  Acc fold(const Version& v, Fun f, Acc acc) const {
    for (const auto& elem : container) {
      auto& vmap = std::get<1>(elem);
      auto it = vmap.upper_bound(v);
      if ( it != vmap.end() ) {
        acc = f(std::move(acc))(it->second);
      }
    }
    return acc;
  }

  template<typename F1, typename F2, typename Z>
  MultiIndexVMap<R_key_value<RT<F1, R>, Z>>
  groupBy(const Version& v, F1 grouper, F2 folder, const Z& init) const
  {
    // Create a map to hold partial results
    using K = RT<F1, R>;
    unordered_map<K, VContainer<Z>> accs;

    for (const auto& elem : container) {
      auto& vmap = std::get<1>(elem);
      auto vit = vmap.upper_bound(v);
      if ( vit != vmap.end() ) {
        K key = grouper(vit->second);
        if (accs.find(key) == accs.end()) {
          accs[key][vit->first] = init;
        }
        accs[key][vit->first] = folder(std::move(accs[key][vit->first]))(vit->second);
      }
    }

    MultiIndexVMap<R_key_value<K,Z>> result;
    for (auto& it : accs) {
      for (auto&& vit : it.second) {
        result.insert(std::move(vit.first), std::move(R_key_value<K, Z> {it.first, std::move(vit.second)}));
      }
    }

    return result;
  }

  template <class Fun>
  auto ext(const Version& v, Fun expand) const -> MultiIndexVMap < typename RT<Fun, R>::ElemType >  {
    typedef typename RT<Fun, R>::ElemType T;
    MultiIndexVMap<T> result;
    for (const auto& elem : container) {
      auto& vmap = std::get<1>(elem);
      auto vit = vmap.upper_bound(v);
      if ( vit != vmap.end() ) {
        for (auto& it2 : expand(vit->second).container) {
          result.insert(vit->first, it2.second);
        }
      }
    }
    return result;
  }


  ///////////////////////////////
  // Multi-version methods.

  template<typename Fun, typename Acc>
  Acc fold_all(Fun f, Acc acc) const {
    for (const auto& elem : container) {
      for (const auto& velem : std::get<1>(elem)) {
        acc = f(std::move(acc))(velem.first)(velem.second);
      }
    }
    return acc;
  }

  // Non-inclusive erase less than version.
  unit_t erase_prefix_all(const Version& v) {
    auto end = container.end();
    for (auto it = container.begin(); it != end;) {
      bool erase_elem = false;
      container.modify(it, [&](auto& elem){
        auto& vmap = std::get<1>(elem);
        auto vlteq = vmap.lower_bound(v);
        auto vless = vmap.upper_bound(v);
        auto vend  = vmap.end();
        if ( vless != vend ) {
          vmap.erase((vlteq == vless)? ++vless : vless, vend);
          erase_elem = vmap.empty();
        }
      });
      if ( erase_elem ) {
        it = container.erase(it);
      } else {
        ++it;
      }
    }
    return unit_t();
  }

  ///////////////////////////////////////////
  //
  // Index operations.

  template <class Index, class Key>
  shared_ptr<R> lookup_before_by_index(const Index& index, const Version& v, Key key) const {
    const auto& it = index.find(key);
    shared_ptr<R> result;
    if (it != index.end()) {
      auto& vmap = std::get<1>(*it);
      auto vit = vmap.upper_bound(v);
      if ( vit != vmap.end() ) {
        result = make_shared<R>(vit->second);
      }
    }
    return result;
  }

  template <class Index, class Key, typename F, typename G>
  auto lookup_with_before_by_index(const Index& index, const Version& v, Key key, F f, G g) const {
    const auto& it = index.find(key);
    if (it == index.end()) {
      return f(unit_t {});
    } else {
      auto& vmap = std::get<1>(*it);
      auto vit = vmap.upper_bound(v);
      if ( vit == vmap.end() ) {
        return f(unit_t {});
      } else {
        return g(vit->second);
      }
    }
  }

  template <class Index, class Key>
  MultiIndexVMap<R, Indexes...> slice_by_index(const Index& index, const Version& v, Key key) const
  {
    MultiIndexVMap<R, Indexes...> result;
    std::pair<typename Index::iterator, typename Index::iterator> p = index.equal_range(key);
    for (typename Index::iterator it = p.first; it != p.second; it++) {
      auto& vmap = std::get<1>(*it);
      auto vit = vmap.upper_bound(v);
      if ( vit != vmap.end() ) {
        result.insert(vit->first, vit->second);
      }
    }
    return result;
  }

  template <class Index, class Key>
  MultiIndexVMap<R, Indexes...> range_by_index(const Index& index, const Version& v, Key a, Key b) const
  {
    MultiIndexVMap<R, Indexes...> result;
    std::pair<typename Index::iterator, typename Index::iterator> p =
      index.range(a <= boost::lambda::_1, b >= boost::lambda::_1);
    for (typename Index::iterator it = p.first; it != p.second; it++) {
      auto& vmap = std::get<1>(*it);
      auto vit = vmap.upper_bound(v);
      if ( vit != vmap.end() ) {
        result.insert(vit->first, vit->second);
      }
    }
    return result;
  }

  template <class Index, class Key, typename Fun, typename Acc>
  Acc fold_slice_by_index(const Index& index, const Version& v, Key key, Fun f, Acc acc) const
  {
    std::pair<typename Index::iterator, typename Index::iterator> p = index.equal_range(key);
    for (typename Index::iterator it = p.first; it != p.second; it++) {
      auto& vmap = std::get<1>(*it);
      auto vit = vmap.upper_bound(v);
      if ( vit != vmap.end() ) {
        acc = f(std::move(acc))(vit->second);
      }
    }
    return acc;
  }

  template <class Index, class Key, typename Fun, typename Acc>
  Acc fold_range_by_index(const Index& index, const Version& v, Key a, Key b, Fun f, Acc acc) const
  {
    std::pair<typename Index::iterator, typename Index::iterator> p =
      index.range(a <= boost::lambda::_1, b >= boost::lambda::_1);
    for (typename Index::iterator it = p.first; it != p.second; it++) {
      auto& vmap = std::get<1>(*it);
      auto vit = vmap.upper_bound(v);
      if ( vit != vmap.end() ) {
        acc = f(std::move(acc))(vit->second);
      }
    }
    return acc;
  }

  //////////////////
  // Comparators.

  bool operator==(const MultiIndexVMap& other) const {
    return container == other.container;
  }

  bool operator!=(const MultiIndexVMap& other) const {
    return container != other.container;
  }

  bool operator<(const MultiIndexVMap& other) const {
    return container < other.container;
  }

  bool operator>(const MultiIndexVMap& other) const {
    return container > other.container;
  }

  Container& getContainer() { return container; }

  // Return a constant reference to the container
  const Container& getConstContainer() const {return container;}

  template<class Archive>
  void serialize(Archive &ar) const {
    ar.write(container.size());
    for (const auto& elem : container) {
      ar & elem;
    }
  }

  template<class Archive>
  void serialize(Archive &ar) {
    size_t sz = 0;
    ar.read(sz);
    while ( sz > 0 ) {
      Elem elem;
      ar & elem;
      container.emplace(elem);
      sz--;
    }
  }

  template<class Archive>
  void serialize(Archive &ar, const unsigned int) {
    ar & boost::serialization::make_nvp("__K3MultiIndexVMap", container);
  }

 private:
  friend class boost::serialization::access;

};


} // Namespace K3

namespace boost {
  namespace serialization {
    template <class _T0, class... _T1>
    class implementation_level<K3::MultiIndexBag<_T0, _T1...>> {
      public:
        typedef  mpl::integral_c_tag tag;
        typedef  mpl::int_<object_serializable> type;
        BOOST_STATIC_CONSTANT(int, value = implementation_level::type::value);
    };

    template <class _T0, class... _T1>
    class implementation_level<K3::MultiIndexMap<_T0, _T1...>> {
      public:
        typedef  mpl::integral_c_tag tag;
        typedef  mpl::int_<object_serializable> type;
        BOOST_STATIC_CONSTANT(int, value = implementation_level::type::value);
    };

    template <class _T0, class... _T1>
    class implementation_level<K3::MultiIndexVMap<_T0, _T1...>> {
      public:
        typedef  mpl::integral_c_tag tag;
        typedef  mpl::int_<object_serializable> type;
        BOOST_STATIC_CONSTANT(int, value = implementation_level::type::value);
    };
  }
}

template <typename... Args>
std::size_t hash_value(K3::MultiIndexBag<Args...> const& b) {
  return hash_collection(b);
}

template <typename... Args>
std::size_t hash_value(K3::MultiIndexMap<Args...> const& b) {
  return hash_collection(b);
}

template <typename... Args>
std::size_t hash_value(K3::MultiIndexVMap<Args...> const& b) {
  return hash_collection(b);
}

namespace JSON {
  using namespace rapidjson;

  template <class E, typename... Indexes>
  struct convert<K3::MultiIndexBag<E, Indexes...>> {
    template <class Allocator>
    static Value encode(const K3::MultiIndexBag<E, Indexes...>& c, Allocator& al) {
     Value v;
     v.SetObject();
     v.AddMember("type", Value("MultiIndexBag"), al);
     Value inner;
     inner.SetArray();
     for (const auto& e : c.getConstContainer()) {
       inner.PushBack(convert<E>::encode(e, al), al);
     }
     v.AddMember("value", inner.Move(), al);
     return v;
    }
  };

  template <class E, typename... Indexes>
  struct convert<K3::MultiIndexMap<E, Indexes...>> {
    template <class Allocator>
    static Value encode(const K3::MultiIndexMap<E, Indexes...>& c, Allocator& al) {
     Value v;
     v.SetObject();
     v.AddMember("type", Value("MultiIndexMap"), al);
     Value inner;
     inner.SetArray();
     for (const auto& e : c.getConstContainer()) {
       inner.PushBack(convert<E>::encode(e, al), al);
     }
     v.AddMember("value", inner.Move(), al);
     return v;
    }
  };

  template <class E, typename... Indexes>
  struct convert<K3::MultiIndexVMap<E, Indexes...>> {
    template <class Allocator>
    static Value encode(const K3::MultiIndexVMap<E, Indexes...>& c, Allocator& al) {
     Value v;
     v.SetObject();
     v.AddMember("type", Value("MultiIndexVMap"), al);
     Value inner;
     inner.SetArray();
     for (const auto& e : c.getConstContainer()) {
      for (const auto& ve : std::get<1>(e)) {
       inner.PushBack(convert<typename K3::MultiIndexVMap<E, Indexes...>::Version>::encode(ve.first, al), al);
       inner.PushBack(convert<E>::encode(ve.second, al), al);
      }
     }
     v.AddMember("value", inner.Move(), al);
     return v;
    }
  };

}


namespace YAML {
  template <class E, typename... Indexes>
  struct convert<K3::MultiIndexBag<E, Indexes...>>
  {
    static Node encode(const K3::MultiIndexBag<E, Indexes...>& c) {
      Node node;
      for (auto i: c.getConstContainer()) {
        node.push_back(convert<E>::encode(i));
      }
      return node;
    }

    static bool decode(const Node& node, K3::MultiIndexBag<E, Indexes...>& c) {
      for (auto& i: node) {
        c.insert(i.as<E>());
      }
      return true;
    }
  };

  template <class E, typename... Indexes>
  struct convert<K3::MultiIndexMap<E, Indexes...>>
  {
    static Node encode(const K3::MultiIndexMap<E, Indexes...>& c) {
      Node node;
      auto container = c.getConstContainer();
      if (container.size() > 0) {
        for (auto i: container) {
          node.push_back(convert<E>::encode(i));
        }
      } else {
        node = YAML::Load("[]");
      }
      return node;
    }

    static bool decode(const Node& node, K3::MultiIndexMap<E, Indexes...>& c) {
      for (auto& i: node) {
        c.insert(i.as<E>());
      }
      return true;
    }
  };

  template <class E, typename... Indexes>
  struct convert<K3::MultiIndexVMap<E, Indexes...>>
  {
    static Node encode(const K3::MultiIndexVMap<E, Indexes...>& c) {
      Node node;
      auto container = c.getConstContainer();
      if (container.size() > 0) {
        for (auto i: container) {
          for (auto j : std::get<1>(i)) {
            node.push_back(convert<typename K3::MultiIndexVMap<E, Indexes...>::Version>::encode(j.first));
            node.push_back(convert<E>::encode(j.second));
          }
        }
      }
      else {
        node = YAML::Load("[]");
      }
      return node;
    }

    static bool decode(const Node& node, K3::MultiIndexVMap<E, Indexes...>& c) {
      bool asVersion = true;
      typename K3::MultiIndexVMap<E, Indexes...>::Version v;
      for (auto& i: node) {
        if ( asVersion ) { v = i.as<typename K3::MultiIndexVMap<E, Indexes...>::Version>(); }
        else { c.insert(v, i.as<E>()); }
        asVersion = !asVersion;
      }
      return true;
    }
  };

}

#endif
