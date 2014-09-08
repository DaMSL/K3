#ifndef __K3_RUNTIME_DATASPACE__
#define __K3_RUNTIME_DATASPACE__

#include <boost/tr1/unordered_set.hpp>
#include <boost/tr1/unordered_map.hpp>
#include "boost/serialization/vector.hpp"
#include "boost/serialization/set.hpp"
#include "boost/serialization/list.hpp"
#include "external/boost_ext/unordered_map.hpp"
#include <boost/serialization/base_object.hpp>

#include <list>
#include <vector>


// TODO verify the type signatures of ext for the various collection types

namespace K3 {

// Utility to give the return type of a Function F expecting an Element E as an argument:
template <class F, class E> using RT = decltype(std::declval<F>()(std::declval<E>()));

using std::shared_ptr;
using std::tuple;

// Utility Typedefs
template<class K, class V>
using unordered_map = std::tr1::unordered_map<K,V>;

template<class K>
using unordered_set = std::tr1::unordered_set<K>;

// StlDS provides the basic Collection transformers via generic implementations
// that should work with any STL container.
template <template<typename...> class StlContainer, class Elem>
class StlDS {
  // The underlying STL container type backing this dataspace:
  typedef StlContainer<Elem> Container;

  // Iterator Types:
  typedef typename Container::const_iterator const_iterator_type;
  typedef typename Container::iterator iterator_type;

 public:
  // Expose the element type with a public typedef  ( needed in ext() ):
  typedef Elem ElemType;

  // Constructors:
  // Default Constructor
  StlDS()
    : container()
  { }

  // Copy Constructor
  StlDS(const StlDS& other)
    : container(other.container)
  { }

  // Move Constructor
  StlDS(StlDS&& other)
    : container(std::move(other.container))
  { }

  // Copy Constructor from container
  StlDS(const Container& con)
    : container(con)
  { }

  // Move Constructor from container
  StlDS(Container&& con)
    : container(std::move(con))
  { }

  // Construct from (container) iterators
  template<typename Iterator>
  StlDS(Iterator begin, Iterator end)
    : container(begin,end)
  {}

  ~StlDS() {}

  // Assign Operators:
  // Copy Assign Operator
  StlDS& operator=(const StlDS& other) {
    container = other.container;
    return *this;
  }

  // Move Assign Operator
  StlDS& operator=(StlDS&& other) {
    // Proxy to the move assign operator of the container
    container = std::move(other.container);
    return *this;
  }

  // Maybe return the first element in the ds
  shared_ptr<Elem> peek(unit_t) const {
    shared_ptr<Elem> res;
    const_iterator_type it = container.begin();
    if (it != container.end()) {
      res = std::make_shared<Elem>(*it);
    }
    return res;
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
    iterator_type it;
    it = std::find(container.begin(), container.end(), v);
    if (it != container.end()) {
      container.erase(it);
    }
    return unit_t();
  }

  // Update by move
  // Find v in the container. Insert (by move) v2 in its position. Erase v.
  unit_t update(const Elem& v, const Elem&& v2) {
    iterator_type it;
    it = std::find(container.begin(), container.end(), v);
    if (it != container.end()) {
      container.insert(it, v2);
      container.erase(it);
    }
    return unit_t();
  }

  // Update by copy
  unit_t update(const Elem& v, const Elem& v2) {
    // Copy v2, then update by move
    return update(v, Elem(v2));
  }

  // Return the number of elements in this ds
  int size(const unit_t&) const {
    return container.size();
  }

  // Return a new DS with data from this and other
  StlDS combine(const StlDS& other) const {
    // copy this DS
    StlDS result = StlDS(*this);
    // copy other DS
    for (const Elem &e : other.container) {
      result.insert(e);
    }
    return result;
  }

  // Split the ds at its midpoint
  tuple<StlDS, StlDS> split(const unit_t&) const {
    // Find midpoint
    const size_t size = container.size();
    const size_t half = size / 2;
    // Setup iterators
    const_iterator_type beg = container.begin();
    const_iterator_type mid = container.begin();
    std::advance(mid, half);
    const_iterator_type end = container.end();
    // Construct DS from iterators
    return std::make_tuple(StlDS(beg,mid), StlDS(mid,end));
  }

  // Apply a function to each element of this ds
  template<typename Fun>
  unit_t iterate(Fun f) {
    for (const Elem& e : container) {
      f(e);
    }
    return unit_t();
  }

  // Produce a new ds by mapping a function over this ds
  template<typename Fun>
  auto map(Fun f) -> StlDS<StlContainer, R_elem<RT<Fun, Elem>>> {
    StlDS<StlContainer, R_elem<RT<Fun, Elem>>> result;
    for (const Elem &e : container) {
      result.insert( R_elem<RT<Fun, Elem>>{ f(e) } ); // Copies e (f is pass by value), then move inserts
    }
    return result;
  }

  // Create a new DS consisting of elements from this ds that satisfy the predicate
  template<typename Fun>
  StlDS filter(Fun predicate) {
    StlDS<StlContainer, Elem> result;
    for (const Elem &e : container) {
      if (predicate(e)) {
        result.insert(e);
      }
    }
    return result;
  }

  // Fold a function over this ds
  template<typename Fun, typename Acc>
  Acc fold(Fun f, const Acc& init_acc) {
    Acc acc = init_acc;
    for (const Elem &e : container) { acc = f(acc)(e); }
    return acc;
  }

  // Group By
  template<typename F1, typename F2, typename Z>
  StlDS<StlContainer, R_key_value<RT<F1, Elem>,Z>> groupBy(F1 grouper, F2 folder, const Z& init) {
       // Create a map to hold partial results
       typedef RT<F1, Elem> K;
       unordered_map<K, Z> accs;

       for (const auto& elem : container) {
          K key = grouper(elem);
          if (accs.find(key) == accs.end()) {
            accs[key] = init;
          }
          accs[key] = folder(accs[key])(elem);
       }

      // Build the R_key_value records and insert them into result
      StlDS<StlContainer, R_key_value<K,Z>> result;
      for(const auto& it : accs) {
        result.insert(R_key_value<K, Z>{it.first, it.second});

      }

       return result;
  }

  // TODO optimize copies
  template <class Fun>
  auto ext(Fun expand) -> StlDS<StlContainer, typename RT<Fun, Elem>::ElemType > {
    typedef typename RT<Fun, Elem>::ElemType T;
    StlDS<StlContainer, T> result;
    for (const Elem& elem : container) {
      for (const T& elem2 : expand(elem).container) {
        // can we force a move here?
        result.insert(elem2);
      }
    }

    return result;
  }

  bool operator==(const StlDS& other) const {
    return container == other.container;
  }

  bool operator!=(const StlDS& other) const {
    return container != other.container;
  }

  bool operator<(const StlDS& other) const {
    return container < other.container;
  }

  Container& getContainer() { return container; }

  // Return a constant reference to the container
  const Container& getConstContainer() const {return container;}

  Container container;
};

// Various DS's achieved through typedefs
template <class Elem>
using ListDS = StlDS<std::list, Elem>;

template <class Elem>
using SetDS = StlDS<unordered_set, Elem>;

template <class Elem>
using SortedDS = StlDS<std::multiset, Elem>;

template <class Elem>
using VectorDS = StlDS<std::vector, Elem>;


// The Colllection variants inherit functionality from a dataspace.
// Many of the dataspace functions return a dataspace, requiring an
// explicit conversion/wrapping in its sub-classes. (call to Superclass constructor)
// Each variant may also add extra functionality.
template <class Elem>
class Collection : public VectorDS<Elem> {
  typedef VectorDS<Elem> Super;

 public:
  // Constructors:
  // Default:
  Collection() : VectorDS<Elem>() { }

  // Copy:
  Collection(const Collection& c): VectorDS<Elem>(c) { }

  // Move:
  Collection(Collection&& c): VectorDS<Elem>(std::move(c)) { }

  // Assign operators:
  // Copy:
  Collection& operator=(const Collection& other) {
    VectorDS<Elem>::operator=(other);
    return *this;
  }

  // Move:
  Collection& operator=(Collection&& other) {
    VectorDS<Elem>::operator=(std::move(other));
    return *this;
  }

  // Superclass constructors: (for conversion from Super to this-type)
  // Copy:
  Collection(const VectorDS<Elem>& c): VectorDS<Elem>(c) { }

  // Move:
  Collection(VectorDS<Elem>&& c): VectorDS<Elem>(std::move(c)) { }

  // Wrapped Dataspace Functions: (call DS function, then construct-by-move)
  Collection combine(const Collection& other) const {
    return Collection<Elem>(Super::combine(other));
  }

  // TODO: force a move when constructing the Collections?
  tuple<Collection, Collection> split(unit_t) const {
    auto t = Super::split(unit_t());
    return std::make_tuple(Collection<Elem>(std::get<0>(t)), Collection<Elem>(std::get<1>(t)));
  }

  template<typename Fun>
  auto map(Fun f) -> Collection<R_elem<RT<Fun, Elem>>> {
    return Collection<R_elem<RT<Fun, Elem>>>(Super::map(f));
  }

  template<typename Fun>
  Collection filter(Fun predicate) {
    return Collection<Elem>(Super::filter(predicate));
  }

  template<typename F1, typename F2, typename Z>
  Collection< R_key_value<RT<F1, Elem>,Z> > groupBy(F1 grouper, F2 folder, const Z& init) {
    return Collection< R_key_value<RT<F1, Elem>,Z> >(Super::groupBy(grouper, folder, init));
  }

  template <class Fun>
  auto ext(Fun expand) -> Collection< typename RT<Fun, Elem>::ElemType > {
    typedef typename RT<Fun, Elem>::ElemType T;
    return Collection<T> (Super::ext(expand));
  }

};

template <class Elem>
class Set : public SetDS<Elem> {
  typedef SetDS<Elem> Super;

 public:
  // Constructors:
  // Default:
  Set() : SetDS<Elem>() { }

  // Copy:
  Set(const Set& c): SetDS<Elem>(c) { }

  // Move:
  Set(Set&& c): SetDS<Elem>(std::move(c)) { }

  // Assign operators:
  // Copy:
  Set& operator=(const Set& other) {
    SetDS<Elem>::operator=(other);
    return *this;
  }

  // Move:
  Set& operator=(Set&& other) {
    SetDS<Elem>::operator=(std::move(other));
    return *this;
  }

  // Superclass constructors: (for conversion from Super to this-type)
  // Copy:
  Set(const SetDS<Elem>& c): SetDS<Elem>(c) { }

  // Move:
  Set(SetDS<Elem>&& c): SetDS<Elem>(std::move(c)) { }

  // Set specific functions
  bool member(const Elem& e) {
    auto it = std::find(Super::getConstContainer().begin(), Super::getConstContainer().end(), e);
    return (it != Super::getConstContainer().end());
  }

  bool isSubsetOf(const Set<Elem>& other) {
    for (const auto &x : Super::getConstContainer()) {
      if (!other.member(x)) { return false; }
    }
    return true;
  }

  // TODO union is a reserved word
  Set union1(const Set<Elem>& other) {
    Set<Elem> result;
    for (const auto &x : Super::getConstContainer()) {
      result.insert(x);
    }
    for (const auto &x : other.getConstContainer()) {
      result.insert(x);
    }
    return result;
  }

  Set intersect(const Set<Elem>& other) {
    Set<Elem> result;
    for (const auto &x : Super::getConstContainer()) {
      if(other.member(x)) {
        result.insert(x);
      }
    }
    return result;
  }

  Set difference(const Set<Elem>& other) {
    Set<Elem> result;
    for (const auto &x : Super::getConstContainer()) {
      if(!other.member(x)) {
        result.insert(x);
      }
    }
    return result;
  }

  // Wrapped Dataspace Functions: (call DS function, then construct-by-move)
  Set combine(const Set& other) const {
    return Set<Elem>(Super::combine(other));
  }

  // TODO: force a move when constructing the Sets?
  tuple<Set, Set> split(unit_t) const {
    auto t = Super::split(unit_t());
    return std::make_tuple(Set<Elem>(std::get<0>(t)), Set<Elem>(std::get<1>(t)));
  }

  template<typename Fun>
  auto map(Fun f) -> Set<R_elem<RT<Fun, Elem>>> {
    return Set<R_elem<RT<Fun, Elem>>>(Super::map(f));
  }

  template<typename Fun>
  Set filter(Fun predicate) {
    return Set<Elem>(Super::filter(predicate));
  }

  template<typename F1, typename F2, typename Z>
  Set< R_key_value<RT<F1, Elem>,Z> > groupBy(F1 grouper, F2 folder, const Z& init) {
    return Set< R_key_value<RT<F1, Elem>,Z> >(Super::groupBy(grouper, folder, init));
  }

  template <class Fun>
  auto ext(Fun expand) -> Set< typename RT<Fun, Elem>::ElemType > {
    typedef typename RT<Fun, Elem>::ElemType T;
    return Set<T> (Super::ext(expand));
  }

};

template <class Elem>
class Seq : public ListDS<Elem> {
  typedef ListDS<Elem> Super;

 public:
  // Constructors:
  // Default:
  Seq() : ListDS<Elem>() { }

  // Copy:
  Seq(const Seq& c): ListDS<Elem>(c) { }

  // Move:
  Seq(Seq&& c): ListDS<Elem>(std::move(c)) { }

  // Assign operators:
  // Copy:
  Seq& operator=(const Seq& other) {
    ListDS<Elem>::operator=(other);
    return *this;
  }

  // Move:
  Seq& operator=(Seq&& other) {
    ListDS<Elem>::operator=(std::move(other));
    return *this;
  }

  // Superclass constructors: (for conversion from Super to this-type)
  // Copy:
  Seq(const ListDS<Elem>& c): ListDS<Elem>(c) { }

  // Move:
  Seq(ListDS<Elem>&& c): ListDS<Elem>(std::move(c)) { }

  // Seq specific functions
  Seq sort(const F<F<int(Elem)>(Elem)>& comp) {
    auto& l(Super::getConstContainer());
    auto f = [&] (Elem& a, Elem& b) {
      return comp(a)(b) < 0;
    };
    l.sort(f);
    // Force Move into ListDS class.
    // Dont reference l again
    return Seq(Super(std::move(l)));
  }

  Elem at(int i) {
    auto& l = Super::getConstContainer();
    auto it = l.begin();
    std::advance(it, i);
    return *it; // TODO: bounds check?
  }

  // Wrapped Dataspace Functions: (call DS function, then construct-by-move)
  Seq combine(const Seq& other) const {
    return Seq<Elem>(Super::combine(other));
  }

  // TODO: force a move when constructing the Sets?
  tuple<Seq, Seq> split(unit_t) const {
    auto t = Super::split(unit_t());
    return std::make_tuple(Seq<Elem>(std::get<0>(t)), Seq<Elem>(std::get<1>(t)));
  }

  template<typename Fun>
  auto map(Fun f) -> Seq<R_elem<RT<Fun, Elem>>> {
    return Seq<R_elem<RT<Fun, Elem>>>(Super::map(f));
  }

  template<typename Fun>
  Seq filter(Fun predicate) {
    return Seq<Elem>(Super::filter(predicate));
  }

  template<typename F1, typename F2, typename Z>
  Seq< R_key_value<RT<F1, Elem>,Z> > groupBy(F1 grouper, F2 folder, const Z& init) {
    return Seq< R_key_value<RT<F1, Elem>,Z> >(Super::groupBy(grouper, folder, init));
  }

  template <class Fun>
  auto ext(Fun expand) -> Seq< typename RT<Fun, Elem>::ElemType > {
    typedef typename RT<Fun, Elem>::ElemType T;
    return Seq<T> (Super::ext(expand));
  }

};

template <class Elem>
class Sorted : public SortedDS<Elem> {
  typedef SortedDS<Elem> Super;

 public:
  // Constructors:
  // Default:
  Sorted() : SortedDS<Elem>() { }

  // Copy:
  Sorted(const Sorted& c): SortedDS<Elem>(c) { }

  // Move:
  Sorted(Sorted&& c): SortedDS<Elem>(std::move(c)) { }

  // Assign operators:
  // Copy:
  Sorted& operator=(const Sorted& other) {
    SortedDS<Elem>::operator=(other);
    return *this;
  }

  // Move:
  Sorted& operator=(Sorted&& other) {
    SortedDS<Elem>::operator=(std::move(other));
    return *this;
  }

  // Superclass constructors: (for conversion from Super to this-type)
  // Copy:
  Sorted(const SortedDS<Elem>& c): SortedDS<Elem>(c) { }

  // Move:
  Sorted(SortedDS<Elem>&& c): SortedDS<Elem>(std::move(c)) { }

  // Sorted specific functions
  std::shared_ptr<Elem> min() {
     const auto& x = Super::getConstContainer();
     auto it = std::min_element(x.begin(), x.end());
     std::shared_ptr<Elem> result = nullptr;
     if (it != x.end()) {
       result = std::make_shared<Elem>(*it);
     }

     return result;
  }

  std::shared_ptr<Elem> max() {
     const auto& x = Super::getConstContainer();
     auto it = std::max_element(x.begin(), x.end());
     std::shared_ptr<Elem> result = nullptr;
     if (it != x.end()) {
      result = std::make_shared<Elem>(*it);
     }

     return result;
  }

  std::shared_ptr<Elem> lowerBound(const Elem& e) {
    const auto& x = Super::getConstContainer();
    auto it = std::lower_bound(x.begin(), x.end(), e);
    std::shared_ptr<Elem> result = nullptr;
    if (it != x.end()) {
      result = std::make_shared<Elem>(*it);
    }

    return result;
  }

  std::shared_ptr<Elem> upperBound(const Elem& e) {
    const auto& x = Super::getConstContainer();
    auto it = std::upper_bound(x.begin(), x.end(), e);
    std::shared_ptr<Elem> result = nullptr;
    if (it != x.end()) {
      result = std::make_shared<Elem>(*it);
    }

    return result;
  }

  Sorted slice(const Elem& a, const Elem& b) {
    const auto& x = Super::getConstContainer();
    Sorted<Elem> result;
    for (Elem e : x) {
      if (e >= a && e <= b) {
        result.insert(e);
      }
      if (e > b) {
        break;
      }
    }
    return result;
  }

  // Wrapped Dataspace Functions: (call DS function, then construct-by-move)
  Sorted combine(const Sorted& other) const {
    return Sorted<Elem>(Super::combine(other));
  }

  // TODO: force a move when constructing the Sets?
  tuple<Sorted, Sorted> split(unit_t) const {
    auto t = Super::split(unit_t());
    return std::make_tuple(Sorted<Elem>(std::get<0>(t)), Sorted<Elem>(std::get<1>(t)));
  }

  template<typename Fun>
  auto map(Fun f) -> Sorted<R_elem<RT<Fun, Elem>>> {
    return Sorted<R_elem<RT<Fun, Elem>>>(Super::map(f));
  }

  template<typename Fun>
  Sorted filter(Fun predicate) {
    return Sorted<Elem>(Super::filter(predicate));
  }

  template<typename F1, typename F2, typename Z>
  Sorted< R_key_value<RT<F1, Elem>,Z> > groupBy(F1 grouper, F2 folder, const Z& init) {
    return Sorted< R_key_value<RT<F1, Elem>,Z> >(Super::groupBy(grouper, folder, init));
  }

  template <class Fun>
  auto ext(Fun expand) -> Sorted< typename RT<Fun, Elem>::ElemType > {
    typedef typename RT<Fun, Elem>::ElemType T;
    return Sorted<T> (Super::ext(expand));
  }

};

// TODO reorder functions to match the others
template<class R>
class Map {

  using Key = typename R::KeyType;
  using Value = typename R::ValueType;
  using iterator_type = typename unordered_map<Key,Value>::iterator;
  using const_iterator_type = typename unordered_map<Key, Value>::const_iterator;

 public:
  typedef R ElemType;

  // Default Constructor
  Map()
    : container()
  {  }

  // Copy Constructor
  Map(const Map& other)
    : container(other.container)
  { }

  // Move Constructor
  Map(Map&& other)
    : container(std::move(other.container))
  { }

  // Copy Constructor from container
  Map(const unordered_map<Key,Value>& con)
    : container(con)
  { }

  // Move Constructor from container
  Map(unordered_map<Key, Value>&& con)
    : container(std::move(con))
  { }

  // Construct from (container) iterators
  template<typename Iterator>
  Map(Iterator begin, Iterator end)
    : container(begin,end)
  {}

  ~Map() { }

  // Copy Assign Operator
  Map& operator=(const Map& other) {
    container = other.container;
    return *this;
  }

  // Move Assign Operator
  Map& operator=(Map&& other) {
    container = std::move(other.container);
    //other.container = NULL;
    return *this;
  }

  // DS Operations:
  // Maybe return the first element in the DS
  shared_ptr<R> peek(const unit_t&) const {
    shared_ptr<R> res;
    const_iterator_type it = container.begin();
    if (it != container.end()) {
      res = std::make_shared<R>();
      res->key = it->first;
      res->value = it->second;
    }
    return res;
  }

  unit_t insert(const R& rec) {
    container[rec.key] = rec.value;
    return unit_t();
  }

  unit_t insert(R&& rec) {
    container[rec.key] = std::move(rec.value);
    return unit_t();
  }

  unit_t erase(const R& rec) {
    iterator_type it;
    it = container.find(rec.key);
    if (it != container.end() && it->second == rec.value) {
        container.erase(it);
    }
    return unit_t();
  }

  unit_t update(const R& rec1, const R& rec2) {
    iterator_type it;
    it = container.find(rec1.key);
    if (it != container.end()) {
      if (rec1.value == it->second) {
        container[rec2.key] = rec2.value;
      }
    }
    return unit_t();
  }

  unit_t update(const R& rec1, const R&& rec2) {
    iterator_type it;
    it = container.find(rec1.key);
    if (it != container.end()) {
      if (rec1.value == it->second) {
        container[rec2.key] = std::move(rec2.value);
      }
    }
    return unit_t();
  }

  template<typename Fun, typename Acc>
  Acc fold(Fun f, const Acc& init_acc) {
    Acc acc = init_acc;
    for (const std::pair<Key, Value>& p : container) {
      R rec;
      rec.key = p.first;
      rec.value = p.second;
      acc = f(acc)(rec);
    }
    return acc;
  }

  template<typename Fun>
  auto map(Fun f) -> Map< RT<Fun, R> > {
    Map< RT<Fun,R> > result;
    for (const std::pair<Key,Value>& p : container) {
      R rec {p.first, p.second};
      result.insert( f(rec) );
    }
    return result;
  }

  template <typename Fun>
  unit_t iterate(Fun f) {
    for (const std::pair<Key,Value>& p : container) {
      R rec;
      rec.key = p.first;
      rec.value = p.second;
      f(rec);
    }
    return unit_t();
  }

  template <typename Fun>
  Map<R> filter(Fun predicate) {
    Map<R> result;
    for (const std::pair<Key,Value>& p : container) {
      R rec;
      rec.key = p.first;
      rec.value = p.second;
      if (predicate(rec)) {
        result.insert(rec);
      }
    }
    return result;
  }

  tuple<Map, Map> split(const unit_t&) {
    // Find midpoint
    const size_t size = container.size();
    const size_t half = size / 2;
    // Setup iterators
    const_iterator_type beg = container.begin();
    const_iterator_type mid = container.begin();
    std::advance(mid, half);
    const_iterator_type end = container.end();
    // Construct DS from iterators
    return std::make_tuple(Map(beg, mid),Map(mid, end));
  }

  Map combine(const Map& other) const {
    // copy this DS
    Map result = Map(*this);
    // copy other DS
    for (const std::pair<Key,Value>& p: other.container) {
      result.container[p.first] = p.second;
    }
    return result;
  }

  template<typename F1, typename F2, typename Z>
  Map< R_key_value< RT<F1, R>,Z >> groupBy(F1 grouper, F2 folder, const Z& init) {
    // Create a map to hold partial results
    typedef RT<F1, R> K;
    unordered_map<K, Z> accs;

    for (const auto& it : container) {
      R rec = R{it.first, it.second};
      K key = grouper(rec);
      if (accs.find(key) == accs.end()) {
        accs[key] = init;
      }
      accs[key] = folder(accs[key])(rec);
    }

    // Force a move from accs into a mapDS
    // Careful! dont reference accs again!
    Map<R_key_value<K,Z>> result(std::move(accs));
    return result;
  }

  // TODO optimize copies. lots of record building here.
  template <class Fun>
  auto ext(Fun expand) -> Map < typename RT<Fun, R>::ElemType > {
    typedef typename RT<Fun, R>::ElemType T;
    Map<T> result;
    for (const auto& it : container) {
      for (const auto& it2 : expand(R{it.first, it.second}).container) {
        result.insert(R{it.first, it.second});
      }
    }

    return result;
  }

  int size(unit_t) const { return container.size(); }

  // lookup ignores the value of the argument
  shared_ptr<R> lookup(const R& r) {
      auto it(container.find(r.key));
      if (it != container.end()) {
        return std::make_shared<R>(R {it->first, it->second} );
      } else {
        return nullptr;
      }
  }

  bool operator==(const Map& other) const {
    return container == other.container;
  }

  bool operator!=(const Map& other) const {
    return container != other.container;
  }

  bool operator<(const Map& other) const {
    return container < other.container;
  }

  unordered_map<Key, Value>& getContainer() { return container; }

  const unordered_map<Key, Value>& getConstContainer() const { return container; }

 protected:
  unordered_map<Key,Value> container;

  private:
  friend class boost::serialization::access;
  template<class Archive>
  void serialize(Archive &ar, const unsigned int version) {
    ar & container;
  }

}; // class Map

} // Namespace K3

template <template<typename...> class Container, class Elem>
std::size_t hash_value(K3::StlDS<Container,Elem> const& b) {
  const auto& c  = b.getConstContainer();
  return boost::hash_range(c.begin(), c.end());
}

template <class Elem>
std::size_t hash_value(K3::Map<Elem> const& b) {
  const auto& c  = b.getConstContainer();
  return boost::hash_range(c.begin(), c.end());
}
#endif
