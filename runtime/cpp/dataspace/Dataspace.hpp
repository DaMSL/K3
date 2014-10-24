#ifndef __K3_RUNTIME_DATASPACE__
#define __K3_RUNTIME_DATASPACE__

#include <list>
#include <vector>
#include <math.h>

#include <boost/tr1/unordered_set.hpp>
#include <boost/tr1/unordered_map.hpp>
#include "boost/serialization/vector.hpp"
#include "boost/serialization/set.hpp"
#include "boost/serialization/list.hpp"
#include "external/boost_ext/unordered_map.hpp"
#include <boost/serialization/base_object.hpp>
#include <boost/multi_index_container.hpp>
#include <boost/multi_index/sequenced_index.hpp>
#include <boost/lambda/lambda.hpp>

#include "Common.hpp"
#include "BaseTypes.hpp"

namespace K3 {

// Utility to give the return type of a Function F expecting an Element E as an argument:
template <class F, class E> using RT = decltype(std::declval<F>()(std::declval<E>()));

// Utility Typedefs
template<class K, class V>
using unordered_map = std::tr1::unordered_map<K,V>;

template<class K>
using unordered_set = std::tr1::unordered_set<K>;

// StlDS provides the basic Collection transformers via generic implementations
// that should work with any STL container.
template <template <typename> class Derived, template<typename...> class StlContainer, class Elem>
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
  StlDS(): container() {}

  // Copy Constructor from container
  StlDS(const Container& con): container(con) {}

  // Move Constructor from container
  StlDS(Container&& con): container(std::move(con)) {}

  // Construct from (container) iterators
  template<typename Iterator>
  StlDS(Iterator begin, Iterator end): container(begin,end) {}

  Elem elemToRecord(const Elem& e) const { return e; }

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
  Derived<Elem> combine(StlDS other) const {
    // copy this DS
    Derived<Elem> result;
    result = StlDS(*this);
    // move insert each element of the other.
    // this is okay since we are working with a copy
    for (Elem &e : other.container) {
      result.insert(std::move(e));
    }
    return result;
  }

  // Split the ds at its midpoint
  tuple<Derived<Elem>, Derived<Elem>> split(const unit_t&) const {
    // Find midpoint
    const size_t size = container.size();
    const size_t half = size / 2;
    // Setup iterators
    const_iterator_type beg = container.begin();
    const_iterator_type mid = container.begin();
    std::advance(mid, half);
    const_iterator_type end = container.end();
    // Construct from iterators
    return std::make_tuple(Derived<Elem>(StlDS(beg,mid)), Derived<Elem>(StlDS(mid,end)));
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
  auto map(Fun f) const -> Derived<R_elem<RT<Fun, Elem>>>  {
    Derived<Elem> result;
    for (const Elem &e : container) {
      result.insert( R_elem<RT<Fun, Elem>>{ f(e) } ); // Copies e (f is pass by value), then move inserts
    }
    return result;
  }

  // Create a new DS consisting of elements from this ds that satisfy the predicate
  template<typename Fun>
  Derived<Elem> filter(Fun predicate) const {
    Derived<Elem> result;
    for (const Elem &e : container) {
      if (predicate(e)) {
        result.insert(e);
      }
    }
    return result;
  }

  // Fold a function over this ds
  template<typename Fun, typename Acc>
  Acc fold(Fun f, Acc acc) const {
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

      // Build the R_key_value records and insert them into result
      Derived<R_key_value<RT<F1, Elem>,Z>> result;
      for(auto& it : accs) {
        // move out of the map as we iterate
        result.insert(R_key_value<K, Z>{std::move(it.first), std::move(it.second)});

      }

      return result;
  }

  template <class Fun>
  auto ext(Fun expand) const -> Derived<typename RT<Fun, Elem>::ElemType> {
    typedef typename RT<Fun, Elem>::ElemType T;
    Derived<T> result;
    for (const Elem& elem : container) {
      for (T& elem2 : expand(elem).container) {
        result.insert(std::move(elem2));
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

  bool operator>(const StlDS& other) const {
    return container < other.container;
  }

  Container& getContainer() { return container; }

  // Return a constant reference to the container
  const Container& getConstContainer() const {return container;}

  Container container;

 private:
  friend class boost::serialization::access;
  template<class Archive>
  void serialize(Archive &ar, const unsigned int version) {
    ar & container;
  }

};

// Various DS's achieved through typedefs
template <template <typename> class Derived, class Elem>
using ListDS = StlDS<Derived, std::list, Elem>;

template <template <typename> class Derived, class Elem>
using SetDS = StlDS<Derived, unordered_set, Elem>;

template <template <typename> class Derived, class Elem>
using SortedDS = StlDS<Derived, std::multiset, Elem>;

template <template <class> class Derived, class Elem>
using VectorDS = StlDS<Derived, std::vector, Elem>;


// The Colllection variants inherit functionality from a dataspace.
// Each variant may also add extra functionality.
template <class Elem>
class Collection: public VectorDS<K3::Collection, Elem> {
  using Super = VectorDS<K3::Collection, Elem>;

 public:
  Collection(): Super() {}
  Collection(const Super& c): Super(c) { }
  Collection(Super&& c): Super(std::move(c)) { }
};

template <class Elem>
class Set : public SetDS<K3::Set, Elem> {
  using Super = SetDS<K3::Set, Elem>;

 public:
  Set(): Super() {}
  Set(const Super& c): Super(c) { }
  Set(Super&& c): Super(std::move(c)) { }

  // Set specific functions
  bool member(const Elem& e) const {
    auto it = std::find(Super::getConstContainer().begin(), Super::getConstContainer().end(), e);
    return (it != Super::getConstContainer().end());
  }

  bool isSubsetOf(const Set<Elem>& other) const {
    for (const auto &x : Super::getConstContainer()) {
      if (!other.member(x)) { return false; }
    }
    return true;
  }

  // TODO union is a reserved word
  Set union1(const Set<Elem>& other) const {
    Set<Elem> result;
    for (const auto &x : Super::getConstContainer()) {
      result.insert(x);
    }
    for (const auto &x : other.getConstContainer()) {
      result.insert(x);
    }
    return result;
  }

  Set intersect(const Set<Elem>& other) const {
    Set<Elem> result;
    for (const auto &x : Super::getConstContainer()) {
      if(other.member(x)) {
        result.insert(x);
      }
    }
    return result;
  }

  Set difference(const Set<Elem>& other) const {
    Set<Elem> result;
    for (const auto &x : Super::getConstContainer()) {
      if(!other.member(x)) {
        result.insert(x);
      }
    }
    return result;
  }
};

template <class Elem>
class Seq : public ListDS<K3::Seq, Elem> {
  using Super = ListDS<K3::Seq, Elem>;
 public:
  Seq(): Super() { }
  Seq(const Super& c): Super(c) { }
  Seq(Super&& c): Super(std::move(c)) { }

  // Seq specific functions
  Seq sort(F<F<int(Elem)>(Elem)> comp) {
    auto& l(Super::getContainer());
    auto f = [&] (Elem& a, Elem& b) mutable {
      return comp(a)(b) < 0;
    };
    l.sort(f);
    // Force Move into ListDS class.
    // Dont reference l again
    return Seq(Super(std::move(l)));
  }

  Elem at(int i) const {
    auto& l = Super::getConstContainer();
    auto it = l.begin();
    std::advance(it, i);
    return *it;
  }

};

template <class Elem>
class Sorted: public SortedDS<K3::Sorted, Elem> {
  using Super = SortedDS<K3::Sorted, Elem>;

 public:
  // Constructors:
  // Default:
  Sorted(): Super() { }
  Sorted(const Super& c): Super(c) { }
  Sorted(Super&& c): Super(std::move(c)) { }

  // Sorted specific functions
  std::shared_ptr<Elem> min() const {
     const auto& x = Super::getConstContainer();
     auto it = std::min_element(x.begin(), x.end());
     std::shared_ptr<Elem> result = nullptr;
     if (it != x.end()) {
       result = std::make_shared<Elem>(*it);
     }

     return result;
  }

  std::shared_ptr<Elem> max() const {
     const auto& x = Super::getConstContainer();
     auto it = std::max_element(x.begin(), x.end());
     std::shared_ptr<Elem> result = nullptr;
     if (it != x.end()) {
      result = std::make_shared<Elem>(*it);
     }

     return result;
  }

  std::shared_ptr<Elem> lowerBound(const Elem& e) const {
    const auto& x = Super::getConstContainer();
    auto it = std::lower_bound(x.begin(), x.end(), e);
    std::shared_ptr<Elem> result = nullptr;
    if (it != x.end()) {
      result = std::make_shared<Elem>(*it);
    }

    return result;
  }

  std::shared_ptr<Elem> upperBound(const Elem& e) const {
    const auto& x = Super::getConstContainer();
    auto it = std::upper_bound(x.begin(), x.end(), e);
    std::shared_ptr<Elem> result = nullptr;
    if (it != x.end()) {
      result = std::make_shared<Elem>(*it);
    }

    return result;
  }

  Sorted slice(const Elem& a, const Elem& b) const {
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

  template <class Pair>
  ElemType elemToRecord(const Pair& e) const { return R {e.first, e.second}; }

  // Default Constructor
  Map(): container() {}
  Map(const unordered_map<Key,Value>& con): container(con) {}
  Map(unordered_map<Key, Value>&& con): container(std::move(con)) {}

  // Construct from (container) iterators
  template<typename Iterator>
  Map(Iterator begin, Iterator end): container(begin,end) {}

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
  Acc fold(Fun f, Acc acc) const {
    for (const std::pair<Key, Value>& p : container) {
      acc = f(std::move(acc))(R {p.first, p.second});
    }
    return acc;
  }

  template<typename Fun>
  auto map(Fun f) const -> Map< RT<Fun, R> > {
    Map< RT<Fun,R> > result;
    for (const std::pair<Key,Value>& p : container) {
      result.insert( f(R {p.first, p.second}) );
    }
    return result;
  }

  template <typename Fun>
  unit_t iterate(Fun f) const {
    for (const std::pair<Key,Value>& p : container) {
      R rec;
      rec.key = p.first;
      rec.value = p.second;
      f(R{p.first, p.second});
    }
    return unit_t();
  }

  template <typename Fun>
  Map<R> filter(Fun predicate) const {
    Map<R> result;
    for (const std::pair<Key,Value>& p : container) {
      R rec;
      rec.key = p.first;
      rec.value = p.second;
      if (predicate(rec)) {
        result.insert(std::move(rec));
      }
    }
    return result;
  }

  tuple<Map, Map> split(const unit_t&) const {
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
  Map< R_key_value< RT<F1, R>,Z >> groupBy(F1 grouper, F2 folder, const Z& init) const {
    // Create a map to hold partial results
    typedef RT<F1, R> K;
    unordered_map<K, Z> accs;

    for (const auto& it : container) {
      R rec = R{it.first, it.second};
      K key = grouper(rec);
      if (accs.find(key) == accs.end()) {
        accs[key] = init;
      }
      accs[key] = folder(std::move(accs[key]))(std::move(rec));
    }

    // Force a move from accs into a mapDS
    Map<R_key_value<K,Z>> result(std::move(accs));
    return result;
  }

  // TODO optimize copies. lots of record building here.
  template <class Fun>
  auto ext(Fun expand) const -> Map < typename RT<Fun, R>::ElemType >  {
    typedef typename RT<Fun, R>::ElemType T;
    Map<T> result;
    for (const auto& it : container) {
      for (auto& it2 : expand(R{it.first, it.second}).container) {
        result.insert(R{std::move(it.first), std::move(it.second)});
      }
    }

    return result;
  }

  int size(unit_t) const { return container.size(); }

  // lookup ignores the value of the argument
  shared_ptr<R> lookup(const R& r) const {
      auto it = container.find(r.key);
      if (it != container.end()) {
        return std::make_shared<R>(it->first, it->second );
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

  bool operator>(const Map& other) const {
    return container > other.container;
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

template <class Elem>
class Vector: public VectorDS<K3::Vector, Elem> {
  using Super = VectorDS<K3::Vector, Elem>;

 public:
  Vector(): Super() {}
  Vector(const Super& c): Super(c) {}
  Vector(Super&& c): Super(std::move(c)) {}

  // TODO bounds checking
  Elem at(int i) const {
    auto& vec = Super::getConstContainer();
    return vec[i];
  }

  // TODO bounds checking
  unit_t set(int i, Elem f) {
    auto& vec = Super::getContainer();
    vec[i] = f;
    return unit_t();
  }

  unit_t inPlaceAdd(const Vector<Elem>& other) {
    auto& vec = Super::getContainer();
    auto& other_vec = other.getConstContainer();
    if (vec.size() != other_vec.size()) {
      throw std::runtime_error("Vector inPlaceAdd size mismatch");
    }
    for (int i =0; i < vec.size(); i++) {
      vec[i].elem += other_vec[i].elem;
    }

    return unit_t {};
  }

  Vector<Elem> add(const Vector<Elem>& other) const {
    auto copy = Vector<Elem>(*this);
    copy.inPlaceAdd(other);
    return copy;
  }

  unit_t inPlaceSub(const Vector<Elem>& other) {
    auto& vec = Super::getContainer();
    auto& other_vec = other.getConstContainer();
    if (vec.size() != other_vec.size()) {
      throw std::runtime_error("Vector inPlaceSub size mismatch");
    }
    for (int i =0; i < vec.size(); i++) {
      vec[i].elem -= other_vec[i].elem;
    }
  }

  Vector<Elem> sub(const Vector<Elem>& other) const {
    auto copy = Vector<Elem>(*this);
    copy.inPlaceSub(other);
    return copy;
  }

  double dot(const Vector<Elem>& other) const {
    auto& vec = Super::getConstContainer();
    auto& other_vec = other.getConstContainer();
    if (vec.size() != other_vec.size()) {
      throw std::runtime_error("Vector dot size mismatch");
    }

    double d = 0;
    for(int i = 0; i < vec.size(); i++) {
      d += vec[i].elem + other_vec[i].elem;
    }
    return d;
  }

  double distance(const Vector<Elem>& other) const {
    double d = 0;
    auto& vec = Super::getConstContainer();
    auto& other_vec = other.getConstContainer();
    if (vec.size() != other_vec.size()) {
      throw std::runtime_error("Vector squareDistance size mismatch");
    }
    for(int i = 0; i < vec.size(); i++) {
      d += pow(vec[i].elem - other_vec[i].elem, 2);
    }
    return sqrt(d);
  }

  string toString(unit_t) const {
    std::ostringstream oss;
    auto& c = Super::getConstContainer();
    int i =0;
    oss << "[";
    for (auto& relem : c) {
      oss << relem.elem;
      i++;
      if (i < c.size()) {
        oss << ",";
      }
    }
    oss << "]";
    return oss.str();
  }

};

template <typename Elem, typename... Indexes>
class MultiIndex {
  public:

  typedef boost::multi_index_container<
    Elem,
    boost::multi_index::indexed_by<
      boost::multi_index::sequenced<>,
      Indexes...
    >
  > Container;

  Container container;

  // Default
  MultiIndex(): container() {}
  MultiIndex(const Container& con): container(con) {}
  MultiIndex(Container&& con): container(std::move(con)) {}

  // Construct from (container) iterators
  template<typename Iterator>
  MultiIndex(Iterator begin, Iterator end): container(begin,end) {}

  // Maybe return the first element in the ds
  shared_ptr<Elem> peek(unit_t) const {
    shared_ptr<Elem> res;
    auto it = container.begin();
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
    auto it = std::find(container.begin(), container.end(), v);
    if (it != container.end()) {
      container.erase(it);
    }
    return unit_t();
  }

  // Update by move
  // Find v in the container. Insert (by move) v2 in its position. Erase v.
  unit_t update(const Elem& v, const Elem&& v2) {
    auto it = std::find(container.begin(), container.end(), v);
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
  MultiIndex<Elem, Indexes...> combine(const MultiIndex& other) const {
    // copy this DS
    MultiIndex<Elem, Indexes...> result;
    result = MultiIndex(*this);
    // copy other DS
    for (const Elem &e : other.container) {
      result.insert(e);
    }
    return result;
  }

  // Split the ds at its midpoint
  tuple<MultiIndex<Elem, Indexes...>, MultiIndex<Elem, Indexes...>> split(const unit_t&) const {
    // Find midpoint
    const size_t size = container.size();
    const size_t half = size / 2;
    // Setup iterators
    auto  beg = container.begin();
    auto mid = container.begin();
    std::advance(mid, half);
    auto end = container.end();
    // Construct from iterators
    return std::make_tuple(MultiIndex(beg,mid), MultiIndex(mid,end));
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
  auto map(Fun f) -> MultiIndex<R_elem<RT<Fun, Elem>>> const {
    MultiIndex<R_elem<RT<Fun, Elem>>> result;
    for (const Elem &e : container) {
      result.insert( R_elem<RT<Fun, Elem>>{ f(e) } ); // Copies e (f is pass by value), then move inserts
    }
    return result;
  }

  // Create a new DS consisting of elements from this ds that satisfy the predicate
  template<typename Fun>
  MultiIndex<Elem, Indexes...> filter(Fun predicate) const {
    MultiIndex<Elem, Indexes...> result;
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
  MultiIndex<R_key_value<RT<F1, Elem>,Z>> groupBy(F1 grouper, F2 folder, const Z& init) const {
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
      MultiIndex<R_key_value<RT<F1, Elem>,Z>> result;
      for(const auto& it : accs) {
        result.insert(R_key_value<K, Z>{std::move(it.first), std::move(it.second)});

      }

      return result;
  }

  template <class Fun>
  auto ext(Fun expand) -> MultiIndex<typename RT<Fun, Elem>::ElemType> const {
    typedef typename RT<Fun, Elem>::ElemType T;
    MultiIndex<T> result;
    for (const Elem& elem : container) {
      for (T& elem2 : expand(elem).container) {
        result.insert(std::move(elem2));
      }
    }

    return result;
  }

  bool operator==(const MultiIndex& other) const {
    return container == other.container;
  }

  bool operator!=(const MultiIndex& other) const {
    return container != other.container;
  }

  bool operator<(const MultiIndex& other) const {
    return container < other.container;
  }

  bool operator>(const MultiIndex& other) const {
    return container > other.container;
  }

  template <class Index, class Key>
  shared_ptr<Elem> lookup_with_index(const Index& index, Key key) const {
    const auto& it = index.find(key);

    shared_ptr<Elem> result;
    if (it != index.end()) {
      result = make_shared<Elem>(*it);

    }
    return result;
  }

  template <class Index, class Key>
  MultiIndex<Elem, Indexes...> slice_with_index(const Index& index, Key a, Key b) const {
    MultiIndex<Elem, Indexes...> result;
    std::pair<typename Index::iterator, typename Index::iterator> p = index.range(a <= boost::lambda::_1, b >= boost::lambda::_1);
    for (typename Index::iterator it = p.first; it != p.second; it++) {
      result.insert(*it);

    }

    return result;
  }

  Container& getContainer() { return container; }

  // Return a constant reference to the container
  const Container& getConstContainer() const {return container;}

 private:
  friend class boost::serialization::access;
  template<class Archive>
  void serialize(Archive &ar, const unsigned int version) {
    ar & container;
  }

};


} // Namespace K3

template <class K3Collection>
std::size_t hash_collection(K3Collection const& b) {
  const auto& c = b.getConstContainer();
  return boost::hash_range(c.begin(), c.end());

}

template <template<typename> class Derived, template<typename...> class Container, class Elem>
std::size_t hash_value(K3::StlDS<Derived, Container,Elem> const& b) {
  return hash_collection(b);
}

template <class Elem>
std::size_t hash_value(K3::Map<Elem> const& b) {
  return hash_collection(b);
}


template <class Elem>
std::size_t hash_value(K3::Vector<Elem> const& b) {
  return hash_collection(b);
}

template <typename... Args>
std::size_t hash_value(K3::MultiIndex<Args...> const& b) {
  return hash_collection(b);
}

#endif
