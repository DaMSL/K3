#include <boost/tr1/unordered_set.hpp>
#include <boost/tr1/unordered_map.hpp>
#include "boost/serialization/vector.hpp"
#include "boost/serialization/set.hpp"
#include "boost/serialization/list.hpp"
#include "external/boost_ext/unordered_map.hpp"
#include <boost/serialization/base_object.hpp>

#include <set>
#include <list>


namespace K3 {

// TODO convert functions to curried form where necessary
using std::shared_ptr;
using std::tuple;

// Utility Typedefs

template<class K, class V>
using unordered_map = std::tr1::unordered_map<K,V>;

// Forward declaration of StlDS
template <template<typename...> class StlContainer, class Elem>
class StlDS;

// Various DS's achieved through typedefs
template <class Elem>
using ListDS = StlDS<std::list, Elem>;

template <class Elem>
using SetDS = StlDS<std::set, Elem>;

template <class Elem>
using SortedDS = StlDS<std::multiset, Elem>;

template <class Elem>
using VectorDS = StlDS<std::vector, Elem>;

// MapDS implementation. For now, map() returns a SetDS, requiring SetDS be defined before MapDS
template<class R>
class MapDS {

  using Key = typename R::KeyType;
  using Value = typename R::ValueType;
  using iterator_type = typename unordered_map<Key,Value>::iterator;
  using const_iterator_type = typename unordered_map<Key, Value>::const_iterator;

 public:
  // Default Constructor
  MapDS()
    : container()
  {  }

  // Copy Constructor
  MapDS(const MapDS& other)
    : container(other.container)
  { }
  // Move Constructor
  MapDS(MapDS&& other)
    : container(std::move(other.container))
  { }
  // Copy Assign Operator
  MapDS& operator=(const MapDS& other) {
    container = other.container;
    return *this;
  }

  // Move Assign Operator
  MapDS& operator=(MapDS&& other) {
    container = std::move(other.container);
    //other.container = NULL;
    return *this;
  }

  // Construct from (container) iterators
  template<typename Iterator>
  MapDS(Iterator begin, Iterator end)
    : container(begin,end)
  {}

  // TODO: Destructor
  ~MapDS() {

  }

  // Copy Constructor from container
  MapDS(const unordered_map<Key,Value>& con)
    : container(con)
  { }
  // Move Constructor from container
  MapDS(unordered_map<Key, Value>&& con)
    : container(std::move(con))
  { }
  // DS Operations:
  // Maybe return the first element in the DS
  shared_ptr<R> peek(unit_t) const {
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

  // TODO: try to eliminate extra copy
  template<typename Acc>
  Acc fold(F<F<Acc(R)>(Acc)> f, const Acc& init_acc) {
    Acc acc = init_acc;
    for (const std::pair<Key, Value>& p : container) {
      R rec;
      rec.key = p.first;
      rec.value = p.second;
      acc = f(acc)(rec);
    }
    return acc;
  }

  template<typename NewR>
  SetDS<R_elem<NewR>> map(const F<NewR(R)>& f) {
    SetDS<R_elem<NewR>> result;
    for (const std::pair<Key,Value>& p : container) {
      R rec {p.first, p.second};
      result.insert( R_elem<NewR>{ f(rec) } );
    }
    return result;
  }

  unit_t iterate(F<unit_t(R)> f) {
    for (const std::pair<Key,Value>& p : container) {
      R rec;
      rec.key = p.first;
      rec.value = p.second;
      f(rec);
    }
    return unit_t();
  }

  MapDS<R> filter(const F<bool(R)>& predicate) {
    MapDS<R> result;
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

  tuple<MapDS, MapDS> split() {
    // Find midpoint
    const size_t size = container.size();
    const size_t half = size / 2;
    // Setup iterators
    const_iterator_type beg = container.begin();
    const_iterator_type mid = container.begin();
    std::advance(mid, half);
    const_iterator_type end = container.end();
    // Construct DS from iterators
    return std::make_tuple(MapDS(beg, mid),MapDS(mid, end));
  }

  MapDS combine(const MapDS& other) const {
    // copy this DS
    MapDS result = MapDS(*this);
    // copy other DS
    for (const std::pair<Key,Value>& p: other.container) {
      result.container[p.first] = p.second;
    }
    return result;
  }

  template <class K, class Z>
  F<F<MapDS<R_key_value<K,Z>>(Z)>(F<F<Z(R)>(Z)>)> groupBy(F<K(R)> grouper) {
    F<F<MapDS<R_key_value<K,Z>>(Z)>(F<F<Z(R)>(Z)>)> r = [=] (F<F<Z(R)>(Z)> folder) {
      F<MapDS<R_key_value<K,Z>>(Z)> r2 = [=] (const Z& init) {
          // Create a map to hold partial results
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
         MapDS<R_key_value<K,Z>> result(std::move(accs));
         return result;
      };
      return r2;
    };
    return r;
  }

  // TODO optimize copies
  template <class T>
  SetDS<T> ext(const F<SetDS<T>(R)>& expand) {
    SetDS<T> result;
    for (const R& elem : container) {
      for (const R& elem2 : expand(elem).container) {
        result.insert(elem2);
      }
    }


    return result;
  }

  int size(unit_t) const { return container.size(); }

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

}; // class MapDS

template <template<typename...> class StlContainer, class Elem>
class StlDS {
  typedef StlContainer<Elem> Container;
  typedef typename Container::const_iterator const_iterator_type;
  typedef typename Container::iterator iterator_type;
  public:
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

    // Copy Assign Operator
    StlDS& operator=(const StlDS& other) {
      container = other.container;
      return *this;
    }

    // Move Assign Operator
    StlDS& operator=(StlDS&& other) {
      container = std::move(other.container);
      //other.container = NULL;
      return *this;
    }

    // Construct from (container) iterators
    template<typename Iterator>
    StlDS(Iterator begin, Iterator end)
      : container(begin,end)
    {}

    // TODO: Destructor
    ~StlDS() {

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

    // Generic implementation:
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

    // Fold a function over this ds
    template<typename Acc>
    Acc fold(F<F<Acc (Elem)> (Acc)> f, const Acc& init_acc) {
      Acc acc = init_acc;
      for (const Elem &e : container) { acc = f(acc)(e); }
      return acc;
    }

    // Produce a new ds by mapping a function over this ds
    template<typename NewElem>
    SetDS<R_elem<NewElem>> map(F<NewElem(Elem)> f) {
      SetDS<R_elem<NewElem>> result;
      for (const Elem &e : container) {
        result.insert( R_elem<NewElem>{ f(e) } ); // Copies e (f is pass by value), then move inserts
      }
      return result;
    }

    // Apply a function to each element of this ds
    unit_t iterate(const F<unit_t(Elem)>& f) {
      for (const Elem& e : container) {
        f(e);
      }
      return unit_t();
    }

    // Create a new DS consisting of elements from this ds that satisfy the predicate
    StlDS filter(const F<bool(Elem)>& predicate) {
      StlDS<StlContainer, Elem> result;
      for (const Elem &e : container) {
        if (predicate(e)) {
          result.insert(e);
        }
      }
      return result;
    }

    // Split the ds at its midpoint
    tuple<StlDS, StlDS> split(unit_t) const {
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

    // Return a new DS with data from this and other
    // TODO Try to optimize this:
    StlDS combine(const StlDS& other) const {
      // copy this DS
      StlDS result = StlDS(*this);
      // copy other DS
      for (const Elem &e : other.container) {
        result.insert(e);
      }
      return result;
    }

    template <class K, class Z>
    F<F<MapDS<R_key_value<K,Z>>(Z)>(F<F<Z(Elem)>(Z)>)> groupBy(F<K(Elem)> grouper) {
      F<F<MapDS<R_key_value<K,Z>>(Z)>(F<F<Z(Elem)>(Z)>)> r = [=] (F<F<Z(Elem)>(Z)> folder) {
        F<MapDS<R_key_value<K,Z>>(Z)> r2 = [=] (const Z& init) {
            // Create a map to hold partial results
           unordered_map<K, Z> accs;

           for (const Elem& elem : container) {
              K key = grouper(elem);
              if (accs.find(key) == accs.end()) {
                accs[key] = init;
              }
              accs[key] = folder(accs[key])(elem);
           }

           // Force a move from accs into a mapDS
           // Careful! dont reference accs again!
           MapDS<R_key_value<K,Z>> result(std::move(accs));
           return result;
        };
        return r2;
      };
      return r;
    }

    // TODO optimize copies
    template <class T>
    SetDS<R_elem<T>> ext(const F<SetDS<T>(Elem)>& expand) {
      StlDS<StlContainer, R_elem<T>> result;
      for (const Elem& elem : container) {
        StlDS<StlContainer, T> expanded = expand(elem);
        for (const Elem& elem2 : expanded.getConstContainer()) {
          result.insert(R_elem<T> { elem2 } );
        }
      }


      return result;
    }


    int size(unit_t) const { return container.size(); }

    Container& getContainer() { return container; }

    const Container& getConstContainer() const { return container; }

  protected:
    Container container;

private:
  friend class boost::serialization::access;
  template<class Archive>
  void serialize(Archive &ar, const unsigned int version) {
    ar & container;
  }

}; // class StlDS

// Forward declare Collection
template <class Elem> class Collection;

template <class Elem>
class Map : public MapDS<Elem> {
  typedef MapDS<Elem> Super;
  public:
  // Constructors
  Map() : MapDS<Elem>() { }

  Map(const Map& c): MapDS<Elem>(c) { }

  Map(Map&& c): MapDS<Elem>(std::move(c)) { }

    // Assign operators
  Map& operator=(const Map& other) {
    MapDS<Elem>::operator=(other);
    return *this;
  }

  Map& operator=(Map&& other) {
    MapDS<Elem>::operator=(std::move(other));
    return *this;
  }

  // Superclass constructors
  Map(const MapDS<Elem>& c): MapDS<Elem>(c) { }

  Map(MapDS<Elem>&& c): MapDS<Elem>(std::move(c)) { }

  // TODO: Map Specific functions (subMap, member, union, etc)

  // Overrides (convert from DS to Collection)
  Map filter(const F<bool(Elem)>& predicate) {
    return Map<Elem>(Super::filter(predicate));
  }

  // TODO: copies?
  tuple<Map, Map> split(unit_t) const {
    auto t = Super::split(unit_t());
    return std::make_tuple(Map<Elem>(std::get<0>(t)), Map<Elem>(std::get<1>(t)));
  }

  Map combine(const Map& other) const {
    return Map<Elem>(Super::combine(other));
  }

  template <class K, class Z>
  F<F<Map<R_key_value<K,Z>>(Z)>(F<F<Z(Elem)>(Z)>)> groupBy(F<K(Elem)> grouper) {
    return [=] (F<F<Z(Elem)>(Z)> folder) {
     return [=] (const Z& init) {
        return Map<R_key_value<K,Z>>(Super::template groupBy<K,Z>(grouper)(folder)(init));
      };
    };
  }

  template<typename NewElem>
  Collection<R_elem<NewElem>> map(F<NewElem(Elem)> f) {
    return Collection<Elem>(Super::map(f));
  }

  template <class T>
  Collection<R_elem<T>> ext(const F<Collection<T>(Elem)>& expand) {
    return Collection<R_elem<T>>(Super::ext(expand));
  }
};



// Typdefs for Collections
// These are to support legacy code. The generated annotation combination classes (e.g _Collection)
// should inherit directly from an appropriate dataspace
template <class Elem>
class Collection : public VectorDS<Elem> {
  typedef VectorDS<Elem> Super;
  public:
  // Constructors
  Collection() : VectorDS<Elem>() { }

  Collection(const Collection& c): VectorDS<Elem>(c) { }

  Collection(Collection&& c): VectorDS<Elem>(std::move(c)) { }

    // Assign operators
  Collection& operator=(const Collection& other) {
    VectorDS<Elem>::operator=(other);
    return *this;
  }

  Collection& operator=(Collection&& other) {
    VectorDS<Elem>::operator=(std::move(other));
    return *this;
  }

  // Superclass constructors
  Collection(const VectorDS<Elem>& c): VectorDS<Elem>(c) { }

  Collection(VectorDS<Elem>&& c): VectorDS<Elem>(std::move(c)) { }

  // Overrides (convert from DS to Collection)
  template<typename NewElem>
  Collection<R_elem<NewElem>> map(F<NewElem(Elem)> f) {
    return Collection<Elem>(Super::map(f));
  }

  Collection filter(const F<bool(Elem)>& predicate) {
    return Collection<Elem>(Super::filter(predicate));
  }

  // TODO: copies?
  tuple<Collection, Collection> split(unit_t) const {
    auto t = Super::split(unit_t());
    return std::make_tuple(Collection<Elem>(std::get<0>(t)), Collection<Elem>(std::get<1>(t)));
  }

  Collection combine(const Collection& other) const {
    return Collection<Elem>(Super::combine(other));
  }

  template <class K, class Z>
  F<F<Map<R_key_value<K,Z>>(Z)>(F<F<Z(Elem)>(Z)>)> groupBy(F<K(Elem)> grouper) {
    return [=] (F<F<Z(Elem)>(Z)> folder) {
     return [=] (const Z& init) {
        return Map<R_key_value<K,Z>>(Super::template groupBy<K,Z>(grouper)(folder)(init));
      };
    };
  }

  template <class T>
  Collection<R_elem<T>> ext(const F<Collection<T>(Elem)>& expand) {
    return Collection<R_elem<T>>(Super::ext(expand));
  }
};

template <class Elem>
class Set : public SetDS<Elem> {
  typedef SetDS<Elem> Super;
  public:
  // Constructors
  Set() : SetDS<Elem>() { }

  Set(const Set& c): SetDS<Elem>(c) { }

  Set(Set&& c): SetDS<Elem>(std::move(c)) { }

    // Assign operators
  Set& operator=(const Set& other) {
    SetDS<Elem>::operator=(other);
    return *this;
  }

  Set& operator=(Set&& other) {
    SetDS<Elem>::operator=(std::move(other));
    return *this;
  }

  // Superclass constructors
  Set(const SetDS<Elem>& c): SetDS<Elem>(c) { }

  Set(SetDS<Elem>&& c): SetDS<Elem>(std::move(c)) { }

  // TODO: Set Specific functions (subset, member, union, etc)

  // Overrides (convert from DS to Collection)
  Set filter(const F<bool(Elem)>& predicate) {
    return Set<Elem>(Super::filter(predicate));
  }

  // TODO: copies?
  tuple<Set, Set> split(unit_t) const {
    auto t = Super::split(unit_t());
    return std::make_tuple(Set<Elem>(std::get<0>(t)), Set<Elem>(std::get<1>(t)));
  }

  Set combine(const Set& other) const {
    return Set<Elem>(Super::combine(other));
  }

  template <class K, class Z>
  F<F<Map<R_key_value<K,Z>>(Z)>(F<F<Z(Elem)>(Z)>)> groupBy(F<K(Elem)> grouper) {
    return [=] (F<F<Z(Elem)>(Z)> folder) {
     return [=] (const Z& init) {
        return Map<R_key_value<K,Z>>(Super::template groupBy<K,Z>(grouper)(folder)(init));
      };
    };
  }

  template<typename NewElem>
  Collection<R_elem<NewElem>> map(F<NewElem(Elem)> f) {
    return Collection<Elem>(Super::map(f));
  }

  template <class T>
  Collection<R_elem<T>> ext(const F<Collection<T>(Elem)>& expand) {
    return Collection<R_elem<T>>(Super::ext(expand));
  }
};

template <class Elem>
class Seq : public ListDS<Elem> {
  typedef ListDS<Elem> Super;
  public:
  // Constructors
  Seq() : ListDS<Elem>() { }

  Seq(const Seq& c): ListDS<Elem>(c) { }

  Seq(Seq&& c): ListDS<Elem>(std::move(c)) { }

    // Assign operators
  Seq& operator=(const Seq& other) {
    ListDS<Elem>::operator=(other);
    return *this;
  }

  Seq& operator=(Seq&& other) {
    ListDS<Elem>::operator=(std::move(other));
    return *this;
  }

  // Superclass constructors
  Seq(const ListDS<Elem>& c): ListDS<Elem>(c) { }

  Seq(ListDS<Elem>&& c): ListDS<Elem>(std::move(c)) { }

  // Seq specific functions (at,...)

  // Overrides (convert from DS to Collection)

  Seq filter(const F<bool(Elem)>& predicate) {
    return Seq<Elem>(Super::filter(predicate));
  }

  // TODO: copies?
  tuple<Seq, Seq> split(unit_t) const {
    auto t = Super::split(unit_t());
    return std::make_tuple(Seq<Elem>(std::get<0>(t)), Seq<Elem>(std::get<1>(t)));
  }

  Seq combine(const Seq& other) const {
    return Seq<Elem>(Super::combine(other));
  }

  template <class K, class Z>
  F<F<Map<R_key_value<K,Z>>(Z)>(F<F<Z(Elem)>(Z)>)> groupBy(F<K(Elem)> grouper) {
    return [=] (F<F<Z(Elem)>(Z)> folder) {
     return [=] (const Z& init) {
        return Map<R_key_value<K,Z>>(Super::template groupBy<K,Z>(grouper)(folder)(init));
      };
    };
  }

  template<typename NewElem>
  Collection<R_elem<NewElem>> map(F<NewElem(Elem)> f) {
    return Collection<Elem>(Super::map(f));
  }

  template <class T>
  Collection<R_elem<T>> ext(const F<Collection<T>(Elem)>& expand) {
    return Collection<R_elem<T>>(Super::ext(expand));
  }
};

template <class Elem>
class Sorted : public SortedDS<Elem> {
  typedef SortedDS<Elem> Super;
  public:
  // Constructors
  Sorted() : SortedDS<Elem>() { }

  Sorted(const Sorted& c): SortedDS<Elem>(c) { }

  Sorted(Sorted&& c): SortedDS<Elem>(std::move(c)) { }

    // Assign operators
  Sorted& operator=(const Sorted& other) {
    SortedDS<Elem>::operator=(other);
    return *this;
  }

  Sorted& operator=(Sorted&& other) {
    SortedDS<Elem>::operator=(std::move(other));
    return *this;
  }

  // Superclass constructors
  Sorted(const SortedDS<Elem>& c): SortedDS<Elem>(c) { }

  Sorted(SortedDS<Elem>&& c): SortedDS<Elem>(std::move(c)) { }

  // Sorted specific functions (at,...)

  // Overrides (convert from DS to Collection)

  Sorted filter(const F<bool(Elem)>& predicate) {
    return Sorted<Elem>(Super::filter(predicate));
  }

  // TODO: copies?
  tuple<Sorted, Sorted> split(unit_t) const {
    auto t = Super::split(unit_t());
    return std::make_tuple(Sorted<Elem>(std::get<0>(t)), Sorted<Elem>(std::get<1>(t)));
  }

  Sorted combine(const Sorted& other) const {
    return Sorted<Elem>(Super::combine(other));
  }

  template <class K, class Z>
  F<F<Map<R_key_value<K,Z>>(Z)>(F<F<Z(Elem)>(Z)>)> groupBy(F<K(Elem)> grouper) {
    return [=] (F<F<Z(Elem)>(Z)> folder) {
     return [=] (const Z& init) {
        return Map<R_key_value<K,Z>>(Super::template groupBy<K,Z>(grouper)(folder)(init));
      };
    };
  }

  template<typename NewElem>
  Collection<R_elem<NewElem>> map(F<NewElem(Elem)> f) {
    return Collection<Elem>(Super::map(f));
  }

  template <class T>
  Collection<R_elem<T>> ext(const F<Collection<T>(Elem)>& expand) {
    return Collection<R_elem<T>>(Super::ext(expand));
  }
};


} // Namespace K3
