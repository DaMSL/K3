#include <boost/tr1/unordered_set.hpp>
#include <boost/tr1/unordered_map.hpp>
#include "boost/serialization/vector.hpp"
#include "boost/serialization/set.hpp"
#include "boost/serialization/list.hpp"
#include "external/boost_ext/unordered_set.hpp"
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

template<class V>
using unordered_set = std::tr1::unordered_set<V>;

// Forward declaration of StlDS
template <template<typename...> class StlContainer, class Elem>
class StlDS;

// Various DS's achieved through typedefs
template <class Elem>
using ListDS = StlDS<std::list, Elem>;

template <class Elem>
using SetDS = StlDS<unordered_set, Elem>;

template <class Elem>
using SortedDS = StlDS<std::multiset, Elem>;

template <class Elem>
using VectorDS = StlDS<std::vector, Elem>;

// MapDS implementation. For now, map() returns a VectorDS, requiring VectorDS be defined before MapDS
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
  VectorDS<R_elem<NewR>> map(const F<NewR(R)>& f) {
    VectorDS<R_elem<NewR>> result;
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
  VectorDS<T> ext(const F<VectorDS<T>(R)>& expand) {
    VectorDS<T> result;
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
    VectorDS<R_elem<NewElem>> map(F<NewElem(Elem)> f) {
      VectorDS<R_elem<NewElem>> result;
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
    VectorDS<R_elem<T>> ext(const F<VectorDS<T>(Elem)>& expand) {
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

// Typdefs for Collections
// These are to support legacy code. The generated annotation combination classes (e.g _Collection)
// should inherit directly from an appropriate dataspace
template <class Elem>
using Collection = VectorDS<Elem>;

template <class Elem>
using Set = SetDS<Elem>;

template <class Elem>
using Seq = ListDS<Elem>;

template <class Elem>
using Sorted = SortedDS<Elem>;

template <class Elem>
using Map = MapDS<Elem>;

} // Namespace K3
