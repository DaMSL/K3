// The K3 Runtime Collections Library.
//
// This file contains definitions for the various operations performed on K3 collections, used by
// the generated C++ code. The C++ realizations of K3 Collections will inherit from the
// K3::Collection class, providing a suitable content type.
//
// TODO:
//  - Use <algorithm> routines to perform collection transformations? In particular, investigate
//    order-of-operations semantics.
//  - Use container agnostic mutation operations?
//  - Use bulk mutation operations (iterator-based insertion, for example)?
//  - Optimize all the unnecessary copies, using std::move?
#ifndef K3_RUNTIME_COLLECTIONS_H
#define K3_RUNTIME_COLLECTIONS_H

#include <functional>
#include <list>
#include <map>
#include <memory>
#include <tuple>

#include <Engine.hpp>
#include <dataspace/StlDS.hpp>
#include <dataspace/SetDS.hpp>
#include <dataspace/ListDS.hpp>
#include <dataspace/SortedDS.hpp>
#include <boost/serialization/base_object.hpp>

namespace K3 {
  template <template <class...> class D, class E>
  class Collection: public D<E> {
    public:
      Collection(Engine * e) : D<E>(e) {};

      template <template <class> class F>
      Collection(const Collection<F, E> other) : D<E>(other)  {}

      template <template <class> class F>
      Collection(Collection<F, E>&& other) : D<E>(other) {}

      Collection(D<E> other) : D<E>(other) {}

      // TODO: shared_ptr vs. value?
      std::shared_ptr<E> peek() { return D<E>::peek(); }

      void insert(const E& elem) { D<E>::insert(elem); }
      void insert(E&& elem) { D<E>::insert(elem); }

      void erase(const E& elem)  { D<E>::erase(elem); }

      void update(const E& v1, const E& v2) { D<E>::update(v1,v2); }
      void update(const E& v1, E&& v2) { D<E>::update(v1,v2); }

      std::tuple<Collection<D, E>, Collection<D, E>> split() {
        auto tup = D<E>::split();
        D<E> ds1 = get<0>(tup);
        D<E> ds2 = get<1>(tup);
        return std::make_tuple(Collection<D,E>(ds1), Collection<D,E>(ds2));
      }

      template <template <class> class F>
      Collection<D, E> combine(const Collection<F, E>& other) {
       return Collection<D,E>(D<E>::combine(other));
      }

      template <class T>
      Collection<D, T> map(std::function<T(E)> f) {
       return Collection<D,T>(D<E>::map(f));
      }

      Collection<D, E> filter(std::function<bool(E)> f) {
       return Collection<D,E>(D<E>::filter(f));
      }
   
      template <class Z>
      Z fold(std::function<Z(Z, E)> f, Z init) {
       return D<E>::fold(f, init);
      }

      template <class K, class Z>
      Collection<D, std::tuple<K, Z>> group_by(
        std::function<K(E)> grouper, std::function<Z(Z, E)> folder, Z init) {
          // Create a map to hold partial results
          std::map<K, Z> accs = std::map<K,Z>();
          // lambda to apply to each element
          std::function<void(E)> f = [&] (E elem) {
            K key = grouper(elem);
            if (accs.find(key) == accs.end()) {
              accs[key] = init;
            }
            accs[key] = folder(accs[key], elem);
          };
          D<E>::iterate(f);
          // Build Collection result
          Collection<D, std::tuple<K,Z>> result = Collection<D,std::tuple<K,Z>>(D<E>::getEngine());
          typename std::map<K,Z>::iterator it;
          for (it = accs.begin(); it != accs.end(); ++it) {
            std::tuple<K,Z> tup = std::make_tuple(it->first, it->second);
            result.insert(tup);
          }
         return result;
      }

      template <template <class> class F, class T>
      Collection<D, T> ext(std::function<Collection<F, T>(E)> expand) {
        Collection<D, T> result = Collection<D,T>(D<E>::getEngine());
        auto add_to_result = [&] (T elem) {result.insert(elem); };
        auto fun = [&] (E elem) {
          expand(elem).iterate(add_to_result);
        };
        D<E>::iterate(fun);
        return result;
      }

  private:
    friend class boost::serialization::access;
    // Serialize a collection by serializing its base-class (a dataspace)
    template<class Archive>
    void serialize(Archive &ar, const unsigned int version) {
      ar & boost::serialization::base_object<D<E>>(*this);
    }

  };

  template <typename E>
  // TODO: make this more generic? ie a SeqDS interface
  class Seq : public Collection<ListDS, E> {
    typedef Collection<ListDS, E> super;
     public:
      Seq(Engine * e) : super(e) {};

      Seq(const Seq<E>& other) : super(other)  {}

      Seq(super other) : super(other) {}

      // Convert from Collection<ListDS, E> to Seq<E>
      std::tuple<Seq<E>, Seq<E>> split() {
        auto tup = super::split();
        super ds1 = get<0>(tup);
        super ds2 = get<1>(tup);
        return std::make_tuple(Seq<E>(ds1), Seq<E>(ds2));
      }

      Seq<E> combine(const Seq<E>& other) {
       return Seq<E>(super::combine(other));
      }

      template <class T>
      Seq<T> map(std::function<T(E)> f) {
       return Seq<T>(super::map(f));
      }

      Seq<E> filter(std::function<bool(E)> f) {
       return Seq<E>(super::filter(f));
      }

      Seq<E> sort(std::function<int(E,E)> f) {
        super s = super(ListDS<E>::sort(f));
        return Seq<E>(s);

      }

      template <class K, class Z>
      Seq<std::tuple<K, Z>> group_by 
      (std::function<K(E)> grouper, std::function<Z(Z, E)> folder, Z init) {
        Collection<ListDS, std::tuple<K,Z>> s = super::group_by(grouper,folder,init);
        return Seq<std::tuple<K,Z>>(s);
      }

      template <class T>
      Seq<T> ext(std::function<Collection<ListDS, T>(E)> expand) {
        Collection<ListDS, T> result = super::ext(expand);
        return Seq<T>(result);
      }
  };

  template <typename E>
  // TODO: make this more generic? ie a SetDS interface
  class Set : public Collection<SetDS, E> {
    typedef Collection<SetDS, E> super;
    typedef SetDS<E> dataspace;
     public:
      Set(Engine * e) : super(e) {};

      Set(const Set<E>& other) : super(other)  {}

      Set(super other) : super(other) {}

      // Convert from Collection<ListDS, E> to Set<E>
      std::tuple<Set<E>, Set<E>> split() {
        auto tup = super::split();
        super ds1 = get<0>(tup);
        super ds2 = get<1>(tup);
        return std::make_tuple(Set<E>(ds1), Set<E>(ds2));
      }

      Set<E> combine(const Set<E>& other) {
       return Set<E>(super::combine(other));
      }

      template <class T>
      Set<T> map(std::function<T(E)> f) {
       return Set<T>(super::map(f));
      }

      Set<E> filter(std::function<bool(E)> f) {
       return Set<E>(super::filter(f));
      }

      template <class K, class Z>
      Set<std::tuple<K, Z>> group_by 
      (std::function<K(E)> grouper, std::function<Z(Z, E)> folder, Z init) {
        Collection<SetDS, std::tuple<K,Z>> s = super::group_by(grouper,folder,init);
        return Set<std::tuple<K,Z>>(s);
      }

      template <class T>
      Set<T> ext(std::function<Collection<SetDS, T>(E)> expand) {
        Collection<SetDS, T> result = super::ext(expand);
        return Set<T>(result);
      }

      bool member(E e) {
        return dataspace::member(e);
      }

      bool isSubsetOf(SetDS<E> other) {
        return dataspace::isSubsetOf(other);
      }

      // TODO union is a reserved word
      Set union1(Set<E> other) {
        super s = super(dataspace::union1(other));
        return Set<E>(s);
      }

      Set intersect(Set<E> other) {
        super s = super(dataspace::intersect(other));
        return Set<E>(s);
      }

      Set difference(Set<E> other) {
        super s = super(dataspace::difference(other));
        return Set<E>(s);
      }

  };

  template <typename E>
  class Sorted : public Collection<SortedDS, E> {
    typedef Collection<SortedDS, E> super;
    typedef SortedDS<E> dataspace;
     public:
      Sorted(Engine * e) : super(e) {};

      Sorted(const Sorted<E>& other) : super(other)  {}

      Sorted(super other) : super(other) {}

      // Convert from Collection<ListDS, E> to Sorted<E>
      std::tuple<Sorted<E>, Sorted<E>> split() {
        auto tup = super::split();
        super ds1 = get<0>(tup);
        super ds2 = get<1>(tup);
        return std::make_tuple(Sorted<E>(ds1), Sorted<E>(ds2));
      }

      Sorted<E> combine(const Sorted<E>& other) {
       return Sorted<E>(super::combine(other));
      }

      template <class T>
      Sorted<T> map(std::function<T(E)> f) {
       return Sorted<T>(super::map(f));
      }

      Sorted<E> filter(std::function<bool(E)> f) {
       return Sorted<E>(super::filter(f));
      }

      template <class K, class Z>
      Sorted<std::tuple<K, Z>> group_by 
      (std::function<K(E)> grouper, std::function<Z(Z, E)> folder, Z init) {
        Collection<SortedDS, std::tuple<K,Z>> s = super::group_by(grouper,folder,init);
        return Sorted<std::tuple<K,Z>>(s);
      }

      template <class T>
      Sorted<T> ext(std::function<Collection<SortedDS, T>(E)> expand) {
        Collection<SortedDS, T> result = super::ext(expand);
        return Sorted<T>(result);
      }

      shared_ptr<E> min() { return dataspace::min(); }

      shared_ptr<E> max() { return dataspace::max(); }

      shared_ptr<E> lowerBound(E a) { return dataspace::lowerBound(a); }

      shared_ptr<E> upperBound(E a) { return dataspace::lowerBound(a); }

      Sorted<E> slice(E a, E b) {
        super s = super(dataspace::slice(a,b));
        return Sorted<E>(s);
      }

  };
}

#endif
