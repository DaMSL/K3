// The K3 Runtime Collections Library.
//
// This file contains definitions for the various operations performed on K3 Collections, used by
// the generated C++ code. The C++ realizations of K3 Collections will inherit from the
// K3::Collection class, providing a suitable content type.
//
// TODO:
//  - Use <algorithm> routines to perform Collection transformations? In particular, investigate
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
#include <dataspace/FileDS.hpp>
#include <dataspace/ListDS.hpp>
#include <dataspace/SetDS.hpp>
#include <dataspace/SortedDS.hpp>
#include <dataspace/StlDS.hpp>
#include <boost/serialization/base_object.hpp>

namespace K3 {

  template <template <class...> class D, class E>
  class BaseCollection: public D<E> {
    public:
      BaseCollection(Engine * e) : D<E>(e) {}

      template <template <class> class F>
      BaseCollection(const BaseCollection<F, E> other) : D<E>(other)  {}

      template <template <class> class F>
      BaseCollection(BaseCollection<F, E>&& other) : D<E>(other) {}

      BaseCollection(D<E> other) : D<E>(other) {}

      template<class Iterator>
      BaseCollection(Engine * e, Iterator start, Iterator finish)
        : D<E>(e, start, finish)
      { }

      std::shared_ptr<E> peek() { return D<E>::peek(); }
      std::shared_ptr<E> peek(unit_t) { return D<E>::peek(); }

      unit_t insert(const E& elem) { D<E>::insert(elem); return unit_t();}
      unit_t insert(E&& elem) { D<E>::insert(elem); return unit_t();}

      unit_t erase(const E& elem)  { D<E>::erase(elem); return unit_t();}

      unit_t update(const E& v1, const E& v2) { D<E>::update(v1,v2); return unit_t();}
      unit_t update(const E& v1, E&& v2) { D<E>::update(v1,v2); return unit_t();}

      std::tuple<BaseCollection<D, E>, BaseCollection<D, E>> split() {
        auto tup = D<E>::split();
        D<E> ds1 = get<0>(tup);
        D<E> ds2 = get<1>(tup);
        return std::make_tuple(BaseCollection<D,E>(ds1), BaseCollection<D,E>(ds2));
      }

      std::tuple<BaseCollection<D, E>, BaseCollection<D, E>> split(unit_t) {
        auto tup = D<E>::split();
        D<E> ds1 = get<0>(tup);
        D<E> ds2 = get<1>(tup);
        return std::make_tuple(BaseCollection<D,E>(ds1), BaseCollection<D,E>(ds2));
      }

      template <template <class> class F>
      BaseCollection<D, E> combine(const BaseCollection<F, E>& other) {
       return BaseCollection<D,E>(D<E>::combine(other));
      }

      template <class T>
      BaseCollection<D, T> map(F<T(E)> f) {
       return BaseCollection<D,T>(D<E>::map(f));
      }

      BaseCollection<D, E> filter(F<bool(E)> f) {
       return BaseCollection<D,E>(D<E>::filter(f));
      }

      template <class Z>
      F<Z(Z)> fold(F<F<Z(E)>(Z)> f) {
           F<Z(Z)> r = [=] (Z init) {
             return D<E>::template fold<Z>(f, init);
           };
           return r;
      }

      template <class K, class Z>
      BaseCollection<D, std::tuple<K, Z>> group_by(
        F<K(E)> grouper, F<F<Z(E)>(E)> folder, Z init) {
          // Create a map to hold partial results
          std::map<K, Z> accs = std::map<K,Z>();
          // lambda to apply to each element
          F<unit_t(E)> f = [&] (E elem) {
            K key = grouper(elem);
            if (accs.find(key) == accs.end()) {
              accs[key] = init;
            }
            accs[key] = folder(accs[key], elem);
            return unit_t();
          };
          D<E>::iterate(f);
          // Build BaseCollection result
          BaseCollection<D, std::tuple<K,Z>> result = BaseCollection<D,std::tuple<K,Z>>(D<E>::getEngine());
          typename std::map<K,Z>::iterator it;
          for (it = accs.begin(); it != accs.end(); ++it) {
            std::tuple<K,Z> tup = std::make_tuple(it->first, it->second);
            result.insert(tup);
          }
         return result;
      }

      template <template <class> class F, class T>

      BaseCollection<D, T> ext(F<BaseCollection<F, T>(E)> expand) {
        BaseCollection<D, T> result = BaseCollection<D,T>(D<E>::getEngine());
        auto add_to_result = [&] (T elem) {result.insert(elem); return unit_t(); };
        auto fun = [&] (E elem) {
          expand(elem).iterate(add_to_result);
        };
        D<E>::iterate(fun);
        return result;
      }

    friend class boost::serialization::access;
    // Serialize a BaseCollection by serializing its base-class (a dataspace)
    template<class Archive>
    void serialize(Archive &ar, const unsigned int version) {
      ar & boost::serialization::base_object<D<E>>(*this);
    }

  };

  template <typename E>
  class Collection : public BaseCollection<ListDS, E> {
    typedef BaseCollection<ListDS, E> Super;
     public:
      Collection(Engine * e) : Super(e) {};

      Collection(const Collection<E>& other) : Super(other)  {}

      Collection(Super other) : Super(other) {}

      template<class Iterator>
      Collection(Engine * e, Iterator start, Iterator finish)
        : Super(e, start, finish)
      { }

      // Convert from BaseCollection<ListDS, E> to Collection<E>
      std::tuple<Collection<E>, Collection<E>> split() {
        auto tup = Super::split();
        Super ds1 = get<0>(tup);
        Super ds2 = get<1>(tup);
        return std::make_tuple(Collection<E>(ds1), Collection<E>(ds2));
      }

      std::tuple<Collection<E>, Collection<E>> split(unit_t) {
        auto tup = Super::split();
        Super ds1 = get<0>(tup);
        Super ds2 = get<1>(tup);
        return std::make_tuple(Collection<E>(ds1), Collection<E>(ds2));
      }

      Collection<E> combine(const Collection<E>& other) {
       return Collection<E>(Super::combine(other));
      }

      template <class T>
      Collection<T> map(F<T(E)> f) {
       return Collection<T>(Super::map(f));
      }

      Collection<E> filter(F<bool(E)> f) {
       return Collection<E>(Super::filter(f));
      }

      template <class K, class Z>
      Collection<std::tuple<K, Z>> group_by
      (F<K(E)> grouper, F<F<Z(E)>(E)> folder, Z init) {
        BaseCollection<ListDS, std::tuple<K,Z>> s = Super::group_by(grouper,folder,init);
        return Collection<std::tuple<K,Z>>(s);
      }

      template <class T>
      Collection<T> ext(F<BaseCollection<ListDS, T>(E)> expand) {
        BaseCollection<ListDS, T> result = Super::ext(expand);
        return Collection<T>(result);
      }

    template<class Archive>
    void serialize(Archive &ar, const unsigned int version) {
      ar & boost::serialization::base_object<BaseCollection<ListDS, E>>(*this);
    }

  };

  template <typename E>
  // TODO: make this more generic? ie a SeqDS interface
  class Seq : public BaseCollection<ListDS, E> {
    typedef BaseCollection<ListDS, E> Super;
     public:
      Seq(Engine * e) : Super(e) {};

      Seq(const Seq<E>& other) : Super(other)  {}

      Seq(Super other) : Super(other) {}

      template<class Iterator>
      Seq(Engine * e, Iterator start, Iterator finish)
        : Super(e, start, finish)
      { }

      // Convert from BaseCollection<ListDS, E> to Seq<E>
      std::tuple<Seq<E>, Seq<E>> split() {
        auto tup = Super::split();
        Super ds1 = get<0>(tup);
        Super ds2 = get<1>(tup);
        return std::make_tuple(Seq<E>(ds1), Seq<E>(ds2));
      }

      Seq<E> combine(const Seq<E>& other) {
       return Seq<E>(Super::combine(other));
      }

      template <class T>
      Seq<T> map(F<T(E)> f) {
       return Seq<T>(Super::map(f));
      }

      Seq<E> filter(F<bool(E)> f) {
       return Seq<E>(Super::filter(f));
      }

      template <class K, class Z>
      Seq<std::tuple<K, Z>> group_by
      (F<K(E)> grouper, F<F<Z(E)>(E)> folder, Z init) {
        BaseCollection<ListDS, std::tuple<K,Z>> s = Super::group_by(grouper,folder,init);
        return Seq<std::tuple<K,Z>>(s);
      }

      template <class T>
      Seq<T> ext(F<BaseCollection<ListDS, T>(E)> expand) {
        BaseCollection<ListDS, T> result = Super::ext(expand);
        return Seq<T>(result);
      }

      Seq<E> sort(F<F<int(E)>(E)> f) {
        Super s = Super(ListDS<E>::sort(f));
        return Seq<E>(s);
      }

  };

  template <typename E>
  // TODO: make this more generic? ie a SetDS interface
  class Set : public BaseCollection<SetDS, E> {
    typedef BaseCollection<SetDS, E> Super;
    typedef SetDS<E> dataspace;
     public:
      Set(Engine * e) : Super(e) {};

      Set(const Set<E>& other) : Super(other)  {}

      Set(Super other) : Super(other) {}

      template<class Iterator>
      Set(Engine * e, Iterator start, Iterator finish)
        : Super(e, start, finish)
      { }

      // Convert from BaseCollection<ListDS, E> to Set<E>
      std::tuple<Set<E>, Set<E>> split() {
        auto tup = Super::split();
        Super ds1 = get<0>(tup);
        Super ds2 = get<1>(tup);
        return std::make_tuple(Set<E>(ds1), Set<E>(ds2));
      }

      Set<E> combine(const Set<E>& other) {
       return Set<E>(Super::combine(other));
      }

      template <class T>
      Set<T> map(F<T(E)> f) {
       return Set<T>(Super::map(f));
      }

      Set<E> filter(F<bool(E)> f) {
       return Set<E>(Super::filter(f));
      }

      template <class K, class Z>
      Set<std::tuple<K, Z>> group_by
      (F<K(E)> grouper, F<F<int(E)>(E)> folder, Z init) {
        BaseCollection<SetDS, std::tuple<K,Z>> s = Super::group_by(grouper,folder,init);
        return Set<std::tuple<K,Z>>(s);
      }

      template <class T>
      Set<T> ext(F<BaseCollection<SetDS, T>(E)> expand) {
        BaseCollection<SetDS, T> result = Super::ext(expand);
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
        Super s = Super(dataspace::union1(other));
        return Set<E>(s);
      }

      Set intersect(Set<E> other) {
        Super s = Super(dataspace::intersect(other));
        return Set<E>(s);
      }

      Set difference(Set<E> other) {
        Super s = Super(dataspace::difference(other));
        return Set<E>(s);
      }

  };

  template <typename E>
  class Sorted : public BaseCollection<SortedDS, E> {
    typedef BaseCollection<SortedDS, E> Super;
    typedef SortedDS<E> dataspace;
     public:
      Sorted(Engine * e) : Super(e) {};

      Sorted(const Sorted<E>& other) : Super(other)  {}

      Sorted(Super other) : Super(other) {}

      template<class Iterator>
      Sorted(Engine * e, Iterator start, Iterator finish)
        : Super(e, start, finish)
      { }

      // Convert from BaseCollection<ListDS, E> to Sorted<E>
      std::tuple<Sorted<E>, Sorted<E>> split() {
        auto tup = Super::split();
        Super ds1 = get<0>(tup);
        Super ds2 = get<1>(tup);
        return std::make_tuple(Sorted<E>(ds1), Sorted<E>(ds2));
      }

      Sorted<E> combine(const Sorted<E>& other) {
       return Sorted<E>(Super::combine(other));
      }

      template <class T>
      Sorted<T> map(F<T(E)> f) {
       return Sorted<T>(Super::map(f));
      }

      Sorted<E> filter(F<bool(E)> f) {
       return Sorted<E>(Super::filter(f));
      }

      template <class K, class Z>
      Sorted<std::tuple<K, Z>> group_by
      (F<K(E)> grouper, F<Z(Z, E)> folder, Z init) {
        BaseCollection<SortedDS, std::tuple<K,Z>> s = Super::group_by(grouper,folder,init);
        return Sorted<std::tuple<K,Z>>(s);
      }

      template <class T>
      Sorted<T> ext(F<BaseCollection<SortedDS, T>(E)> expand) {
        BaseCollection<SortedDS, T> result = Super::ext(expand);
        return Sorted<T>(result);
      }

      shared_ptr<E> min() { return dataspace::min(); }

      shared_ptr<E> max() { return dataspace::max(); }

      shared_ptr<E> lowerBound(E a) { return dataspace::lowerBound(a); }

      shared_ptr<E> upperBound(E a) { return dataspace::upperBound(a); }

      Sorted<E> slice(E a, E b) {
        Super s = Super(dataspace::slice(a,b));
        return Sorted<E>(s);
      }

  };

  template <typename E>
  // TODO: make this more generic? ie an ExternalDS interface
  class ExternalBaseCollection : public BaseCollection<FileDS, E> {
    typedef BaseCollection<FileDS, E> Super;
    typedef FileDS<E> Dataspace;
     public:
      ExternalBaseCollection(Engine * e) : Super(e) {};

      ExternalBaseCollection(const ExternalBaseCollection<E>& other) : Super(other)  {}

      ExternalBaseCollection(const Super& other) : Super(other) {}
      ExternalBaseCollection(Super other) : Super(other) {}

      template<class Iterator>
      ExternalBaseCollection(Engine * e, Iterator start, Iterator finish)
        : Super(e, start, finish)
      { }

      // Convert from BaseCollection<ListDS, E> to Set<E>
      std::tuple<Set<E>, Set<E>> split() {
        auto tup = Super::split();
        Super& ds1 = get<0>(tup);
        Super& ds2 = get<1>(tup);
        return std::make_tuple(ExternalBaseCollection<E>(ds1), ExternalBaseCollection<E>(ds2));
      }

      ExternalBaseCollection<E> combine(const ExternalBaseCollection<E>& other) {
       return ExternalBaseCollection<E>(Super::combine(other));
      }

      template <class T>
      ExternalBaseCollection<T> map(F<T(E)> f) {
       return ExternalBaseCollection<T>(Super::map(f));
      }

      ExternalBaseCollection<E> filter(F<bool(E)> f) {
       return ExternalBaseCollection<E>(Super::filter(f));
      }

      template <class K, class Z>
      ExternalBaseCollection<std::tuple<K, Z>> group_by
            (F<K(E)> grouper, F<F<Z(E)>(Z)> folder, Z init) {
        BaseCollection<FileDS, std::tuple<K,Z>> s = Super::group_by(grouper,folder,init);
        return ExternalBaseCollection<std::tuple<K,Z>>(s);
      }

      template <class T>
      ExternalBaseCollection<T> ext(F<BaseCollection<SetDS, T>(E)> expand) {
        BaseCollection<SetDS, T> result = Super::ext(expand);
        return ExternalBaseCollection<T>(result);
      }
  };
}

#endif
