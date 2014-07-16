// The K3 Runtime Collections Library.
//
// This file contains definitions for the various operations performed on K3 Collections, used by
// the generated C++ code. The C++ realizations of K3 Collections will inherit from the
// K3::BaseCollection class, providing a suitable content type.
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
#include <memory>
#include <tuple>
#include <boost/tr1/unordered_map.hpp>
#include <boost/tr1/unordered_set.hpp>
#include "boost/serialization/vector.hpp"
#include "boost/serialization/set.hpp"
#include "external/boost_ext/unordered_set.hpp"
#include "external/boost_ext/unordered_map.hpp"

#include <BaseTypes.hpp>
#include <dataspace/FileDS.hpp>
#include <dataspace/ListDS.hpp>
#include <dataspace/SetDS.hpp>
#include <dataspace/SortedDS.hpp>
#include <dataspace/StlDS.hpp>
#include <dataspace/MapDS.hpp>
#include <boost/serialization/base_object.hpp>

namespace K3 {
  class Engine;
  template <class E> using MapReturnType = R_elem<E>;
  template <class K, class V> using GroupByReturnType = R_key_value<K,V>;

  template <template <class...> class D, class E>
  class BaseCollection: public D<E> {
    public:
      BaseCollection(Engine* e) : D<E>(e) {}

      template <template <class> class F>
      BaseCollection(const BaseCollection<F, E>& other) : D<E>(other)  {}

      template <template <class> class F>
      BaseCollection(BaseCollection<F, E>& other) : D<E>(other) {}

      BaseCollection(D<E> other) : D<E>(other) {}

      template<class Iterator>
      BaseCollection(Engine* e, Iterator start, Iterator finish)
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
        D<E>& ds1 = get<0>(tup);
        D<E>& ds2 = get<1>(tup);
        return std::make_tuple(BaseCollection<D,E>(ds1), BaseCollection<D,E>(ds2));
      }

      std::tuple<BaseCollection<D, E>, BaseCollection<D, E>> split(unit_t) {
        return split();
      }

      BaseCollection<D, E> combine(const BaseCollection<D, E>& other) const {
       return BaseCollection<D,E>(D<E>::combine(other));
      }

      template <class T>
      BaseCollection<D, MapReturnType<T>> map(F<T(E)> f) {
       BaseCollection<D, MapReturnType<T>> result(D<E>::getEngine());
       std::function<unit_t(E)> wrap = [&] (const E& e) {
         MapReturnType<T> r;
         r.elem = f(e);
         result.insert(r);

         return unit_t();
       };
       D<E>::iterate(wrap);
       return result;
      }

      BaseCollection<D, E> filter(const F<bool(E)>& f) {
       return BaseCollection<D,E>(D<E>::filter(f));
      }

      template <class Z>
      F<Z(const Z&)> fold(F<F<Z(E)>(Z)> f) {
           return [&] (const Z& init) {
             return D<E>::template fold<Z>(f, init);
           };
      }

      template <class K, class Z>
      F<F<BaseCollection<MapDS, GroupByReturnType<K,Z>>(Z)>(F<F<Z(E)>(Z)>)> groupBy(F<K(E)> grouper) {
        F<F<BaseCollection<MapDS, GroupByReturnType<K,Z>>(Z)>(F<F<Z(E)>(Z)>)> r = [=] (F<F<Z(E)>(Z)> folder) {
          F<BaseCollection<MapDS, GroupByReturnType<K,Z>>(Z)> r2 = [=] (const Z& init) {
              // Create a map to hold partial results
              std::tr1::unordered_map<K, Z> accs;
              // lambda to apply to each element
              F<unit_t(const E&)> f = [&] (const E& elem) {
                K key = grouper(elem);
                if (accs.find(key) == accs.end()) {
                  accs[key] = init;
                }
                accs[key] = folder(accs[key])(elem);
                return unit_t();
              };
              D<E>::iterate(f);
              // Build BaseCollection result
              BaseCollection<MapDS, GroupByReturnType<K,Z>> result(D<E>::getEngine());
              typename std::tr1::unordered_map<K,Z>::iterator it;
              for (it = accs.begin(); it != accs.end(); ++it) {
                GroupByReturnType<K,Z> r;
                r.key = it->first;
                r.value = it->second;
                result.insert(r);
              }
             return result;
          };
          return r2;
        };
        return r;
      }

      // Requires same dataspace
      template <class T>
      BaseCollection<D, T> ext(const F<BaseCollection<D, T>(E)>& expand) {
        BaseCollection<D, T> result(D<E>::getEngine());
        auto add_to_result = [&] (T& elem) {
          result.insert(elem);
          return unit_t();
        };
        std::function<unit_t(E)> fun = [&] (E& elem) {
          expand(elem).iterate(add_to_result);
          return unit_t();
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

  template <class E>
  class Map : public BaseCollection<MapDS, E> {
    typedef BaseCollection<MapDS, E>  Super;
     public:
      Map(Engine * e) : Super(e) {}

      Map(Map<E>& other) : Super(other)  {}

      Map(Super other) : Super(other) {}

      template<class Iterator>
      Map(Engine * e, Iterator start, Iterator finish)
        : Super(e, start, finish)
      { }

      std::tuple<Map<E>, Map<E>> split() {
        auto tup = Super::split();
        Super& ds1 = get<0>(tup);
        Super& ds2 = get<1>(tup);
        return std::make_tuple(Map<E>(ds1), Map<E>(ds2));
      }

      std::tuple<Map<E>, Map<E>> split(unit_t) {
        return split();
      }

      Map<E> combine(const Map<E>& other) const {
       return Map<E>(Super::combine(other));
      }

      template <class T>
      Map<MapReturnType<T>> map(const F<T(E)>& f) {
       return Map<MapReturnType<T>>(Super::template map<T>(f));
      }

      Map<E> filter(const F<bool(E)>& f) {
       return Map<E>(Super::filter(f));
      }

      // TODO: specialize groupBy (don't user super::groupBy). Maybe even a different type signature.
      // i.e group by key automatically, without using a user specified lambda.
      template <class K, class Z>
      F<F<Map<GroupByReturnType<K,Z>>(Z)>(F<F<Z(E)>(Z)>)> groupBy(F<K(E)> grouper) {
        F<F<Map<GroupByReturnType<K,Z>>(Z)>(F<F<Z(E)>(Z)>)> r = [=] (F<F<Z(E)>(Z)> folder) {
          F<Map<GroupByReturnType<K,Z>>(Z)> r2 = [=] (const Z& init) {
            BaseCollection<MapDS, GroupByReturnType<K,Z>> s = Super::template groupBy<K,Z>(grouper)(folder)(init);
            return Map<GroupByReturnType<K,Z>>(s);
          };
          return r2;
        };
        return r;
      }

      // TODO: correct type signature?
      template <class T>
      Map<T> ext(const F<Map<T>(E)>& expand) {
        BaseCollection<MapDS, T> result = Super::template ext<T>(expand);
        return Map<T>(result);
      }

    template<class Archive>
    void serialize(Archive &ar, const unsigned int version) {
      ar & boost::serialization::base_object<BaseCollection<MapDS, E>>(*this);
    }

  };

  template <typename E>
  class Collection : public BaseCollection<ListDS, E> {
    typedef BaseCollection<ListDS, E> Super;
     public:
      Collection(Engine * e) : Super(e) {};

      Collection(const Collection<E>& other) : Super(other)  {}

      Collection(Collection& other) : Super(other)  {}

      Collection(Super other) : Super(other) {}

      template<class Iterator>
      Collection(Engine * e, Iterator start, Iterator finish)
        : Super(e, start, finish)
      { }

      // Convert from BaseCollection<ListDS, E> to Collection<E>
      // TODO: this should be cleaned up and not duplicated
      //       Also, check if converting to Collection hurts performance
      std::tuple<Collection<E>, Collection<E>> split() {
        auto tup = Super::split();
        Super& ds1 = get<0>(tup);
        Super& ds2 = get<1>(tup);
        return std::make_tuple(Collection<E>(ds1), Collection<E>(ds2));
      }

      std::tuple<Collection<E>, Collection<E>> split(unit_t) {
        return split();
      }

      Collection<E> combine(const Collection<E>& other) const {
       return Collection<E>(Super::combine(other));
      }

      template <class T>
      Collection<MapReturnType<T>> map(const F<T(E)>& f) {
       return Collection<MapReturnType<T>>(Super::template map<T>(f));
      }

      Collection<E> filter(const F<bool(E)>& f) {
       return Collection<E>(Super::filter(f));
      }

      template <class K, class Z>
      F<F<Map<GroupByReturnType<K,Z>>(Z)>(F<F<Z(E)>(Z)>)> groupBy(F<K(E)> grouper) {
        F<F<Map<GroupByReturnType<K,Z>>(Z)>(F<F<Z(E)>(Z)>)> r = [=] (F<F<Z(E)>(Z)> folder) {
          F<Map<GroupByReturnType<K,Z>>(Z)> r2 = [=] (const Z& init) {
            BaseCollection<MapDS, GroupByReturnType<K,Z>> s = Super::template groupBy<K,Z>(grouper)(folder)(init);
            return Map<GroupByReturnType<K,Z>>(s);
          };
          return r2;
        };
        return r;
      }

      template <class T>
      Collection<T> ext(const F<Collection<T>(E)>& expand) {
        BaseCollection<ListDS, T> result = Super::template ext<T>(expand);
        return Collection<T>(result);
      }

    template<class Archive>
    void serialize(Archive &ar, const unsigned int version) {
      ar & boost::serialization::base_object<BaseCollection<ListDS, E>>(*this);
    }

  };

  template <typename E>
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
        Super& ds1 = get<0>(tup);
        Super& ds2 = get<1>(tup);
        return std::make_tuple(Seq<E>(ds1), Seq<E>(ds2));
      }

      Seq<E> combine(const Seq<E>& other) const {
       return Seq<E>(Super::combine(other));
      }

      template <class T>
      Seq<MapReturnType<T>> map(const F<T(E)>& f) {
       return Seq<MapReturnType<T>>(Super::template map<T>(f));
      }


      Seq<E> filter(const F<bool(E)>& f) {
       return Seq<E>(Super::filter(f));
      }

      template <class K, class Z>
      F<F<Map<GroupByReturnType<K,Z>>(Z)>(F<F<Z(E)>(Z)>)> groupBy(const F<K(E)>& grouper) {
        F<F<Map<GroupByReturnType<K,Z>>(Z)>(F<F<Z(E)>(Z)>)> r = [&] (const F<F<Z(E)>(Z)>& folder) {
          F<Map<GroupByReturnType<K,Z>>(Z)> r2 = [&] (const Z& init) {
            BaseCollection<MapDS, GroupByReturnType<K,Z>> s = Super::template groupBy<K,Z>(grouper)(folder)(init);
            return Map<GroupByReturnType<K,Z>>(s);
          };
          return r2;
        };
        return r;
      }

      template <class T>
      Collection<T> ext(const F<Collection<T>(E)>& expand) {
        BaseCollection<ListDS, T> result = Super::template ext<T>(expand);
        return Collection<T>(result);
      }

      Seq<E> sort(const F<F<int(E)>(E)>& f) {
        Super s(ListDS<E>::sort(f));
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

      Set(Set<E>& other) : Super(other)  {}

      Set(Super& other) : Super(other) {}

      template<class Iterator>
      Set(Engine * e, Iterator start, Iterator finish)
        : Super(e, start, finish)
      { }

      // Convert from BaseCollection<ListDS, E> to Set<E>
      std::tuple<Set<E>, Set<E>> split() {
        auto tup = Super::split();
        Super& ds1 = get<0>(tup);
        Super& ds2 = get<1>(tup);
        return std::make_tuple(Set<E>(ds1), Set<E>(ds2));
      }

      Set<E> combine(const Set<E>& other) const {
       return Set<E>(Super::combine(other));
      }

      template <class T>
      Collection<MapReturnType<T>> map(const F<T(E)>& f) {
       return Collection<MapReturnType<T>>(Super::template map<T>(f));
      }

      Set<E> filter(const F<bool(E)>& f) {
       return Set<E>(Super::filter(f));
      }

      template <class K, class Z>
      F<F<Map<GroupByReturnType<K,Z>>(Z)>(F<F<Z(E)>(Z)>)> groupBy(const F<K(E)>& grouper) {
        F<F<Map<GroupByReturnType<K,Z>>(Z)>(F<F<Z(E)>(Z)>)> r = [&] (const F<F<Z(E)>(Z)>& folder) {
          F<Map<GroupByReturnType<K,Z>>(Z)> r2 = [&] (const Z& init) {
            BaseCollection<MapDS, GroupByReturnType<K,Z>> s = Super::template groupBy<K,Z>(grouper)(folder)(init);
            return Map<GroupByReturnType<K,Z>>(s);
          };
          return r2;
        };
        return r;
      }

      template <class T>
      Collection<T> ext(const F<Collection<T>(E)>& expand) {
        BaseCollection<ListDS, T> result = Super::template ext<T>(expand);
        return Collection<T>(result);
      }

      bool member(const E& e) {
        return dataspace::member(e);
      }

      bool isSubsetOf(const SetDS<E>& other) {
        return dataspace::isSubsetOf(other);
      }

      // TODO union is a reserved word
      Set union1(const Set<E>& other) {
        Super s(dataspace::union1(other));
        return Set<E>(s);
      }

      Set intersect(const Set<E>& other) {
        Super s(dataspace::intersect(other));
        return Set<E>(s);
      }

      Set difference(const Set<E>& other) {
        Super s(dataspace::difference(other));
        return Set<E>(s);
      }

  };

  template <typename E>
  class Sorted : public BaseCollection<SortedDS, E> {
    typedef BaseCollection<SortedDS, E> Super;
    typedef SortedDS<E> dataspace;
     public:
      Sorted(Engine* e) : Super(e) {};

      Sorted(const Sorted<E>& other) : Super(other)  {}

      Sorted(Super& other) : Super(other) {}

      template<class Iterator>
      Sorted(Engine * e, Iterator start, Iterator finish)
        : Super(e, start, finish)
      { }

      // Convert from BaseCollection<ListDS, E> to Sorted<E>
      std::tuple<Sorted<E>, Sorted<E>> split() {
        auto tup = Super::split();
        Super& ds1 = get<0>(tup);
        Super& ds2 = get<1>(tup);
        return std::make_tuple(Sorted<E>(ds1), Sorted<E>(ds2));
      }

      Sorted<E> combine(const Sorted<E>& other) const {
       return Sorted<E>(Super::combine(other));
      }

      template <class T>
      Collection<MapReturnType<T>> map(const F<T(E)>& f) {
       return Collection<MapReturnType<T>>(Super::template map<T>(f));
      }

      Sorted<E> filter(const F<bool(E)>& f) {
       return Sorted<E>(Super::filter(f));
      }

      template <class K, class Z>
      F<F<Map<GroupByReturnType<K,Z>>(Z)>(F<F<Z(E)>(Z)>)> groupBy(const F<K(E)>& grouper) {
        F<F<Map<GroupByReturnType<K,Z>>(Z)>(F<F<Z(E)>(Z)>)> r = [&] (const F<F<Z(E)>(Z)>& folder) {
          F<Map<GroupByReturnType<K,Z>>(Z)> r2 = [&] (const Z& init) {
            BaseCollection<MapDS, GroupByReturnType<K,Z>> s = Super::template groupBy<K,Z>(grouper)(folder)(init);
            return Map<GroupByReturnType<K,Z>>(s);
          };
          return r2;
        };
        return r;
      }

      template <class T>
      Collection<T> ext(const F<Collection<T>(E)>& expand) {
        BaseCollection<ListDS, T> result = Super::template ext<T>(expand);
        return Collection<T>(result);
      }

      shared_ptr<E> min() { return dataspace::min(); }

      shared_ptr<E> max() { return dataspace::max(); }

      shared_ptr<E> lowerBound(const E& a) { return dataspace::lowerBound(a); }

      shared_ptr<E> upperBound(const E& a) { return dataspace::upperBound(a); }

      Sorted<E> slice(const E& a, const E& b) {
        Super s = Super(dataspace::slice(a,b));
        return Sorted<E>(s);
      }

  };

  template <typename E>
  // TODO: make this more generic? ie an ExternalDS interface
  class ExternalCollection : public BaseCollection<FileDS, E> {
    typedef BaseCollection<FileDS, E> Super;
    typedef FileDS<E> Dataspace;
     public:
      ExternalCollection(Engine * e) : Super(e) {};

      ExternalCollection(const ExternalCollection<E>& other) : Super(other)  {}

      ExternalCollection(const Super& other) : Super(other) {}

      ExternalCollection(Super& other) : Super(other) {}

      template<class Iterator>
      ExternalCollection(Engine * e, Iterator start, Iterator finish)
        : Super(e, start, finish)
      { }

      // Convert from BaseCollection<ListDS, E> to Set<E>
      std::tuple<Set<E>, Set<E>> split() {
        auto tup = Super::split();
        Super& ds1 = get<0>(tup);
        Super& ds2 = get<1>(tup);
        return std::make_tuple(ExternalCollection<E>(ds1), ExternalCollection<E>(ds2));
      }

      ExternalCollection<E> combine(const ExternalCollection<E>& other) const {
       return ExternalCollection<E>(Super::combine(other));
      }

      template <class T>
      ExternalCollection<MapReturnType<T>> map(const F<T(E)>& f) {
       return ExternalCollection<MapReturnType<T>>(Super::template map<T>(f));
      }

      ExternalCollection<E> filter(const F<bool(E)>& f) {
       return ExternalCollection<E>(Super::filter(f));
      }

      // TODO: reconcile types with Annotation/External.k3
      template <class K, class Z>
      F<F<Map<GroupByReturnType<K,Z>>(Z)>(F<F<Z(E)>(Z)>)> groupBy(const F<K(E)>& grouper) {
        F<F<Map<GroupByReturnType<K,Z>>(Z)>(F<F<Z(E)>(Z)>)> r = [&] (const F<F<Z(E)>(Z)>& folder) {
          F<Map<GroupByReturnType<K,Z>>(Z)> r2 = [&] (const Z& init) {
            BaseCollection<MapDS, GroupByReturnType<K,Z>> s = Super::template groupBy<K,Z>(grouper)(folder)(init);
            return Map<GroupByReturnType<K,Z>>(s);
          };
          return r2;
        };
        return r;
      }

      template <class T>
      Collection<T> ext(const F<Collection<T>(E)>& expand) {
        BaseCollection<ListDS, T> result = Super::template ext<T>(expand);
        return Collection<T>(result);
      }

  };

} // namespace K3

#endif
