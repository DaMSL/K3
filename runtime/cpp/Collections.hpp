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

      template <class R, class T>
      BaseCollection<D, R> map(F<T(E)> f) {
       BaseCollection<D, R> result = BaseCollection<D, R>(D<E>::getEngine());
       std::function<unit_t(E)> wrap = [&] (E e) { R r; T t = f(e); r.elem = t; result.insert(r);return unit_t(); };
       D<E>::iterate(wrap);
       return result;
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

      template <class R,class K, class Z>
      F<F<BaseCollection<D, R>(Z)>(F<F<Z(E)>(Z)>)> groupBy(F<K(E)> grouper) {
        F<F<BaseCollection<D, R>(Z)>(F<F<Z(E)>(Z)>)> r = [=] (F<F<Z(E)>(Z)> folder) {
          F<BaseCollection<D, R>(Z)> r2 = [=] (Z init) {
              // Create a map to hold partial results
              std::map<K, Z> accs = std::map<K,Z>();
              // lambda to apply to each element
              F<unit_t(E)> f = [&] (E elem) {
                K key = grouper(elem);
                if (accs.find(key) == accs.end()) {
                  accs[key] = init;
                }
                accs[key] = folder(accs[key])(elem);
                return unit_t();
              };
              D<E>::iterate(f);
              // Build BaseCollection result
              BaseCollection<D, R> result = BaseCollection<D, R>(D<E>::getEngine());
              typename std::map<K,Z>::iterator it;
              for (it = accs.begin(); it != accs.end(); ++it) {
                R r;
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
      BaseCollection<D, T> ext(F<BaseCollection<D, T>(E)> expand) {
        BaseCollection<D, T> result = BaseCollection<D,T>(D<E>::getEngine());
        auto add_to_result = [&] (T elem) {result.insert(elem); return unit_t(); };
        std::function<unit_t(E)> fun = [&] (E elem) {
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

      template <class R, class T>
      Collection<R> map(F<T(E)> f) {
       return Collection<R>(Super::template map<R,T>(f));
      }

      Collection<E> filter(F<bool(E)> f) {
       return Collection<E>(Super::filter(f));
      }

      template <class R, class K, class Z>
      F<F<Collection<R>(Z)>(F<F<Z(E)>(Z)>)> groupBy(F<K(E)> grouper) {
        F<F<Collection<R>(Z)>(F<F<Z(E)>(Z)>)> r = [=] (F<F<Z(E)>(Z)> folder) {
          F<Collection<R>(Z)> r2 = [=] (Z init) {
            BaseCollection<ListDS, R> s = Super::template groupBy<R,K,Z>(grouper)(folder)(init);
            return Collection<R>(s);
          };
          return r2;
        };
        return r;
      }

      template <class T>
      Collection<T> ext(F<Collection<T>(E)> expand) {
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
        Super ds1 = get<0>(tup);
        Super ds2 = get<1>(tup);
        return std::make_tuple(Seq<E>(ds1), Seq<E>(ds2));
      }

      Seq<E> combine(const Seq<E>& other) {
       return Seq<E>(Super::combine(other));
      }

      template <class R, class T>
      Seq<R> map(F<T(E)> f) {
       return Seq<R>(Super::template map<R,T>(f));
      }


      Seq<E> filter(F<bool(E)> f) {
       return Seq<E>(Super::filter(f));
      }

      template <class R, class K, class Z>
      F<F<Collection<R>(Z)>(F<F<Z(E)>(Z)>)> groupBy(F<K(E)> grouper) {
        F<F<Collection<R>(Z)>(F<F<Z(E)>(Z)>)> r = [=] (F<F<Z(E)>(Z)> folder) {
          F<Collection<R>(Z)> r2 = [=] (Z init) {
            BaseCollection<ListDS, R> s = Super::template groupBy<R,K,Z>(grouper)(folder)(init);
            return Collection<R>(s);
          };
          return r2;
        };
        return r;
      }

      template <class T>
      Collection<T> ext(F<Collection<T>(E)> expand) {
        BaseCollection<ListDS, T> result = Super::template ext<T>(expand);
        return Collection<T>(result);
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

      template <class R, class T>
      Collection<R> map(F<T(E)> f) {
       return Collection<R>(Super::template map<R,T>(f));
      }

      Set<E> filter(F<bool(E)> f) {
       return Set<E>(Super::filter(f));
      }

      template <class R, class K, class Z>
      F<F<Collection<R>(Z)>(F<F<Z(E)>(Z)>)> groupBy(F<K(E)> grouper) {
        F<F<Collection<R>(Z)>(F<F<Z(E)>(Z)>)> r = [=] (F<F<Z(E)>(Z)> folder) {
          F<Collection<R>(Z)> r2 = [=] (Z init) {
            BaseCollection<ListDS, R> s = Super::template groupBy<R,K,Z>(grouper)(folder)(init);
            return Collection<R>(s);
          };
          return r2;
        };
        return r;
      }

      template <class T>
      Collection<T> ext(F<Collection<T>(E)> expand) {
        BaseCollection<ListDS, T> result = Super::template ext<T>(expand);
        return Collection<T>(result);
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

      template <class R, class T>
      Collection<R> map(F<T(E)> f) {
       return Collection<R>(Super::template map<R,T>(f));
      }

      Sorted<E> filter(F<bool(E)> f) {
       return Sorted<E>(Super::filter(f));
      }

      template <class R, class K, class Z>
      F<F<Collection<R>(Z)>(F<F<Z(E)>(Z)>)> groupBy(F<K(E)> grouper) {
        F<F<Collection<R>(Z)>(F<F<Z(E)>(Z)>)> r = [=] (F<F<Z(E)>(Z)> folder) {
          F<Collection<R>(Z)> r2 = [=] (Z init) {
            BaseCollection<ListDS, R> s = Super::template groupBy<R,K,Z>(grouper)(folder)(init);
            return Collection<R>(s);
          };
          return r2;
        };
        return r;
      }

      template <class T>
      Collection<T> ext(F<Collection<T>(E)> expand) {
        BaseCollection<ListDS, T> result = Super::template ext<T>(expand);
        return Collection<T>(result);
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
  class ExternalCollection : public BaseCollection<FileDS, E> {
    typedef BaseCollection<FileDS, E> Super;
    typedef FileDS<E> Dataspace;
     public:
      ExternalCollection(Engine * e) : Super(e) {};

      ExternalCollection(const ExternalCollection<E>& other) : Super(other)  {}

      ExternalCollection(const Super& other) : Super(other) {}
      ExternalCollection(Super other) : Super(other) {}

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

      ExternalCollection<E> combine(const ExternalCollection<E>& other) {
       return ExternalCollection<E>(Super::combine(other));
      }

      template <class R, class T>
      ExternalCollection<R> map(F<T(E)> f) {
       return ExternalCollection<R>(Super::template map<R,T>(f));
      }

      ExternalCollection<E> filter(F<bool(E)> f) {
       return ExternalCollection<E>(Super::filter(f));
      }

      // TODO: reconcile types with Annotation/External.k3
      template <class R, class K, class Z>
      F<F<Collection<R>(Z)>(F<F<Z(E)>(Z)>)> groupBy(F<K(E)> grouper) {
        F<F<Collection<R>(Z)>(F<F<Z(E)>(Z)>)> r = [=] (F<F<Z(E)>(Z)> folder) {
          F<Collection<R>(Z)> r2 = [=] (Z init) {
            BaseCollection<ListDS, R> s = Super::template groupBy<R,K,Z>(grouper)(folder)(init);
            return Collection<R>(s);
          };
          return r2;
        };
        return r;
      }

      template <class T>
      Collection<T> ext(F<Collection<T>(E)> expand) {
        BaseCollection<ListDS, T> result = Super::template ext<T>(expand);
        return Collection<T>(result);
      }

  };
}

#endif
