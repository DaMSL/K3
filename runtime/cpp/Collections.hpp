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

      // TODO: shared_ptr vs. value?
      std::shared_ptr<E> peek() { return D<E>::peek(); }

      void insert(const E& elem) { D<E>::insert(elem); }
      void insert(E&& elem) { D<E>::insert(elem); }

      void erase(const E& elem)  { D<E>::erase(elem); }

      void update(const E& v1, const E& v2) { D<E>::update(v1,v2); }
      void update(const E& v1, E&& v2) { D<E>::update(v1,v2); }

      std::tuple<Collection<D, E>, Collection<D, E>> split() { return D<E>::split(); }

      template <template <class> class F>
      Collection<D, E> combine(const Collection<F, E>& other) {
        return D<E>::combine(other);
      }

      template <template <class> class F>
      Collection<D, E> combine(Collection<F, E>&& other) {
        return D<E>::combine(other);
      }

      template <class T>
      Collection<D, T> map(std::function<T(E)> f) {
        return D<E>::map(f);
      }

      Collection<D, E> filter(std::function<bool(E)> f) {
        return D<E>::filter(f);
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
}

#endif
