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

namespace K3 {
  template <template <class> class D, class E>
  class Collection: public D<E> {
    public:
      Collection();

      template <template <class> class F>
      Collection(const Collection<F, E>);

      template <template <class> class F>
      Collection(Collection<F, E>&&);

      // TODO: shared_ptr vs. value?
      std::shared_ptr<E> peek();

      void insert(const E&);
      void insert(E&&);

      void erase(const E&);

      void update(const E&, const E&);
      void update(const E&, E&&);

      std::tuple<Collection<D, E>, Collection<D, E>> split();

      template <template <class> class F>
      Collection<D, E> combine(const Collection<F, E>&);

      template <template <class> class F>
      Collection<D, E> combine(Collection<F, E>&&);

      template <class T>
      Collection<D, T> map(std::function<T(E)>);

      Collection<D, E> filter(std::function<bool(E)>);

      template <class Z>
      Z fold(std::function<Z(Z, E)>, Z);

      template <class K, class Z>
      Collection<D, std::tuple<K, Z>> group_by(std::function<K(E)>, std::function<Z(Z, E)>, Z);

      template <template <class> class F, class T>
      Collection<D, T> ext(std::function<Collection<F, T>(E)>);
  };
}

#endif
