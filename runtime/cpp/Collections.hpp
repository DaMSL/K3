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
#include <map>
#include <memory>
#include <tuple>
#include <list>

#include "test/FileDataspace.hpp"

namespace K3 {
    template <typename E>
    class Collection {
        typedef ListDataspace Dataspace;
        std::shared_ptr<Dataspace> __dataspace;

        public:
            Collection() {}
            Collection(std::shared_ptr<Dataspace> new_ds): __dataspace(new_ds) {}
            Collection(const Collection& c): __dataspace(std::make_shared<Dataspace>(c.__dataspace)) {}
            Collection(Collection&& c): __dataspace(std::move(c.__dataspace)) {}

            std::shared_ptr<E> peek();

            void insert(const E&);
            void erase(const E&);
            void update(const E&, const E&);

            std::tuple<Collection<E>, Collection<E>> split();
            Collection<E> combine(Collection<E>);

            void iterate(std::function<void(E)>);

            template <typename T>
            Collection<T> map(std::function<T(E)>);

            Collection<E> filter(std::function<bool(E)>);

            template <typename Z>
            Z fold(std::function<Z(Z, E)>, Z);

            template <typename K, typename Z>
            Collection<std::tuple<K, Z>> group_by(std::function<K(E)>, std::function<Z(Z, E)>, Z);

            template <typename T>
            Collection<T> ext(std::function<Collection<T>(E)>);
    };

    template <typename E>
    std::shared_ptr<E> Collection<E>::peek() {
        return __dataspace->peek();
    }

    template <typename E>
    void Collection<E>::insert(const E& e) {
        __dataspace->insert(e);
    }

    template <typename E>
    void Collection<E>::erase(const E& e) {
        __dataspace->erase(e);
    }

    template <typename E>
    void Collection<E>::update(const E& prev, const E& next) {
        __dataspace->update(prev, next);
    }

    template <typename E>
    std::tuple<Collection<E>, Collection<E>> Collection<E>::split() {
        std::tuple<Dataspace, Dataspace> ds_tuple = __dataspace->split();
        // The dataspaces in this tuple should be move-constructed into
        // their new collections
        return std::make_tuple(
                Collection<E>(std::make_shared<Dataspace>(std::move(std::get<0>(ds_tuple)))),
                Collection<E>(std::make_shared<Dataspace>(std::move(std::get<1>(ds_tuple))))
              );
    }

    template <typename E>
    Collection<E> Collection<E>::combine(Collection<E> other) {
        Dataspace new_ds = __dataspace->combine(other.__dataspace);
        Collection<E> result(std::make_shared<Dataspace>(std::move(new_ds)));
        return result;
    }

    template <typename E>
    void Collection<E>::iterate(std::function<void(E)> f) {
        __dataspace->iterate(f);
    }

    template <typename E>
    template <typename T>
    Collection<T> Collection<E>::map(std::function<T(E)> f) {
        Dataspace new_ds = __dataspace->map(f);
        return Collection<T>(std::make_shared<Dataspace>(std::move(new_ds)));
    }

    template <typename E>
    Collection<E> Collection<E>::filter(std::function<bool(E)> p) {
        Dataspace new_ds = __dataspace->filter(p);
        return Collection<E>(std::make_shared<Dataspace>(std::move(new_ds)));
    }

    template <typename E>
    template <typename Z>
    Z Collection<E>::fold(std::function<Z(Z, E)> f, Z z) {
        return __dataspace->fold(f, z);
    }

    template <typename E>
    template <typename K, typename Z>
    Collection<std::tuple<K, Z>> Collection<E>::group_by(std::function<K(E)> g, std::function<Z(Z, E)> f, Z z) {
        std::map<K, Z> m;

        for (auto i: __data) {
            K k = g(i);
            if (m.find(k)) {
                m[k] = f(i, m[k]);
            } else {
                m[k] = f(z, i);
            }
        }

        chunk<std::tuple<K, Z>> v;
        v.reserve(m.count());

        for (auto i: m) {
            v.push_back(std::make_tuple(i.first, i.second));
        }

        return Collection<std::tuple<K, Z>>(v);
    }

    template <typename E>
    template <typename T>
    Collection<T> Collection<E>::ext(std::function<Collection<T>(E)> f) {
        chunk<T> result;

        for (auto i: __data) {
            result.splice(end(result), f(i));
        }

        return Collection<T>(result);
    }
}

#endif
