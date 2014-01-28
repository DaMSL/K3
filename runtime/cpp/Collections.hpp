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

namespace K3 {
    template <typename E> using chunk = std::list<E>;

    template <typename E>
    class Collection {
        public:
            Collection() {}
            Collection(const chunk<E>& v): __data(v) {}
            Collection(const Collection& c): __data(c.__data) {}
            Collection(Collection&& c): __data(c.__data) {}

            std::shared_ptr<E> peek();

            void insert_basic(const E&);
            void delete_first(const E&);
            void update_first(const E&, const E&);

            template <typename T>
            Collection<T> map(std::function<T(E)>);

            Collection<E> filter(std::function<bool(E)>);

            template <typename Z>
            Z fold(std::function<Z(Z, E)>, Z);

            template <typename K, typename Z>
            Collection<std::tuple<K, Z>> group_by(std::function<K(E)>, std::function<Z(Z, E)>, Z);

            chunk<E> __data;
    };

    template <typename E>
    std::shared_ptr<E> Collection<E>::peek() {
        if (!__data.empty()) {
            return std::shared_ptr<E>(__data.front());
        } else {
            return nullptr;
        }

    }

    template <typename E>
    void Collection<E>::insert_basic(const E& e) {
        __data.push_back(e);
    }

    template <typename E>
    void Collection<E>::delete_first(const E& e) {
        auto location = find(begin(__data), end(__data), e);

        if (location != end(__data)) {
            __data.erase(location);
        }

        return;
    }

    template <typename E>
    void Collection<E>::update_first(const E& prev, const E& next) {
        auto location = find(begin(__data), end(__data), prev);

        if (location != end(__data)) {
            *location = next;
        }

        return;
    }

    template <typename E>
    template <typename T>
    Collection<T> Collection<E>::map(std::function<T(E)> f) {
        chunk<T> v;
        v.reserve(__data.size());

        for (auto i : __data) {
            v.push_back(f(i));
        }

        return Collection<T>(v);
    }

    template <typename E>
    Collection<E> Collection<E>::filter(std::function<bool(E)> p) {
        chunk<E> v;

        for (auto i: __data) {
            if (p(i)) {
                v.push_back(i);
            }
        }

        return Collection(v);
    }

    template <typename E>
    template <typename Z>
    Z Collection<E>::fold(std::function<Z(Z, E)> f, Z z) {
        for (auto i: __data) {
            z = f(z, i);
        }

        return z;
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
}

#endif
