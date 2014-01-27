// The K3 Runtime Collections Library.
//
// This file contains definitions for the various operations performed on K3 collections, used by
// the generated C++ code. The C++ realizations of K3 Collections will inherit from the
// K3::Collection class, providing a suitable content type.
#ifndef K3_RUNTIME_COLLECTIONS_H
#define K3_RUNTIME_COLLECTIONS_H

#include <functional>
#include <map>
#include <tuple>
#include <vector>

namespace K3 {
    template <typename E>
    class Collection {
        public:
            Collection() {}
            Collection(std::vector<E> v): __data(v) {}
            Collection(const Collection& c): __data(c.__data) {}
            Collection(Collection&& c): __data(c.__data) {}

            // Virtual sub-class constructor.
            template <typename T>
            virtual Collection<T> create(Collection<T>) = 0;

            template <typename T>
            Collection<T> map(std::function<T(E)>);

            template <typename K, typename Z>
            Collection<std::tuple<K, Z>> group_by(std::function<K(E)>, std::function<Z(Z, E)>, Z);
        private:
            std::vector<E> __data;
    };

    template <typename E>
    template <typename T>
    Collection<T> Collection<E>::map(std::function<T(E)> f) {
        std::vector<T> v;
        v.reserve(__data.size());

        for (auto i : __data) {
            v.push_back(f(i));
        }

        return create(Collection(v));
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

        std::vector<std::tuple<K, Z>> v;
        v.reserve(m.count());

        for (auto i: m) {
            v.push_back(std::make_tuple(i.first, i.second));
        }

        return create(Collection(v));
    }
}

#endif
