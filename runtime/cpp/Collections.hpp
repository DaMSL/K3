// The K3 Runtime Collections Library.
//
// This file contains definitions for the various operations performed on K3 collections, used by
// the generated C++ code. The C++ realizations of K3 Collections will inherit from the
// K3::Collection class, providing a suitable content type.
#ifndef K3_RUNTIME_COLLECTIONS_H
#define K3_RUNTIME_COLLECTIONS_H

#include <functional>

namespace K3 {
    template <typename E>
    class Collection {
        public:
            Collection() {}
            Collection(std::vector<C> v): __data(v) {}
            Collection(const Collection& c): __data(c.__data) {}
            Collection(Collection&& c): __data(c.__data) {}

            template <typename T>
            virtual Collection<T> create(Collection<T>) = 0;

            template <typename T>
            Collection<T> map(std::function<T(E)>);
        private:
            std::vector<E> __data;
    }

    template <typename T>
    Collection<T> Collection::map(std::function<T(E)> f) {
        std::vector<T> v;
        v.reserve(__data.size());

        for (auto i : __data) {
            v.push_back(f(i));
        }

        return create(Collection(v));
    }
}

#endif
