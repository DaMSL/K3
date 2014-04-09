#include <algorithm>
#include <functional>
#include <string>
#include <tuple>
#include <vector>

#include <Common.hpp>
#include <Engine.hpp>

namespace K3
{
  using namespace std;
  typedef Value E;

  class ListDataspace
  {
    using chunk = list<E>;

    protected:
    chunk __data;

    public:
    ListDataspace(K3::Engine *)
    { }

    template<typename Iterator>
    ListDataspace(K3::Engine *, Iterator start, Iterator finish)
        : __data(start, finish)
    { }

    ListDataspace(const ListDataspace& other)
        : __data(other.__data)
    { }
    private:
    ListDataspace(list<E> other)
        : __data(other)
    { }

    public:
    shared_ptr<E> peek() const
    {
        if (__data.empty())
            return shared_ptr<E>(nullptr);
        else
            return make_shared<E>(__data.front());
    }

    void insert_basic(const E& e)
    {
        __data.emplace_back(e);
    }
    void delete_first(const E& e)
    {
        auto location = std::find(std::begin(__data), std::end(__data), e);

        if (location != std::end(__data)) {
            __data.erase(location);
        }
    }
    void update_first(const E& prev, const E& next) {
        auto location = std::find(std::begin(__data), std::end(__data), prev);

        if (location != std::end(__data)) {
            *location = next;
        }
        else {
            __data.push_back(next);
        }

        return;
    }

    tuple<ListDataspace, ListDataspace> split() {
        if (__data.size() < 2) {
            // First of the pair is a copy of the original collection, the second is empty.
            return make_tuple(ListDataspace(*this), ListDataspace(chunk()));
        } else {
            typename chunk::iterator s = std::begin(__data);
            for (unsigned i = 0; i < __data.size()/2; ++i, ++s);

            chunk left(std::begin(__data), s);
            chunk right(s, std::end(__data));

            return make_tuple(ListDataspace(left), ListDataspace(right));
        }
    }

    ListDataspace combine(ListDataspace other)
    {
        chunk result;

        result.insert(std::end(result), std::begin(__data), std::end(__data));
        result.insert(std::end(result), std::begin(other.__data), std::end(other.__data));

        return result;
    }

    void iterate(std::function<void(E)> f) {
        for (auto i: __data) {
            f(i);
        }

        return;
    }

    //template <typename T>
    typedef K3::Value T;
    ListDataspace map(std::function<T(E)> f) {
        chunk v;
        //v.reserve(__data.size());

        for (auto i : __data) {
            v.push_back(f(i));
        }

        return ListDataspace(v);
    }

    //template <typename E>
    typedef K3::Value E;
    ListDataspace filter(std::function<bool(E)> p) {
        chunk v;

        for (auto i: __data) {
            if (p(i)) {
                v.push_back(i);
            }
        }

        return ListDataspace(v);
    }

    //template <typename E>
    template <typename Z>
    Z fold(std::function<Z(Z, E)> f, Z z) {
        for (auto i: __data) {
            z = f(z, i);
        }

        return z;
    }
  };
};