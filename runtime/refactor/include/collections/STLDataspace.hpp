#ifndef K3_STLDATASPACE
#define K3_STLDATASPACE

#include <tuple>
#include <unordered_map>

#include "boost/serialization/vector.hpp"
#include "boost/serialization/set.hpp"
#include "boost/serialization/map.hpp"
#include "boost/serialization/list.hpp"
#include "boost/serialization/nvp.hpp"
#include "boost/serialization/base_object.hpp"

#include "csvpp/csv.h"
#include "csvpp/string.h"
#include "csvpp/array.h"
#include "csvpp/deque.h"
#include "csvpp/list.h"
#include "csvpp/set.h"
#include "csvpp/vector.h"

namespace K3 {

using std::tuple;
using std::unordered_map;

// Utility to give the return type of a Function F expecting an Element E as an argument:
template <class F, class E> using RT = decltype(std::declval<F>()(std::declval<E>()));

// A Generic STL based dataspace.
template <template <typename> class Derived, template<typename...> class STLContainer, class Elem>
class STLDS {
  typedef STLContainer<Elem> Container;

 public:
  // Constructors
  STLDS(): container() {}
  STLDS(const Container& con): container(con) {}
  STLDS(Container&& con): container(std::move(con)) {}

  Container& getContainer() {
    return container;
  }

  const Container& getConstContainer() const {
    return container;
  }

  // Functionality
  shared_ptr<Elem> peek(unit_t) const {
    shared_ptr<Elem> res(nullptr);
    const_iterator it = container.begin();
    if (it != container.end()) {
      res = std::make_shared<Elem>(*it);
    }
    return res;
  }

  template <class T>
  unit_t insert(T &&e) {
    container.insert(container.end(), std::forward<T>(e));
    return unit_t();
  }

  unit_t erase(const Elem& v) {
    iterator it;
    it = std::find(container.begin(), container.end(), v);
    if (it != container.end()) {
      container.erase(it);
    }
    return unit_t();
  }

  template <class T>
  unit_t update(const Elem& v, T&& v2) {
    iterator it;
    it = std::find(container.begin(), container.end(), v);
    if (it != container.end()) {
      *it = std::forward<T>(v2);
    }
    return unit_t();
  }

  int size(const unit_t&) const {
    return container.size();
  }

  Derived<Elem> combine(const STLDS& other) const {
    Derived<Elem> result;
    result = STLDS(*this);
    for (auto& e : other.container) {
      result.insert(e);
    }
    return result;
  }

  tuple<Derived<Elem>, Derived<Elem>> split(const unit_t&) const {
    // Find midpoint
    const size_t size = container.size();
    const size_t half = size / 2;
    // Setup iterators
    const_iterator beg = container.begin();
    const_iterator mid = container.begin();
    std::advance(mid, half);
    const_iterator end = container.end();
    // Construct from iterators
    return std::make_tuple(Derived<Elem>(STLDS(beg, mid)), Derived<Elem>(STLDS(mid, end)));
  }

  template<typename Fun>
  unit_t iterate(Fun f) const {
    for (const Elem& e : container) {
      f(e);
    }
    return unit_t();
  }

  template<typename Fun>
  auto map(Fun f) const -> Derived<R_elem<RT<Fun, Elem>>>  const {
    Derived<R_elem<RT<Fun, Elem>>> result;
    for (const Elem &e : container) {
      result.insert(R_elem<RT<Fun, Elem>> { f(e) });
    }
    return result;
  }

  template<typename Fun>
  Derived<Elem> filter(Fun predicate) const {
    Derived<Elem> result;
    for (const Elem &e : container) {
      if (predicate(e)) {
        result.insert(e);
      }
    }
    return result;
  }

  template<typename Fun, typename Acc>
  Acc fold(Fun f, Acc acc) const {
    for (const Elem &e : container) {
      acc = f(std::move(acc))(e);
    }
    return acc;
  }

  template<typename F1, typename F2, typename Z>
  Derived<R_key_value<RT<F1, Elem>, Z>> groupBy(F1 grouper, F2 folder, const Z& init) const {
    // Create a map to hold partial results
    typedef RT<F1, Elem> K;
    unordered_map<K, Z> accs;

    for (const auto& elem : container) {
      K key = grouper(elem);
      if (accs.find(key) == accs.end()) {
        accs[key] = init;
      }

      accs[key] = folder(std::move(accs[key]))(elem);
    }

    // Build the R_key_value records and insert them into result
    Derived<R_key_value<RT<F1, Elem>, Z>> result;
    for (auto& it : accs) {
      // move out of the map as we iterate
      result.insert(R_key_value<K, Z> {std::move(it.first), std::move(it.second)});
    }
    return result;
  }

  template <class Fun>
  auto ext(Fun expand) const -> Derived<typename RT<Fun, Elem>::ElemType> {
    typedef typename RT<Fun, Elem>::ElemType T;
    Derived<T> result;
    for (const Elem& elem : container) {
      for (T& elem2 : expand(elem).container) {
        result.insert(std::move(elem2));
      }
    }
  }

  // Iterators
  using iterator = typename Container::iterator;
  using const_iterator = typename Container::const_iterator;
  using reverse_iterator = typename Container::reverse_iterator;

  iterator begin() {
    return container.begin();
  }

  iterator end() {
    return container.end();
  }

  const_iterator begin() const {
    return container.cbegin();
  }

  const_iterator end() const {
    return container.cend();
  }

  const_iterator cbegin() const {
    return container.cbegin();
  }

  const_iterator cend() const {
    return container.cend();
  }

  reverse_iterator rbegin() {
    return container.rbegin();
  }

  reverse_iterator rend() {
    return container.rend();
  }


  bool operator==(const STLDS& other) const {
    return container == other.container;
  }

  bool operator!=(const STLDS& other) const {
    return container != other.container;
  }

  bool operator<(const STLDS& other) const {
    return container < other.container;
  }

  bool operator>(const STLDS& other) const {
    return container < other.container;
  }

  template<class Archive>
  void serialize(Archive &ar) {
    ar & container;
  }

  template<class Archive>
  void serialize(Archive &ar, const unsigned int) {
    ar & container;
  }

 private:
  Container container;
  friend class boost::serialization::access;
};

}  // namespace K3

#endif
