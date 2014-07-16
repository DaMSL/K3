#ifndef K3_RUNTIME_DATASPACE_STLDS_H
#define K3_RUNTIME_DATASPACE_STLDS_H

#include <algorithm>
#include <iostream>
#include <set>
#include <string>
#include <functional>
// K3
#include <Engine.hpp>

namespace K3 {

using K3::Engine;
using std::shared_ptr;
using std::tuple;

template <typename Elem, template<typename...> class Container>
class StlDS {
  // Iterator Types
  typedef typename Container<Elem>::iterator iterator_type;
  typedef typename Container<Elem>::const_iterator const_iterator_type;

 public:
  // Constructors
  StlDS(Engine* eng) {}

  template<typename Iterator>
  StlDS(Engine* eng, Iterator start, Iterator finish)
      : container(start,finish) {}

  StlDS(const StlDS& other) : container(other.container) {}

  StlDS(const Container<Elem>& con) : container(con) {}

  // DS Operations:
  // Maybe return the first element in the DS
  shared_ptr<Elem> peek() const {
    shared_ptr<Elem> res;
    const_iterator_type it = container.begin();
    if (it != container.end()) {
      res = std::make_shared<Elem>(*it);
    }
    return res;
  }

  void insert(const Elem& v) {
    container.insert(container.end(),v);
  }

  void erase(const Elem& v) {
    iterator_type it;
    it = std::find(container.begin(), container.end(), v);
    if (it != container.end()) {
      container.erase(it);
    }
  }

  void update(const Elem& v, const Elem& v2) {
    iterator_type it;
    it = std::find(container.begin(), container.end(), v);
    container.insert(it, v2);
    container.erase(it);
  }

  template<typename Acc>
  Acc fold(F<F<Acc(Elem)>(Acc)> f, const Acc& init_acc) {
    Acc acc = init_acc;
    for (const Elem &e : container) { acc = f(acc)(e); }
    return acc;
  }

  template<typename NewElem>
  StlDS<NewElem, Container> map(const F<NewElem(Elem)>& f) {
    StlDS<NewElem, Container> result(getEngine());
    for (const Elem &e : container) {
      result.insert(f(e));
    }
    return result;
  }

  unit_t iterate(const F<unit_t(Elem)>& f) {
    for (const Elem& e : container) {
      f(e);
    }
    return unit_t();
  }

  StlDS filter(const F<bool(Elem)>& predicate) {
    StlDS<Elem, Container> result(getEngine());
    for (const Elem &e : container) {
      if (predicate(e)) {
        result.insert(e);
      }
    }
    return result;
  }

  tuple<StlDS, StlDS> split() {
    // Find midpoint
    const size_t size = container.size();
    const size_t half = size / 2;
    // Setup iterators
    const_iterator_type beg = container.begin();
    const_iterator_type mid = container.begin();
    std::advance(mid, half);
    const_iterator_type end = container.end();
    // Construct DS from iterators
    StlDS p1(nullptr, beg, mid);
    StlDS p2(nullptr, mid, end);
    return std::make_tuple(p1, p2);
  }

  StlDS combine(const StlDS& other) const {
    // copy this DS
    StlDS result = StlDS(nullptr, container.begin(), container.end());
    // copy other DS
    for (const Elem &e : other.container) {
      result.insert(e);
    }
    return result;
  }

  int size(unit_t) const {
    return container.size();
  }

 Container<Elem>& getContainer() { return container; }

 const Container<Elem>& getConstContainer() const { return container; }

 Engine* getEngine() {return nullptr; }
 protected:
  Container<Elem> container;


  // In-memory Dataspaces do not keep a handle to an engine


 private:
  friend class boost::serialization::access;
  template<class Archive>
  void serialize(Archive &ar, const unsigned int version) {
    ar & container;
  }

};
}

#endif // K3_RUNTIME_DATASPACE_STLDS_H
