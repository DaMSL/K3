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
  StlDS(Engine * eng) : container() {}

  template<typename Iterator>
  StlDS(Engine * eng, Iterator start, Iterator finish)
      : container(start,finish) {}

  StlDS(const StlDS& other) : container(other.container) {}

  StlDS(Container<Elem>& con) : container(con) {}

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
  Acc fold(std::function<Acc(Acc, Elem)> f, Acc init_acc) {
    Acc acc = init_acc;
    for (Elem e : container) { acc = f(acc, e); }
    return acc;
  }

  template<typename NewElem>
  StlDS<NewElem, Container> map(std::function<NewElem(Elem)> f) {
    StlDS<NewElem, Container> result = StlDS<NewElem, Container>(getEngine());
    for (Elem e : container) {
      NewElem new_e = f(e);
      result.insert(new_e);
    }
    return result;
  }

  void iterate(std::function<void(Elem)> f) {
    for (Elem e : container) { f(e); }
  }

  StlDS filter(std::function<bool(Elem)> predicate) {
    StlDS<Elem, Container> result = StlDS<Elem, Container>(getEngine());
    for (Elem e : container) {
      if (predicate(e)) {
        result.insert(e);
      }
    }
    return result;
  }

  tuple< StlDS, StlDS > split() {
    // Find midpoint
    size_t size = container.size();
    size_t half = size / 2;
    // Setup iterators
    const_iterator_type beg = container.begin();
    const_iterator_type mid = container.begin();
    std::advance(mid, half);
    const_iterator_type end = container.end();
    // Construct DS from iterators
    StlDS p1 = StlDS(nullptr, beg, mid);
    StlDS p2 = StlDS(nullptr, mid, end);
    return std::make_tuple(p1, p2);
  }

  StlDS combine(StlDS other) {
    // copy this DS
    StlDS result = StlDS(nullptr,container.begin(), container.end());
    // copy other DS
    for (Elem e : other.container) {
      result.insert(e);
    }
    return result;
  }
 Container<Elem> getContainer() { return container; }
 protected:
  Container<Elem> container;


  // In-memory Dataspaces do not keep a handle to an engine
  Engine* getEngine() {return nullptr; }

 private:
  friend class boost::serialization::access;
  template<class Archive>
  void serialize(Archive &ar, const unsigned int version) {
    ar & container;
  }

};
}

#endif // K3_RUNTIME_DATASPACE_STLDS_H
