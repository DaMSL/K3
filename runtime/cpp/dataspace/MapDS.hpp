#ifndef K3_RUNTIME_DATASPACE_MAPDS_H
#define K3_RUNTIME_DATASPACE_MAPDS_H

#include <algorithm>
#include <iostream>
#include <map>
#include <string>
#include <functional>

// K3
#include <Engine.hpp>

namespace K3 {

using K3::Engine;
using std::shared_ptr;
using std::tuple;

using std::map;

// MapDS only works on R_key_value
template<class R>
class MapDS {
  using Key = typename R::KeyType;
  using Value = typename R::ValueType;
  using iterator_type = typename map<Key,Value>::iterator;
  using const_iterator_type = typename map<Key, Value>::const_iterator;

 public:
    // Constructors
  MapDS(Engine * eng) : container() {}

  template<typename Iterator>
  MapDS(Engine * eng, Iterator start, Iterator finish)
      : container(start,finish) {}

  MapDS(const MapDS& other) : container(other.container) {}

  MapDS(const map<Key,Value>& con) : container(con) {}

  const map<Key, Value>& getContainer() const { return container; }

  // DS Operations:
  // Maybe return the first element in the DS
  shared_ptr<R> peek() const {
    shared_ptr<R> res;
    const_iterator_type it = container.begin();
    if (it != container.end()) {
      res = std::make_shared<R>();
      res->key = it->first;
      res->value = it->second;
    }
    return res;
  }

  void insert(const R& rec) {
    container.insert(make_pair(rec.key,rec.value));
  }

  void erase(const R& rec) {
    iterator_type it;
    it = container.find(rec.key);
    if (it != container.end() && it->second == rec.value) {
        container.erase(it);
    }
  }

  void update(const R& rec1, const R& rec2) {
    iterator_type it;
    it = container.find(rec1.key);
    if (it != container.end()) {
      if (rec1.value == it->second) {
        container[rec2.key] = rec2.value;
      }
    }
  }

  // TODO: try to eliminate extra copy
  template<typename Acc>
  Acc fold(F<F<Acc(R)>(Acc)> f, const Acc& init_acc) {
    Acc acc = init_acc;
    for (const std::pair<Key, Value>& p : container) {
      R rec;
      rec.key = p.first;
      rec.value = p.second;
      acc = f(acc)(rec);
    }
    return acc;
  }

  template<typename NewElem>
  ListDS<NewElem> map(const F<NewElem(R)>& f) {
    ListDS<NewElem> result(getEngine());
    for (const std::pair<Key,Value>& p : container) {
      R rec;
      rec.key = p.first;
      rec.value = p.second;
      NewElem new_e = f(rec);
      result.insert(new_e);
    }
    return result;
  }

  unit_t iterate(const F<unit_t(R)>& f) {
    for (const std::pair<Key,Value>& p : container) {
      R rec;
      rec.key = p.first;
      rec.value = p.second;
      f(rec);
    }
    return unit_t();
  }

  MapDS<R> filter(const F<bool(R)>& predicate) {
    MapDS<R> result = MapDS<R>(getEngine());
    for (const std::pair<Key,Value>& p : container) {
      R rec;
      rec.key = p.first;
      rec.value = p.second;
      if (predicate(rec)) {
        result.insert(rec);
      }
    }
    return result;
  }

  tuple<MapDS, MapDS> split() {
    // Find midpoint
    const size_t size = container.size();
    const size_t half = size / 2;
    // Setup iterators
    const_iterator_type beg = container.begin();
    const_iterator_type mid = container.begin();
    std::advance(mid, half);
    const_iterator_type end = container.end();
    // Construct DS from iterators
    MapDS& p1 = MapDS(nullptr, beg, mid);
    MapDS& p2 = MapDS(nullptr, mid, end);
    return std::make_tuple(p1, p2);
  }

  MapDS combine(const MapDS& other) const {
    // copy this DS
    MapDS result = MapDS(nullptr, container.begin(), container.end());
    // copy other DS
    for (const std::pair<Key,Value>& p: other.container) {
      result.container.insert(p);
    }
    return result;
  }

 protected:
  std::map<Key,Value> container;

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
#endif // K3_RUNTIME_DATASPACE_MAPDS_H
