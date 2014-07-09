#ifndef K3_RUNTIME_DATASPACE_MAPDS_H
#define K3_RUNTIME_DATASPACE_MAPDS_H

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

using std::unordered_map;

template<class K, class V>
class R_key_value<K,V>;

template<class Key, class Value>
class MapDS {
  using  R_key_value<Key,Value> = RecType;
 public:
    // Constructors
  MapDS(Engine * eng) : map() {}

  template<typename Iterator>
  MapDS(Engine * eng, Iterator start, Iterator finish)
      : map(start,finish) {}

  MapDS(const MapDS& other) : map(other.map) {}

  MapDS(map<Key,Value>& con) : map(con) {}

  // DS Operations:
  // Maybe return the first element in the DS
  shared_ptr<RecType> peek() const {
    shared_ptr<RecType> res;
    const_iterator_type it = map.begin();
    if (it != map.end()) {
      RecType rec;
      rec.key = it->first;
      rec.value = it->second;
      res = std::make_shared<RecType>(rec);
    }
    return res;
  }

  void insert(const RecType& rec) {
    map.insert(make_pair(rec.key,rec.value));
  }

  void erase(const RecType& rec) {
    iterator_type it;
    it = map.find(rec.key);
    if (it != map.end()) {
      if (it->second == map.value) {
        map.erase(it);
      }
    }
  }

  void update(const RecType& rec1, const RecType& rec2) {
    iterator_type it;
    it = map.find(rec1.key);
    if (it != map.end()) {
      if (rec1.value == it->second) {
        map[rec1.key] = rec2.value;
      }
    }
  }

  template<typename Acc>
  Acc fold(F<F<Acc(RecType)>(Acc)> f, Acc init_acc) {
    Acc acc = init_acc;
    for (std::pair<Key,Value> p : map) {
      RecType rec;
      rec.key = p->first;
      rec.value = p->second;
      acc = f(acc)(rec);
    }
    return acc;
  }

  template<typename NewElem>
  ListDS<NewElem, Container> map(F<NewElem(Elem)> f) {
    ListDS<NewElem> result = ListDS<NewElem>(getEngine());
    for (std::pair<Key,Value> p : map) {
      RecType rec;
      rec.key = p->first;
      rec.value = p->second;
      NewElem new_e = f(rec);
      result.insert(new_e);
    }
    return result;
  }

  unit_t iterate(F<unit_t(Elem)> f) {
    for (Elem e : container) { f(e); }
    return unit_t();
  }

  MapDS<Key,Value> filter(F<bool(RecType)> predicate) {
    MapDS<Key, Value> result = MapDS<Key, Value>(getEngine());
    for (std::pair<Key,Value> p : map) {
      RecType rec;
      rec.key = p->first;
      rec.value = p->second;
      if (predicate(rec)) {
        result.insert(rec);
      }
    }
    return result;
  }

  tuple< MapDS, MapDS > split() {
    // Find midpoint
    size_t size = map.size();
    size_t half = size / 2;
    // Setup iterators
    const_iterator_type beg = map.begin();
    const_iterator_type mid = map.begin();
    std::advance(mid, half);
    const_iterator_type end = map.end();
    // Construct DS from iterators
    MapDS p1 = MapDS(nullptr, beg, mid);
    MapDS p2 = MapDS(nullptr, mid, end);
    return std::make_tuple(p1, p2);
  }

  MapDS combine(MapDS other) const {
    // copy this DS
    MapDS result = MapDS(nullptr,map.begin(), map.end());
    // copy other DS
    for (std::pair<Key,Value> p: other.map) {
      result.map.insert(p);
    }
    return result;
  }

 //Container<Elem> getContainer() { return container; }
 protected:
  unordered_map<Key,Value> map;

  // In-memory Dataspaces do not keep a handle to an engine
  Engine* getEngine() {return nullptr; }

 private:
  friend class boost::serialization::access;
  template<class Archive>
  void serialize(Archive &ar, const unsigned int version) {
    ar & map;
  }
};

}