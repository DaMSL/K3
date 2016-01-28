#ifndef K3_BITSET
#define K3_BITSET

#include <vector>

#include "serialization/Yaml.hpp"
#include "serialization/Json.hpp"

namespace K3 {

template <class Ignore>
class BitSet {

 public:

  unit_t clear(unit_t) {
    std::clear();
    return unit_t();
  }

  unit_t erase_b(int v) {
    container[v] = 0;
    return unit_t();
  }

  unit_t insert_b(int v) {
    container[v] = 1;
    return unit_t();
  }

  bool member_b(int v) {
    return container[v];
  }

  template<typename Fun>
  unit_t iterate_b(Fun f) const {
    int count = 0;
    for (bool e : container) {
      if (e) {
        f(count);
      }
      ++count;
    }
    return unit_t();
  }

  template<typename Fun, typename Acc>
  Acc fold(Fun f, Acc acc) const {
    int count = 0;
    for (bool e : container) {
      if (e) {
        acc = f(std::move(acc), count);
      }
      ++count;
    }
    return acc;
  }

  template<class Archive>
  void serialize(Archive &ar) {
    ar & container;
  }

  template <class Archive>
  void serialize(Archive& ar, const unsigned int) {
    ar & container;
  }

  const std::vector<bool>& getConstContainer() { return container; }
  std::vector<bool>& getContainer() { return container; }

 private:
  std::vector<bool> container;
  friend class boost::serialization::access;
};

}  // namespace K3

namespace YAML {
template <class E>
struct convert<K3::BitSet<E>> {
  static Node encode(const K3::BitSet<E>& c) {
    Node node;
    int count = 0;
    for (auto i : c.getConstContainer()) {
      if (i) {
        node.push_back(convert<int>::encode(count));
      }
      ++count;
    }
    return node;
  }

  static bool decode(const Node& node, K3::BitSet<E>& c) {
    for (auto& i : node) {
      c.insert_b(i.as<int>());
    }
    return true;
  }
};
}  // namespace YAML

namespace JSON {
using namespace rapidjson;
template <class E>
struct convert<K3::BitSet<E>> {
  template <class Allocator>
  static Value encode(const K3::BitSet<E>& c, Allocator& al) {
    Value v;
    v.SetObject();
    v.AddMember("type", Value("BitSet"), al);
    Value inner;
    inner.SetArray();
    int count = 0;
    for (auto e : c.getConstContainer()) {
      if (e) {
        inner.PushBack(convert<int>::encode(count, al), al);
      }
      ++count;
    }
    v.AddMember("value", inner.Move(), al);
    return v;
  }
};

}  // namespace JSON
#endif // K3_BITSET
