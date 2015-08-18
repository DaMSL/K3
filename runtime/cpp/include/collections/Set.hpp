#ifndef K3_SET
#define K3_SET

#include <unordered_map>
#include <unordered_set>

#include "STLDataspace.hpp"
#include "serialization/Yaml.hpp"
#include "serialization/Json.hpp"

namespace K3 {

template <template <typename> class Derived, class Elem>
using SetDS = STLDS<Derived, std::unordered_set, Elem>;

template <class Elem>
class Set : public SetDS<K3::Set, Elem> {
  using Super = SetDS<K3::Set, Elem>;

 public:
  typedef Elem ElemType;
  Set() : Super() {}
  Set(const Super& c) : Super(c) {}
  Set(Super&& c) : Super(std::move(c)) {}

  /////////////////////////////////////////////////
  // Modifier overrides to exploit container type.

  template <class T>
  unit_t update(const Elem& v, T&& v2) {
    auto& x = Super::getContainer();
    iterator it = x.find(v);
    if (it != x.end()) {
      *it = std::forward<T>(v2);
    }
    return unit_t();
  }

  unit_t erase(const Elem& v) {
    auto& x = Super::getContainer();
    iterator it = x.find(v);
    if (it != x.end()) {
      x.erase(it);
    }
    return unit_t();
  }

  // Set specific functions
  bool member(const Elem& e) const {
    return Super::container.find(e) != Super::container.end();
  }

  bool is_subset_of(const Set<Elem>& other) const {
    for (const auto& x : this->getConstContainer()) {
      if (!other.member(x)) {
        return false;
      }
    }
    return true;
  }

  Set<Elem> intersect(const Set<Elem>& other) const {
    Set<Elem> result;
    for (const auto& x : this->getConstContainer()) {
      if (other.member(x)) {
        result.insert(x);
      }
    }
    return result;
  }

  Set<Elem> difference(const Set<Elem>& other) const {
    Set<Elem> result;
    for (const auto& x : this->getConstContainer()) {
      if (!other.member(x)) {
        result.insert(x);
      }
    }
    return result;
  }

  template<class Archive>
  void serialize(Archive &ar) {
    ar & yas::base_object<SetDS<K3::Set, Elem>>(*this);
  }

  template <class Archive>
  void serialize(Archive& ar, const unsigned int) {
    ar& boost::serialization::base_object<SetDS<K3::Set, Elem>>(*this);
  }

 private:
  friend class boost::serialization::access;
};

}  // namespace K3

namespace YAML {
template <class E>
struct convert<K3::Set<E>> {
  static Node encode(const K3::Set<E>& c) {
    Node node;
    for (auto i : c.getConstContainer()) {
      node.push_back(convert<E>::encode(i));
    }
    return node;
  }

  static bool decode(const Node& node, K3::Set<E>& c) {
    for (auto& i : node) {
      c.insert(i.as<E>());
    }
    return true;
  }
};
}  // namespace YAML

namespace JSON {
using namespace rapidjson;
template <class E>
struct convert<K3::Set<E>> {
  template <class Allocator>
  static Value encode(const K3::Set<E>& c, Allocator& al) {
    Value v;
    v.SetObject();
    v.AddMember("type", Value("Set"), al);
    Value inner;
    inner.SetArray();
    for (const auto& e : c.getConstContainer()) {
      inner.PushBack(convert<E>::encode(e, al), al);
    }
    v.AddMember("value", inner.Move(), al);
    return v;
  }
};

}  // namespace JSON
#endif
