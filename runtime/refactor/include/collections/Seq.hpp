#ifndef K3_SEQ
#define K3_SEQ

#include <algorithm>
#include <list>

#include "STLDataspace.hpp"
#include "serialization/Json.hpp"
#include "serialization/Yaml.hpp"

namespace K3 {

template <template <typename> class Derived, class Elem>
using ListDS = STLDS<Derived, std::list, Elem>;

template <class Elem>
class Seq : public ListDS<K3::Seq, Elem> {
  using Super = ListDS<K3::Seq, Elem>;

 public:
  Seq() : Super() {}
  Seq(const Super& c) : Super(c) {}
  Seq(Super&& c) : Super(std::move(c)) {}

  // Seq specific functions
  template <class F>
  Seq sort(F comp) {
    auto l(Super::getContainer());
    auto f = [&](Elem& a, Elem& b) mutable { return comp(a)(b) < 0; };
    l.sort(f);
    return Seq(Super(std::move(l)));
  }

  Elem at(int i) const {
    auto& l = Super::getConstContainer();
    auto it = l.begin();
    std::advance(it, i);
    return *it;
  }

  template<class Archive>
  void serialize(Archive &ar) {
    ar & yas::base_object<ListDS<K3::Seq, Elem>>(*this);
  }

  template <class Archive>
  void serialize(Archive& ar, const unsigned int) {
    ar& boost::serialization::base_object<ListDS<K3::Seq, Elem>>(
        *this);
  }

 private:
  friend class boost::serialization::access;
};
}  // namespace K3

namespace YAML {
template <class E>
struct convert<K3::Seq<E>> {
  static Node encode(const K3::Seq<E>& c) {
    Node node;
    auto container = c.getConstContainer();
    if (container.size() > 0) {
      for (auto i : container) {
        node.push_back(convert<E>::encode(i));
      }
    } else {
      node = YAML::Load("[]");
    }

    return node;
  }

  static bool decode(const Node& node, K3::Seq<E>& c) {
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
struct convert<K3::Seq<E>> {
  template <class Allocator>
  static Value encode(const K3::Seq<E>& c, Allocator& al) {
    Value v;
    v.SetObject();
    v.AddMember("type", Value("Seq"), al);
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
