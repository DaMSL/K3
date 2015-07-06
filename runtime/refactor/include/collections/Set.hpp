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
  typedef std::unordered_set<Elem> Container;
  typedef typename Container::const_iterator const_iterator_type;
  typedef typename Container::iterator iterator_type;

 public:
  typedef Elem ElemType;
  Set() : Super() {}
  Set(const Super& c) : Super(c) {}
  Set(Super&& c) : Super(std::move(c)) {}
  template <typename Iterator>
  Set(Iterator begin, Iterator end)
      : container(begin, end) {}

  //using iterator = typename Container::iterator;
  //using const_iterator = typename Container::const_iterator;

  //iterator begin() { return iterator(container.begin()); }

  //iterator end() { return iterator(container.end()); }

  //const_iterator begin() const { return const_iterator(container.cbegin()); }

  //const_iterator end() const { return const_iterator(container.cend()); }

  // Set specific functions
  bool member(const Elem& e) const {
    return container.find(e) != container.end();
  }

  bool isSubsetOf(const Set<Elem>& other) const {
    for (const auto& x : getConstContainer()) {
      if (!other.member(x)) {
        return false;
      }
    }
    return true;
  }

  // union is a reserved word
  Set<Elem> union1(const Set<Elem>& other) const {
    Set<Elem> result;
    for (const auto& x : getConstContainer()) {
      result.insert(x);
    }
    for (const auto& x : other.getConstContainer()) {
      result.insert(x);
    }
    return result;
  }

  Set<Elem> intersect(const Set<Elem>& other) const {
    Set<Elem> result;
    for (const auto& x : getConstContainer()) {
      if (other.member(x)) {
        result.insert(x);
      }
    }
    return result;
  }

  Set<Elem> difference(const Set<Elem>& other) const {
    Set<Elem> result;
    for (const auto& x : getConstContainer()) {
      if (!other.member(x)) {
        result.insert(x);
      }
    }
    return result;
  }

  Container& getContainer() { return container; }

  // Return a constant reference to the container
  const Container& getConstContainer() const { return container; }

  Container container;

  template<class Archive>
  void serialize(Archive &ar) {
    ar & yas::base_object<SetDS<K3::Set, Elem>>(*this);
  }

  template <class Archive>
  void serialize(Archive& ar, const unsigned int) {
    ar& boost::serialization::base_object<SetDS<K3::Set, Elem>>(
        *this);
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
