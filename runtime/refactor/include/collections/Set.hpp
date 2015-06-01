#ifndef K3_SET
#define K3_SET

#include <set>

#include "STLDataspace.hpp"

// TODO(jbw) switch to unordered set

namespace K3 {

template <template <typename> class Derived, class Elem>
using SetDS = STLDS<Derived, std::set, Elem>;

template <class Elem>
class Set {
  typedef std::set<Elem> Container;
  typedef typename Container::const_iterator const_iterator_type;
  typedef typename Container::iterator iterator_type;

 public:
  typedef Elem ElemType;
  Set() : container() {}
  Set(const Container& con) : container(con) {}
  Set(Container&& con) : container(std::move(con)) {}
  template <typename Iterator>
  Set(Iterator begin, Iterator end)
      : container(begin, end) {}

  using iterator = typename Container::iterator;
  using const_iterator = typename Container::const_iterator;

  iterator begin() { return iterator(container.begin()); }

  iterator end() { return iterator(container.end()); }

  const_iterator begin() const { return const_iterator(container.cbegin()); }

  const_iterator end() const { return const_iterator(container.cend()); }

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

  template <class Archive>
  void serialize(Archive& ar) {
    ar& container;
  }

  template <class Archive>
  void serialize(Archive& ar, const unsigned int) {
    ar& boost::serialization::make_nvp("__K3Set", container);
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

#endif
