#ifndef K3_SORTED
#define K3_SORTED

#include <set>

#include "STLDataspace.hpp"

namespace K3 {
template <template <typename> class Derived, class Elem>
using SortedDS = STLDS<Derived, std::multiset, Elem>;

template <class Elem>
class Sorted : public SortedDS<K3::Sorted, Elem> {
  typedef std::multiset<Elem> Container;
  using const_iterator_type = typename Container::const_iterator;
  using iterator_type = typename Container::iterator;
  using Super = SortedDS<K3::Sorted, Elem>;

 public:
  typedef Elem ElemType;
  Sorted() : Super() {}
  Sorted(const Container& con) : Super(con) {}
  Sorted(Container&& con) : Super(std::move(con)) {}
  template <typename Iterator>
  Sorted(Iterator begin, Iterator end)
      : Super(begin, end) {}

  std::shared_ptr<Elem> min() const {
    // begin is the smallest element
    const auto& x = Super::getConstContainer();
    auto it = x.begin();
    std::shared_ptr<Elem> result(nullptr);
    if (it != x.end()) {
      result = std::make_shared<Elem>(*it);
    }
    return result;
  }

  std::shared_ptr<Elem> max() const {
    const auto& x = Super::getConstContainer();
    auto it = x.rbegin();
    std::shared_ptr<Elem> result(nullptr);
    if (it != x.rend()) {
      result = std::make_shared<Elem>(*it);
    }
    return result;
  }

  std::shared_ptr<Elem> lowerBound(const Elem& e) const {
    const auto& x = Super::getConstContainer();
    auto it = x.lower_bound(e);
    std::shared_ptr<Elem> result(nullptr);
    if (it != x.end()) {
      result = std::make_shared<Elem>(*it);
    }
    return result;
  }

  std::shared_ptr<Elem> upperBound(const Elem& e) const {
    const auto& x = Super::getConstContainer();
    auto it = x.upper_bound(e);
    std::shared_ptr<Elem> result(nullptr);
    if (it != x.end()) {
      result = std::make_shared<Elem>(*it);
    }
    return result;
  }

  Sorted<Elem> slice(const Elem& a, const Elem& b) const {
    const auto& x = Super::getConstContainer();
    Sorted<Elem> result;
    for (Elem e : x) {
      if (e >= a && e <= b) {
        result.insert(e);
      }
      if (e > b) {
        break;
      }
    }
    return result;
  }

  template<class Archive>
  void serialize(Archive &ar) {
   ar & yas::base_object<SortedDS<K3::Sorted, Elem>>(*this);
  }

  template <class Archive>
  void serialize(Archive& ar, const unsigned int) {
    ar& boost::serialization::base_object<SortedDS<K3::Sorted, Elem>>(
        *this);
  }

 private:
  friend class boost::serialization::access;
};

}  // namespace K3

namespace YAML {
template <class E>
struct convert<K3::Sorted<E>> {
  static Node encode(const K3::Sorted<E>& c) {
    Node node;
    for (auto i : c.getConstContainer()) {
      node.push_back(convert<E>::encode(i));
    }

    return node;
  }

  static bool decode(const Node& node, K3::Sorted<E>& c) {
    for (auto& i : node) {
      c.insert(i.as<E>());
    }

    return true;
  }
};
}  // namespace YAML

#endif
