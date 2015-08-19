#ifndef K3_SORTED
#define K3_SORTED

#include <set>

#include "STLDataspace.hpp"
#include "serialization/Json.hpp"
#include "serialization/Yaml.hpp"

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
  Sorted(Iterator begin, Iterator end) : Super(begin, end) {}

  /////////////////////////////////////////////////
  // Modifier overrides to exploit container type.

  template <class T>
  unit_t update(const Elem& v, T&& v2) {
    auto& x = Super::getContainer();
    auto it = x.find(v);
    if (it != x.end()) {
      *it = std::forward<T>(v2);
    }
    return unit_t();
  }

  unit_t erase(const Elem& v) {
    auto& x = Super::getContainer();
    auto it = x.find(v);
    if (it != x.end()) {
      x.erase(it);
    }
    return unit_t();
  }


  //////////////////////////
  // Sort-order methods.

  template <class F, class G>
  auto min(F f, G g) const {
    // begin is the smallest element
    const auto& x = Super::getConstContainer();
    auto it = x.begin();
    if (it == x.end()) {
      return f(unit_t{});
    } else {
      return g(*it);
    }
  }

  template <class F, class G>
  auto max(F f, G g) const {
    const auto& x = Super::getConstContainer();
    auto it = x.rbegin();
    if (it == x.rend()) {
      return f(unit_t{});
    } else {
      return g(*it);
    }
  }

  template <class F, class G>
  auto lower_bound(const Elem& e, F f, G g) const {
    const auto& x = Super::getConstContainer();
    auto it = x.lower_bound(e);
    if (it == x.end()) {
      return f(unit_t{});
    } else {
      return g(*it);
    }
  }

  template <class F, class G>
  auto upper_bound(const Elem& e, F f, G g) const {
    const auto& x = Super::getConstContainer();
    auto it = x.upper_bound(e);
    if (it == x.end()) {
      return f(unit_t{});
    } else {
      return g(*it);
    }
  }

  Sorted filter_lt(const Elem& bound) const {
    const auto& x =Super::getConstContainer();
    auto it = x.lower_bound(bound);
    if (it != x.begin()) --it;
    return Sorted<Elem>(x.begin(), it);
  }

  Sorted filter_gt(const Elem& bound) const {
    const auto& x =Super::getConstContainer();
    auto it = x.upper_bound(bound);
    return Sorted<Elem>(it, x.end());
  }

  Sorted filter_geq(const Elem& bound) const {
    const auto& x =Super::getConstContainer();
    auto it = x.lower_bound(bound);
    return Sorted<Elem>(it, x.end());
  }

  Sorted filter_leq(const Elem& bound) const {
    const auto& x =Super::getConstContainer();
    auto it = x.upper_bound(bound);
    if (it != x.begin()) --it;
    return Sorted<Elem>(x.begin(), it);
  }

  Sorted between(const Elem& a, const Elem& b) const {
    const auto& x =Super::getConstContainer();
    auto it = x.lower_bound(a);
    auto end = x.upper_bound(b);
    if (it != x.end()) {
      return Sorted<Elem>(it, end);
    } else {
      return Sorted<Elem>();
    }
  }

  ///////////////////////////////////
  // Range-based modification.

  unit_t erase_before(const Elem& bound) {
    auto& x = Super::getContainer();
    auto it = x.lower_bound(bound);
    if (it != x.end()) {
      x.erase(x.cbegin(), it);
    }
    return unit_t();
  }

  unit_t erase_after(const Elem& bound) {
    auto& x = Super::getContainer();
    auto it = x.upper_bound(bound);
    if (it != x.end()) {
      x.erase(it, x.cend());
    }
    return unit_t();
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

namespace JSON {
using namespace rapidjson;
template <class E>
struct convert<K3::Sorted<E>> {
  template <class Allocator>
  static Value encode(const K3::Sorted<E>& c, Allocator& al) {
    Value v;
    v.SetObject();
    v.AddMember("type", Value("Sorted"), al);
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
