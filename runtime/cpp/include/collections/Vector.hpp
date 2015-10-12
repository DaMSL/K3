#ifndef K3_VECTOR
#define K3_VECTOR

#include "boost/serialization/vector.hpp"
#include "boost/serialization/base_object.hpp"

#include "STLDataspace.hpp"
#include "serialization/Json.hpp"
#include "serialization/Yaml.hpp"

namespace K3 {
template <template <class> class Derived, class Elem>
using VectorDS = STLDS<Derived, std::vector, Elem>;

template <class Elem>
class Vector : public VectorDS<K3::Vector, Elem> {
  using Super = VectorDS<K3::Vector, Elem>;

 public:
  Vector() : Super() {}
  Vector(const Super& c) : Super(c) {}
  Vector(Super&& c) : Super(std::move(c)) {}

  Elem at(int i) const {
    auto& vec = Super::getConstContainer();
    return vec[i];
  }

  template<class F, class G>
  auto safe_at(int i, F f, G g) const {
    auto& vec = Super::getConstContainer();
    if (i < vec.size()) {
      return g(vec[i]);
    } else {
      return f(unit_t {});
    }
  }

  template<class F>
  auto unsafe_at(int i, F f) const {
    auto& vec = Super::getConstContainer();
    return f(vec[i]);
  }

  template <class Q>
  unit_t set(int i, Q&& q) {
    auto& vec = Super::getContainer();
    vec[i] = std::forward<Q>(q);
    return unit_t();
  }

  template <class Q>
  unit_t insert_at(int i, Q&& q) {
    auto& vec = Super::getContainer();
    if (i >= vec.size()) {
      vec.resize(i + 1);
    }
    vec[i] = std::forward<Q>(q);
    return unit_t();
  }

  template <class F>
  unit_t update_at(int i, F f) {
    auto& vec = Super::getContainer();
    vec[i] = f(std::move(vec[i]));
    return unit_t();
  }

  auto erase_at(int i) {
    auto& vec = Super::getContainer();
    return std::move(vec[i]);
  }

  template <class Q>
  Elem swap(int i, Q&& q) {
    auto& vec = Super::getContainer();
    auto old = std::move(vec[i]);
    vec[i] = std::forward<Q>(q);
    return old;
  }

  template<class Archive>
  void serialize(Archive &ar) {
    ar & yas::base_object<VectorDS<K3::Vector, Elem>>(*this);
  }

  template <class Archive>
  void serialize(Archive& ar, const unsigned int) {
    ar& boost::serialization::base_object<VectorDS<K3::Vector, Elem>>(
        *this);
  }

 private:
  friend class boost::serialization::access;
};


// Specialization for a vector of bools.
template <>
class Vector<R_elem<bool>> : public VectorDS<K3::Vector, bool> {
  using Elem = bool;
  using RElem = R_elem<bool>;
  using Super = VectorDS<K3::Vector, bool>;
  using Container = std::vector<bool>;

 public:
  Vector() : Super() {}
  Vector(const Super& c) : Super(c) {}
  Vector(Super&& c) : Super(std::move(c)) {}

  template <class I>
  class bool_iterator : public std::iterator<std::forward_iterator_tag, RElem>
  {
   public:
    template <class _I>
    bool_iterator(Container& _c, _I&& _i) : c(_c), i(static_cast<I>(std::forward<_I>(_i))) {}

    bool_iterator& operator++() { i++; return *this; }
    bool_iterator operator++(int) { bool_iterator t = *this; i++; return t; }

    auto operator -> () const {
      current = RElem(*i);
      return &current;
    }

    auto& operator*() const {
      current = RElem(*i);
      return current;
    }

    bool operator==(const bool_iterator& other) const { return i == other.i; }
    bool operator!=(const bool_iterator& other) const { return i != other.i; }

   private:
    Container& c;
    I i;
    RElem current;
  };

  using iterator = bool_iterator<std::vector<bool>::iterator>;
  using const_iterator = bool_iterator<std::vector<bool>::const_iterator>;

  iterator begin() {
    auto c = Super::getContainer();
    return iterator(c, c.begin());
  }

  iterator end() {
    auto c = Super::getContainer();
    return iterator(c, c.end());
  }

  const_iterator begin() const {
    auto c = Super::getConstContainer();
    return const_iterator(c, c.begin());
  }

  const_iterator end() const {
    auto c = Super::getConstContainer();
    return const_iterator(c, c.end());
  }

  ////////////////////////////////////
  // Accessors.

  template <class F, class G>
  auto peek(F f, G g) const {
    auto it = container.begin();
    if (it == container.end()) {
      return f(unit_t{});
    } else {
      return g(RElem{ *it });
    }
  }

  RElem at(int i) const {
    auto& vec = Super::getConstContainer();
    return RElem{ vec[i] };
  }

  template<class F, class G>
  auto safe_at(int i, F f, G g) const {
    auto& vec = Super::getConstContainer();
    if (i < vec.size()) {
      return g(RElem{ vec[i] });
    } else {
      return f(unit_t {});
    }
  }

  template<class F>
  auto unsafe_at(int i, F f) const {
    auto& vec = Super::getConstContainer();
    return f(RElem{ vec[i] });
  }

  unit_t set(int i, RElem&& v) {
    auto& vec = Super::getContainer();
    vec[i] = v.elem;
    return unit_t();
  }

  unit_t insert(RElem &&e) {
    container.insert(container.end(), e.elem);
    return unit_t();
  }

  unit_t update(const RElem& v, RElem&& v2) {
    auto it = std::find(container.begin(), container.end(), v.elem);
    if (it != container.end()) {
      *it = v2.elem;
    }
    return unit_t();
  }

  unit_t erase(const RElem& v) {
    auto it = std::find(container.begin(), container.end(), v.elem);
    if (it != container.end()) {
      container.erase(it);
    }
    return unit_t();
  }

  unit_t insert_at(int i, RElem&& v) {
    auto& vec = Super::getContainer();
    if (i >= vec.size()) {
      vec.resize(i + 1);
    }
    vec[i] = v.elem;
    return unit_t();
  }

  template <class F>
  unit_t update_at(int i, F f) {
    auto& vec = Super::getContainer();
    vec[i] = f(RElem{ vec[i] }).elem;
    return unit_t();
  }

  auto erase_at(int i) {
    auto& vec = Super::getContainer();
    return RElem { vec[i] };
  }

  RElem swap(int i, RElem&& q) {
    auto& vec = Super::getContainer();
    auto old = vec[i];
    vec[i] = q.elem;
    return RElem{ old };
  }

  template<typename Fun>
  unit_t iterate(Fun f) const {
    for (const Elem& e : container) {
      f(RElem{ e });
    }
    return unit_t();
  }

  template<typename Fun>
  auto map(Fun f) const -> Vector<R_elem<RT<Fun, RElem>>>  const {
    Vector<R_elem<RT<Fun, RElem>>> result;
    for (const Elem &e : container) {
      result.insert(R_elem<RT<Fun, RElem>> { f(RElem{ e }) });
    }
    return result;
  }

  template<typename Fun>
  Vector<RElem> filter(Fun predicate) const {
    Vector<RElem> result;
    for (const Elem &e : container) {
      if (predicate(RElem{ e })) {
        result.insert(e);
      }
    }
    return result;
  }

  template<typename Fun, typename Acc>
  Acc fold(Fun f, Acc acc) const {
    for (const Elem &e : container) {
      acc = f(std::move(acc), RElem{ e });
    }
    return acc;
  }

  // TODO: group by and ext transformers.

  template<class Archive>
  void serialize(Archive &ar) {
    ar & yas::base_object<VectorDS<K3::Vector, Elem>>(*this);
  }

  template <class Archive>
  void serialize(Archive& ar, const unsigned int) {
    ar& boost::serialization::base_object<VectorDS<K3::Vector, Elem>>(*this);
  }

 private:
  friend class boost::serialization::access;
};

}  // namespace K3

namespace YAML {
template <class E>
struct convert<K3::Vector<E>> {
  static Node encode(const K3::Vector<E>& c) {
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

  static bool decode(const Node& node, K3::Vector<E>& c) {
    for (auto& i : node) {
      c.insert(i.as<E>());
    }

    return true;
  }
};

// Specialization for a vector of bools.
template <>
struct convert<K3::Vector<R_elem<bool>>> {
  static Node encode(const K3::Vector<R_elem<bool>>& c) {
    Node node;
    auto container = c.getConstContainer();
    if (container.size() > 0) {
      for (auto i : container) {
        node.push_back(convert<bool>::encode(i));
      }
    } else {
      node = YAML::Load("[]");
    }
    return node;
  }

  static bool decode(const Node& node, K3::Vector<R_elem<bool>>& c) {
    for (auto& i : node) {
      c.insert(i.as<bool>());
    }

    return true;
  }
};
}  // namespace YAML

namespace JSON {
using namespace rapidjson;
template <class E>
struct convert<K3::Vector<E>> {
  template <class Allocator>
  static Value encode(const K3::Vector<E>& c, Allocator& al) {
    Value v;
    v.SetObject();
    v.AddMember("type", Value("Vector"), al);
    Value inner;
    inner.SetArray();
    for (const auto& e : c.getConstContainer()) {
      inner.PushBack(convert<E>::encode(e, al), al);
    }
    v.AddMember("value", inner.Move(), al);
    return v;
  }
};

template <>
struct convert<K3::Vector<R_elem<bool>>> {
  template <class Allocator>
  static Value encode(const K3::Vector<R_elem<bool>>& c, Allocator& al) {
    Value v;
    v.SetObject();
    v.AddMember("type", Value("Vector"), al);
    Value inner;
    inner.SetArray();
    for (const auto& e : c.getConstContainer()) {
      inner.PushBack(convert<bool>::encode(e, al), al);
    }
    v.AddMember("value", inner.Move(), al);
    return v;
  }
};
}  // namespace JSON

#endif
