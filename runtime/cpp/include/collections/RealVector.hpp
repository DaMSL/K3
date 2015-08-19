#ifndef K3_REALVECTOR
#define K3_REALVECTOR

#include <vector>
#include <string>

#include "yaml-cpp/yaml.h"
#include "boost/serialization/vector.hpp"
#include "boost/serialization/base_object.hpp"

#include "Common.hpp"
#include "STLDataspace.hpp"
#include "serialization/Json.hpp"
#include "serialization/Yaml.hpp"

namespace K3 {

template <class Elem>
class RealVector : public STLDS<K3::RealVector, std::vector, Elem> {
  using Super = STLDS<K3::RealVector, std::vector, Elem>;

 public:
  RealVector() : Super() {}
  RealVector(const Super& c) : Super(c) {}
  RealVector(Super&& c) : Super(std::move(c)) {}

  Elem at(int i) const {
    auto& vec = Super::getConstContainer();
    return vec[i];
  }

  template<class F, class G>
  auto safe_at(int i, F f, G g) const {
    auto& vec = Super::getConstContainer();
    if ( i < vec.size() ) {
      return g(vec[i]);
    } else {
      return f(unit_t {});
    }
  }

  unit_t set(int i, Elem f) {
    auto& vec = Super::getContainer();
    vec[i] = f;
    return unit_t();
  }

  Elem swap(int i, Elem n) {
    auto& vec = Super::getContainer();
    auto old = vec[i];
    vec[i] = n;
    return old;
  }

  ///////////////////////////////////////////////////
  // Vector-specific transformations.

  unit_t add_with(const RealVector<Elem>& other) {
    auto& vec = Super::getContainer();
    auto& other_vec = other.getConstContainer();
    if (vec.size() != other_vec.size()) {
      throw std::runtime_error("RealVector add_with size mismatch");
    }

    #pragma clang loop vectorize(enable) interleave(enable)
    for (int i = 0; i < vec.size(); i++) {
      vec[i].elem += other_vec[i].elem;
    }

    return unit_t{};
  }

  RealVector<Elem> add(const RealVector<Elem>& other) const {
    auto copy = RealVector<Elem>(*this);
    copy.add_with(other);
    return copy;
  }

  RealVector<Elem> add(RealVector<Elem>&& other) const {
    other.add_with(*this);
    return other;
  }

  unit_t sub_with(const RealVector<Elem>& other) {
    auto& vec = Super::getContainer();
    auto& other_vec = other.getConstContainer();
    if (vec.size() != other_vec.size()) {
      throw std::runtime_error("RealVector sub_with size mismatch");
    }

    #pragma clang loop vectorize(enable) interleave(enable)
    for (int i = 0; i < vec.size(); i++) {
      vec[i].elem -= other_vec[i].elem;
    }

    return unit_t{};
  }

  unit_t sub_from(const RealVector<Elem>& other) {
    auto& vec = Super::getContainer();
    auto& other_vec = other.getConstContainer();
    if (vec.size() != other_vec.size()) {
      throw std::runtime_error("RealVector sub_with size mismatch");
    }

    #pragma clang loop vectorize(enable) interleave(enable)
    for (int i = 0; i < vec.size(); i++) {
      vec[i].elem = other_vec[i].elem - vec[i].elem;
    }

    return unit_t{};
  }

  RealVector<Elem> sub(const RealVector<Elem>& other) const {
    auto copy = RealVector<Elem>(*this);
    copy.sub_with(other);
    return copy;
  }

  RealVector<Elem> sub(RealVector<Elem>&& other) const {
    other.sub_from(*this);
    return other;
  }

  double dot(const RealVector<Elem>& other) const {
    auto& vec = Super::getConstContainer();
    auto& other_vec = other.getConstContainer();
    if (vec.size() != other_vec.size()) {
      throw std::runtime_error("RealVector dot size mismatch");
    }

    double d = 0;

    #pragma clang loop vectorize(enable) interleave(enable)
    for (int i = 0; i < vec.size(); i++) {
      d += vec[i].elem * other_vec[i].elem;
    }
    return d;
  }

  double distance(const RealVector<Elem>& other) const {
    double d = 0;
    auto& vec = Super::getConstContainer();
    auto& other_vec = other.getConstContainer();
    if (vec.size() != other_vec.size()) {
      throw std::runtime_error("RealVector distance size mismatch");
    }

    #pragma clang loop vectorize(enable) interleave(enable)
    for (int i = 0; i < vec.size(); i++) {
      d += pow(vec[i].elem - other_vec[i].elem, 2);
    }

    return sqrt(d);
  }

  string toString(unit_t) const {
    std::ostringstream oss;
    auto& c = Super::getConstContainer();
    int i = 0;
    oss << "[";
    for (auto& relem : c) {
      oss << relem.elem;
      i++;
      if (i < c.size()) {
        oss << ",";
      }
    }
    oss << "]";
    return oss.str();
  }

  // PERF: RealVectorize.
  unit_t scale_with(double c) {
    #pragma clang loop vectorize(enable) interleave(enable)
    for (auto& i : this->getContainer()) {
      i.elem *= c;
    }
  }

  RealVector<Elem> scale(double c) const {
    auto result = RealVector<Elem>(*this);
    auto& vec = result.getContainer();

    #pragma clang loop vectorize(enable) interleave(enable)
    for (auto i = 0; i < vec.size(); ++i) {
      vec[i].elem *= c;
    }

    return result;
  }

  template <class G, class F, class Z>
  RealVector<R_elem<Z>> group_by(G grouper, F folder, const Z& zero) const
  {
    typedef RT<G, Elem> K;
    auto table = std::vector<R_elem<Z>>();
    for (const auto& elem : Super::getContainer()) {
      K key = grouper(elem);
      if (table.size() < key) {
        table.resize(key, zero);
      }
      table[key] = folder(std::move(table[key]), elem);
    }
    return RealVector<R_elem<Z>>{ std::move(Super(std::move(table))) };
  }

  template <typename F1, typename F2, typename Z>
  Vector<R_key_value<RT<F1, Elem>, Z>>
  group_by_generic(F1 grouper, F2 folder, const Z& init) const
  {
    // Create a map to hold partial results
    typedef RT<F1, Elem> K;
    unordered_map<K, Z> accs;

    for (const auto& elem : Super::getContainer()) {
      K key = grouper(elem);
      if (accs.find(key) == accs.end()) {
        accs[key] = init;
      }
      accs[key] = folder(std::move(accs[key]), elem);
    }

    Vector<R_key_value<RT<F1, Elem>, Z>> result;
    for (auto& it : accs) {
      // move out of the map as we iterate
      result.insert(R_key_value<K, Z>{std::move(it.first), std::move(it.second)});
    }
    return result;
  }

  template <class G, class F, class Z>
  RealVector<R_elem<Z>>
  group_by_contiguous(G grouper, F folder, const Z& zero, const int& size) const
  {
    auto table = std::vector<R_elem<Z>>(size, R_elem<Z> { zero });
    for (const auto& elem : Super::getContainer()) {
      auto key = grouper(elem);
      table[key] = folder(std::move(table[key]), elem);
    }
    return RealVector<R_elem<Z>>{ std::move(Super(std::move(table))) };
  }

  template <class Fun>
  auto ext_generic(Fun expand) const -> Vector<typename RT<Fun, Elem>::ElemType> {
    typedef typename RT<Fun, Elem>::ElemType T;
    Vector<T> result;
    for (const Elem& elem : Super::getContainer()) {
      for (T&& elem2 : expand(elem).container) {
        result.insert(std::move(elem2));
      }
    }
    return result;
  }
};

}  // namespace K3

namespace YAML {

template <class E>
struct convert<K3::RealVector<E>> {
  static Node encode(const K3::RealVector<E>& c) {
    Node node;
    auto container = c.getConstContainer();
    if (container.size() > 0) {
      for (auto i : c) {
        node.push_back(convert<E>::encode(i));
      }
    } else {
      node = YAML::Load("[]");
    }
    return node;
  }

  static bool decode(const Node& node, K3::RealVector<E>& c) {
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
struct convert<K3::RealVector<E>> {
  template <class Allocator>
  static Value encode(const K3::RealVector<E>& c, Allocator& al) {
    Value v;
    v.SetObject();
    v.AddMember("type", Value("RealVector"), al);
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
