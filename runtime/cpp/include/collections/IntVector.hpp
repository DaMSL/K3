#ifndef K3_INTVECTOR
#define K3_INTVECTOR

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
class IntVector : public STLDS<K3::IntVector, std::vector, Elem> {
  using Super = STLDS<K3::IntVector, std::vector, Elem>;

 public:
  IntVector() : Super() {}
  IntVector(const Super& c) : Super(c) {}
  IntVector(Super&& c) : Super(std::move(c)) {}

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

  unit_t add_with(const IntVector<Elem>& other) {
    auto& vec = Super::getContainer();
    auto& other_vec = other.getConstContainer();
    if (vec.size() != other_vec.size()) {
      throw std::runtime_error("IntVector add_with size mismatch");
    }

    #pragma clang loop vectorize(enable) interleave(enable)
    for (int i = 0; i < vec.size(); i++) {
      vec[i].elem += other_vec[i].elem;
    }

    return unit_t{};
  }

  IntVector<Elem> add(const IntVector<Elem>& other) const {
    auto copy = IntVector<Elem>(*this);
    copy.add_with(other);
    return copy;
  }

  IntVector<Elem> add(IntVector<Elem>&& other) const {
    other.add_with(*this);
    return other;
  }

  unit_t sub_with(const IntVector<Elem>& other) {
    auto& vec = Super::getContainer();
    auto& other_vec = other.getConstContainer();
    if (vec.size() != other_vec.size()) {
      throw std::runtime_error("IntVector sub_with size mismatch");
    }

    #pragma clang loop vectorize(enable) interleave(enable)
    for (int i = 0; i < vec.size(); i++) {
      vec[i].elem -= other_vec[i].elem;
    }

    return unit_t{};
  }

  unit_t sub_from(const IntVector<Elem>& other) {
    auto& vec = Super::getContainer();
    auto& other_vec = other.getConstContainer();
    if (vec.size() != other_vec.size()) {
      throw std::runtime_error("IntVector sub_with size mismatch");
    }

    #pragma clang loop vectorize(enable) interleave(enable)
    for (int i = 0; i < vec.size(); i++) {
      vec[i].elem = other_vec[i].elem - vec[i].elem;
    }

    return unit_t{};
  }

  IntVector<Elem> sub(const IntVector<Elem>& other) const {
    auto copy = IntVector<Elem>(*this);
    copy.sub_with(other);
    return copy;
  }

  IntVector<Elem> sub(IntVector<Elem>&& other) const {
    other.sub_from(*this);
    return other;
  }

  double dot(const IntVector<Elem>& other) const {
    auto& vec = Super::getConstContainer();
    auto& other_vec = other.getConstContainer();
    if (vec.size() != other_vec.size()) {
      throw std::runtime_error("IntVector dot size mismatch");
    }

    double d = 0;

    #pragma clang loop vectorize(enable) interleave(enable)
    for (int i = 0; i < vec.size(); i++) {
      d += vec[i].elem * other_vec[i].elem;
    }
    return d;
  }

  double distance(const IntVector<Elem>& other) const {
    double d = 0;
    auto& vec = Super::getConstContainer();
    auto& other_vec = other.getConstContainer();
    if (vec.size() != other_vec.size()) {
      throw std::runtime_error("IntVector distance size mismatch");
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
  unit_t scale_with(int c) {
    #pragma clang loop vectorize(enable) interleave(enable)
    for (auto& i : this->getContainer()) {
      i.elem *= c;
    }
  }

  IntVector<Elem> scale(int c) const {
    auto result = IntVector<Elem>(*this);
    auto& vec = result.getContainer();

    #pragma clang loop vectorize(enable) interleave(enable)
    for (auto i = 0; i < vec.size(); ++i) {
      vec[i].elem *= c;
    }

    return result;
  }
};

}  // namespace K3

namespace YAML {

template <class E>
struct convert<K3::IntVector<E>> {
  static Node encode(const K3::IntVector<E>& c) {
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

  static bool decode(const Node& node, K3::IntVector<E>& c) {
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
struct convert<K3::IntVector<E>> {
  template <class Allocator>
  static Value encode(const K3::IntVector<E>& c, Allocator& al) {
    Value v;
    v.SetObject();
    v.AddMember("type", Value("IntVector"), al);
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
