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

  unit_t set(int i, Elem f) {
    auto& vec = Super::getContainer();
    vec[i] = f;
    return unit_t();
  }

  unit_t inPlaceAdd(const RealVector<Elem>& other) {
    auto& vec = Super::getContainer();
    auto& other_vec = other.getConstContainer();
    if (vec.size() != other_vec.size()) {
      throw std::runtime_error("RealVector inPlaceAdd size mismatch");
    }

    #pragma clang loop vectorize(enable) interleave(enable)
    for (int i = 0; i < vec.size(); i++) {
      vec[i].elem += other_vec[i].elem;
    }

    return unit_t{};
  }

  RealVector<Elem> add(const RealVector<Elem>& other) const {
    auto copy = RealVector<Elem>(*this);
    copy.inPlaceAdd(other);
    return copy;
  }

  RealVector<Elem> add(RealVector<Elem>&& other) const {
    other.inPlaceAdd(*this);
    return other;
  }

  unit_t inPlaceSub(const RealVector<Elem>& other) {
    auto& vec = Super::getContainer();
    auto& other_vec = other.getConstContainer();
    if (vec.size() != other_vec.size()) {
      throw std::runtime_error("RealVector inPlaceSub size mismatch");
    }

    #pragma clang loop vectorize(enable) interleave(enable)
    for (int i = 0; i < vec.size(); i++) {
      vec[i].elem -= other_vec[i].elem;
    }

    return unit_t{};
  }

  unit_t inPlaceSubFrom(const RealVector<Elem>& other) {
    auto& vec = Super::getContainer();
    auto& other_vec = other.getConstContainer();
    if (vec.size() != other_vec.size()) {
      throw std::runtime_error("RealVector inPlaceSub size mismatch");
    }

    #pragma clang loop vectorize(enable) interleave(enable)
    for (int i = 0; i < vec.size(); i++) {
      vec[i].elem = other_vec[i].elem - vec[i].elem;
    }

    return unit_t{};
  }

  RealVector<Elem> sub(const RealVector<Elem>& other) const {
    auto copy = RealVector<Elem>(*this);
    copy.inPlaceSub(other);
    return copy;
  }

  RealVector<Elem> sub(RealVector<Elem>&& other) const {
    other.inPlaceSubFrom(*this);
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
  unit_t inPlaceScalarMult(double c) {
    #pragma clang loop vectorize(enable) interleave(enable)
    for (auto& i : this->getContainer()) {
      i.elem *= c;
    }
  }

  RealVector<Elem> scalarMult(double c) const {
    auto result = RealVector<Elem>(*this);
    auto& vec = result.getContainer();

    #pragma clang loop vectorize(enable) interleave(enable)
    for (auto i = 0; i < vec.size(); ++i) {
      vec[i].elem *= c;
    }

    return result;
  }

  Elem update_at(int i, Elem n) {
    auto old = Super::getContainer()[i];
    Super::getContainer()[i] = n;
    return old;
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
      for (auto i : container) {
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
