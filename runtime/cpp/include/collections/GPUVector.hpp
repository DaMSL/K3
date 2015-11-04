#ifndef K3_GPU_VECTOR
#define K3_GPU_VECTOR

#include "thrust/host_vector.h"

#include "STLDataspace.hpp"
#include "serialization/Json.hpp"
#include "serialization/Yaml.hpp"

namespace K3 {
template <template <class> class Derived, class Elem>
using GPUVectorDS = STLDS<Derived, thrust::host_vector, Elem>;

template <class Elem>
class GPUVector : public GPUVectorDS<K3::GPUVector, Elem> {

  using Super = GPUVectorDS<K3::GPUVector, Elem>;

 public:
  GPUVector() : Super() {}
  GPUVector(const Super& c) : Super(c) {}
  GPUVector(Super&& c) : Super(std::move(c)) {}

  /*
   * Implementing functions associated with k3 signature
   */
  auto erase_at(int i) {
    auto& vec = Super::getContainer();
    return std::move(vec[i]);
  }

  template<class F>
  auto unsafe_at(int i, F f) const {
    auto& vec = Super::getConstContainer();
    return f(vec[i]);
  }
};

} // end namespace K3

namespace YAML {
template <class E>
struct convert<K3::GPUVector<E>> {
  static Node encode(const K3::GPUVector<E>& c) {
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

  static bool decode(const Node& node, K3::GPUVector<E>& c) {
    for (auto& i : node) {
      c.insert(i.as<E>());
    }

    return true;
  }
};
} // end namespace YAML

namespace JSON {
using namespace rapidjson;
template <class E>
struct convert<K3::GPUVector<E>> {
  template <class Allocator>
  static Value encode(const K3::GPUVector<E>& c, Allocator& al) {
    Value v;
    v.SetObject();
    v.AddMember("type", Value("GPUVector"), al);
    Value inner;
    inner.SetArray();
    for (const auto& e : c.getConstContainer()) {
      inner.PushBack(convert<E>::encode(e, al), al);
    }
    v.AddMember("value", inner.Move(), al);
    return v;
  }
};

} // end namespace JSON


#endif
