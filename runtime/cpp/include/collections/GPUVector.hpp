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
#endif
