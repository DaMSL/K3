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

}
#endif
