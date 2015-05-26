#ifndef K3_COLLECTION
#define K3_COLLECTION

#include "STLDataspace.hpp"

namespace K3 {
template <template <class> class Derived, class Elem>
using VectorDS = STLDS<Derived, std::vector, Elem>;

template <class Elem>
class Collection: public VectorDS<K3::Collection, Elem> {
  using Super = VectorDS<K3::Collection, Elem>;

 public:
  Collection(): Super() {}
  Collection(const Super& c): Super(c) { }
  Collection(Super&& c): Super(std::move(c)) { }

  shared_ptr<Elem> at(int i) const {
    auto& c = Super::getConstContainer();
    if (i < c.size()) {
      return std::make_shared<Elem>(c[i]);
    } else {
      return nullptr;
    }
  }

  template <class F>
  auto at_with(int i, F f) {
    return f(Super::getConstContainer()[i]);
  }

  //template<class Archive>
  //void serialize(Archive &ar) {
  //  ar & yas::base_object<VectorDS<K3::Collection, Elem>>(*this);
  //}

  template<class Archive>
  void serialize(Archive &ar, const unsigned int) {
    ar &  boost::serialization::make_nvp("__K3Collection",
                                         boost::serialization::base_object<VectorDS<K3::Collection, Elem>>(*this));
  }

 private:
  friend class boost::serialization::access;
};
}  // namespace K3

#endif
