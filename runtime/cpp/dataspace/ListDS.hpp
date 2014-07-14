#ifndef K3_RUNTIME_DATASPACE_LISTDS_H
#define K3_RUNTIME_DATASPACE_LISTDS_H

#include <algorithm>
#include <functional>
#include <string>
#include <tuple>
#include <vector>

#include <dataspace/StlDS.hpp>

namespace K3
{
  using namespace std;

  template <typename Elem>
  class ListDS : public StlDS<Elem, std::vector> {
    typedef StlDS<Elem, std::vector> Super;

    public:
      // Constructors
      ListDS(Engine * eng) : Super(eng) {}

      template<typename Iterator>
      ListDS(Engine * eng, Iterator start, Iterator finish)
        : Super(eng, start, finish) {}

      ListDS(const ListDS& other) : Super(other) {}

      ListDS(StlDS<Elem, std::vector>& other) : Super(other) {}

      ListDS(const std::vector<Elem>& container) : Super(container) {}

    // Need to convert from StlDS to ListDS
    template<typename NewElem>
    ListDS<NewElem> map(const F<NewElem(Elem)>& f) {
      return ListDS<NewElem>(Super::map(f));
    }

    ListDS filter(const F<bool(const Elem&)>& pred) {
      return ListDS(Super::filter(pred));
    }

    tuple< ListDS, ListDS > split() {
      tuple<Super, Super> tup = Super::split();
      ListDS& ds1 = ListDS(get<0>(tup));
      ListDS& ds2 = ListDS(get<1>(tup));
      return std::make_tuple(ds1, ds2);
    }

    ListDS combine(const ListDS& other) const {
      return ListDS(Super::combine(other));
    }

    ListDS sort(const F<F<int(Elem)>(Elem)>& comp) {
      std::vector<Elem> l(Super::getContainer());
      auto f = [&] (Elem& a, Elem& b) {
        return comp(a)(b) < 0;
      };
      l.sort(f);
      return ListDS(l);
    }

    Elem at(int i) {
      std::vector<Elem> v = Super::getContainer();
      return v[i];
    }

  };
}

#endif // K3_RUNTIME_DATASPACE_LISTDS_H
