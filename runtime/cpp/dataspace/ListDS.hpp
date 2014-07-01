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
  class ListDS : public StlDS<Elem, std::list> {
    typedef StlDS<Elem, std::list> super;

    public:
      // Constructors
      ListDS(Engine * eng) : StlDS<Elem, std::list>(eng) {}

      template<typename Iterator>
      ListDS(Engine * eng, Iterator start, Iterator finish)
        : StlDS<Elem, std::list>(eng, start, finish) {}

      ListDS(const ListDS& other) : StlDS<Elem,std::list>(other) {}

      ListDS(StlDS<Elem,std::list> other) : StlDS<Elem,std::list>(other) {}

      ListDS(std::list<Elem> container) : StlDS<Elem, std::list>(container) {}

    // Need to convert from StlDS to ListDS
    template<typename NewElem>
    ListDS<NewElem> map(F<NewElem(Elem)> f) {
      StlDS<NewElem, std::list> s = super::map(f);
      return ListDS<NewElem>(s);
    }

    ListDS filter(F<bool(Elem)> pred) {
      super s = super::filter(pred);
      return ListDS(s);
    }

    tuple< ListDS, ListDS > split() {
      tuple<super, super> tup = super::split();
      ListDS ds1 = ListDS(get<0>(tup));
      ListDS ds2 = ListDS(get<1>(tup));
      return std::make_tuple(ds1, ds2);
    }

    ListDS combine(ListDS other) const {
      super s = super::combine(other);
      return ListDS(s);
    }

    ListDS sort(F<F<int(Elem)>(Elem)> comp) {
      std::list<Elem> l = std::list<Elem>(super::getContainer());
      std::function<bool(Elem,Elem)> f = [&] (Elem a, Elem b) { return comp(a)(b) < 0; };
      l.sort(f);
      return ListDS(l);
    }

  };
}

#endif // K3_RUNTIME_DATASPACE_LISTDS_H
