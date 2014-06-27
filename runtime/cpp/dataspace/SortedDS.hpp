#include <set>
#include <algorithm>

namespace K3 {

template <typename Elem>
class SortedDS : public StlDS<Elem, std::multiset> {
  // Iterator Types
  typedef typename std::multiset<Elem>::iterator iterator_type;
  typedef typename std::multiset<Elem>::const_iterator const_iterator_type;

  public:
    SortedDS(Engine * eng) : StlDS<Elem, std::multiset>(eng) {}

    template<typename Iterator>
    SortedDS(Engine * eng, Iterator start, Iterator finish)
        : StlDS<Elem,std::multiset>(eng,start,finish) {}

    SortedDS(const SortedDS& other) : StlDS<Elem,std::multiset>(other) {}

    SortedDS(StlDS<Elem,std::multiset> other) : StlDS<Elem,std::multiset>(other) {}


    SortedDS(std::multiset<Elem> container) : StlDS<Elem, std::multiset>(container) {}

    typedef StlDS<Elem, std::multiset> super;

     // Need to convert from StlDS to SortedDS
    template<typename NewElem>
    SortedDS<NewElem> map(F<NewElem(Elem)> f) {
      StlDS<NewElem, std::multiset> s = super::map(f);
      return SortedDS<NewElem>(s);
    }

    SortedDS filter(F<bool(Elem)> pred) {
      super s = super::filter(pred);
      return SortedDS(s);
    }

    std::tuple< SortedDS, SortedDS > split() {
      tuple<super, super> tup = super::split();
      SortedDS ds1 = SortedDS(get<0>(tup));
      SortedDS ds2 = SortedDS(get<1>(tup));
      return std::make_tuple(ds1, ds2);
    }

    SortedDS combine(SortedDS other) {
      super s = super::combine(other);
      return SortedDS(s);


    }

    std::shared_ptr<Elem> min() {
      std::multiset<Elem> x = this->getContainer();
      auto it = std::min_element(x.begin(), x.end());
      std::shared_ptr<Elem> result = nullptr;
      if (it != x.end()) {
        result = make_shared<Elem>(*it);
      }

      return result;
    }

    std::shared_ptr<Elem> max() {
      std::multiset<Elem> x = this->getContainer();
      auto it = std::max_element(x.begin(), x.end());
      std::shared_ptr<Elem> result = nullptr;
      if (it != x.end()) {
        result = make_shared<Elem>(*it);
      }

      return result;
    }

    std::shared_ptr<Elem> lowerBound(Elem e) {
      std::multiset<Elem> x = this->getContainer();
      auto it = std::lower_bound(x.begin(), x.end(), e);
      std::shared_ptr<Elem> result = nullptr;
      if (it != x.end()) {
        result = make_shared<Elem>(*it);
      }

      return result;
    }

    std::shared_ptr<Elem> upperBound(Elem e) {
      std::multiset<Elem> x = this->getContainer();
      auto it = std::upper_bound(x.begin(), x.end(), e);
      std::shared_ptr<Elem> result = nullptr;
      if (it != x.end()) {
        result = make_shared<Elem>(*it);
      }

      return result;
    }

    SortedDS slice(Elem a, Elem b) {
      std::multiset<Elem> x = this->getContainer();
      SortedDS<Elem> result = SortedDS<Elem>(this->getEngine());
      for (Elem e : x) {
        if (e >= a && e <= b) {
          result.insert(e);
        }
        if (e > b) {
          break;
        }
      }
      return result;
    }
};

}
