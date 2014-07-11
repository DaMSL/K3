#ifndef K3_RUNTIME_DATASPACE_SORTEDDS_H
#define K3_RUNTIME_DATASPACE_SORTEDDS_H

#include <set>
#include <algorithm>

namespace K3 {

template <typename Elem>
class SortedDS : public StlDS<Elem, std::multiset> {
  // Iterator Types
  typedef typename std::multiset<Elem>::iterator iterator_type;
  typedef typename std::multiset<Elem>::const_iterator const_iterator_type;
  typedef StlDS<Elem, std::multiset> Super;

  public:
    SortedDS(Engine * eng) : Super(eng) {}

    template<typename Iterator>
    SortedDS(Engine * eng, Iterator start, Iterator finish)
        : Super(eng,start,finish) {}

    SortedDS(const SortedDS& other) : Super(other) {}

    SortedDS(StlDS<Elem,std::multiset>& other) : Super(other) {}

    SortedDS(const std::multiset<Elem> &container) : Super(container) {}


     // Need to convert from StlDS to SortedDS
    template<typename NewElem>
    SortedDS<NewElem> map(const F<NewElem(Elem)>& f) {
      StlDS<NewElem, std::multiset> s = Super::map(f);
      return SortedDS<NewElem>(s);
    }

    SortedDS filter(const F<bool(Elem)>& pred) {
      Super s = Super::filter(pred);
      return SortedDS(s);
    }

    std::tuple<SortedDS, SortedDS> split() {
      tuple<Super, Super> tup = Super::split();
      SortedDS& ds1 = SortedDS(get<0>(tup));
      SortedDS& ds2 = SortedDS(get<1>(tup));
      return std::make_tuple(ds1, ds2);
    }

    SortedDS combine(const SortedDS& other) const {
      return SortedDS(Super::combine(other));
    }

    std::shared_ptr<Elem> min() {
      std::multiset<Elem>& x = Super::getContainer();
      auto it = std::min_element(x.begin(), x.end());
      std::shared_ptr<Elem> result = nullptr;
      if (it != x.end()) {
        result = make_shared<Elem>(*it);
      }

      return result;
    }

    std::shared_ptr<Elem> max() {
      std::multiset<Elem>& x = Super::getContainer();
      auto it = std::max_element(x.begin(), x.end());
      std::shared_ptr<Elem> result = nullptr;
      if (it != x.end()) {
        result = make_shared<Elem>(*it);
      }

      return result;
    }

    std::shared_ptr<Elem> lowerBound(const Elem& e) {
      std::multiset<Elem>& x = Super::getContainer();
      auto it = std::lower_bound(x.begin(), x.end(), e);
      std::shared_ptr<Elem> result = nullptr;
      if (it != x.end()) {
        result = make_shared<Elem>(*it);
      }

      return result;
    }

    std::shared_ptr<Elem> upperBound(const Elem& e) {
      std::multiset<Elem>& x = Super::getContainer();
      auto it = std::upper_bound(x.begin(), x.end(), e);
      std::shared_ptr<Elem> result = nullptr;
      if (it != x.end()) {
        result = make_shared<Elem>(*it);
      }

      return result;
    }

    SortedDS slice(const Elem& a, const Elem& b) {
      std::multiset<Elem>& x = Super::getContainer();
      SortedDS<Elem> result = SortedDS<Elem>(Super::getEngine());
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

} //namespace K3

#endif // K3_RUNTIME_DATASPACE_SORTEDDS_H
