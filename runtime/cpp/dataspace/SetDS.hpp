#ifndef K3_RUNTIME_DATASPACE_SETDS_H
#define K3_RUNTIME_DATASPACE_SETDS_H

#include <unordered_set>
#include <dataspace/StlDS.hpp>

namespace K3 {

template <typename Elem>
class SetDS : public StlDS<Elem, std::unordered_set> {
  // Iterator Types
  typedef typename std::unordered_set<Elem>::iterator iterator_type;
  typedef typename std::unordered_set<Elem>::const_iterator const_iterator_type;
  typedef StlDS<Elem, std::unordered_set> Super;

  public:
    SetDS(Engine * eng) : Super(eng) {}

    template<typename Iterator>
    SetDS(Engine * eng, Iterator start, Iterator finish)
        : Super(eng,start,finish) {}

    SetDS(const SetDS& other) : Super(other) {}

    SetDS(const Super& other) : Super(other) {}

    SetDS(const std::unordered_set<Elem>& container) : Super(container) {}

     // Need to convert from StlDS to SetDS
    template<typename NewElem>
    SetDS<NewElem> map(const F<NewElem(Elem)>& f) {
      StlDS<NewElem, std::unordered_set> s = Super::map(f);
      return SetDS<NewElem>(s);
    }

    SetDS filter(const F<bool(Elem)>& pred) {
      Super s = Super::filter(pred);
      return SetDS(s);
    }

    tuple<SetDS, SetDS> split() {
      tuple<Super, Super> tup = Super::split();
      SetDS& ds1 = SetDS(get<0>(tup));
      SetDS& ds2 = SetDS(get<1>(tup));
      return std::make_tuple(ds1, ds2);
    }

    SetDS combine(const SetDS& other) const {
      return SetDS(Super::combine(other));
    }

    bool member(const Elem& e) {
      iterator_type it;
      it = std::find(Super::getContainer().begin(), Super::getContainer().end(), e);
      return (it != Super::getContainer().end());
    }

    bool isSubsetOf(const SetDS<Elem>& other) {
      for (auto &x : Super::getContainer()) {
        if (!other.member(x)) { return false; }
      }
      return true;
    }
  
    // TODO union is a reserved word
    SetDS union1(const SetDS<Elem>& other) {
      SetDS<Elem> result(Super::getEngine());
      for (auto &x : Super::getContainer()) {
        result.insert(x);
      }
      for (auto &x : other.getContainer()) {
        result.insert(x);
      }
      return result;
    } 

    SetDS intersect(const SetDS<Elem>& other) {
      SetDS<Elem> result(Super::getEngine());
      for (auto &x : Super::getContainer()) {
        if(other.member(x)) {
          result.insert(x);
        }
      }
      return result;
    } 


    SetDS difference(const SetDS<Elem>& other) {
      SetDS<Elem> result(Super::getEngine());
      for (auto &x : Super::getContainer()) {
        if(!other.member(x)) {
          result.insert(x);
        }
      }
      return result;
    } 
};

} // namespace K3

#endif // RUNTIME_CPP_DATASPACE_SETDS
#define RUNTIME_CPP_DATASPACE_SETDS_H
