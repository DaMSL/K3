#include <set>

namespace K3 {
template <typename Elem>
class SetDS : public StlDS<Elem, std::unordered_set> {
  // Iterator Types
  typedef typename std::unordered_set<Elem>::iterator iterator_type;
  typedef typename std::unordered_set<Elem>::const_iterator const_iterator_type;

  public:
    SetDS(Engine * eng) : StlDS<Elem, std::unordered_set>(eng) {}
    
    template<typename Iterator>
    SetDS(Engine * eng, Iterator start, Iterator finish)
        : StlDS<Elem,std::unordered_set>(eng,start,finish) {}

    SetDS(const SetDS& other) : StlDS<Elem,std::unordered_set>(other) {}

    SetDS(StlDS<Elem,std::unordered_set> other) : StlDS<Elem,std::unordered_set>(other) {}


    SetDS(std::unordered_set<Elem> container) : StlDS<Elem, std::unordered_set>(container) {}

    typedef StlDS<Elem, std::unordered_set> super;

     // Need to convert from StlDS to SetDS
    template<typename NewElem>
    SetDS<NewElem> map(std::function<NewElem(Elem)> f) {
      StlDS<NewElem, std::unordered_set> s = super::map(f);
      return SetDS<NewElem>(s);
    }

    SetDS filter(std::function<bool(Elem)> pred) {
      super s = super::filter(pred);
      return SetDS(s);
    }

    tuple< SetDS, SetDS > split() {
      tuple<super, super> tup = super::split();
      SetDS ds1 = SetDS(get<0>(tup));
      SetDS ds2 = SetDS(get<1>(tup));
      return std::make_tuple(ds1, ds2);
    }

    SetDS combine(SetDS other) {
      super s = super::combine(other);
      return SetDS(s);
    }

    bool member(Elem e) {
      iterator_type it;
      it = std::find(this->container.begin(), this->container.end(), e);
      return (it != this->container.end());
    }

    bool isSubsetOf(SetDS<Elem> other) {
      for (auto x : this->container) {
        if (!other.member(x)) { return false; }
      }
      return true;
    }
  
    // TODO union is a reserved word
    SetDS union1(SetDS<Elem> other) {
      SetDS<Elem> result = SetDS<Elem>(this->getEngine());
      for (auto x : this->container) {
        result.insert(x);
      }
      for (auto x : other.container) {
        result.insert(x);
      }
      return result;
    } 

    SetDS intersect(SetDS<Elem> other) {
      SetDS<Elem> result = SetDS<Elem>(this->getEngine());
      for (auto x : this->container) {
        if(other.member(x)) {
          result.insert(x);
        }
      }
      return result;
    } 


    SetDS difference(SetDS<Elem> other) {
      SetDS<Elem> result = SetDS<Elem>(this->getEngine());
      for (auto x : this->container) {
        if(!other.member(x)) {
          result.insert(x);
        }
      }
      return result;
    } 
};
}

