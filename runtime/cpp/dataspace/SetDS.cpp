
#include <dataspace/StlDS.cpp>

namespace K3 {
template <typename Elem, template<typename...> class Container>
class SetDS : public StlDS<Elem, Container> {
  // Iterator Types
  typedef typename Container<Elem>::iterator iterator_type;
  typedef typename Container<Elem>::const_iterator const_iterator_type;

  public:
    SetDS(Engine * eng) : StlDS<Elem, Container>(eng) {}
    
    template<typename Iterator>
    SetDS(Engine * eng, Iterator start, Iterator finish)
        : StlDS<Elem,Container>(eng,start,finish) {}

    SetDS(const SetDS& other) : StlDS<Elem,Container>(other) {}

    bool member(Elem e) {
      iterator_type it;
      it = std::find(this->container.begin(), this->container.end(), e);
      return (it != this->container.end());
    }

    bool isSubsetOf(SetDS<Elem, Container> other) {
      for (auto x : this->container) {
        if (!other.member(x)) { return false; }
      }
      return true;
    }
  
    // TODO union is a reserved word
    SetDS union1(SetDS<Elem, Container> other) {
      SetDS<Elem, Container> result = SetDS<Elem, Container>(this->getEngine());
      for (auto x : this->container) {
        result.insert(x);
      }
      for (auto x : other.container) {
        result.insert(x);
      }
      return result;
    } 

    SetDS intersect(SetDS<Elem, Container> other) {
      SetDS<Elem, Container> result = SetDS<Elem, Container>(this->getEngine());
      for (auto x : this->container) {
        if(other.member(x)) {
          result.insert(x);
        }
      }
      return result;
    } 


    SetDS difference(SetDS<Elem, Container> other) {
      SetDS<Elem, Container> result = SetDS<Elem, Container>(this->getEngine());
      for (auto x : this->container) {
        if(!other.member(x)) {
          result.insert(x);
        }
      }
      return result;
    } 
};
}

