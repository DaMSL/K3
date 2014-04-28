#include <Engine.hpp>
#include <dataspace/StlDS.cpp>

namespace K3 {
template <typename Elem, template<typename...> class Container>
class SeqDS : public StlDS<Elem, Container> {
  public:
    SeqDS(Engine * eng) : StlDS<Elem, Container>(eng) {}
    
    template<typename Iterator>
    SeqDS(Engine * eng, Iterator start, Iterator finish)
        : StlDS<Elem,Container>(eng,start,finish) {}

    SeqDS(const SeqDS& other) : StlDS<Elem,Container>(other) {}

    // TODO: better implementation
    SeqDS sort(std::function<int(Elem,Elem)> comp) {
      auto f = [&] (Elem a, Elem b) { return comp(a,b) < 0; };
      Container<Elem> cpy = Container<Elem>(this->container);
      cpy.sort(f);
      return SeqDS<Elem, Container>(this->getEngine(), cpy.begin(), cpy.end());
    }

};
}

