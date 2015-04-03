#include "DCDCodec.hpp"

/*
namespace K3{

  template<>
  shared_ptr<R_x_y_z<CoordVec, CoordVec, CoordVec>> DCDCodec::decode<R_x_y_z<CoordVec, CoordVec, CoordVec>>(const string& s){
    char* p = const_cast<char*>(s.data());
    int siz = *reinterpret_cast<int*>(p);

    shared_ptr<R_x_y_z<CoordVec, CoordVec, CoordVec>> result = make_shared<R_x_y_z<CoordVec, CoordVec, CoordVec>>();
    result->x.getContainer().resize(siz);
    result->y.getContainer().resize(siz);
    result->z.getContainer().resize(siz);

    for(int i=0; i <siz; ++i){
      R_elem<double> elem(*reinterpret_cast<float*>(p+i*sizeof(float)));
      result->x.set(i, elem);
    }

    p += siz*sizeof(float);
    for(int i=0; i <siz; ++i){
      R_elem<double> elem(*reinterpret_cast<float*>(p+i*sizeof(float)));
      result->y.set(i, elem);
    }

    p += siz*sizeof(float);
    for(int i=0; i <siz; ++i){
      R_elem<double> elem(*reinterpret_cast<float*>(p+i*sizeof(float)));
      result->z.set(i, elem);
    }
    return result;

  }
}
*/