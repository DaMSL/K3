#ifndef K3_RUNTIME_DCDCODEC_HPP
#define K3_RUNTIME_DCDCODEC_HPP

#include <type_traits>

#include "Codec.hpp"
#include "Serialization.hpp"
#include "dataspace/Dataspace.hpp"


#ifndef K3_R_x_y_z
#define K3_R_x_y_z
template <class _T0, class _T1, class _T2>
class R_x_y_z {
  public:
      R_x_y_z(): x(), y(), z()  {}
      template <class __T0, class __T1, class __T2>
      R_x_y_z(__T0&& _x, __T1&& _y, __T2&& _z): x(std::forward<__T0>(_x)),
      y(std::forward<__T1>(_y)), z(std::forward<__T2>(_z))  {}
      bool operator==(const R_x_y_z<_T0, _T1, _T2>& __other) const {
        return x == __other.x && y == __other.y && z == __other.z;
      }
      bool operator!=(const R_x_y_z<_T0, _T1, _T2>& __other) const {
        return std::tie(x, y, z) != std::tie(__other.x, __other.y, __other.z);
      }
      bool operator<(const R_x_y_z<_T0, _T1, _T2>& __other) const {
        return std::tie(x, y, z) < std::tie(__other.x, __other.y, __other.z);
      }
      bool operator>(const R_x_y_z<_T0, _T1, _T2>& __other) const {
        return std::tie(x, y, z) > std::tie(__other.x, __other.y, __other.z);
      }
      bool operator<=(const R_x_y_z<_T0, _T1, _T2>& __other) const {
        return std::tie(x, y, z) <= std::tie(__other.x, __other.y, __other.z);
      }
      bool operator>=(const R_x_y_z<_T0, _T1, _T2>& __other) const {
        return std::tie(x, y, z) >= std::tie(__other.x, __other.y, __other.z);
      }
      template <class archive>
      void serialize(archive& _archive, const unsigned int _version)  {
        _archive & BOOST_SERIALIZATION_NVP(x);
        _archive & BOOST_SERIALIZATION_NVP(y);
        _archive & BOOST_SERIALIZATION_NVP(z);
      }
      template <class archive>
      void serialize(archive& _archive)  {
        _archive & x;
        _archive & y;
        _archive & z;
      }
      _T0 x;
      _T1 y;
      _T2 z;
};
#endif
#ifndef K3_R_x_y_z_srimpl_lvl
#define K3_R_x_y_z_srimpl_lvl
namespace boost {
  namespace serialization {
    template <class _T0, class _T1, class _T2>
    class implementation_level<R_x_y_z<_T0, _T1, _T2>> {
      public:
          typedef  mpl::integral_c_tag tag;
          typedef  mpl::int_<object_serializable> type;
          BOOST_STATIC_CONSTANT(int, value = implementation_level::type::value);
    };
  }
}
#endif

namespace K3 {

using CoordVec  = Vector<R_elem<double>>;
using CoordVec3 = R_x_y_z<CoordVec, CoordVec, CoordVec>;

class DCDCodec : public virtual Codec, public virtual LogMT {
  public:

    DCDCodec(CodecFormat f): Codec(f), LogMT("DCDCodec") {}
    virtual ~DCDCodec() {}

    shared_ptr<Codec> freshClone() {
      return std::dynamic_pointer_cast<Codec, DCDCodec>(make_shared<DCDCodec>(format_));
    }

    template<typename T> string encode(const T& v) {
      return "";
    }

    template<typename T> shared_ptr<T> decode(const string& s) {
      return decode_with_container<T>(s,
        cv3_type<std::is_base_of<CoordVec, typename extract_cv3_container<T>::type>::value>());
    }

    template<typename T> shared_ptr<T> decode(const char *s, size_t sz) {
      string v(s, sz);
      return decode<T>(v);
    }

  private:
    template<class T>
    struct extract_cv3_container { typedef T type; };

    template<template<typename, typename, typename> class R3,
             typename CX, typename CY, typename CZ>
    struct extract_cv3_container<R3<CX,CY,CZ>> { typedef CX type; };

    template<bool> struct cv3_type {};

    template<typename T>
    shared_ptr<T> decode_with_container(const string& s, cv3_type<false>) {
      throw std::runtime_error("Invalid DCD value");
    }

    template<typename T>
    shared_ptr<T> decode_with_container(const string& s, cv3_type<true>)
    {
      char* p = const_cast<char*>(s.data());
      size_t sz = static_cast<size_t>(*reinterpret_cast<int*>(p));

      shared_ptr<T> result = make_shared<T>();
      result->x.getContainer().resize(sz);
      result->y.getContainer().resize(sz);
      result->z.getContainer().resize(sz);

      char* x = p + sizeof(int);
      char* y = x + sz*sizeof(float);
      char* z = y + sz*sizeof(float);
      for(size_t i = 0; i < sz; ++i) {
        result->x.set(i, *reinterpret_cast<float*>(x+i*sizeof(float)));
        result->y.set(i, *reinterpret_cast<float*>(y+i*sizeof(float)));
        result->z.set(i, *reinterpret_cast<float*>(z+i*sizeof(float)));
      }
      return result;
    }
};

} // End namespace K3
#endif
