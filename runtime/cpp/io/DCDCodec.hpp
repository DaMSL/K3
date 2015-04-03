#ifndef K3_RUNTIME_DCDCODEC_HPP
#define K3_RUNTIME_DCDCODEC_HPP

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
      throw std::runtime_error("Invalid DCD value");
    }

    template<typename T> shared_ptr<T> decode(const char *s, size_t sz) {
      string v(s, sz);
      return decode<T>(v);
    }
};

template<>
inline shared_ptr<CoordVec3> DCDCodec::decode<CoordVec3>(const string& s)
{
  throw std::runtime_error("Invalid DCD value, but found CoordVec3");
}

#endif

