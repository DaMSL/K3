#include "functional"
#include "map"
#include "memory"
#include "sstream"
#include "string"
#include "tuple"
#include "boost/multi_index_container.hpp"
#include "boost/multi_index/ordered_index.hpp"
#include "boost/multi_index/member.hpp"
#include "boost/multi_index/composite_key.hpp"
#include "BaseTypes.hpp"
#include "BaseString.hpp"
#include "Common.hpp"
#include "Context.hpp"
#include "Dispatch.hpp"
#include "Engine.hpp"
#include "MessageProcessor.hpp"
#include "Literals.hpp"
#include "Serialization.hpp"
#include "serialization/yaml.hpp"
#include "serialization/json.hpp"
#include "Builtins.hpp"
#include "Run.hpp"
#include "Prettify.hpp"
#include "dataspace/Dataspace.hpp"
#include "dataspace/MapDataspace.hpp"
#include "yaml-cpp/yaml.h"
using K3::Address;
using K3::Engine;
using K3::string_impl;
using K3::Options;
using K3::ValDispatcher;
using K3::Dispatcher;
using K3::virtualizing_message_processor;
using K3::make_address;
using K3::__k3_context;
using K3::runProgram;
using K3::SystemEnvironment;
using K3::processRoles;
using K3::defaultEnvironment;
using K3::createContexts;
using K3::getAddrs;
using K3::currentTime;
using std::make_tuple;
using std::make_shared;
using std::shared_ptr;
using std::get;
using std::map;
using std::list;
using std::ostringstream;
#ifndef K3_R_addr
#define K3_R_addr
template <class _T0>
class R_addr {
  public:
      R_addr(): addr()  {}
      R_addr(const _T0& _addr): addr(_addr)  {}
      R_addr(_T0&& _addr): addr(std::move(_addr))  {}
      bool operator==(const R_addr<_T0>& __other) const {
        return addr == __other.addr;
      }
      bool operator!=(const R_addr<_T0>& __other) const {
        return std::tie(addr) != std::tie(__other.addr);
      }
      bool operator<(const R_addr<_T0>& __other) const {
        return std::tie(addr) < std::tie(__other.addr);
      }
      bool operator>(const R_addr<_T0>& __other) const {
        return std::tie(addr) > std::tie(__other.addr);
      }
      bool operator<=(const R_addr<_T0>& __other) const {
        return std::tie(addr) <= std::tie(__other.addr);
      }
      bool operator>=(const R_addr<_T0>& __other) const {
        return std::tie(addr) >= std::tie(__other.addr);
      }
      template <class archive>
      void serialize(archive& _archive, const unsigned int _version)  {
        _archive & BOOST_SERIALIZATION_NVP(addr);
      }
      template <class archive>
      void serialize(archive& _archive)  {
        _archive & addr;
      }
      _T0 addr;
};
#endif
#ifndef K3_R_addr_srimpl_lvl
#define K3_R_addr_srimpl_lvl
namespace boost {
  namespace serialization {
    template <class _T0>
    class implementation_level<R_addr<_T0>> {
      public:
          typedef  mpl::integral_c_tag tag;
          typedef  mpl::int_<object_serializable> type;
          BOOST_STATIC_CONSTANT(int, value = implementation_level::type::value);
    };
  }
}
#endif
namespace JSON {
  template <class _T0>
  class convert<R_addr<_T0>> {
    public:
        template <class Allocator>
        static rapidjson::Value encode(const R_addr<_T0>& r, Allocator& al)  {
          using namespace rapidjson;
          Value val;
          Value inner;
          val.SetObject();
          inner.SetObject();
          inner.AddMember("addr", convert<_T0>::encode(r.addr, al), al);
          val.AddMember("type", Value("record"), al);
          val.AddMember("value", inner.Move(), al);
          return val;
        }
  };
}
namespace YAML {
  template <class _T0>
  class convert<R_addr<_T0>> {
    public:
        static Node encode(const R_addr<_T0>& r)  {
          Node node;
          node["addr"] = convert<_T0>::encode(r.addr);
          return node;
        }
        static bool decode(const Node& node, R_addr<_T0>& r)  {
          if (!node.IsMap()) {
            return false;
          }
          if (node["addr"]) {
            r.addr = node["addr"].as<_T0>();
          }
          return true;
        }
  };
}
#ifndef K3_R_addr_hash_value
#define K3_R_addr_hash_value
template <class _T0>
std::size_t hash_value(const R_addr<_T0>& r)  {
  boost::hash<std::tuple<_T0>> hasher;
  return hasher(std::tie(r.addr));
}
#endif
#ifndef K3_R_arg
#define K3_R_arg
template <class _T0>
class R_arg {
  public:
      R_arg(): arg()  {}
      R_arg(const _T0& _arg): arg(_arg)  {}
      R_arg(_T0&& _arg): arg(std::move(_arg))  {}
      bool operator==(const R_arg<_T0>& __other) const {
        return arg == __other.arg;
      }
      bool operator!=(const R_arg<_T0>& __other) const {
        return std::tie(arg) != std::tie(__other.arg);
      }
      bool operator<(const R_arg<_T0>& __other) const {
        return std::tie(arg) < std::tie(__other.arg);
      }
      bool operator>(const R_arg<_T0>& __other) const {
        return std::tie(arg) > std::tie(__other.arg);
      }
      bool operator<=(const R_arg<_T0>& __other) const {
        return std::tie(arg) <= std::tie(__other.arg);
      }
      bool operator>=(const R_arg<_T0>& __other) const {
        return std::tie(arg) >= std::tie(__other.arg);
      }
      template <class archive>
      void serialize(archive& _archive, const unsigned int _version)  {
        _archive & BOOST_SERIALIZATION_NVP(arg);
      }
      template <class archive>
      void serialize(archive& _archive)  {
        _archive & arg;
      }
      _T0 arg;
};
#endif
#ifndef K3_R_arg_srimpl_lvl
#define K3_R_arg_srimpl_lvl
namespace boost {
  namespace serialization {
    template <class _T0>
    class implementation_level<R_arg<_T0>> {
      public:
          typedef  mpl::integral_c_tag tag;
          typedef  mpl::int_<object_serializable> type;
          BOOST_STATIC_CONSTANT(int, value = implementation_level::type::value);
    };
  }
}
#endif
namespace JSON {
  template <class _T0>
  class convert<R_arg<_T0>> {
    public:
        template <class Allocator>
        static rapidjson::Value encode(const R_arg<_T0>& r, Allocator& al)  {
          using namespace rapidjson;
          Value val;
          Value inner;
          val.SetObject();
          inner.SetObject();
          inner.AddMember("arg", convert<_T0>::encode(r.arg, al), al);
          val.AddMember("type", Value("record"), al);
          val.AddMember("value", inner.Move(), al);
          return val;
        }
  };
}
namespace YAML {
  template <class _T0>
  class convert<R_arg<_T0>> {
    public:
        static Node encode(const R_arg<_T0>& r)  {
          Node node;
          node["arg"] = convert<_T0>::encode(r.arg);
          return node;
        }
        static bool decode(const Node& node, R_arg<_T0>& r)  {
          if (!node.IsMap()) {
            return false;
          }
          if (node["arg"]) {
            r.arg = node["arg"].as<_T0>();
          }
          return true;
        }
  };
}
#ifndef K3_R_arg_hash_value
#define K3_R_arg_hash_value
template <class _T0>
std::size_t hash_value(const R_arg<_T0>& r)  {
  boost::hash<std::tuple<_T0>> hasher;
  return hasher(std::tie(r.arg));
}
#endif
#ifndef K3_R_i
#define K3_R_i
template <class _T0>
class R_i {
  public:
      R_i(): i()  {}
      R_i(const _T0& _i): i(_i)  {}
      R_i(_T0&& _i): i(std::move(_i))  {}
      bool operator==(const R_i<_T0>& __other) const {
        return i == __other.i;
      }
      bool operator!=(const R_i<_T0>& __other) const {
        return std::tie(i) != std::tie(__other.i);
      }
      bool operator<(const R_i<_T0>& __other) const {
        return std::tie(i) < std::tie(__other.i);
      }
      bool operator>(const R_i<_T0>& __other) const {
        return std::tie(i) > std::tie(__other.i);
      }
      bool operator<=(const R_i<_T0>& __other) const {
        return std::tie(i) <= std::tie(__other.i);
      }
      bool operator>=(const R_i<_T0>& __other) const {
        return std::tie(i) >= std::tie(__other.i);
      }
      template <class archive>
      void serialize(archive& _archive, const unsigned int _version)  {
        _archive & BOOST_SERIALIZATION_NVP(i);
      }
      template <class archive>
      void serialize(archive& _archive)  {
        _archive & i;
      }
      _T0 i;
};
#endif
#ifndef K3_R_i_srimpl_lvl
#define K3_R_i_srimpl_lvl
namespace boost {
  namespace serialization {
    template <class _T0>
    class implementation_level<R_i<_T0>> {
      public:
          typedef  mpl::integral_c_tag tag;
          typedef  mpl::int_<object_serializable> type;
          BOOST_STATIC_CONSTANT(int, value = implementation_level::type::value);
    };
  }
}
#endif
namespace JSON {
  template <class _T0>
  class convert<R_i<_T0>> {
    public:
        template <class Allocator>
        static rapidjson::Value encode(const R_i<_T0>& r, Allocator& al)  {
          using namespace rapidjson;
          Value val;
          Value inner;
          val.SetObject();
          inner.SetObject();
          inner.AddMember("i", convert<_T0>::encode(r.i, al), al);
          val.AddMember("type", Value("record"), al);
          val.AddMember("value", inner.Move(), al);
          return val;
        }
  };
}
namespace YAML {
  template <class _T0>
  class convert<R_i<_T0>> {
    public:
        static Node encode(const R_i<_T0>& r)  {
          Node node;
          node["i"] = convert<_T0>::encode(r.i);
          return node;
        }
        static bool decode(const Node& node, R_i<_T0>& r)  {
          if (!node.IsMap()) {
            return false;
          }
          if (node["i"]) {
            r.i = node["i"].as<_T0>();
          }
          return true;
        }
  };
}
#ifndef K3_R_i_hash_value
#define K3_R_i_hash_value
template <class _T0>
std::size_t hash_value(const R_i<_T0>& r)  {
  boost::hash<std::tuple<_T0>> hasher;
  return hasher(std::tie(r.i));
}
#endif
#ifndef K3_R_key_value
#define K3_R_key_value
template <class _T0, class _T1>
class R_key_value {
  public:
      R_key_value(): key(), value()  {}
      template <class __T0, class __T1>
      R_key_value(__T0&& _key, __T1&& _value): key(std::forward<__T0>(_key)),
      value(std::forward<__T1>(_value))  {}
      bool operator==(const R_key_value<_T0, _T1>& __other) const {
        return key == __other.key && value == __other.value;
      }
      bool operator!=(const R_key_value<_T0, _T1>& __other) const {
        return std::tie(key, value) != std::tie(__other.key, __other.value);
      }
      bool operator<(const R_key_value<_T0, _T1>& __other) const {
        return std::tie(key, value) < std::tie(__other.key, __other.value);
      }
      bool operator>(const R_key_value<_T0, _T1>& __other) const {
        return std::tie(key, value) > std::tie(__other.key, __other.value);
      }
      bool operator<=(const R_key_value<_T0, _T1>& __other) const {
        return std::tie(key, value) <= std::tie(__other.key, __other.value);
      }
      bool operator>=(const R_key_value<_T0, _T1>& __other) const {
        return std::tie(key, value) >= std::tie(__other.key, __other.value);
      }
      template <class archive>
      void serialize(archive& _archive, const unsigned int _version)  {
        _archive & BOOST_SERIALIZATION_NVP(key);
        _archive & BOOST_SERIALIZATION_NVP(value);
      }
      template <class archive>
      void serialize(archive& _archive)  {
        _archive & key;
        _archive & value;
      }
      _T0 key;
      _T1 value;
};
#endif
#ifndef K3_R_key_value_srimpl_lvl
#define K3_R_key_value_srimpl_lvl
namespace boost {
  namespace serialization {
    template <class _T0, class _T1>
    class implementation_level<R_key_value<_T0, _T1>> {
      public:
          typedef  mpl::integral_c_tag tag;
          typedef  mpl::int_<object_serializable> type;
          BOOST_STATIC_CONSTANT(int, value = implementation_level::type::value);
    };
  }
}
#endif
namespace JSON {
  template <class _T0, class _T1>
  class convert<R_key_value<_T0, _T1>> {
    public:
        template <class Allocator>
        static rapidjson::Value encode(const R_key_value<_T0, _T1>& r, Allocator& al)  {
          using namespace rapidjson;
          Value val;
          Value inner;
          val.SetObject();
          inner.SetObject();
          inner.AddMember("key", convert<_T0>::encode(r.key, al), al);
          inner.AddMember("value", convert<_T1>::encode(r.value, al), al);
          val.AddMember("type", Value("record"), al);
          val.AddMember("value", inner.Move(), al);
          return val;
        }
  };
}
namespace YAML {
  template <class _T0, class _T1>
  class convert<R_key_value<_T0, _T1>> {
    public:
        static Node encode(const R_key_value<_T0, _T1>& r)  {
          Node node;
          node["key"] = convert<_T0>::encode(r.key);
          node["value"] = convert<_T1>::encode(r.value);
          return node;
        }
        static bool decode(const Node& node, R_key_value<_T0, _T1>& r)  {
          if (!node.IsMap()) {
            return false;
          }
          if (node["key"]) {
            r.key = node["key"].as<_T0>();
          }
          if (node["value"]) {
            r.value = node["value"].as<_T1>();
          }
          return true;
        }
  };
}
#ifndef K3_R_key_value_hash_value
#define K3_R_key_value_hash_value
template <class _T0, class _T1>
std::size_t hash_value(const R_key_value<_T0, _T1>& r)  {
  boost::hash<std::tuple<_T0, _T1>> hasher;
  return hasher(std::tie(r.key, r.value));
}
#endif
#ifndef K3_R_r1_r2_r3
#define K3_R_r1_r2_r3
template <class _T0, class _T1, class _T2>
class R_r1_r2_r3 {
  public:
      R_r1_r2_r3(): r1(), r2(), r3()  {}
      template <class __T0, class __T1, class __T2>
      R_r1_r2_r3(__T0&& _r1, __T1&& _r2, __T2&& _r3): r1(std::forward<__T0>(_r1)),
      r2(std::forward<__T1>(_r2)), r3(std::forward<__T2>(_r3))  {}
      bool operator==(const R_r1_r2_r3<_T0, _T1, _T2>& __other) const {
        return r1 == __other.r1 && r2 == __other.r2 && r3 == __other.r3;
      }
      bool operator!=(const R_r1_r2_r3<_T0, _T1, _T2>& __other) const {
        return std::tie(r1, r2, r3) != std::tie(__other.r1, __other.r2, __other.r3);
      }
      bool operator<(const R_r1_r2_r3<_T0, _T1, _T2>& __other) const {
        return std::tie(r1, r2, r3) < std::tie(__other.r1, __other.r2, __other.r3);
      }
      bool operator>(const R_r1_r2_r3<_T0, _T1, _T2>& __other) const {
        return std::tie(r1, r2, r3) > std::tie(__other.r1, __other.r2, __other.r3);
      }
      bool operator<=(const R_r1_r2_r3<_T0, _T1, _T2>& __other) const {
        return std::tie(r1, r2, r3) <= std::tie(__other.r1, __other.r2, __other.r3);
      }
      bool operator>=(const R_r1_r2_r3<_T0, _T1, _T2>& __other) const {
        return std::tie(r1, r2, r3) >= std::tie(__other.r1, __other.r2, __other.r3);
      }
      template <class archive>
      void serialize(archive& _archive, const unsigned int _version)  {
        _archive & BOOST_SERIALIZATION_NVP(r1);
        _archive & BOOST_SERIALIZATION_NVP(r2);
        _archive & BOOST_SERIALIZATION_NVP(r3);
      }
      template <class archive>
      void serialize(archive& _archive)  {
        _archive & r1;
        _archive & r2;
        _archive & r3;
      }
      _T0 r1;
      _T1 r2;
      _T2 r3;
};
#endif
#ifndef K3_R_r1_r2_r3_srimpl_lvl
#define K3_R_r1_r2_r3_srimpl_lvl
namespace boost {
  namespace serialization {
    template <class _T0, class _T1, class _T2>
    class implementation_level<R_r1_r2_r3<_T0, _T1, _T2>> {
      public:
          typedef  mpl::integral_c_tag tag;
          typedef  mpl::int_<object_serializable> type;
          BOOST_STATIC_CONSTANT(int, value = implementation_level::type::value);
    };
  }
}
#endif
namespace JSON {
  template <class _T0, class _T1, class _T2>
  class convert<R_r1_r2_r3<_T0, _T1, _T2>> {
    public:
        template <class Allocator>
        static rapidjson::Value encode(const R_r1_r2_r3<_T0, _T1, _T2>& r, Allocator& al)  {
          using namespace rapidjson;
          Value val;
          Value inner;
          val.SetObject();
          inner.SetObject();
          inner.AddMember("r1", convert<_T0>::encode(r.r1, al), al);
          inner.AddMember("r2", convert<_T1>::encode(r.r2, al), al);
          inner.AddMember("r3", convert<_T2>::encode(r.r3, al), al);
          val.AddMember("type", Value("record"), al);
          val.AddMember("value", inner.Move(), al);
          return val;
        }
  };
}
namespace YAML {
  template <class _T0, class _T1, class _T2>
  class convert<R_r1_r2_r3<_T0, _T1, _T2>> {
    public:
        static Node encode(const R_r1_r2_r3<_T0, _T1, _T2>& r)  {
          Node node;
          node["r1"] = convert<_T0>::encode(r.r1);
          node["r2"] = convert<_T1>::encode(r.r2);
          node["r3"] = convert<_T2>::encode(r.r3);
          return node;
        }
        static bool decode(const Node& node, R_r1_r2_r3<_T0, _T1, _T2>& r)  {
          if (!node.IsMap()) {
            return false;
          }
          if (node["r1"]) {
            r.r1 = node["r1"].as<_T0>();
          }
          if (node["r2"]) {
            r.r2 = node["r2"].as<_T1>();
          }
          if (node["r3"]) {
            r.r3 = node["r3"].as<_T2>();
          }
          return true;
        }
  };
}
#ifndef K3_R_r1_r2_r3_hash_value
#define K3_R_r1_r2_r3_hash_value
template <class _T0, class _T1, class _T2>
std::size_t hash_value(const R_r1_r2_r3<_T0, _T1, _T2>& r)  {
  boost::hash<std::tuple<_T0, _T1, _T2>> hasher;
  return hasher(std::tie(r.r1, r.r2, r.r3));
}
#endif
#ifndef K3_R_r1_r2_r3_r4
#define K3_R_r1_r2_r3_r4
template <class _T0, class _T1, class _T2, class _T3>
class R_r1_r2_r3_r4 {
  public:
      R_r1_r2_r3_r4(): r1(), r2(), r3(), r4()  {}
      template <class __T0, class __T1, class __T2, class __T3>
      R_r1_r2_r3_r4(__T0&& _r1, __T1&& _r2, __T2&& _r3, __T3&& _r4): r1(std::forward<__T0>(_r1)),
      r2(std::forward<__T1>(_r2)), r3(std::forward<__T2>(_r3)), r4(std::forward<__T3>(_r4))  {}
      bool operator==(const R_r1_r2_r3_r4<_T0, _T1, _T2, _T3>& __other) const {
        return r1 == __other.r1 && r2 == __other.r2 && r3 == __other.r3 && r4 == __other.r4;
      }
      bool operator!=(const R_r1_r2_r3_r4<_T0, _T1, _T2, _T3>& __other) const {
        return std::tie(r1, r2, r3, r4) != std::tie(__other.r1, __other.r2, __other.r3, __other.r4);
      }
      bool operator<(const R_r1_r2_r3_r4<_T0, _T1, _T2, _T3>& __other) const {
        return std::tie(r1, r2, r3, r4) < std::tie(__other.r1, __other.r2, __other.r3, __other.r4);
      }
      bool operator>(const R_r1_r2_r3_r4<_T0, _T1, _T2, _T3>& __other) const {
        return std::tie(r1, r2, r3, r4) > std::tie(__other.r1, __other.r2, __other.r3, __other.r4);
      }
      bool operator<=(const R_r1_r2_r3_r4<_T0, _T1, _T2, _T3>& __other) const {
        return std::tie(r1, r2, r3, r4) <= std::tie(__other.r1, __other.r2, __other.r3, __other.r4);
      }
      bool operator>=(const R_r1_r2_r3_r4<_T0, _T1, _T2, _T3>& __other) const {
        return std::tie(r1, r2, r3, r4) >= std::tie(__other.r1, __other.r2, __other.r3, __other.r4);
      }
      template <class archive>
      void serialize(archive& _archive, const unsigned int _version)  {
        _archive & BOOST_SERIALIZATION_NVP(r1);
        _archive & BOOST_SERIALIZATION_NVP(r2);
        _archive & BOOST_SERIALIZATION_NVP(r3);
        _archive & BOOST_SERIALIZATION_NVP(r4);
      }
      template <class archive>
      void serialize(archive& _archive)  {
        _archive & r1;
        _archive & r2;
        _archive & r3;
        _archive & r4;
      }
      _T0 r1;
      _T1 r2;
      _T2 r3;
      _T3 r4;
};
#endif
#ifndef K3_R_r1_r2_r3_r4_srimpl_lvl
#define K3_R_r1_r2_r3_r4_srimpl_lvl
namespace boost {
  namespace serialization {
    template <class _T0, class _T1, class _T2, class _T3>
    class implementation_level<R_r1_r2_r3_r4<_T0, _T1, _T2, _T3>> {
      public:
          typedef  mpl::integral_c_tag tag;
          typedef  mpl::int_<object_serializable> type;
          BOOST_STATIC_CONSTANT(int, value = implementation_level::type::value);
    };
  }
}
#endif
namespace JSON {
  template <class _T0, class _T1, class _T2, class _T3>
  class convert<R_r1_r2_r3_r4<_T0, _T1, _T2, _T3>> {
    public:
        template <class Allocator>
        static rapidjson::Value encode(const R_r1_r2_r3_r4<_T0, _T1, _T2, _T3>& r, Allocator& al)  {
          using namespace rapidjson;
          Value val;
          Value inner;
          val.SetObject();
          inner.SetObject();
          inner.AddMember("r1", convert<_T0>::encode(r.r1, al), al);
          inner.AddMember("r2", convert<_T1>::encode(r.r2, al), al);
          inner.AddMember("r3", convert<_T2>::encode(r.r3, al), al);
          inner.AddMember("r4", convert<_T3>::encode(r.r4, al), al);
          val.AddMember("type", Value("record"), al);
          val.AddMember("value", inner.Move(), al);
          return val;
        }
  };
}
namespace YAML {
  template <class _T0, class _T1, class _T2, class _T3>
  class convert<R_r1_r2_r3_r4<_T0, _T1, _T2, _T3>> {
    public:
        static Node encode(const R_r1_r2_r3_r4<_T0, _T1, _T2, _T3>& r)  {
          Node node;
          node["r1"] = convert<_T0>::encode(r.r1);
          node["r2"] = convert<_T1>::encode(r.r2);
          node["r3"] = convert<_T2>::encode(r.r3);
          node["r4"] = convert<_T3>::encode(r.r4);
          return node;
        }
        static bool decode(const Node& node, R_r1_r2_r3_r4<_T0, _T1, _T2, _T3>& r)  {
          if (!node.IsMap()) {
            return false;
          }
          if (node["r1"]) {
            r.r1 = node["r1"].as<_T0>();
          }
          if (node["r2"]) {
            r.r2 = node["r2"].as<_T1>();
          }
          if (node["r3"]) {
            r.r3 = node["r3"].as<_T2>();
          }
          if (node["r4"]) {
            r.r4 = node["r4"].as<_T3>();
          }
          return true;
        }
  };
}
#ifndef K3_R_r1_r2_r3_r4_hash_value
#define K3_R_r1_r2_r3_r4_hash_value
template <class _T0, class _T1, class _T2, class _T3>
std::size_t hash_value(const R_r1_r2_r3_r4<_T0, _T1, _T2, _T3>& r)  {
  boost::hash<std::tuple<_T0, _T1, _T2, _T3>> hasher;
  return hasher(std::tie(r.r1, r.r2, r.r3, r.r4));
}
#endif
#ifndef K3_R_r1_r2_r3_r4_r5
#define K3_R_r1_r2_r3_r4_r5
template <class _T0, class _T1, class _T2, class _T3, class _T4>
class R_r1_r2_r3_r4_r5 {
  public:
      R_r1_r2_r3_r4_r5(): r1(), r2(), r3(), r4(), r5()  {}
      template <class __T0, class __T1, class __T2, class __T3, class __T4>
      R_r1_r2_r3_r4_r5(__T0&& _r1, __T1&& _r2, __T2&& _r3, __T3&& _r4,
      __T4&& _r5): r1(std::forward<__T0>(_r1)), r2(std::forward<__T1>(_r2)),
      r3(std::forward<__T2>(_r3)), r4(std::forward<__T3>(_r4)), r5(std::forward<__T4>(_r5))  {}
      bool operator==(const R_r1_r2_r3_r4_r5<_T0, _T1, _T2, _T3, _T4>& __other) const {
        return r1 == __other.r1 && r2 == __other.r2 && r3 == __other.r3 && r4 == __other.r4 && r5 == __other.r5;
      }
      bool operator!=(const R_r1_r2_r3_r4_r5<_T0, _T1, _T2, _T3, _T4>& __other) const {
        return std::tie(r1, r2, r3, r4, r5) != std::tie(__other.r1, __other.r2, __other.r3,
        __other.r4, __other.r5);
      }
      bool operator<(const R_r1_r2_r3_r4_r5<_T0, _T1, _T2, _T3, _T4>& __other) const {
        return std::tie(r1, r2, r3, r4, r5) < std::tie(__other.r1, __other.r2, __other.r3,
        __other.r4, __other.r5);
      }
      bool operator>(const R_r1_r2_r3_r4_r5<_T0, _T1, _T2, _T3, _T4>& __other) const {
        return std::tie(r1, r2, r3, r4, r5) > std::tie(__other.r1, __other.r2, __other.r3,
        __other.r4, __other.r5);
      }
      bool operator<=(const R_r1_r2_r3_r4_r5<_T0, _T1, _T2, _T3, _T4>& __other) const {
        return std::tie(r1, r2, r3, r4, r5) <= std::tie(__other.r1, __other.r2, __other.r3,
        __other.r4, __other.r5);
      }
      bool operator>=(const R_r1_r2_r3_r4_r5<_T0, _T1, _T2, _T3, _T4>& __other) const {
        return std::tie(r1, r2, r3, r4, r5) >= std::tie(__other.r1, __other.r2, __other.r3,
        __other.r4, __other.r5);
      }
      template <class archive>
      void serialize(archive& _archive, const unsigned int _version)  {
        _archive & BOOST_SERIALIZATION_NVP(r1);
        _archive & BOOST_SERIALIZATION_NVP(r2);
        _archive & BOOST_SERIALIZATION_NVP(r3);
        _archive & BOOST_SERIALIZATION_NVP(r4);
        _archive & BOOST_SERIALIZATION_NVP(r5);
      }
      template <class archive>
      void serialize(archive& _archive)  {
        _archive & r1;
        _archive & r2;
        _archive & r3;
        _archive & r4;
        _archive & r5;
      }
      _T0 r1;
      _T1 r2;
      _T2 r3;
      _T3 r4;
      _T4 r5;
};
#endif
#ifndef K3_R_r1_r2_r3_r4_r5_srimpl_lvl
#define K3_R_r1_r2_r3_r4_r5_srimpl_lvl
namespace boost {
  namespace serialization {
    template <class _T0, class _T1, class _T2, class _T3, class _T4>
    class implementation_level<R_r1_r2_r3_r4_r5<_T0, _T1, _T2, _T3, _T4>> {
      public:
          typedef  mpl::integral_c_tag tag;
          typedef  mpl::int_<object_serializable> type;
          BOOST_STATIC_CONSTANT(int, value = implementation_level::type::value);
    };
  }
}
#endif
namespace JSON {
  template <class _T0, class _T1, class _T2, class _T3, class _T4>
  class convert<R_r1_r2_r3_r4_r5<_T0, _T1, _T2, _T3, _T4>> {
    public:
        template <class Allocator>
        static rapidjson::Value encode(const R_r1_r2_r3_r4_r5<_T0, _T1, _T2, _T3, _T4>& r,
        Allocator& al)  {
          using namespace rapidjson;
          Value val;
          Value inner;
          val.SetObject();
          inner.SetObject();
          inner.AddMember("r1", convert<_T0>::encode(r.r1, al), al);
          inner.AddMember("r2", convert<_T1>::encode(r.r2, al), al);
          inner.AddMember("r3", convert<_T2>::encode(r.r3, al), al);
          inner.AddMember("r4", convert<_T3>::encode(r.r4, al), al);
          inner.AddMember("r5", convert<_T4>::encode(r.r5, al), al);
          val.AddMember("type", Value("record"), al);
          val.AddMember("value", inner.Move(), al);
          return val;
        }
  };
}
namespace YAML {
  template <class _T0, class _T1, class _T2, class _T3, class _T4>
  class convert<R_r1_r2_r3_r4_r5<_T0, _T1, _T2, _T3, _T4>> {
    public:
        static Node encode(const R_r1_r2_r3_r4_r5<_T0, _T1, _T2, _T3, _T4>& r)  {
          Node node;
          node["r1"] = convert<_T0>::encode(r.r1);
          node["r2"] = convert<_T1>::encode(r.r2);
          node["r3"] = convert<_T2>::encode(r.r3);
          node["r4"] = convert<_T3>::encode(r.r4);
          node["r5"] = convert<_T4>::encode(r.r5);
          return node;
        }
        static bool decode(const Node& node, R_r1_r2_r3_r4_r5<_T0, _T1, _T2, _T3, _T4>& r)  {
          if (!node.IsMap()) {
            return false;
          }
          if (node["r1"]) {
            r.r1 = node["r1"].as<_T0>();
          }
          if (node["r2"]) {
            r.r2 = node["r2"].as<_T1>();
          }
          if (node["r3"]) {
            r.r3 = node["r3"].as<_T2>();
          }
          if (node["r4"]) {
            r.r4 = node["r4"].as<_T3>();
          }
          if (node["r5"]) {
            r.r5 = node["r5"].as<_T4>();
          }
          return true;
        }
  };
}
#ifndef K3_R_r1_r2_r3_r4_r5_hash_value
#define K3_R_r1_r2_r3_r4_r5_hash_value
template <class _T0, class _T1, class _T2, class _T3, class _T4>
std::size_t hash_value(const R_r1_r2_r3_r4_r5<_T0, _T1, _T2, _T3, _T4>& r)  {
  boost::hash<std::tuple<_T0, _T1, _T2, _T3, _T4>> hasher;
  return hasher(std::tie(r.r1, r.r2, r.r3, r.r4, r.r5));
}
#endif
#ifndef K3_R_r1_r2_r3_r4_r5_r6
#define K3_R_r1_r2_r3_r4_r5_r6
template <class _T0, class _T1, class _T2, class _T3, class _T4, class _T5>
class R_r1_r2_r3_r4_r5_r6 {
  public:
      R_r1_r2_r3_r4_r5_r6(): r1(), r2(), r3(), r4(), r5(), r6()  {}
      template <class __T0, class __T1, class __T2, class __T3, class __T4, class __T5>
      R_r1_r2_r3_r4_r5_r6(__T0&& _r1, __T1&& _r2, __T2&& _r3, __T3&& _r4, __T4&& _r5,
      __T5&& _r6): r1(std::forward<__T0>(_r1)), r2(std::forward<__T1>(_r2)),
      r3(std::forward<__T2>(_r3)), r4(std::forward<__T3>(_r4)), r5(std::forward<__T4>(_r5)),
      r6(std::forward<__T5>(_r6))  {}
      bool operator==(const R_r1_r2_r3_r4_r5_r6<_T0, _T1, _T2, _T3, _T4, _T5>& __other) const {
        return r1 == __other.r1 && r2 == __other.r2 && r3 == __other.r3 && r4 == __other.r4 && r5 == __other.r5 && r6 == __other.r6;
      }
      bool operator!=(const R_r1_r2_r3_r4_r5_r6<_T0, _T1, _T2, _T3, _T4, _T5>& __other) const {
        return std::tie(r1, r2, r3, r4, r5, r6) != std::tie(__other.r1, __other.r2, __other.r3,
        __other.r4, __other.r5, __other.r6);
      }
      bool operator<(const R_r1_r2_r3_r4_r5_r6<_T0, _T1, _T2, _T3, _T4, _T5>& __other) const {
        return std::tie(r1, r2, r3, r4, r5, r6) < std::tie(__other.r1, __other.r2, __other.r3,
        __other.r4, __other.r5, __other.r6);
      }
      bool operator>(const R_r1_r2_r3_r4_r5_r6<_T0, _T1, _T2, _T3, _T4, _T5>& __other) const {
        return std::tie(r1, r2, r3, r4, r5, r6) > std::tie(__other.r1, __other.r2, __other.r3,
        __other.r4, __other.r5, __other.r6);
      }
      bool operator<=(const R_r1_r2_r3_r4_r5_r6<_T0, _T1, _T2, _T3, _T4, _T5>& __other) const {
        return std::tie(r1, r2, r3, r4, r5, r6) <= std::tie(__other.r1, __other.r2, __other.r3,
        __other.r4, __other.r5, __other.r6);
      }
      bool operator>=(const R_r1_r2_r3_r4_r5_r6<_T0, _T1, _T2, _T3, _T4, _T5>& __other) const {
        return std::tie(r1, r2, r3, r4, r5, r6) >= std::tie(__other.r1, __other.r2, __other.r3,
        __other.r4, __other.r5, __other.r6);
      }
      template <class archive>
      void serialize(archive& _archive, const unsigned int _version)  {
        _archive & BOOST_SERIALIZATION_NVP(r1);
        _archive & BOOST_SERIALIZATION_NVP(r2);
        _archive & BOOST_SERIALIZATION_NVP(r3);
        _archive & BOOST_SERIALIZATION_NVP(r4);
        _archive & BOOST_SERIALIZATION_NVP(r5);
        _archive & BOOST_SERIALIZATION_NVP(r6);
      }
      template <class archive>
      void serialize(archive& _archive)  {
        _archive & r1;
        _archive & r2;
        _archive & r3;
        _archive & r4;
        _archive & r5;
        _archive & r6;
      }
      _T0 r1;
      _T1 r2;
      _T2 r3;
      _T3 r4;
      _T4 r5;
      _T5 r6;
};
#endif
#ifndef K3_R_r1_r2_r3_r4_r5_r6_srimpl_lvl
#define K3_R_r1_r2_r3_r4_r5_r6_srimpl_lvl
namespace boost {
  namespace serialization {
    template <class _T0, class _T1, class _T2, class _T3, class _T4, class _T5>
    class implementation_level<R_r1_r2_r3_r4_r5_r6<_T0, _T1, _T2, _T3, _T4, _T5>> {
      public:
          typedef  mpl::integral_c_tag tag;
          typedef  mpl::int_<object_serializable> type;
          BOOST_STATIC_CONSTANT(int, value = implementation_level::type::value);
    };
  }
}
#endif
namespace JSON {
  template <class _T0, class _T1, class _T2, class _T3, class _T4, class _T5>
  class convert<R_r1_r2_r3_r4_r5_r6<_T0, _T1, _T2, _T3, _T4, _T5>> {
    public:
        template <class Allocator>
        static rapidjson::Value encode(const R_r1_r2_r3_r4_r5_r6<_T0, _T1, _T2, _T3, _T4, _T5>& r,
        Allocator& al)  {
          using namespace rapidjson;
          Value val;
          Value inner;
          val.SetObject();
          inner.SetObject();
          inner.AddMember("r1", convert<_T0>::encode(r.r1, al), al);
          inner.AddMember("r2", convert<_T1>::encode(r.r2, al), al);
          inner.AddMember("r3", convert<_T2>::encode(r.r3, al), al);
          inner.AddMember("r4", convert<_T3>::encode(r.r4, al), al);
          inner.AddMember("r5", convert<_T4>::encode(r.r5, al), al);
          inner.AddMember("r6", convert<_T5>::encode(r.r6, al), al);
          val.AddMember("type", Value("record"), al);
          val.AddMember("value", inner.Move(), al);
          return val;
        }
  };
}
namespace YAML {
  template <class _T0, class _T1, class _T2, class _T3, class _T4, class _T5>
  class convert<R_r1_r2_r3_r4_r5_r6<_T0, _T1, _T2, _T3, _T4, _T5>> {
    public:
        static Node encode(const R_r1_r2_r3_r4_r5_r6<_T0, _T1, _T2, _T3, _T4, _T5>& r)  {
          Node node;
          node["r1"] = convert<_T0>::encode(r.r1);
          node["r2"] = convert<_T1>::encode(r.r2);
          node["r3"] = convert<_T2>::encode(r.r3);
          node["r4"] = convert<_T3>::encode(r.r4);
          node["r5"] = convert<_T4>::encode(r.r5);
          node["r6"] = convert<_T5>::encode(r.r6);
          return node;
        }
        static bool decode(const Node& node, R_r1_r2_r3_r4_r5_r6<_T0, _T1, _T2, _T3, _T4,
        _T5>& r)  {
          if (!node.IsMap()) {
            return false;
          }
          if (node["r1"]) {
            r.r1 = node["r1"].as<_T0>();
          }
          if (node["r2"]) {
            r.r2 = node["r2"].as<_T1>();
          }
          if (node["r3"]) {
            r.r3 = node["r3"].as<_T2>();
          }
          if (node["r4"]) {
            r.r4 = node["r4"].as<_T3>();
          }
          if (node["r5"]) {
            r.r5 = node["r5"].as<_T4>();
          }
          if (node["r6"]) {
            r.r6 = node["r6"].as<_T5>();
          }
          return true;
        }
  };
}
#ifndef K3_R_r1_r2_r3_r4_r5_r6_hash_value
#define K3_R_r1_r2_r3_r4_r5_r6_hash_value
template <class _T0, class _T1, class _T2, class _T3, class _T4, class _T5>
std::size_t hash_value(const R_r1_r2_r3_r4_r5_r6<_T0, _T1, _T2, _T3, _T4, _T5>& r)  {
  boost::hash<std::tuple<_T0, _T1, _T2, _T3, _T4, _T5>> hasher;
  return hasher(std::tie(r.r1, r.r2, r.r3, r.r4, r.r5, r.r6));
}
#endif
#ifndef K3_R_r1_r2_r3_r4_r5_r6_r7
#define K3_R_r1_r2_r3_r4_r5_r6_r7
template <class _T0, class _T1, class _T2, class _T3, class _T4, class _T5, class _T6>
class R_r1_r2_r3_r4_r5_r6_r7 {
  public:
      R_r1_r2_r3_r4_r5_r6_r7(): r1(), r2(), r3(), r4(), r5(), r6(), r7()  {}
      template <class __T0, class __T1, class __T2, class __T3, class __T4, class __T5, class __T6>
      R_r1_r2_r3_r4_r5_r6_r7(__T0&& _r1, __T1&& _r2, __T2&& _r3, __T3&& _r4, __T4&& _r5, __T5&& _r6,
      __T6&& _r7): r1(std::forward<__T0>(_r1)), r2(std::forward<__T1>(_r2)),
      r3(std::forward<__T2>(_r3)), r4(std::forward<__T3>(_r4)), r5(std::forward<__T4>(_r5)),
      r6(std::forward<__T5>(_r6)), r7(std::forward<__T6>(_r7))  {}
      bool operator==(const R_r1_r2_r3_r4_r5_r6_r7<_T0, _T1, _T2, _T3, _T4, _T5,
      _T6>& __other) const {
        return r1 == __other.r1 && r2 == __other.r2 && r3 == __other.r3 && r4 == __other.r4 && r5 == __other.r5 && r6 == __other.r6 && r7 == __other.r7;
      }
      bool operator!=(const R_r1_r2_r3_r4_r5_r6_r7<_T0, _T1, _T2, _T3, _T4, _T5,
      _T6>& __other) const {
        return std::tie(r1, r2, r3, r4, r5, r6, r7) != std::tie(__other.r1, __other.r2, __other.r3,
        __other.r4, __other.r5, __other.r6, __other.r7);
      }
      bool operator<(const R_r1_r2_r3_r4_r5_r6_r7<_T0, _T1, _T2, _T3, _T4, _T5,
      _T6>& __other) const {
        return std::tie(r1, r2, r3, r4, r5, r6, r7) < std::tie(__other.r1, __other.r2, __other.r3,
        __other.r4, __other.r5, __other.r6, __other.r7);
      }
      bool operator>(const R_r1_r2_r3_r4_r5_r6_r7<_T0, _T1, _T2, _T3, _T4, _T5,
      _T6>& __other) const {
        return std::tie(r1, r2, r3, r4, r5, r6, r7) > std::tie(__other.r1, __other.r2, __other.r3,
        __other.r4, __other.r5, __other.r6, __other.r7);
      }
      bool operator<=(const R_r1_r2_r3_r4_r5_r6_r7<_T0, _T1, _T2, _T3, _T4, _T5,
      _T6>& __other) const {
        return std::tie(r1, r2, r3, r4, r5, r6, r7) <= std::tie(__other.r1, __other.r2, __other.r3,
        __other.r4, __other.r5, __other.r6, __other.r7);
      }
      bool operator>=(const R_r1_r2_r3_r4_r5_r6_r7<_T0, _T1, _T2, _T3, _T4, _T5,
      _T6>& __other) const {
        return std::tie(r1, r2, r3, r4, r5, r6, r7) >= std::tie(__other.r1, __other.r2, __other.r3,
        __other.r4, __other.r5, __other.r6, __other.r7);
      }
      template <class archive>
      void serialize(archive& _archive, const unsigned int _version)  {
        _archive & BOOST_SERIALIZATION_NVP(r1);
        _archive & BOOST_SERIALIZATION_NVP(r2);
        _archive & BOOST_SERIALIZATION_NVP(r3);
        _archive & BOOST_SERIALIZATION_NVP(r4);
        _archive & BOOST_SERIALIZATION_NVP(r5);
        _archive & BOOST_SERIALIZATION_NVP(r6);
        _archive & BOOST_SERIALIZATION_NVP(r7);
      }
      template <class archive>
      void serialize(archive& _archive)  {
        _archive & r1;
        _archive & r2;
        _archive & r3;
        _archive & r4;
        _archive & r5;
        _archive & r6;
        _archive & r7;
      }
      _T0 r1;
      _T1 r2;
      _T2 r3;
      _T3 r4;
      _T4 r5;
      _T5 r6;
      _T6 r7;
};
#endif
#ifndef K3_R_r1_r2_r3_r4_r5_r6_r7_srimpl_lvl
#define K3_R_r1_r2_r3_r4_r5_r6_r7_srimpl_lvl
namespace boost {
  namespace serialization {
    template <class _T0, class _T1, class _T2, class _T3, class _T4, class _T5, class _T6>
    class implementation_level<R_r1_r2_r3_r4_r5_r6_r7<_T0, _T1, _T2, _T3, _T4, _T5, _T6>> {
      public:
          typedef  mpl::integral_c_tag tag;
          typedef  mpl::int_<object_serializable> type;
          BOOST_STATIC_CONSTANT(int, value = implementation_level::type::value);
    };
  }
}
#endif
namespace JSON {
  template <class _T0, class _T1, class _T2, class _T3, class _T4, class _T5, class _T6>
  class convert<R_r1_r2_r3_r4_r5_r6_r7<_T0, _T1, _T2, _T3, _T4, _T5, _T6>> {
    public:
        template <class Allocator>
        static rapidjson::Value encode(const R_r1_r2_r3_r4_r5_r6_r7<_T0, _T1, _T2, _T3, _T4, _T5,
        _T6>& r, Allocator& al)  {
          using namespace rapidjson;
          Value val;
          Value inner;
          val.SetObject();
          inner.SetObject();
          inner.AddMember("r1", convert<_T0>::encode(r.r1, al), al);
          inner.AddMember("r2", convert<_T1>::encode(r.r2, al), al);
          inner.AddMember("r3", convert<_T2>::encode(r.r3, al), al);
          inner.AddMember("r4", convert<_T3>::encode(r.r4, al), al);
          inner.AddMember("r5", convert<_T4>::encode(r.r5, al), al);
          inner.AddMember("r6", convert<_T5>::encode(r.r6, al), al);
          inner.AddMember("r7", convert<_T6>::encode(r.r7, al), al);
          val.AddMember("type", Value("record"), al);
          val.AddMember("value", inner.Move(), al);
          return val;
        }
  };
}
namespace YAML {
  template <class _T0, class _T1, class _T2, class _T3, class _T4, class _T5, class _T6>
  class convert<R_r1_r2_r3_r4_r5_r6_r7<_T0, _T1, _T2, _T3, _T4, _T5, _T6>> {
    public:
        static Node encode(const R_r1_r2_r3_r4_r5_r6_r7<_T0, _T1, _T2, _T3, _T4, _T5, _T6>& r)  {
          Node node;
          node["r1"] = convert<_T0>::encode(r.r1);
          node["r2"] = convert<_T1>::encode(r.r2);
          node["r3"] = convert<_T2>::encode(r.r3);
          node["r4"] = convert<_T3>::encode(r.r4);
          node["r5"] = convert<_T4>::encode(r.r5);
          node["r6"] = convert<_T5>::encode(r.r6);
          node["r7"] = convert<_T6>::encode(r.r7);
          return node;
        }
        static bool decode(const Node& node, R_r1_r2_r3_r4_r5_r6_r7<_T0, _T1, _T2, _T3, _T4, _T5,
        _T6>& r)  {
          if (!node.IsMap()) {
            return false;
          }
          if (node["r1"]) {
            r.r1 = node["r1"].as<_T0>();
          }
          if (node["r2"]) {
            r.r2 = node["r2"].as<_T1>();
          }
          if (node["r3"]) {
            r.r3 = node["r3"].as<_T2>();
          }
          if (node["r4"]) {
            r.r4 = node["r4"].as<_T3>();
          }
          if (node["r5"]) {
            r.r5 = node["r5"].as<_T4>();
          }
          if (node["r6"]) {
            r.r6 = node["r6"].as<_T5>();
          }
          if (node["r7"]) {
            r.r7 = node["r7"].as<_T6>();
          }
          return true;
        }
  };
}
#ifndef K3_R_r1_r2_r3_r4_r5_r6_r7_hash_value
#define K3_R_r1_r2_r3_r4_r5_r6_r7_hash_value
template <class _T0, class _T1, class _T2, class _T3, class _T4, class _T5, class _T6>
std::size_t hash_value(const R_r1_r2_r3_r4_r5_r6_r7<_T0, _T1, _T2, _T3, _T4, _T5, _T6>& r)  {
  boost::hash<std::tuple<_T0, _T1, _T2, _T3, _T4, _T5, _T6>> hasher;
  return hasher(std::tie(r.r1, r.r2, r.r3, r.r4, r.r5, r.r6, r.r7));
}
#endif
#ifndef K3_R_r1_r2_r3_r4_r5_r6_r7_r8
#define K3_R_r1_r2_r3_r4_r5_r6_r7_r8
template <class _T0, class _T1, class _T2, class _T3, class _T4, class _T5, class _T6, class _T7>
class R_r1_r2_r3_r4_r5_r6_r7_r8 {
  public:
      R_r1_r2_r3_r4_r5_r6_r7_r8(): r1(), r2(), r3(), r4(), r5(), r6(), r7(), r8()  {}
      template <class __T0, class __T1, class __T2, class __T3, class __T4, class __T5, class __T6,
      class __T7>
      R_r1_r2_r3_r4_r5_r6_r7_r8(__T0&& _r1, __T1&& _r2, __T2&& _r3, __T3&& _r4, __T4&& _r5,
      __T5&& _r6, __T6&& _r7, __T7&& _r8): r1(std::forward<__T0>(_r1)), r2(std::forward<__T1>(_r2)),
      r3(std::forward<__T2>(_r3)), r4(std::forward<__T3>(_r4)), r5(std::forward<__T4>(_r5)),
      r6(std::forward<__T5>(_r6)), r7(std::forward<__T6>(_r7)), r8(std::forward<__T7>(_r8))  {}
      bool operator==(const R_r1_r2_r3_r4_r5_r6_r7_r8<_T0, _T1, _T2, _T3, _T4, _T5, _T6,
      _T7>& __other) const {
        return r1 == __other.r1 && r2 == __other.r2 && r3 == __other.r3 && r4 == __other.r4 && r5 == __other.r5 && r6 == __other.r6 && r7 == __other.r7 && r8 == __other.r8;
      }
      bool operator!=(const R_r1_r2_r3_r4_r5_r6_r7_r8<_T0, _T1, _T2, _T3, _T4, _T5, _T6,
      _T7>& __other) const {
        return std::tie(r1, r2, r3, r4, r5, r6, r7, r8) != std::tie(__other.r1, __other.r2,
        __other.r3, __other.r4, __other.r5, __other.r6, __other.r7, __other.r8);
      }
      bool operator<(const R_r1_r2_r3_r4_r5_r6_r7_r8<_T0, _T1, _T2, _T3, _T4, _T5, _T6,
      _T7>& __other) const {
        return std::tie(r1, r2, r3, r4, r5, r6, r7, r8) < std::tie(__other.r1, __other.r2,
        __other.r3, __other.r4, __other.r5, __other.r6, __other.r7, __other.r8);
      }
      bool operator>(const R_r1_r2_r3_r4_r5_r6_r7_r8<_T0, _T1, _T2, _T3, _T4, _T5, _T6,
      _T7>& __other) const {
        return std::tie(r1, r2, r3, r4, r5, r6, r7, r8) > std::tie(__other.r1, __other.r2,
        __other.r3, __other.r4, __other.r5, __other.r6, __other.r7, __other.r8);
      }
      bool operator<=(const R_r1_r2_r3_r4_r5_r6_r7_r8<_T0, _T1, _T2, _T3, _T4, _T5, _T6,
      _T7>& __other) const {
        return std::tie(r1, r2, r3, r4, r5, r6, r7, r8) <= std::tie(__other.r1, __other.r2,
        __other.r3, __other.r4, __other.r5, __other.r6, __other.r7, __other.r8);
      }
      bool operator>=(const R_r1_r2_r3_r4_r5_r6_r7_r8<_T0, _T1, _T2, _T3, _T4, _T5, _T6,
      _T7>& __other) const {
        return std::tie(r1, r2, r3, r4, r5, r6, r7, r8) >= std::tie(__other.r1, __other.r2,
        __other.r3, __other.r4, __other.r5, __other.r6, __other.r7, __other.r8);
      }
      template <class archive>
      void serialize(archive& _archive, const unsigned int _version)  {
        _archive & BOOST_SERIALIZATION_NVP(r1);
        _archive & BOOST_SERIALIZATION_NVP(r2);
        _archive & BOOST_SERIALIZATION_NVP(r3);
        _archive & BOOST_SERIALIZATION_NVP(r4);
        _archive & BOOST_SERIALIZATION_NVP(r5);
        _archive & BOOST_SERIALIZATION_NVP(r6);
        _archive & BOOST_SERIALIZATION_NVP(r7);
        _archive & BOOST_SERIALIZATION_NVP(r8);
      }
      template <class archive>
      void serialize(archive& _archive)  {
        _archive & r1;
        _archive & r2;
        _archive & r3;
        _archive & r4;
        _archive & r5;
        _archive & r6;
        _archive & r7;
        _archive & r8;
      }
      _T0 r1;
      _T1 r2;
      _T2 r3;
      _T3 r4;
      _T4 r5;
      _T5 r6;
      _T6 r7;
      _T7 r8;
};
#endif
#ifndef K3_R_r1_r2_r3_r4_r5_r6_r7_r8_srimpl_lvl
#define K3_R_r1_r2_r3_r4_r5_r6_r7_r8_srimpl_lvl
namespace boost {
  namespace serialization {
    template <class _T0, class _T1, class _T2, class _T3, class _T4, class _T5, class _T6,
    class _T7>
    class implementation_level<R_r1_r2_r3_r4_r5_r6_r7_r8<_T0, _T1, _T2, _T3, _T4, _T5, _T6, _T7>> {
      public:
          typedef  mpl::integral_c_tag tag;
          typedef  mpl::int_<object_serializable> type;
          BOOST_STATIC_CONSTANT(int, value = implementation_level::type::value);
    };
  }
}
#endif
namespace JSON {
  template <class _T0, class _T1, class _T2, class _T3, class _T4, class _T5, class _T6, class _T7>
  class convert<R_r1_r2_r3_r4_r5_r6_r7_r8<_T0, _T1, _T2, _T3, _T4, _T5, _T6, _T7>> {
    public:
        template <class Allocator>
        static rapidjson::Value encode(const R_r1_r2_r3_r4_r5_r6_r7_r8<_T0, _T1, _T2, _T3, _T4, _T5,
        _T6, _T7>& r, Allocator& al)  {
          using namespace rapidjson;
          Value val;
          Value inner;
          val.SetObject();
          inner.SetObject();
          inner.AddMember("r1", convert<_T0>::encode(r.r1, al), al);
          inner.AddMember("r2", convert<_T1>::encode(r.r2, al), al);
          inner.AddMember("r3", convert<_T2>::encode(r.r3, al), al);
          inner.AddMember("r4", convert<_T3>::encode(r.r4, al), al);
          inner.AddMember("r5", convert<_T4>::encode(r.r5, al), al);
          inner.AddMember("r6", convert<_T5>::encode(r.r6, al), al);
          inner.AddMember("r7", convert<_T6>::encode(r.r7, al), al);
          inner.AddMember("r8", convert<_T7>::encode(r.r8, al), al);
          val.AddMember("type", Value("record"), al);
          val.AddMember("value", inner.Move(), al);
          return val;
        }
  };
}
namespace YAML {
  template <class _T0, class _T1, class _T2, class _T3, class _T4, class _T5, class _T6, class _T7>
  class convert<R_r1_r2_r3_r4_r5_r6_r7_r8<_T0, _T1, _T2, _T3, _T4, _T5, _T6, _T7>> {
    public:
        static Node encode(const R_r1_r2_r3_r4_r5_r6_r7_r8<_T0, _T1, _T2, _T3, _T4, _T5, _T6,
        _T7>& r)  {
          Node node;
          node["r1"] = convert<_T0>::encode(r.r1);
          node["r2"] = convert<_T1>::encode(r.r2);
          node["r3"] = convert<_T2>::encode(r.r3);
          node["r4"] = convert<_T3>::encode(r.r4);
          node["r5"] = convert<_T4>::encode(r.r5);
          node["r6"] = convert<_T5>::encode(r.r6);
          node["r7"] = convert<_T6>::encode(r.r7);
          node["r8"] = convert<_T7>::encode(r.r8);
          return node;
        }
        static bool decode(const Node& node, R_r1_r2_r3_r4_r5_r6_r7_r8<_T0, _T1, _T2, _T3, _T4, _T5,
        _T6, _T7>& r)  {
          if (!node.IsMap()) {
            return false;
          }
          if (node["r1"]) {
            r.r1 = node["r1"].as<_T0>();
          }
          if (node["r2"]) {
            r.r2 = node["r2"].as<_T1>();
          }
          if (node["r3"]) {
            r.r3 = node["r3"].as<_T2>();
          }
          if (node["r4"]) {
            r.r4 = node["r4"].as<_T3>();
          }
          if (node["r5"]) {
            r.r5 = node["r5"].as<_T4>();
          }
          if (node["r6"]) {
            r.r6 = node["r6"].as<_T5>();
          }
          if (node["r7"]) {
            r.r7 = node["r7"].as<_T6>();
          }
          if (node["r8"]) {
            r.r8 = node["r8"].as<_T7>();
          }
          return true;
        }
  };
}
#ifndef K3_R_r1_r2_r3_r4_r5_r6_r7_r8_hash_value
#define K3_R_r1_r2_r3_r4_r5_r6_r7_r8_hash_value
template <class _T0, class _T1, class _T2, class _T3, class _T4, class _T5, class _T6, class _T7>
std::size_t hash_value(const R_r1_r2_r3_r4_r5_r6_r7_r8<_T0, _T1, _T2, _T3, _T4, _T5, _T6,
_T7>& r)  {
  boost::hash<std::tuple<_T0, _T1, _T2, _T3, _T4, _T5, _T6, _T7>> hasher;
  return hasher(std::tie(r.r1, r.r2, r.r3, r.r4, r.r5, r.r6, r.r7, r.r8));
}
#endif
template <class __CONTENT>
class _Collection: public K3::Collection<__CONTENT> {
  public:
      _Collection(): K3::Collection<__CONTENT>()  {}
      _Collection(const K3::Collection<__CONTENT>& __other1): K3::Collection<__CONTENT>(__other1)  {}
      _Collection(K3::Collection<__CONTENT>&& __other1): K3::Collection<__CONTENT>(std::move(__other1))  {}
      template <class archive>
      void serialize(archive& _archive, const unsigned int _version)  {
        _archive & boost::serialization::make_nvp("Collection",
        boost::serialization::base_object<K3::Collection<__CONTENT>>(*this));
      }
      template <class archive>
      void serialize(archive& _archive)  {
        _archive & yas::base_object<K3::Collection<__CONTENT>>(*this);
      }
};
namespace boost {
  namespace serialization {
    template <class __CONTENT>
    class implementation_level<_Collection<__CONTENT>> {
      public:
          typedef  mpl::integral_c_tag tag;
          typedef  mpl::int_<object_serializable> type;
          BOOST_STATIC_CONSTANT(int, value = implementation_level::type::value);
    };
  }
}
namespace YAML {
  template <class __CONTENT>
  class convert<_Collection<__CONTENT>> {
    public:
        static Node encode(const _Collection<__CONTENT>& c)  {
          return convert<K3::Collection<__CONTENT>>::encode(c);
        }
        static bool decode(const Node& node, _Collection<__CONTENT>& c)  {
          return convert<K3::Collection<__CONTENT>>::decode(node, c);
        }
  };
}
namespace JSON {
  template <class __CONTENT>
  class convert<_Collection<__CONTENT>> {
    public:
        template <class Allocator>
        static rapidjson::Value encode(const _Collection<__CONTENT>& c, Allocator& al)  {
          return convert<K3::Collection<__CONTENT>>::encode(c, al);
        }
  };
}
template <class __CONTENT>
class _Map: public K3::Map<__CONTENT> {
  public:
      _Map(): K3::Map<__CONTENT>()  {}
      _Map(const K3::Map<__CONTENT>& __other1): K3::Map<__CONTENT>(__other1)  {}
      _Map(K3::Map<__CONTENT>&& __other1): K3::Map<__CONTENT>(std::move(__other1))  {}
      template <class archive>
      void serialize(archive& _archive, const unsigned int _version)  {
        _archive & boost::serialization::make_nvp("Map",
        boost::serialization::base_object<K3::Map<__CONTENT>>(*this));
      }
      template <class archive>
      void serialize(archive& _archive)  {
        _archive & yas::base_object<K3::Map<__CONTENT>>(*this);
      }
};
namespace boost {
  namespace serialization {
    template <class __CONTENT>
    class implementation_level<_Map<__CONTENT>> {
      public:
          typedef  mpl::integral_c_tag tag;
          typedef  mpl::int_<object_serializable> type;
          BOOST_STATIC_CONSTANT(int, value = implementation_level::type::value);
    };
  }
}
namespace YAML {
  template <class __CONTENT>
  class convert<_Map<__CONTENT>> {
    public:
        static Node encode(const _Map<__CONTENT>& c)  {
          return convert<K3::Map<__CONTENT>>::encode(c);
        }
        static bool decode(const Node& node, _Map<__CONTENT>& c)  {
          return convert<K3::Map<__CONTENT>>::decode(node, c);
        }
  };
}
namespace JSON {
  template <class __CONTENT>
  class convert<_Map<__CONTENT>> {
    public:
        template <class Allocator>
        static rapidjson::Value encode(const _Map<__CONTENT>& c, Allocator& al)  {
          return convert<K3::Map<__CONTENT>>::encode(c, al);
        }
  };
}
template <class __CONTENT>
class _Seq: public K3::Seq<__CONTENT> {
  public:
      _Seq(): K3::Seq<__CONTENT>()  {}
      _Seq(const K3::Seq<__CONTENT>& __other1): K3::Seq<__CONTENT>(__other1)  {}
      _Seq(K3::Seq<__CONTENT>&& __other1): K3::Seq<__CONTENT>(std::move(__other1))  {}
      template <class archive>
      void serialize(archive& _archive, const unsigned int _version)  {
        _archive & boost::serialization::make_nvp("Seq",
        boost::serialization::base_object<K3::Seq<__CONTENT>>(*this));
      }
      template <class archive>
      void serialize(archive& _archive)  {
        _archive & yas::base_object<K3::Seq<__CONTENT>>(*this);
      }
};
namespace boost {
  namespace serialization {
    template <class __CONTENT>
    class implementation_level<_Seq<__CONTENT>> {
      public:
          typedef  mpl::integral_c_tag tag;
          typedef  mpl::int_<object_serializable> type;
          BOOST_STATIC_CONSTANT(int, value = implementation_level::type::value);
    };
  }
}
namespace YAML {
  template <class __CONTENT>
  class convert<_Seq<__CONTENT>> {
    public:
        static Node encode(const _Seq<__CONTENT>& c)  {
          return convert<K3::Seq<__CONTENT>>::encode(c);
        }
        static bool decode(const Node& node, _Seq<__CONTENT>& c)  {
          return convert<K3::Seq<__CONTENT>>::decode(node, c);
        }
  };
}
namespace JSON {
  template <class __CONTENT>
  class convert<_Seq<__CONTENT>> {
    public:
        template <class Allocator>
        static rapidjson::Value encode(const _Seq<__CONTENT>& c, Allocator& al)  {
          return convert<K3::Seq<__CONTENT>>::encode(c, al);
        }
  };
}
template <class __CONTENT>
class _Set: public K3::Set<__CONTENT> {
  public:
      _Set(): K3::Set<__CONTENT>()  {}
      _Set(const K3::Set<__CONTENT>& __other1): K3::Set<__CONTENT>(__other1)  {}
      _Set(K3::Set<__CONTENT>&& __other1): K3::Set<__CONTENT>(std::move(__other1))  {}
      template <class archive>
      void serialize(archive& _archive, const unsigned int _version)  {
        _archive & boost::serialization::make_nvp("Set",
        boost::serialization::base_object<K3::Set<__CONTENT>>(*this));
      }
      template <class archive>
      void serialize(archive& _archive)  {
        _archive & yas::base_object<K3::Set<__CONTENT>>(*this);
      }
};
namespace boost {
  namespace serialization {
    template <class __CONTENT>
    class implementation_level<_Set<__CONTENT>> {
      public:
          typedef  mpl::integral_c_tag tag;
          typedef  mpl::int_<object_serializable> type;
          BOOST_STATIC_CONSTANT(int, value = implementation_level::type::value);
    };
  }
}
namespace YAML {
  template <class __CONTENT>
  class convert<_Set<__CONTENT>> {
    public:
        static Node encode(const _Set<__CONTENT>& c)  {
          return convert<K3::Set<__CONTENT>>::encode(c);
        }
        static bool decode(const Node& node, _Set<__CONTENT>& c)  {
          return convert<K3::Set<__CONTENT>>::decode(node, c);
        }
  };
}
namespace JSON {
  template <class __CONTENT>
  class convert<_Set<__CONTENT>> {
    public:
        template <class Allocator>
        static rapidjson::Value encode(const _Set<__CONTENT>& c, Allocator& al)  {
          return convert<K3::Set<__CONTENT>>::encode(c, al);
        }
  };
}
class __global_context: public K3::__standard_context, public K3::__string_context,
public K3::__time_context, public K3::__pcm_context, public K3::__tcmalloc_context,
public K3::__jemalloc_context {
  public:
      __global_context(Engine& __engine): K3::__standard_context(__engine), K3::__string_context(),
      K3::__time_context(), K3::__pcm_context(), K3::__tcmalloc_context()  {
        dispatch_table[__switchController_tid] = [this] (void* payload, const Address& source)  {
          auto ct = currentTime();
          auto v = *static_cast<unit_t*>(payload);
          switchController(v);
          if (this->__engine.logJsonEnabled()) {
            this->__engine.logJson(ct, me, "switchController",
            K3::serialization::json::encode<unit_t>(v), __jsonify(), source);
          }
        };
        dispatch_table[__delete_R_rcv_corrective_s14_m___SQL_SUM_AGGREGATE_1_mR1_tid] = [this] (void* payload,
        const Address& source)  {
          auto ct = currentTime();
          auto v = *static_cast<R_r1_r2_r3_r4_r5_r6_r7<Address, int, R_key_value<int, int>, int,
          R_key_value<int, int>, _Seq<R_key_value<int, int>>, _Set<R_key_value<int,
          int>>>*>(payload);
          delete_R_rcv_corrective_s14_m___SQL_SUM_AGGREGATE_1_mR1(v);
          if (this->__engine.logJsonEnabled()) {
            this->__engine.logJson(ct, me,
            "delete_R_rcv_corrective_s14_m___SQL_SUM_AGGREGATE_1_mR1",
            K3::serialization::json::encode<R_r1_r2_r3_r4_r5_r6_r7<Address, int, R_key_value<int,
            int>, int, R_key_value<int, int>, _Seq<R_key_value<int, int>>, _Set<R_key_value<int,
            int>>>>(v), __jsonify(), source);
          }
        };
        dispatch_table[__delete_R_rcv_corrective_s14_m___SQL_SUM_AGGREGATE_2_mR1_tid] = [this] (void* payload,
        const Address& source)  {
          auto ct = currentTime();
          auto v = *static_cast<R_r1_r2_r3_r4_r5_r6_r7<Address, int, R_key_value<int, int>, int,
          R_key_value<int, int>, _Seq<R_key_value<int, int>>, _Set<R_key_value<int,
          int>>>*>(payload);
          delete_R_rcv_corrective_s14_m___SQL_SUM_AGGREGATE_2_mR1(v);
          if (this->__engine.logJsonEnabled()) {
            this->__engine.logJson(ct, me,
            "delete_R_rcv_corrective_s14_m___SQL_SUM_AGGREGATE_2_mR1",
            K3::serialization::json::encode<R_r1_r2_r3_r4_r5_r6_r7<Address, int, R_key_value<int,
            int>, int, R_key_value<int, int>, _Seq<R_key_value<int, int>>, _Set<R_key_value<int,
            int>>>>(v), __jsonify(), source);
          }
        };
        dispatch_table[__delete_R_rcv_corrective_s12_m___SQL_SUM_AGGREGATE_1_mR1_tid] = [this] (void* payload,
        const Address& source)  {
          auto ct = currentTime();
          auto v = *static_cast<R_r1_r2_r3_r4_r5_r6_r7<Address, int, R_key_value<int, int>, int,
          R_key_value<int, int>, _Seq<R_key_value<int, int>>, _Set<R_key_value<int,
          int>>>*>(payload);
          delete_R_rcv_corrective_s12_m___SQL_SUM_AGGREGATE_1_mR1(v);
          if (this->__engine.logJsonEnabled()) {
            this->__engine.logJson(ct, me,
            "delete_R_rcv_corrective_s12_m___SQL_SUM_AGGREGATE_1_mR1",
            K3::serialization::json::encode<R_r1_r2_r3_r4_r5_r6_r7<Address, int, R_key_value<int,
            int>, int, R_key_value<int, int>, _Seq<R_key_value<int, int>>, _Set<R_key_value<int,
            int>>>>(v), __jsonify(), source);
          }
        };
        dispatch_table[__nd_delete_R_do_complete_s15_trig_tid] = [this] (void* payload,
        const Address& source)  {
          auto ct = currentTime();
          auto v = *static_cast<R_r1_r2_r3<R_key_value<int, int>, int, int>*>(payload);
          nd_delete_R_do_complete_s15_trig(v);
          if (this->__engine.logJsonEnabled()) {
            this->__engine.logJson(ct, me, "nd_delete_R_do_complete_s15_trig",
            K3::serialization::json::encode<R_r1_r2_r3<R_key_value<int, int>, int, int>>(v),
            __jsonify(), source);
          }
        };
        dispatch_table[__nd_delete_R_do_complete_s13_trig_tid] = [this] (void* payload,
        const Address& source)  {
          auto ct = currentTime();
          auto v = *static_cast<R_r1_r2_r3<R_key_value<int, int>, int, int>*>(payload);
          nd_delete_R_do_complete_s13_trig(v);
          if (this->__engine.logJsonEnabled()) {
            this->__engine.logJson(ct, me, "nd_delete_R_do_complete_s13_trig",
            K3::serialization::json::encode<R_r1_r2_r3<R_key_value<int, int>, int, int>>(v),
            __jsonify(), source);
          }
        };
        dispatch_table[__nd_delete_R_rcv_push_s14_m___SQL_SUM_AGGREGATE_1_mR1_tid] = [this] (void* payload,
        const Address& source)  {
          auto ct = currentTime();
          auto v = *static_cast<R_r1_r2_r3_r4<_Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>,
          R_key_value<int, int>, int, int>*>(payload);
          nd_delete_R_rcv_push_s14_m___SQL_SUM_AGGREGATE_1_mR1(v);
          if (this->__engine.logJsonEnabled()) {
            this->__engine.logJson(ct, me, "nd_delete_R_rcv_push_s14_m___SQL_SUM_AGGREGATE_1_mR1",
            K3::serialization::json::encode<R_r1_r2_r3_r4<_Set<R_r1_r2_r3<R_key_value<int, int>,
            int, int>>, R_key_value<int, int>, int, int>>(v), __jsonify(), source);
          }
        };
        dispatch_table[__nd_delete_R_rcv_push_s14_m___SQL_SUM_AGGREGATE_2_mR1_tid] = [this] (void* payload,
        const Address& source)  {
          auto ct = currentTime();
          auto v = *static_cast<R_r1_r2_r3_r4<_Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>,
          R_key_value<int, int>, int, int>*>(payload);
          nd_delete_R_rcv_push_s14_m___SQL_SUM_AGGREGATE_2_mR1(v);
          if (this->__engine.logJsonEnabled()) {
            this->__engine.logJson(ct, me, "nd_delete_R_rcv_push_s14_m___SQL_SUM_AGGREGATE_2_mR1",
            K3::serialization::json::encode<R_r1_r2_r3_r4<_Set<R_r1_r2_r3<R_key_value<int, int>,
            int, int>>, R_key_value<int, int>, int, int>>(v), __jsonify(), source);
          }
        };
        dispatch_table[__nd_delete_R_rcv_push_s12_m___SQL_SUM_AGGREGATE_1_mR1_tid] = [this] (void* payload,
        const Address& source)  {
          auto ct = currentTime();
          auto v = *static_cast<R_r1_r2_r3_r4<_Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>,
          R_key_value<int, int>, int, int>*>(payload);
          nd_delete_R_rcv_push_s12_m___SQL_SUM_AGGREGATE_1_mR1(v);
          if (this->__engine.logJsonEnabled()) {
            this->__engine.logJson(ct, me, "nd_delete_R_rcv_push_s12_m___SQL_SUM_AGGREGATE_1_mR1",
            K3::serialization::json::encode<R_r1_r2_r3_r4<_Set<R_r1_r2_r3<R_key_value<int, int>,
            int, int>>, R_key_value<int, int>, int, int>>(v), __jsonify(), source);
          }
        };
        dispatch_table[__nd_delete_R_send_push_s14_m___SQL_SUM_AGGREGATE_1_mR1_tid] = [this] (void* payload,
        const Address& source)  {
          auto ct = currentTime();
          auto v = *static_cast<R_r1_r2_r3<R_key_value<int, int>, int, int>*>(payload);
          nd_delete_R_send_push_s14_m___SQL_SUM_AGGREGATE_1_mR1(v);
          if (this->__engine.logJsonEnabled()) {
            this->__engine.logJson(ct, me, "nd_delete_R_send_push_s14_m___SQL_SUM_AGGREGATE_1_mR1",
            K3::serialization::json::encode<R_r1_r2_r3<R_key_value<int, int>, int, int>>(v),
            __jsonify(), source);
          }
        };
        dispatch_table[__nd_delete_R_send_push_s14_m___SQL_SUM_AGGREGATE_2_mR1_tid] = [this] (void* payload,
        const Address& source)  {
          auto ct = currentTime();
          auto v = *static_cast<R_r1_r2_r3<R_key_value<int, int>, int, int>*>(payload);
          nd_delete_R_send_push_s14_m___SQL_SUM_AGGREGATE_2_mR1(v);
          if (this->__engine.logJsonEnabled()) {
            this->__engine.logJson(ct, me, "nd_delete_R_send_push_s14_m___SQL_SUM_AGGREGATE_2_mR1",
            K3::serialization::json::encode<R_r1_r2_r3<R_key_value<int, int>, int, int>>(v),
            __jsonify(), source);
          }
        };
        dispatch_table[__nd_delete_R_send_push_s12_m___SQL_SUM_AGGREGATE_1_mR1_tid] = [this] (void* payload,
        const Address& source)  {
          auto ct = currentTime();
          auto v = *static_cast<R_r1_r2_r3<R_key_value<int, int>, int, int>*>(payload);
          nd_delete_R_send_push_s12_m___SQL_SUM_AGGREGATE_1_mR1(v);
          if (this->__engine.logJsonEnabled()) {
            this->__engine.logJson(ct, me, "nd_delete_R_send_push_s12_m___SQL_SUM_AGGREGATE_1_mR1",
            K3::serialization::json::encode<R_r1_r2_r3<R_key_value<int, int>, int, int>>(v),
            __jsonify(), source);
          }
        };
        dispatch_table[__nd_delete_R_rcv_fetch_tid] = [this] (void* payload,
        const Address& source)  {
          auto ct = currentTime();
          auto v = *static_cast<R_r1_r2_r3_r4<_Collection<R_key_value<int, int>>, R_key_value<int,
          int>, int, int>*>(payload);
          nd_delete_R_rcv_fetch(v);
          if (this->__engine.logJsonEnabled()) {
            this->__engine.logJson(ct, me, "nd_delete_R_rcv_fetch",
            K3::serialization::json::encode<R_r1_r2_r3_r4<_Collection<R_key_value<int, int>>,
            R_key_value<int, int>, int, int>>(v), __jsonify(), source);
          }
        };
        dispatch_table[__nd_delete_R_rcv_put_tid] = [this] (void* payload, const Address& source)  {
          auto ct = currentTime();
          auto v = *static_cast<R_r1_r2_r3_r4_r5<Address, _Collection<R_key_value<int, int>>,
          R_key_value<int, int>, int, int>*>(payload);
          nd_delete_R_rcv_put(v);
          if (this->__engine.logJsonEnabled()) {
            this->__engine.logJson(ct, me, "nd_delete_R_rcv_put",
            K3::serialization::json::encode<R_r1_r2_r3_r4_r5<Address, _Collection<R_key_value<int,
            int>>, R_key_value<int, int>, int, int>>(v), __jsonify(), source);
          }
        };
        dispatch_table[__insert_R_rcv_corrective_s10_m___SQL_SUM_AGGREGATE_1_mR1_tid] = [this] (void* payload,
        const Address& source)  {
          auto ct = currentTime();
          auto v = *static_cast<R_r1_r2_r3_r4_r5_r6_r7<Address, int, R_key_value<int, int>, int,
          R_key_value<int, int>, _Seq<R_key_value<int, int>>, _Set<R_key_value<int,
          int>>>*>(payload);
          insert_R_rcv_corrective_s10_m___SQL_SUM_AGGREGATE_1_mR1(v);
          if (this->__engine.logJsonEnabled()) {
            this->__engine.logJson(ct, me,
            "insert_R_rcv_corrective_s10_m___SQL_SUM_AGGREGATE_1_mR1",
            K3::serialization::json::encode<R_r1_r2_r3_r4_r5_r6_r7<Address, int, R_key_value<int,
            int>, int, R_key_value<int, int>, _Seq<R_key_value<int, int>>, _Set<R_key_value<int,
            int>>>>(v), __jsonify(), source);
          }
        };
        dispatch_table[__insert_R_rcv_corrective_s10_m___SQL_SUM_AGGREGATE_2_mR1_tid] = [this] (void* payload,
        const Address& source)  {
          auto ct = currentTime();
          auto v = *static_cast<R_r1_r2_r3_r4_r5_r6_r7<Address, int, R_key_value<int, int>, int,
          R_key_value<int, int>, _Seq<R_key_value<int, int>>, _Set<R_key_value<int,
          int>>>*>(payload);
          insert_R_rcv_corrective_s10_m___SQL_SUM_AGGREGATE_2_mR1(v);
          if (this->__engine.logJsonEnabled()) {
            this->__engine.logJson(ct, me,
            "insert_R_rcv_corrective_s10_m___SQL_SUM_AGGREGATE_2_mR1",
            K3::serialization::json::encode<R_r1_r2_r3_r4_r5_r6_r7<Address, int, R_key_value<int,
            int>, int, R_key_value<int, int>, _Seq<R_key_value<int, int>>, _Set<R_key_value<int,
            int>>>>(v), __jsonify(), source);
          }
        };
        dispatch_table[__insert_R_rcv_corrective_s8_m___SQL_SUM_AGGREGATE_1_mR1_tid] = [this] (void* payload,
        const Address& source)  {
          auto ct = currentTime();
          auto v = *static_cast<R_r1_r2_r3_r4_r5_r6_r7<Address, int, R_key_value<int, int>, int,
          R_key_value<int, int>, _Seq<R_key_value<int, int>>, _Set<R_key_value<int,
          int>>>*>(payload);
          insert_R_rcv_corrective_s8_m___SQL_SUM_AGGREGATE_1_mR1(v);
          if (this->__engine.logJsonEnabled()) {
            this->__engine.logJson(ct, me, "insert_R_rcv_corrective_s8_m___SQL_SUM_AGGREGATE_1_mR1",
            K3::serialization::json::encode<R_r1_r2_r3_r4_r5_r6_r7<Address, int, R_key_value<int,
            int>, int, R_key_value<int, int>, _Seq<R_key_value<int, int>>, _Set<R_key_value<int,
            int>>>>(v), __jsonify(), source);
          }
        };
        dispatch_table[__nd_insert_R_do_complete_s11_trig_tid] = [this] (void* payload,
        const Address& source)  {
          auto ct = currentTime();
          auto v = *static_cast<R_r1_r2_r3<R_key_value<int, int>, int, int>*>(payload);
          nd_insert_R_do_complete_s11_trig(v);
          if (this->__engine.logJsonEnabled()) {
            this->__engine.logJson(ct, me, "nd_insert_R_do_complete_s11_trig",
            K3::serialization::json::encode<R_r1_r2_r3<R_key_value<int, int>, int, int>>(v),
            __jsonify(), source);
          }
        };
        dispatch_table[__nd_insert_R_do_complete_s9_trig_tid] = [this] (void* payload,
        const Address& source)  {
          auto ct = currentTime();
          auto v = *static_cast<R_r1_r2_r3<R_key_value<int, int>, int, int>*>(payload);
          nd_insert_R_do_complete_s9_trig(v);
          if (this->__engine.logJsonEnabled()) {
            this->__engine.logJson(ct, me, "nd_insert_R_do_complete_s9_trig",
            K3::serialization::json::encode<R_r1_r2_r3<R_key_value<int, int>, int, int>>(v),
            __jsonify(), source);
          }
        };
        dispatch_table[__nd_insert_R_rcv_push_s10_m___SQL_SUM_AGGREGATE_1_mR1_tid] = [this] (void* payload,
        const Address& source)  {
          auto ct = currentTime();
          auto v = *static_cast<R_r1_r2_r3_r4<_Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>,
          R_key_value<int, int>, int, int>*>(payload);
          nd_insert_R_rcv_push_s10_m___SQL_SUM_AGGREGATE_1_mR1(v);
          if (this->__engine.logJsonEnabled()) {
            this->__engine.logJson(ct, me, "nd_insert_R_rcv_push_s10_m___SQL_SUM_AGGREGATE_1_mR1",
            K3::serialization::json::encode<R_r1_r2_r3_r4<_Set<R_r1_r2_r3<R_key_value<int, int>,
            int, int>>, R_key_value<int, int>, int, int>>(v), __jsonify(), source);
          }
        };
        dispatch_table[__nd_insert_R_rcv_push_s10_m___SQL_SUM_AGGREGATE_2_mR1_tid] = [this] (void* payload,
        const Address& source)  {
          auto ct = currentTime();
          auto v = *static_cast<R_r1_r2_r3_r4<_Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>,
          R_key_value<int, int>, int, int>*>(payload);
          nd_insert_R_rcv_push_s10_m___SQL_SUM_AGGREGATE_2_mR1(v);
          if (this->__engine.logJsonEnabled()) {
            this->__engine.logJson(ct, me, "nd_insert_R_rcv_push_s10_m___SQL_SUM_AGGREGATE_2_mR1",
            K3::serialization::json::encode<R_r1_r2_r3_r4<_Set<R_r1_r2_r3<R_key_value<int, int>,
            int, int>>, R_key_value<int, int>, int, int>>(v), __jsonify(), source);
          }
        };
        dispatch_table[__nd_insert_R_rcv_push_s8_m___SQL_SUM_AGGREGATE_1_mR1_tid] = [this] (void* payload,
        const Address& source)  {
          auto ct = currentTime();
          auto v = *static_cast<R_r1_r2_r3_r4<_Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>,
          R_key_value<int, int>, int, int>*>(payload);
          nd_insert_R_rcv_push_s8_m___SQL_SUM_AGGREGATE_1_mR1(v);
          if (this->__engine.logJsonEnabled()) {
            this->__engine.logJson(ct, me, "nd_insert_R_rcv_push_s8_m___SQL_SUM_AGGREGATE_1_mR1",
            K3::serialization::json::encode<R_r1_r2_r3_r4<_Set<R_r1_r2_r3<R_key_value<int, int>,
            int, int>>, R_key_value<int, int>, int, int>>(v), __jsonify(), source);
          }
        };
        dispatch_table[__nd_insert_R_send_push_s10_m___SQL_SUM_AGGREGATE_1_mR1_tid] = [this] (void* payload,
        const Address& source)  {
          auto ct = currentTime();
          auto v = *static_cast<R_r1_r2_r3<R_key_value<int, int>, int, int>*>(payload);
          nd_insert_R_send_push_s10_m___SQL_SUM_AGGREGATE_1_mR1(v);
          if (this->__engine.logJsonEnabled()) {
            this->__engine.logJson(ct, me, "nd_insert_R_send_push_s10_m___SQL_SUM_AGGREGATE_1_mR1",
            K3::serialization::json::encode<R_r1_r2_r3<R_key_value<int, int>, int, int>>(v),
            __jsonify(), source);
          }
        };
        dispatch_table[__nd_insert_R_send_push_s10_m___SQL_SUM_AGGREGATE_2_mR1_tid] = [this] (void* payload,
        const Address& source)  {
          auto ct = currentTime();
          auto v = *static_cast<R_r1_r2_r3<R_key_value<int, int>, int, int>*>(payload);
          nd_insert_R_send_push_s10_m___SQL_SUM_AGGREGATE_2_mR1(v);
          if (this->__engine.logJsonEnabled()) {
            this->__engine.logJson(ct, me, "nd_insert_R_send_push_s10_m___SQL_SUM_AGGREGATE_2_mR1",
            K3::serialization::json::encode<R_r1_r2_r3<R_key_value<int, int>, int, int>>(v),
            __jsonify(), source);
          }
        };
        dispatch_table[__nd_insert_R_send_push_s8_m___SQL_SUM_AGGREGATE_1_mR1_tid] = [this] (void* payload,
        const Address& source)  {
          auto ct = currentTime();
          auto v = *static_cast<R_r1_r2_r3<R_key_value<int, int>, int, int>*>(payload);
          nd_insert_R_send_push_s8_m___SQL_SUM_AGGREGATE_1_mR1(v);
          if (this->__engine.logJsonEnabled()) {
            this->__engine.logJson(ct, me, "nd_insert_R_send_push_s8_m___SQL_SUM_AGGREGATE_1_mR1",
            K3::serialization::json::encode<R_r1_r2_r3<R_key_value<int, int>, int, int>>(v),
            __jsonify(), source);
          }
        };
        dispatch_table[__nd_insert_R_rcv_fetch_tid] = [this] (void* payload,
        const Address& source)  {
          auto ct = currentTime();
          auto v = *static_cast<R_r1_r2_r3_r4<_Collection<R_key_value<int, int>>, R_key_value<int,
          int>, int, int>*>(payload);
          nd_insert_R_rcv_fetch(v);
          if (this->__engine.logJsonEnabled()) {
            this->__engine.logJson(ct, me, "nd_insert_R_rcv_fetch",
            K3::serialization::json::encode<R_r1_r2_r3_r4<_Collection<R_key_value<int, int>>,
            R_key_value<int, int>, int, int>>(v), __jsonify(), source);
          }
        };
        dispatch_table[__nd_insert_R_rcv_put_tid] = [this] (void* payload, const Address& source)  {
          auto ct = currentTime();
          auto v = *static_cast<R_r1_r2_r3_r4_r5<Address, _Collection<R_key_value<int, int>>,
          R_key_value<int, int>, int, int>*>(payload);
          nd_insert_R_rcv_put(v);
          if (this->__engine.logJsonEnabled()) {
            this->__engine.logJson(ct, me, "nd_insert_R_rcv_put",
            K3::serialization::json::encode<R_r1_r2_r3_r4_r5<Address, _Collection<R_key_value<int,
            int>>, R_key_value<int, int>, int, int>>(v), __jsonify(), source);
          }
        };
        dispatch_table[__delete_S_rcv_corrective_s6_m___SQL_SUM_AGGREGATE_2_mS3_tid] = [this] (void* payload,
        const Address& source)  {
          auto ct = currentTime();
          auto v = *static_cast<R_r1_r2_r3_r4_r5_r6_r7<Address, int, R_key_value<int, int>, int,
          R_key_value<int, int>, _Seq<R_key_value<int, int>>, _Set<R_key_value<int,
          int>>>*>(payload);
          delete_S_rcv_corrective_s6_m___SQL_SUM_AGGREGATE_2_mS3(v);
          if (this->__engine.logJsonEnabled()) {
            this->__engine.logJson(ct, me, "delete_S_rcv_corrective_s6_m___SQL_SUM_AGGREGATE_2_mS3",
            K3::serialization::json::encode<R_r1_r2_r3_r4_r5_r6_r7<Address, int, R_key_value<int,
            int>, int, R_key_value<int, int>, _Seq<R_key_value<int, int>>, _Set<R_key_value<int,
            int>>>>(v), __jsonify(), source);
          }
        };
        dispatch_table[__delete_S_rcv_corrective_s6_m___SQL_SUM_AGGREGATE_1_mS1_tid] = [this] (void* payload,
        const Address& source)  {
          auto ct = currentTime();
          auto v = *static_cast<R_r1_r2_r3_r4_r5_r6_r7<Address, int, R_key_value<int, int>, int,
          R_key_value<int, int>, _Seq<R_key_value<int, int>>, _Set<R_key_value<int,
          int>>>*>(payload);
          delete_S_rcv_corrective_s6_m___SQL_SUM_AGGREGATE_1_mS1(v);
          if (this->__engine.logJsonEnabled()) {
            this->__engine.logJson(ct, me, "delete_S_rcv_corrective_s6_m___SQL_SUM_AGGREGATE_1_mS1",
            K3::serialization::json::encode<R_r1_r2_r3_r4_r5_r6_r7<Address, int, R_key_value<int,
            int>, int, R_key_value<int, int>, _Seq<R_key_value<int, int>>, _Set<R_key_value<int,
            int>>>>(v), __jsonify(), source);
          }
        };
        dispatch_table[__delete_S_rcv_corrective_s4_m___SQL_SUM_AGGREGATE_1_mS1_tid] = [this] (void* payload,
        const Address& source)  {
          auto ct = currentTime();
          auto v = *static_cast<R_r1_r2_r3_r4_r5_r6_r7<Address, int, R_key_value<int, int>, int,
          R_key_value<int, int>, _Seq<R_key_value<int, int>>, _Set<R_key_value<int,
          int>>>*>(payload);
          delete_S_rcv_corrective_s4_m___SQL_SUM_AGGREGATE_1_mS1(v);
          if (this->__engine.logJsonEnabled()) {
            this->__engine.logJson(ct, me, "delete_S_rcv_corrective_s4_m___SQL_SUM_AGGREGATE_1_mS1",
            K3::serialization::json::encode<R_r1_r2_r3_r4_r5_r6_r7<Address, int, R_key_value<int,
            int>, int, R_key_value<int, int>, _Seq<R_key_value<int, int>>, _Set<R_key_value<int,
            int>>>>(v), __jsonify(), source);
          }
        };
        dispatch_table[__nd_delete_S_do_complete_s7_trig_tid] = [this] (void* payload,
        const Address& source)  {
          auto ct = currentTime();
          auto v = *static_cast<R_r1_r2_r3<R_key_value<int, int>, int, int>*>(payload);
          nd_delete_S_do_complete_s7_trig(v);
          if (this->__engine.logJsonEnabled()) {
            this->__engine.logJson(ct, me, "nd_delete_S_do_complete_s7_trig",
            K3::serialization::json::encode<R_r1_r2_r3<R_key_value<int, int>, int, int>>(v),
            __jsonify(), source);
          }
        };
        dispatch_table[__nd_delete_S_do_complete_s5_trig_tid] = [this] (void* payload,
        const Address& source)  {
          auto ct = currentTime();
          auto v = *static_cast<R_r1_r2_r3<R_key_value<int, int>, int, int>*>(payload);
          nd_delete_S_do_complete_s5_trig(v);
          if (this->__engine.logJsonEnabled()) {
            this->__engine.logJson(ct, me, "nd_delete_S_do_complete_s5_trig",
            K3::serialization::json::encode<R_r1_r2_r3<R_key_value<int, int>, int, int>>(v),
            __jsonify(), source);
          }
        };
        dispatch_table[__nd_delete_S_rcv_push_s6_m___SQL_SUM_AGGREGATE_2_mS3_tid] = [this] (void* payload,
        const Address& source)  {
          auto ct = currentTime();
          auto v = *static_cast<R_r1_r2_r3_r4<_Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>,
          R_key_value<int, int>, int, int>*>(payload);
          nd_delete_S_rcv_push_s6_m___SQL_SUM_AGGREGATE_2_mS3(v);
          if (this->__engine.logJsonEnabled()) {
            this->__engine.logJson(ct, me, "nd_delete_S_rcv_push_s6_m___SQL_SUM_AGGREGATE_2_mS3",
            K3::serialization::json::encode<R_r1_r2_r3_r4<_Set<R_r1_r2_r3<R_key_value<int, int>,
            int, int>>, R_key_value<int, int>, int, int>>(v), __jsonify(), source);
          }
        };
        dispatch_table[__nd_delete_S_rcv_push_s6_m___SQL_SUM_AGGREGATE_1_mS1_tid] = [this] (void* payload,
        const Address& source)  {
          auto ct = currentTime();
          auto v = *static_cast<R_r1_r2_r3_r4<_Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>,
          R_key_value<int, int>, int, int>*>(payload);
          nd_delete_S_rcv_push_s6_m___SQL_SUM_AGGREGATE_1_mS1(v);
          if (this->__engine.logJsonEnabled()) {
            this->__engine.logJson(ct, me, "nd_delete_S_rcv_push_s6_m___SQL_SUM_AGGREGATE_1_mS1",
            K3::serialization::json::encode<R_r1_r2_r3_r4<_Set<R_r1_r2_r3<R_key_value<int, int>,
            int, int>>, R_key_value<int, int>, int, int>>(v), __jsonify(), source);
          }
        };
        dispatch_table[__nd_delete_S_rcv_push_s4_m___SQL_SUM_AGGREGATE_1_mS1_tid] = [this] (void* payload,
        const Address& source)  {
          auto ct = currentTime();
          auto v = *static_cast<R_r1_r2_r3_r4<_Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>,
          R_key_value<int, int>, int, int>*>(payload);
          nd_delete_S_rcv_push_s4_m___SQL_SUM_AGGREGATE_1_mS1(v);
          if (this->__engine.logJsonEnabled()) {
            this->__engine.logJson(ct, me, "nd_delete_S_rcv_push_s4_m___SQL_SUM_AGGREGATE_1_mS1",
            K3::serialization::json::encode<R_r1_r2_r3_r4<_Set<R_r1_r2_r3<R_key_value<int, int>,
            int, int>>, R_key_value<int, int>, int, int>>(v), __jsonify(), source);
          }
        };
        dispatch_table[__nd_delete_S_send_push_s6_m___SQL_SUM_AGGREGATE_2_mS3_tid] = [this] (void* payload,
        const Address& source)  {
          auto ct = currentTime();
          auto v = *static_cast<R_r1_r2_r3<R_key_value<int, int>, int, int>*>(payload);
          nd_delete_S_send_push_s6_m___SQL_SUM_AGGREGATE_2_mS3(v);
          if (this->__engine.logJsonEnabled()) {
            this->__engine.logJson(ct, me, "nd_delete_S_send_push_s6_m___SQL_SUM_AGGREGATE_2_mS3",
            K3::serialization::json::encode<R_r1_r2_r3<R_key_value<int, int>, int, int>>(v),
            __jsonify(), source);
          }
        };
        dispatch_table[__nd_delete_S_send_push_s6_m___SQL_SUM_AGGREGATE_1_mS1_tid] = [this] (void* payload,
        const Address& source)  {
          auto ct = currentTime();
          auto v = *static_cast<R_r1_r2_r3<R_key_value<int, int>, int, int>*>(payload);
          nd_delete_S_send_push_s6_m___SQL_SUM_AGGREGATE_1_mS1(v);
          if (this->__engine.logJsonEnabled()) {
            this->__engine.logJson(ct, me, "nd_delete_S_send_push_s6_m___SQL_SUM_AGGREGATE_1_mS1",
            K3::serialization::json::encode<R_r1_r2_r3<R_key_value<int, int>, int, int>>(v),
            __jsonify(), source);
          }
        };
        dispatch_table[__nd_delete_S_send_push_s4_m___SQL_SUM_AGGREGATE_1_mS1_tid] = [this] (void* payload,
        const Address& source)  {
          auto ct = currentTime();
          auto v = *static_cast<R_r1_r2_r3<R_key_value<int, int>, int, int>*>(payload);
          nd_delete_S_send_push_s4_m___SQL_SUM_AGGREGATE_1_mS1(v);
          if (this->__engine.logJsonEnabled()) {
            this->__engine.logJson(ct, me, "nd_delete_S_send_push_s4_m___SQL_SUM_AGGREGATE_1_mS1",
            K3::serialization::json::encode<R_r1_r2_r3<R_key_value<int, int>, int, int>>(v),
            __jsonify(), source);
          }
        };
        dispatch_table[__nd_delete_S_rcv_fetch_tid] = [this] (void* payload,
        const Address& source)  {
          auto ct = currentTime();
          auto v = *static_cast<R_r1_r2_r3_r4<_Collection<R_key_value<int, int>>, R_key_value<int,
          int>, int, int>*>(payload);
          nd_delete_S_rcv_fetch(v);
          if (this->__engine.logJsonEnabled()) {
            this->__engine.logJson(ct, me, "nd_delete_S_rcv_fetch",
            K3::serialization::json::encode<R_r1_r2_r3_r4<_Collection<R_key_value<int, int>>,
            R_key_value<int, int>, int, int>>(v), __jsonify(), source);
          }
        };
        dispatch_table[__nd_delete_S_rcv_put_tid] = [this] (void* payload, const Address& source)  {
          auto ct = currentTime();
          auto v = *static_cast<R_r1_r2_r3_r4_r5<Address, _Collection<R_key_value<int, int>>,
          R_key_value<int, int>, int, int>*>(payload);
          nd_delete_S_rcv_put(v);
          if (this->__engine.logJsonEnabled()) {
            this->__engine.logJson(ct, me, "nd_delete_S_rcv_put",
            K3::serialization::json::encode<R_r1_r2_r3_r4_r5<Address, _Collection<R_key_value<int,
            int>>, R_key_value<int, int>, int, int>>(v), __jsonify(), source);
          }
        };
        dispatch_table[__insert_S_rcv_corrective_s2_m___SQL_SUM_AGGREGATE_2_mS3_tid] = [this] (void* payload,
        const Address& source)  {
          auto ct = currentTime();
          auto v = *static_cast<R_r1_r2_r3_r4_r5_r6_r7<Address, int, R_key_value<int, int>, int,
          R_key_value<int, int>, _Seq<R_key_value<int, int>>, _Set<R_key_value<int,
          int>>>*>(payload);
          insert_S_rcv_corrective_s2_m___SQL_SUM_AGGREGATE_2_mS3(v);
          if (this->__engine.logJsonEnabled()) {
            this->__engine.logJson(ct, me, "insert_S_rcv_corrective_s2_m___SQL_SUM_AGGREGATE_2_mS3",
            K3::serialization::json::encode<R_r1_r2_r3_r4_r5_r6_r7<Address, int, R_key_value<int,
            int>, int, R_key_value<int, int>, _Seq<R_key_value<int, int>>, _Set<R_key_value<int,
            int>>>>(v), __jsonify(), source);
          }
        };
        dispatch_table[__insert_S_rcv_corrective_s2_m___SQL_SUM_AGGREGATE_1_mS1_tid] = [this] (void* payload,
        const Address& source)  {
          auto ct = currentTime();
          auto v = *static_cast<R_r1_r2_r3_r4_r5_r6_r7<Address, int, R_key_value<int, int>, int,
          R_key_value<int, int>, _Seq<R_key_value<int, int>>, _Set<R_key_value<int,
          int>>>*>(payload);
          insert_S_rcv_corrective_s2_m___SQL_SUM_AGGREGATE_1_mS1(v);
          if (this->__engine.logJsonEnabled()) {
            this->__engine.logJson(ct, me, "insert_S_rcv_corrective_s2_m___SQL_SUM_AGGREGATE_1_mS1",
            K3::serialization::json::encode<R_r1_r2_r3_r4_r5_r6_r7<Address, int, R_key_value<int,
            int>, int, R_key_value<int, int>, _Seq<R_key_value<int, int>>, _Set<R_key_value<int,
            int>>>>(v), __jsonify(), source);
          }
        };
        dispatch_table[__insert_S_rcv_corrective_s0_m___SQL_SUM_AGGREGATE_1_mS1_tid] = [this] (void* payload,
        const Address& source)  {
          auto ct = currentTime();
          auto v = *static_cast<R_r1_r2_r3_r4_r5_r6_r7<Address, int, R_key_value<int, int>, int,
          R_key_value<int, int>, _Seq<R_key_value<int, int>>, _Set<R_key_value<int,
          int>>>*>(payload);
          insert_S_rcv_corrective_s0_m___SQL_SUM_AGGREGATE_1_mS1(v);
          if (this->__engine.logJsonEnabled()) {
            this->__engine.logJson(ct, me, "insert_S_rcv_corrective_s0_m___SQL_SUM_AGGREGATE_1_mS1",
            K3::serialization::json::encode<R_r1_r2_r3_r4_r5_r6_r7<Address, int, R_key_value<int,
            int>, int, R_key_value<int, int>, _Seq<R_key_value<int, int>>, _Set<R_key_value<int,
            int>>>>(v), __jsonify(), source);
          }
        };
        dispatch_table[__nd_insert_S_do_complete_s3_trig_tid] = [this] (void* payload,
        const Address& source)  {
          auto ct = currentTime();
          auto v = *static_cast<R_r1_r2_r3<R_key_value<int, int>, int, int>*>(payload);
          nd_insert_S_do_complete_s3_trig(v);
          if (this->__engine.logJsonEnabled()) {
            this->__engine.logJson(ct, me, "nd_insert_S_do_complete_s3_trig",
            K3::serialization::json::encode<R_r1_r2_r3<R_key_value<int, int>, int, int>>(v),
            __jsonify(), source);
          }
        };
        dispatch_table[__nd_insert_S_do_complete_s1_trig_tid] = [this] (void* payload,
        const Address& source)  {
          auto ct = currentTime();
          auto v = *static_cast<R_r1_r2_r3<R_key_value<int, int>, int, int>*>(payload);
          nd_insert_S_do_complete_s1_trig(v);
          if (this->__engine.logJsonEnabled()) {
            this->__engine.logJson(ct, me, "nd_insert_S_do_complete_s1_trig",
            K3::serialization::json::encode<R_r1_r2_r3<R_key_value<int, int>, int, int>>(v),
            __jsonify(), source);
          }
        };
        dispatch_table[__nd_insert_S_rcv_push_s2_m___SQL_SUM_AGGREGATE_2_mS3_tid] = [this] (void* payload,
        const Address& source)  {
          auto ct = currentTime();
          auto v = *static_cast<R_r1_r2_r3_r4<_Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>,
          R_key_value<int, int>, int, int>*>(payload);
          nd_insert_S_rcv_push_s2_m___SQL_SUM_AGGREGATE_2_mS3(v);
          if (this->__engine.logJsonEnabled()) {
            this->__engine.logJson(ct, me, "nd_insert_S_rcv_push_s2_m___SQL_SUM_AGGREGATE_2_mS3",
            K3::serialization::json::encode<R_r1_r2_r3_r4<_Set<R_r1_r2_r3<R_key_value<int, int>,
            int, int>>, R_key_value<int, int>, int, int>>(v), __jsonify(), source);
          }
        };
        dispatch_table[__nd_insert_S_rcv_push_s2_m___SQL_SUM_AGGREGATE_1_mS1_tid] = [this] (void* payload,
        const Address& source)  {
          auto ct = currentTime();
          auto v = *static_cast<R_r1_r2_r3_r4<_Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>,
          R_key_value<int, int>, int, int>*>(payload);
          nd_insert_S_rcv_push_s2_m___SQL_SUM_AGGREGATE_1_mS1(v);
          if (this->__engine.logJsonEnabled()) {
            this->__engine.logJson(ct, me, "nd_insert_S_rcv_push_s2_m___SQL_SUM_AGGREGATE_1_mS1",
            K3::serialization::json::encode<R_r1_r2_r3_r4<_Set<R_r1_r2_r3<R_key_value<int, int>,
            int, int>>, R_key_value<int, int>, int, int>>(v), __jsonify(), source);
          }
        };
        dispatch_table[__nd_insert_S_rcv_push_s0_m___SQL_SUM_AGGREGATE_1_mS1_tid] = [this] (void* payload,
        const Address& source)  {
          auto ct = currentTime();
          auto v = *static_cast<R_r1_r2_r3_r4<_Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>,
          R_key_value<int, int>, int, int>*>(payload);
          nd_insert_S_rcv_push_s0_m___SQL_SUM_AGGREGATE_1_mS1(v);
          if (this->__engine.logJsonEnabled()) {
            this->__engine.logJson(ct, me, "nd_insert_S_rcv_push_s0_m___SQL_SUM_AGGREGATE_1_mS1",
            K3::serialization::json::encode<R_r1_r2_r3_r4<_Set<R_r1_r2_r3<R_key_value<int, int>,
            int, int>>, R_key_value<int, int>, int, int>>(v), __jsonify(), source);
          }
        };
        dispatch_table[__nd_insert_S_send_push_s2_m___SQL_SUM_AGGREGATE_2_mS3_tid] = [this] (void* payload,
        const Address& source)  {
          auto ct = currentTime();
          auto v = *static_cast<R_r1_r2_r3<R_key_value<int, int>, int, int>*>(payload);
          nd_insert_S_send_push_s2_m___SQL_SUM_AGGREGATE_2_mS3(v);
          if (this->__engine.logJsonEnabled()) {
            this->__engine.logJson(ct, me, "nd_insert_S_send_push_s2_m___SQL_SUM_AGGREGATE_2_mS3",
            K3::serialization::json::encode<R_r1_r2_r3<R_key_value<int, int>, int, int>>(v),
            __jsonify(), source);
          }
        };
        dispatch_table[__nd_insert_S_send_push_s2_m___SQL_SUM_AGGREGATE_1_mS1_tid] = [this] (void* payload,
        const Address& source)  {
          auto ct = currentTime();
          auto v = *static_cast<R_r1_r2_r3<R_key_value<int, int>, int, int>*>(payload);
          nd_insert_S_send_push_s2_m___SQL_SUM_AGGREGATE_1_mS1(v);
          if (this->__engine.logJsonEnabled()) {
            this->__engine.logJson(ct, me, "nd_insert_S_send_push_s2_m___SQL_SUM_AGGREGATE_1_mS1",
            K3::serialization::json::encode<R_r1_r2_r3<R_key_value<int, int>, int, int>>(v),
            __jsonify(), source);
          }
        };
        dispatch_table[__nd_insert_S_send_push_s0_m___SQL_SUM_AGGREGATE_1_mS1_tid] = [this] (void* payload,
        const Address& source)  {
          auto ct = currentTime();
          auto v = *static_cast<R_r1_r2_r3<R_key_value<int, int>, int, int>*>(payload);
          nd_insert_S_send_push_s0_m___SQL_SUM_AGGREGATE_1_mS1(v);
          if (this->__engine.logJsonEnabled()) {
            this->__engine.logJson(ct, me, "nd_insert_S_send_push_s0_m___SQL_SUM_AGGREGATE_1_mS1",
            K3::serialization::json::encode<R_r1_r2_r3<R_key_value<int, int>, int, int>>(v),
            __jsonify(), source);
          }
        };
        dispatch_table[__nd_insert_S_rcv_fetch_tid] = [this] (void* payload,
        const Address& source)  {
          auto ct = currentTime();
          auto v = *static_cast<R_r1_r2_r3_r4<_Collection<R_key_value<int, int>>, R_key_value<int,
          int>, int, int>*>(payload);
          nd_insert_S_rcv_fetch(v);
          if (this->__engine.logJsonEnabled()) {
            this->__engine.logJson(ct, me, "nd_insert_S_rcv_fetch",
            K3::serialization::json::encode<R_r1_r2_r3_r4<_Collection<R_key_value<int, int>>,
            R_key_value<int, int>, int, int>>(v), __jsonify(), source);
          }
        };
        dispatch_table[__nd_insert_S_rcv_put_tid] = [this] (void* payload, const Address& source)  {
          auto ct = currentTime();
          auto v = *static_cast<R_r1_r2_r3_r4_r5<Address, _Collection<R_key_value<int, int>>,
          R_key_value<int, int>, int, int>*>(payload);
          nd_insert_S_rcv_put(v);
          if (this->__engine.logJsonEnabled()) {
            this->__engine.logJson(ct, me, "nd_insert_S_rcv_put",
            K3::serialization::json::encode<R_r1_r2_r3_r4_r5<Address, _Collection<R_key_value<int,
            int>>, R_key_value<int, int>, int, int>>(v), __jsonify(), source);
          }
        };
        dispatch_table[__nd_rcv_corr_done_tid] = [this] (void* payload, const Address& source)  {
          auto ct = currentTime();
          auto v = *static_cast<R_r1_r2_r3_r4<R_key_value<int, int>, int, int, int>*>(payload);
          nd_rcv_corr_done(v);
          if (this->__engine.logJsonEnabled()) {
            this->__engine.logJson(ct, me, "nd_rcv_corr_done",
            K3::serialization::json::encode<R_r1_r2_r3_r4<R_key_value<int, int>, int, int, int>>(v),
            __jsonify(), source);
          }
        };
        dispatch_table[__sw_driver_trig_tid] = [this] (void* payload, const Address& source)  {
          auto ct = currentTime();
          auto v = *static_cast<unit_t*>(payload);
          sw_driver_trig(v);
          if (this->__engine.logJsonEnabled()) {
            this->__engine.logJson(ct, me, "sw_driver_trig",
            K3::serialization::json::encode<unit_t>(v), __jsonify(), source);
          }
        };
        dispatch_table[__sw_demux_tid] = [this] (void* payload, const Address& source)  {
          auto ct = currentTime();
          auto v = *static_cast<R_r1_r2_r3_r4_r5<int, int, int, int, int>*>(payload);
          sw_demux(v);
          if (this->__engine.logJsonEnabled()) {
            this->__engine.logJson(ct, me, "sw_demux",
            K3::serialization::json::encode<R_r1_r2_r3_r4_r5<int, int, int, int, int>>(v),
            __jsonify(), source);
          }
        };
        dispatch_table[__tm_check_time_tid] = [this] (void* payload, const Address& source)  {
          auto ct = currentTime();
          auto v = *static_cast<unit_t*>(payload);
          tm_check_time(v);
          if (this->__engine.logJsonEnabled()) {
            this->__engine.logJson(ct, me, "tm_check_time",
            K3::serialization::json::encode<unit_t>(v), __jsonify(), source);
          }
        };
        dispatch_table[__tm_insert_timer_tid] = [this] (void* payload, const Address& source)  {
          auto ct = currentTime();
          auto v = *static_cast<R_r1_r2_r3<int, int, Address>*>(payload);
          tm_insert_timer(v);
          if (this->__engine.logJsonEnabled()) {
            this->__engine.logJson(ct, me, "tm_insert_timer",
            K3::serialization::json::encode<R_r1_r2_r3<int, int, Address>>(v), __jsonify(), source);
          }
        };
        dispatch_table[__sw_rcv_token_tid] = [this] (void* payload, const Address& source)  {
          auto ct = currentTime();
          auto v = *static_cast<R_key_value<int, int>*>(payload);
          sw_rcv_token(v);
          if (this->__engine.logJsonEnabled()) {
            this->__engine.logJson(ct, me, "sw_rcv_token",
            K3::serialization::json::encode<R_key_value<int, int>>(v), __jsonify(), source);
          }
        };
        dispatch_table[__do_gc_tid] = [this] (void* payload, const Address& source)  {
          auto ct = currentTime();
          auto v = *static_cast<R_key_value<int, int>*>(payload);
          do_gc(v);
          if (this->__engine.logJsonEnabled()) {
            this->__engine.logJson(ct, me, "do_gc", K3::serialization::json::encode<R_key_value<int,
            int>>(v), __jsonify(), source);
          }
        };
        dispatch_table[__ms_send_gc_req_tid] = [this] (void* payload, const Address& source)  {
          auto ct = currentTime();
          auto v = *static_cast<unit_t*>(payload);
          ms_send_gc_req(v);
          if (this->__engine.logJsonEnabled()) {
            this->__engine.logJson(ct, me, "ms_send_gc_req",
            K3::serialization::json::encode<unit_t>(v), __jsonify(), source);
          }
        };
        dispatch_table[__rcv_req_gc_vid_tid] = [this] (void* payload, const Address& source)  {
          auto ct = currentTime();
          auto v = *static_cast<unit_t*>(payload);
          rcv_req_gc_vid(v);
          if (this->__engine.logJsonEnabled()) {
            this->__engine.logJson(ct, me, "rcv_req_gc_vid",
            K3::serialization::json::encode<unit_t>(v), __jsonify(), source);
          }
        };
        dispatch_table[__ms_rcv_gc_vid_tid] = [this] (void* payload, const Address& source)  {
          auto ct = currentTime();
          auto v = *static_cast<R_key_value<Address, R_key_value<int, int>>*>(payload);
          ms_rcv_gc_vid(v);
          if (this->__engine.logJsonEnabled()) {
            this->__engine.logJson(ct, me, "ms_rcv_gc_vid",
            K3::serialization::json::encode<R_key_value<Address, R_key_value<int, int>>>(v),
            __jsonify(), source);
          }
        };
        dispatch_table[__sw_ack_rcv_tid] = [this] (void* payload, const Address& source)  {
          auto ct = currentTime();
          auto v = *static_cast<R_key_value<Address, R_key_value<int, int>>*>(payload);
          sw_ack_rcv(v);
          if (this->__engine.logJsonEnabled()) {
            this->__engine.logJson(ct, me, "sw_ack_rcv",
            K3::serialization::json::encode<R_key_value<Address, R_key_value<int, int>>>(v),
            __jsonify(), source);
          }
        };
        dispatch_table[__ms_rcv_switch_done_tid] = [this] (void* payload, const Address& source)  {
          auto ct = currentTime();
          auto v = *static_cast<unit_t*>(payload);
          ms_rcv_switch_done(v);
          if (this->__engine.logJsonEnabled()) {
            this->__engine.logJson(ct, me, "ms_rcv_switch_done",
            K3::serialization::json::encode<unit_t>(v), __jsonify(), source);
          }
        };
        dispatch_table[__nd_rcv_done_tid] = [this] (void* payload, const Address& source)  {
          auto ct = currentTime();
          auto v = *static_cast<unit_t*>(payload);
          nd_rcv_done(v);
          if (this->__engine.logJsonEnabled()) {
            this->__engine.logJson(ct, me, "nd_rcv_done",
            K3::serialization::json::encode<unit_t>(v), __jsonify(), source);
          }
        };
        dispatch_table[__ms_rcv_node_done_tid] = [this] (void* payload, const Address& source)  {
          auto ct = currentTime();
          auto v = *static_cast<bool*>(payload);
          ms_rcv_node_done(v);
          if (this->__engine.logJsonEnabled()) {
            this->__engine.logJson(ct, me, "ms_rcv_node_done",
            K3::serialization::json::encode<bool>(v), __jsonify(), source);
          }
        };
        dispatch_table[__ms_shutdown_tid] = [this] (void* payload, const Address& source)  {
          auto ct = currentTime();
          auto v = *static_cast<unit_t*>(payload);
          ms_shutdown(v);
          if (this->__engine.logJsonEnabled()) {
            this->__engine.logJson(ct, me, "ms_shutdown",
            K3::serialization::json::encode<unit_t>(v), __jsonify(), source);
          }
        };
        dispatch_table[__shutdown_trig_tid] = [this] (void* payload, const Address& source)  {
          auto ct = currentTime();
          auto v = *static_cast<unit_t*>(payload);
          shutdown_trig(v);
          if (this->__engine.logJsonEnabled()) {
            this->__engine.logJson(ct, me, "shutdown_trig",
            K3::serialization::json::encode<unit_t>(v), __jsonify(), source);
          }
        };
        dispatch_table[__ms_send_addr_self_tid] = [this] (void* payload, const Address& source)  {
          auto ct = currentTime();
          auto v = *static_cast<unit_t*>(payload);
          ms_send_addr_self(v);
          if (this->__engine.logJsonEnabled()) {
            this->__engine.logJson(ct, me, "ms_send_addr_self",
            K3::serialization::json::encode<unit_t>(v), __jsonify(), source);
          }
        };
        dispatch_table[__rcv_master_addr_tid] = [this] (void* payload, const Address& source)  {
          auto ct = currentTime();
          auto v = *static_cast<Address*>(payload);
          rcv_master_addr(v);
          if (this->__engine.logJsonEnabled()) {
            this->__engine.logJson(ct, me, "rcv_master_addr",
            K3::serialization::json::encode<Address>(v), __jsonify(), source);
          }
        };
        dispatch_table[__ms_rcv_job_tid] = [this] (void* payload, const Address& source)  {
          auto ct = currentTime();
          auto v = *static_cast<R_key_value<Address, int>*>(payload);
          ms_rcv_job(v);
          if (this->__engine.logJsonEnabled()) {
            this->__engine.logJson(ct, me, "ms_rcv_job",
            K3::serialization::json::encode<R_key_value<Address, int>>(v), __jsonify(), source);
          }
        };
        dispatch_table[__rcv_jobs_tid] = [this] (void* payload, const Address& source)  {
          auto ct = currentTime();
          auto v = *static_cast<_Map<R_key_value<Address, int>>*>(payload);
          rcv_jobs(v);
          if (this->__engine.logJsonEnabled()) {
            this->__engine.logJson(ct, me, "rcv_jobs",
            K3::serialization::json::encode<_Map<R_key_value<Address, int>>>(v), __jsonify(),
            source);
          }
        };
        dispatch_table[__ms_rcv_jobs_ack_tid] = [this] (void* payload, const Address& source)  {
          auto ct = currentTime();
          auto v = *static_cast<unit_t*>(payload);
          ms_rcv_jobs_ack(v);
          if (this->__engine.logJsonEnabled()) {
            this->__engine.logJson(ct, me, "ms_rcv_jobs_ack",
            K3::serialization::json::encode<unit_t>(v), __jsonify(), source);
          }
        };
        dispatch_table[__sw_rcv_init_tid] = [this] (void* payload, const Address& source)  {
          auto ct = currentTime();
          auto v = *static_cast<unit_t*>(payload);
          sw_rcv_init(v);
          if (this->__engine.logJsonEnabled()) {
            this->__engine.logJson(ct, me, "sw_rcv_init",
            K3::serialization::json::encode<unit_t>(v), __jsonify(), source);
          }
        };
        dispatch_table[__ms_rcv_sw_init_ack_tid] = [this] (void* payload, const Address& source)  {
          auto ct = currentTime();
          auto v = *static_cast<unit_t*>(payload);
          ms_rcv_sw_init_ack(v);
          if (this->__engine.logJsonEnabled()) {
            this->__engine.logJson(ct, me, "ms_rcv_sw_init_ack",
            K3::serialization::json::encode<unit_t>(v), __jsonify(), source);
          }
        };
      }
      static int __ms_rcv_sw_init_ack_tid;
      bool ____ms_rcv_sw_init_ack_tid_set__ = false;
      static int __sw_rcv_init_tid;
      bool ____sw_rcv_init_tid_set__ = false;
      static int __ms_rcv_jobs_ack_tid;
      bool ____ms_rcv_jobs_ack_tid_set__ = false;
      static int __rcv_jobs_tid;
      bool ____rcv_jobs_tid_set__ = false;
      static int __ms_rcv_job_tid;
      bool ____ms_rcv_job_tid_set__ = false;
      static int __rcv_master_addr_tid;
      bool ____rcv_master_addr_tid_set__ = false;
      static int __ms_send_addr_self_tid;
      bool ____ms_send_addr_self_tid_set__ = false;
      static int __shutdown_trig_tid;
      bool ____shutdown_trig_tid_set__ = false;
      static int __ms_shutdown_tid;
      bool ____ms_shutdown_tid_set__ = false;
      static int __ms_rcv_node_done_tid;
      bool ____ms_rcv_node_done_tid_set__ = false;
      static int __nd_rcv_done_tid;
      bool ____nd_rcv_done_tid_set__ = false;
      static int __ms_rcv_switch_done_tid;
      bool ____ms_rcv_switch_done_tid_set__ = false;
      static int __sw_ack_rcv_tid;
      bool ____sw_ack_rcv_tid_set__ = false;
      static int __ms_rcv_gc_vid_tid;
      bool ____ms_rcv_gc_vid_tid_set__ = false;
      static int __rcv_req_gc_vid_tid;
      bool ____rcv_req_gc_vid_tid_set__ = false;
      static int __ms_send_gc_req_tid;
      bool ____ms_send_gc_req_tid_set__ = false;
      static int __do_gc_tid;
      bool ____do_gc_tid_set__ = false;
      static int __sw_rcv_token_tid;
      bool ____sw_rcv_token_tid_set__ = false;
      static int __tm_insert_timer_tid;
      bool ____tm_insert_timer_tid_set__ = false;
      static int __tm_check_time_tid;
      bool ____tm_check_time_tid_set__ = false;
      static int __sw_demux_tid;
      bool ____sw_demux_tid_set__ = false;
      static int __sw_driver_trig_tid;
      bool ____sw_driver_trig_tid_set__ = false;
      static int __nd_rcv_corr_done_tid;
      bool ____nd_rcv_corr_done_tid_set__ = false;
      static int __nd_insert_S_rcv_put_tid;
      bool ____nd_insert_S_rcv_put_tid_set__ = false;
      static int __nd_insert_S_rcv_fetch_tid;
      bool ____nd_insert_S_rcv_fetch_tid_set__ = false;
      static int __nd_insert_S_send_push_s0_m___SQL_SUM_AGGREGATE_1_mS1_tid;
      bool ____nd_insert_S_send_push_s0_m___SQL_SUM_AGGREGATE_1_mS1_tid_set__ = false;
      static int __nd_insert_S_send_push_s2_m___SQL_SUM_AGGREGATE_1_mS1_tid;
      bool ____nd_insert_S_send_push_s2_m___SQL_SUM_AGGREGATE_1_mS1_tid_set__ = false;
      static int __nd_insert_S_send_push_s2_m___SQL_SUM_AGGREGATE_2_mS3_tid;
      bool ____nd_insert_S_send_push_s2_m___SQL_SUM_AGGREGATE_2_mS3_tid_set__ = false;
      static int __nd_insert_S_rcv_push_s0_m___SQL_SUM_AGGREGATE_1_mS1_tid;
      bool ____nd_insert_S_rcv_push_s0_m___SQL_SUM_AGGREGATE_1_mS1_tid_set__ = false;
      static int __nd_insert_S_rcv_push_s2_m___SQL_SUM_AGGREGATE_1_mS1_tid;
      bool ____nd_insert_S_rcv_push_s2_m___SQL_SUM_AGGREGATE_1_mS1_tid_set__ = false;
      static int __nd_insert_S_rcv_push_s2_m___SQL_SUM_AGGREGATE_2_mS3_tid;
      bool ____nd_insert_S_rcv_push_s2_m___SQL_SUM_AGGREGATE_2_mS3_tid_set__ = false;
      static int __nd_insert_S_do_complete_s1_trig_tid;
      bool ____nd_insert_S_do_complete_s1_trig_tid_set__ = false;
      static int __nd_insert_S_do_complete_s3_trig_tid;
      bool ____nd_insert_S_do_complete_s3_trig_tid_set__ = false;
      static int __insert_S_rcv_corrective_s0_m___SQL_SUM_AGGREGATE_1_mS1_tid;
      bool ____insert_S_rcv_corrective_s0_m___SQL_SUM_AGGREGATE_1_mS1_tid_set__ = false;
      static int __insert_S_rcv_corrective_s2_m___SQL_SUM_AGGREGATE_1_mS1_tid;
      bool ____insert_S_rcv_corrective_s2_m___SQL_SUM_AGGREGATE_1_mS1_tid_set__ = false;
      static int __insert_S_rcv_corrective_s2_m___SQL_SUM_AGGREGATE_2_mS3_tid;
      bool ____insert_S_rcv_corrective_s2_m___SQL_SUM_AGGREGATE_2_mS3_tid_set__ = false;
      static int __nd_delete_S_rcv_put_tid;
      bool ____nd_delete_S_rcv_put_tid_set__ = false;
      static int __nd_delete_S_rcv_fetch_tid;
      bool ____nd_delete_S_rcv_fetch_tid_set__ = false;
      static int __nd_delete_S_send_push_s4_m___SQL_SUM_AGGREGATE_1_mS1_tid;
      bool ____nd_delete_S_send_push_s4_m___SQL_SUM_AGGREGATE_1_mS1_tid_set__ = false;
      static int __nd_delete_S_send_push_s6_m___SQL_SUM_AGGREGATE_1_mS1_tid;
      bool ____nd_delete_S_send_push_s6_m___SQL_SUM_AGGREGATE_1_mS1_tid_set__ = false;
      static int __nd_delete_S_send_push_s6_m___SQL_SUM_AGGREGATE_2_mS3_tid;
      bool ____nd_delete_S_send_push_s6_m___SQL_SUM_AGGREGATE_2_mS3_tid_set__ = false;
      static int __nd_delete_S_rcv_push_s4_m___SQL_SUM_AGGREGATE_1_mS1_tid;
      bool ____nd_delete_S_rcv_push_s4_m___SQL_SUM_AGGREGATE_1_mS1_tid_set__ = false;
      static int __nd_delete_S_rcv_push_s6_m___SQL_SUM_AGGREGATE_1_mS1_tid;
      bool ____nd_delete_S_rcv_push_s6_m___SQL_SUM_AGGREGATE_1_mS1_tid_set__ = false;
      static int __nd_delete_S_rcv_push_s6_m___SQL_SUM_AGGREGATE_2_mS3_tid;
      bool ____nd_delete_S_rcv_push_s6_m___SQL_SUM_AGGREGATE_2_mS3_tid_set__ = false;
      static int __nd_delete_S_do_complete_s5_trig_tid;
      bool ____nd_delete_S_do_complete_s5_trig_tid_set__ = false;
      static int __nd_delete_S_do_complete_s7_trig_tid;
      bool ____nd_delete_S_do_complete_s7_trig_tid_set__ = false;
      static int __delete_S_rcv_corrective_s4_m___SQL_SUM_AGGREGATE_1_mS1_tid;
      bool ____delete_S_rcv_corrective_s4_m___SQL_SUM_AGGREGATE_1_mS1_tid_set__ = false;
      static int __delete_S_rcv_corrective_s6_m___SQL_SUM_AGGREGATE_1_mS1_tid;
      bool ____delete_S_rcv_corrective_s6_m___SQL_SUM_AGGREGATE_1_mS1_tid_set__ = false;
      static int __delete_S_rcv_corrective_s6_m___SQL_SUM_AGGREGATE_2_mS3_tid;
      bool ____delete_S_rcv_corrective_s6_m___SQL_SUM_AGGREGATE_2_mS3_tid_set__ = false;
      static int __nd_insert_R_rcv_put_tid;
      bool ____nd_insert_R_rcv_put_tid_set__ = false;
      static int __nd_insert_R_rcv_fetch_tid;
      bool ____nd_insert_R_rcv_fetch_tid_set__ = false;
      static int __nd_insert_R_send_push_s8_m___SQL_SUM_AGGREGATE_1_mR1_tid;
      bool ____nd_insert_R_send_push_s8_m___SQL_SUM_AGGREGATE_1_mR1_tid_set__ = false;
      static int __nd_insert_R_send_push_s10_m___SQL_SUM_AGGREGATE_2_mR1_tid;
      bool ____nd_insert_R_send_push_s10_m___SQL_SUM_AGGREGATE_2_mR1_tid_set__ = false;
      static int __nd_insert_R_send_push_s10_m___SQL_SUM_AGGREGATE_1_mR1_tid;
      bool ____nd_insert_R_send_push_s10_m___SQL_SUM_AGGREGATE_1_mR1_tid_set__ = false;
      static int __nd_insert_R_rcv_push_s8_m___SQL_SUM_AGGREGATE_1_mR1_tid;
      bool ____nd_insert_R_rcv_push_s8_m___SQL_SUM_AGGREGATE_1_mR1_tid_set__ = false;
      static int __nd_insert_R_rcv_push_s10_m___SQL_SUM_AGGREGATE_2_mR1_tid;
      bool ____nd_insert_R_rcv_push_s10_m___SQL_SUM_AGGREGATE_2_mR1_tid_set__ = false;
      static int __nd_insert_R_rcv_push_s10_m___SQL_SUM_AGGREGATE_1_mR1_tid;
      bool ____nd_insert_R_rcv_push_s10_m___SQL_SUM_AGGREGATE_1_mR1_tid_set__ = false;
      static int __nd_insert_R_do_complete_s9_trig_tid;
      bool ____nd_insert_R_do_complete_s9_trig_tid_set__ = false;
      static int __nd_insert_R_do_complete_s11_trig_tid;
      bool ____nd_insert_R_do_complete_s11_trig_tid_set__ = false;
      static int __insert_R_rcv_corrective_s8_m___SQL_SUM_AGGREGATE_1_mR1_tid;
      bool ____insert_R_rcv_corrective_s8_m___SQL_SUM_AGGREGATE_1_mR1_tid_set__ = false;
      static int __insert_R_rcv_corrective_s10_m___SQL_SUM_AGGREGATE_2_mR1_tid;
      bool ____insert_R_rcv_corrective_s10_m___SQL_SUM_AGGREGATE_2_mR1_tid_set__ = false;
      static int __insert_R_rcv_corrective_s10_m___SQL_SUM_AGGREGATE_1_mR1_tid;
      bool ____insert_R_rcv_corrective_s10_m___SQL_SUM_AGGREGATE_1_mR1_tid_set__ = false;
      static int __nd_delete_R_rcv_put_tid;
      bool ____nd_delete_R_rcv_put_tid_set__ = false;
      static int __nd_delete_R_rcv_fetch_tid;
      bool ____nd_delete_R_rcv_fetch_tid_set__ = false;
      static int __nd_delete_R_send_push_s12_m___SQL_SUM_AGGREGATE_1_mR1_tid;
      bool ____nd_delete_R_send_push_s12_m___SQL_SUM_AGGREGATE_1_mR1_tid_set__ = false;
      static int __nd_delete_R_send_push_s14_m___SQL_SUM_AGGREGATE_2_mR1_tid;
      bool ____nd_delete_R_send_push_s14_m___SQL_SUM_AGGREGATE_2_mR1_tid_set__ = false;
      static int __nd_delete_R_send_push_s14_m___SQL_SUM_AGGREGATE_1_mR1_tid;
      bool ____nd_delete_R_send_push_s14_m___SQL_SUM_AGGREGATE_1_mR1_tid_set__ = false;
      static int __nd_delete_R_rcv_push_s12_m___SQL_SUM_AGGREGATE_1_mR1_tid;
      bool ____nd_delete_R_rcv_push_s12_m___SQL_SUM_AGGREGATE_1_mR1_tid_set__ = false;
      static int __nd_delete_R_rcv_push_s14_m___SQL_SUM_AGGREGATE_2_mR1_tid;
      bool ____nd_delete_R_rcv_push_s14_m___SQL_SUM_AGGREGATE_2_mR1_tid_set__ = false;
      static int __nd_delete_R_rcv_push_s14_m___SQL_SUM_AGGREGATE_1_mR1_tid;
      bool ____nd_delete_R_rcv_push_s14_m___SQL_SUM_AGGREGATE_1_mR1_tid_set__ = false;
      static int __nd_delete_R_do_complete_s13_trig_tid;
      bool ____nd_delete_R_do_complete_s13_trig_tid_set__ = false;
      static int __nd_delete_R_do_complete_s15_trig_tid;
      bool ____nd_delete_R_do_complete_s15_trig_tid_set__ = false;
      static int __delete_R_rcv_corrective_s12_m___SQL_SUM_AGGREGATE_1_mR1_tid;
      bool ____delete_R_rcv_corrective_s12_m___SQL_SUM_AGGREGATE_1_mR1_tid_set__ = false;
      static int __delete_R_rcv_corrective_s14_m___SQL_SUM_AGGREGATE_2_mR1_tid;
      bool ____delete_R_rcv_corrective_s14_m___SQL_SUM_AGGREGATE_2_mR1_tid_set__ = false;
      static int __delete_R_rcv_corrective_s14_m___SQL_SUM_AGGREGATE_1_mR1_tid;
      bool ____delete_R_rcv_corrective_s14_m___SQL_SUM_AGGREGATE_1_mR1_tid_set__ = false;
      static int __switchController_tid;
      bool ____switchController_tid_set__ = false;
      Address me;
      bool __me_set__ = false;
      _Collection<R_addr<Address>> peers;
      bool __peers_set__ = false;
      std::tuple<_Collection<R_arg<K3::base_string>>, _Collection<R_key_value<K3::base_string,
      K3::base_string>>> args;
      bool __args_set__ = false;
      K3::base_string role;
      bool __role_set__ = false;
      _Collection<R_i<Address>> my_peers;
      bool __my_peers_set__ = false;
      bool nd_sent_done;
      bool __nd_sent_done_set__ = false;
      bool sw_sent_done;
      bool __sw_sent_done_set__ = false;
      int ms_rcv_sw_init_ack_cnt;
      bool __ms_rcv_sw_init_ack_cnt_set__ = false;
      int ms_rcv_jobs_ack_cnt;
      bool __ms_rcv_jobs_ack_cnt_set__ = false;
      int ms_rcv_job_cnt;
      bool __ms_rcv_job_cnt_set__ = false;
      int ms_rcv_node_done_cnt;
      bool __ms_rcv_node_done_cnt_set__ = false;
      int ms_rcv_switch_done_cnt;
      bool __ms_rcv_switch_done_cnt_set__ = false;
      R_key_value<int, int> g_init_vid;
      bool __g_init_vid_set__ = false;
      R_key_value<int, int> g_min_vid;
      bool __g_min_vid_set__ = false;
      R_key_value<int, int> g_max_vid;
      bool __g_max_vid_set__ = false;
      R_key_value<int, int> g_start_vid;
      bool __g_start_vid_set__ = false;
      int job_master;
      bool __job_master_set__ = false;
      int job_switch;
      bool __job_switch_set__ = false;
      int job_node;
      bool __job_node_set__ = false;
      int job_timer;
      bool __job_timer_set__ = false;
      int job;
      bool __job_set__ = false;
      _Map<R_key_value<Address, int>> jobs;
      bool __jobs_set__ = false;
      Address master_addr;
      bool __master_addr_set__ = false;
      Address timer_addr;
      bool __timer_addr_set__ = false;
      _Collection<R_i<Address>> nodes;
      bool __nodes_set__ = false;
      _Collection<R_i<Address>> switches;
      bool __switches_set__ = false;
      int num_peers;
      bool __num_peers_set__ = false;
      int num_switches;
      bool __num_switches_set__ = false;
      int num_nodes;
      bool __num_nodes_set__ = false;
      _Collection<R_r1_r2_r3<int, K3::base_string, int>> map_ids;
      bool __map_ids_set__ = false;
      _Collection<R_r1_r2_r3<int, K3::base_string, _Seq<R_i<int>>>> trig_ids;
      bool __trig_ids_set__ = false;
      _Map<R_key_value<R_key_value<R_key_value<int, int>, int>, R_key_value<int,
      _Map<R_key_value<int, int>>>>> nd_stmt_cntrs;
      bool __nd_stmt_cntrs_set__ = false;
      _Set<R_key_value<R_key_value<int, int>, int>> nd_log_master;
      bool __nd_log_master_set__ = false;
      bool nd_rcvd_sys_done;
      bool __nd_rcvd_sys_done_set__ = false;
      bool sw_init;
      bool __sw_init_set__ = false;
      bool sw_seen_sentry;
      bool __sw_seen_sentry_set__ = false;
      _Seq<R_i<int>> sw_trig_buf_idx;
      bool __sw_trig_buf_idx_set__ = false;
      int ms_start_time;
      bool __ms_start_time_set__ = false;
      int ms_end_time;
      bool __ms_end_time_set__ = false;
      _Seq<R_key_value<int, int>> sw_buf_insert_S;
      bool __sw_buf_insert_S_set__ = false;
      _Seq<R_key_value<int, int>> sw_buf_delete_S;
      bool __sw_buf_delete_S_set__ = false;
      _Seq<R_key_value<int, int>> sw_buf_insert_R;
      bool __sw_buf_insert_R_set__ = false;
      _Seq<R_key_value<int, int>> sw_buf_delete_R;
      bool __sw_buf_delete_R_set__ = false;
      _Map<R_key_value<R_key_value<int, int>, R_key_value<int, int>>> nd_log_insert_S;
      bool __nd_log_insert_S_set__ = false;
      _Map<R_key_value<R_key_value<int, int>, R_key_value<int, int>>> nd_log_delete_S;
      bool __nd_log_delete_S_set__ = false;
      _Map<R_key_value<R_key_value<int, int>, R_key_value<int, int>>> nd_log_insert_R;
      bool __nd_log_insert_R_set__ = false;
      _Map<R_key_value<R_key_value<int, int>, R_key_value<int, int>>> nd_log_delete_R;
      bool __nd_log_delete_R_set__ = false;
      shared_ptr<_Set<R_key_value<R_key_value<int, int>, int>>> __SQL_SUM_AGGREGATE_1;
      bool ____SQL_SUM_AGGREGATE_1_set__ = false;
      shared_ptr<_Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>> __SQL_SUM_AGGREGATE_1_mS1;
      bool ____SQL_SUM_AGGREGATE_1_mS1_set__ = false;
      shared_ptr<_Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>> __SQL_SUM_AGGREGATE_1_mR1;
      bool ____SQL_SUM_AGGREGATE_1_mR1_set__ = false;
      shared_ptr<_Set<R_key_value<R_key_value<int, int>, int>>> __SQL_SUM_AGGREGATE_2;
      bool ____SQL_SUM_AGGREGATE_2_set__ = false;
      shared_ptr<_Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>> __SQL_SUM_AGGREGATE_2_mS3;
      bool ____SQL_SUM_AGGREGATE_2_mS3_set__ = false;
      shared_ptr<_Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>> __SQL_SUM_AGGREGATE_2_mR1;
      bool ____SQL_SUM_AGGREGATE_2_mR1_set__ = false;
      shared_ptr<_Set<R_r1_r2_r3<R_key_value<int, int>, int,
      int>>> map___SQL_SUM_AGGREGATE_1_mS1_s0_buf;
      bool __map___SQL_SUM_AGGREGATE_1_mS1_s0_buf_set__ = false;
      shared_ptr<_Set<R_r1_r2_r3<R_key_value<int, int>, int,
      int>>> map___SQL_SUM_AGGREGATE_1_mS1_s2_buf;
      bool __map___SQL_SUM_AGGREGATE_1_mS1_s2_buf_set__ = false;
      shared_ptr<_Set<R_r1_r2_r3<R_key_value<int, int>, int,
      int>>> map___SQL_SUM_AGGREGATE_2_mS3_s2_buf;
      bool __map___SQL_SUM_AGGREGATE_2_mS3_s2_buf_set__ = false;
      shared_ptr<_Set<R_r1_r2_r3<R_key_value<int, int>, int,
      int>>> map___SQL_SUM_AGGREGATE_1_mS1_s4_buf;
      bool __map___SQL_SUM_AGGREGATE_1_mS1_s4_buf_set__ = false;
      shared_ptr<_Set<R_r1_r2_r3<R_key_value<int, int>, int,
      int>>> map___SQL_SUM_AGGREGATE_1_mS1_s6_buf;
      bool __map___SQL_SUM_AGGREGATE_1_mS1_s6_buf_set__ = false;
      shared_ptr<_Set<R_r1_r2_r3<R_key_value<int, int>, int,
      int>>> map___SQL_SUM_AGGREGATE_2_mS3_s6_buf;
      bool __map___SQL_SUM_AGGREGATE_2_mS3_s6_buf_set__ = false;
      shared_ptr<_Set<R_r1_r2_r3<R_key_value<int, int>, int,
      int>>> map___SQL_SUM_AGGREGATE_1_mR1_s8_buf;
      bool __map___SQL_SUM_AGGREGATE_1_mR1_s8_buf_set__ = false;
      shared_ptr<_Set<R_r1_r2_r3<R_key_value<int, int>, int,
      int>>> map___SQL_SUM_AGGREGATE_2_mR1_s10_buf;
      bool __map___SQL_SUM_AGGREGATE_2_mR1_s10_buf_set__ = false;
      shared_ptr<_Set<R_r1_r2_r3<R_key_value<int, int>, int,
      int>>> map___SQL_SUM_AGGREGATE_1_mR1_s10_buf;
      bool __map___SQL_SUM_AGGREGATE_1_mR1_s10_buf_set__ = false;
      shared_ptr<_Set<R_r1_r2_r3<R_key_value<int, int>, int,
      int>>> map___SQL_SUM_AGGREGATE_1_mR1_s12_buf;
      bool __map___SQL_SUM_AGGREGATE_1_mR1_s12_buf_set__ = false;
      shared_ptr<_Set<R_r1_r2_r3<R_key_value<int, int>, int,
      int>>> map___SQL_SUM_AGGREGATE_2_mR1_s14_buf;
      bool __map___SQL_SUM_AGGREGATE_2_mR1_s14_buf_set__ = false;
      shared_ptr<_Set<R_r1_r2_r3<R_key_value<int, int>, int,
      int>>> map___SQL_SUM_AGGREGATE_1_mR1_s14_buf;
      bool __map___SQL_SUM_AGGREGATE_1_mR1_s14_buf_set__ = false;
      _Seq<R_r1_r2_r3<int, int, Address>> tm_timer_list;
      bool __tm_timer_list_set__ = false;
      Address sw_next_switch_addr;
      bool __sw_next_switch_addr_set__ = false;
      int sw_need_vid_cntr;
      bool __sw_need_vid_cntr_set__ = false;
      _Seq<R_key_value<R_key_value<int, int>, int>> sw_token_vid_list;
      bool __sw_token_vid_list_set__ = false;
      R_key_value<int, int> sw_highest_vid;
      bool __sw_highest_vid_set__ = false;
      _Seq<R_key_value<Address, int>> node_ring;
      bool __node_ring_set__ = false;
      int replicas;
      bool __replicas_set__ = false;
      _Seq<R_key_value<K3::base_string, _Seq<R_key_value<int, int>>>> pmap_input;
      bool __pmap_input_set__ = false;
      _Seq<R_key_value<int, _Seq<R_key_value<int, int>>>> pmap_data;
      bool __pmap_data_set__ = false;
      int sw_num_ack;
      bool __sw_num_ack_set__ = false;
      int sw_num_sent;
      bool __sw_num_sent_set__ = false;
      _Map<R_key_value<R_key_value<int, int>, int>> sw_ack_log;
      bool __sw_ack_log_set__ = false;
      int ms_gc_interval;
      bool __ms_gc_interval_set__ = false;
      _Map<R_key_value<Address, R_key_value<int, int>>> ms_gc_vid_map;
      bool __ms_gc_vid_map_set__ = false;
      int ms_gc_vid_ctr;
      bool __ms_gc_vid_ctr_set__ = false;
      int ms_num_gc_expected;
      bool __ms_num_gc_expected_set__ = false;
      unit_t nd_log_master_write(const R_key_value<R_key_value<int, int>, int>& b1)  {
        {
          auto& vid = b1.key;
          auto& stmt_id = b1.value;
          return nd_log_master.insert(R_key_value<R_key_value<int, int>, int> {vid, stmt_id});
        }
      }
      unit_t nd_log_write_insert_S(const R_r1_r2_r3<R_key_value<int, int>, int, int>& b1)  {
        {
          auto& vid = b1.r1;
          auto& S_B = b1.r2;
          auto& S_C = b1.r3;
          return nd_log_insert_S.insert(R_key_value<R_key_value<int, int>, R_key_value<int,
          int>> {vid, R_key_value<int, int> {S_B, S_C}});
        }
      }
      unit_t nd_log_write_delete_S(const R_r1_r2_r3<R_key_value<int, int>, int, int>& b1)  {
        {
          auto& vid = b1.r1;
          auto& S_B = b1.r2;
          auto& S_C = b1.r3;
          return nd_log_delete_S.insert(R_key_value<R_key_value<int, int>, R_key_value<int,
          int>> {vid, R_key_value<int, int> {S_B, S_C}});
        }
      }
      unit_t nd_log_write_insert_R(const R_r1_r2_r3<R_key_value<int, int>, int, int>& b1)  {
        {
          auto& vid = b1.r1;
          auto& R_A = b1.r2;
          auto& R_B = b1.r3;
          return nd_log_insert_R.insert(R_key_value<R_key_value<int, int>, R_key_value<int,
          int>> {vid, R_key_value<int, int> {R_A, R_B}});
        }
      }
      unit_t nd_log_write_delete_R(const R_r1_r2_r3<R_key_value<int, int>, int, int>& b1)  {
        {
          auto& vid = b1.r1;
          auto& R_A = b1.r2;
          auto& R_B = b1.r3;
          return nd_log_delete_R.insert(R_key_value<R_key_value<int, int>, R_key_value<int,
          int>> {vid, R_key_value<int, int> {R_A, R_B}});
        }
      }
      R_key_value<int, int> nd_log_get_bound_insert_S(const R_key_value<int, int>& vid)  {
        R_key_value<R_key_value<int, int>, R_key_value<int, int>> __25;
        shared_ptr<R_key_value<R_key_value<int, int>, R_key_value<int, int>>> __26;
        __26 = nd_log_insert_S.filter([this, &vid] (const R_key_value<R_key_value<int, int>,
        R_key_value<int, int>>& b1) mutable {
          {
            auto& key = b1.key;
            auto& value = b1.value;
            return key == vid;
          }
        }).peek(unit_t {});
        if (__26) {
          R_key_value<R_key_value<int, int>, R_key_value<int, int>>& x = *__26;
          __25 = x;
        } else {
          __25 = error<R_key_value<R_key_value<int, int>, R_key_value<int,
          int>>>(print("failed to find log"));
        }
        return __25.value;
      }
      R_key_value<int, int> nd_log_get_bound_delete_S(const R_key_value<int, int>& vid)  {
        R_key_value<R_key_value<int, int>, R_key_value<int, int>> __27;
        shared_ptr<R_key_value<R_key_value<int, int>, R_key_value<int, int>>> __28;
        __28 = nd_log_delete_S.filter([this, &vid] (const R_key_value<R_key_value<int, int>,
        R_key_value<int, int>>& b1) mutable {
          {
            auto& key = b1.key;
            auto& value = b1.value;
            return key == vid;
          }
        }).peek(unit_t {});
        if (__28) {
          R_key_value<R_key_value<int, int>, R_key_value<int, int>>& x = *__28;
          __27 = x;
        } else {
          __27 = error<R_key_value<R_key_value<int, int>, R_key_value<int,
          int>>>(print("failed to find log"));
        }
        return __27.value;
      }
      R_key_value<int, int> nd_log_get_bound_insert_R(const R_key_value<int, int>& vid)  {
        R_key_value<R_key_value<int, int>, R_key_value<int, int>> __29;
        shared_ptr<R_key_value<R_key_value<int, int>, R_key_value<int, int>>> __30;
        __30 = nd_log_insert_R.filter([this, &vid] (const R_key_value<R_key_value<int, int>,
        R_key_value<int, int>>& b1) mutable {
          {
            auto& key = b1.key;
            auto& value = b1.value;
            return key == vid;
          }
        }).peek(unit_t {});
        if (__30) {
          R_key_value<R_key_value<int, int>, R_key_value<int, int>>& x = *__30;
          __29 = x;
        } else {
          __29 = error<R_key_value<R_key_value<int, int>, R_key_value<int,
          int>>>(print("failed to find log"));
        }
        return __29.value;
      }
      R_key_value<int, int> nd_log_get_bound_delete_R(const R_key_value<int, int>& vid)  {
        R_key_value<R_key_value<int, int>, R_key_value<int, int>> __31;
        shared_ptr<R_key_value<R_key_value<int, int>, R_key_value<int, int>>> __32;
        __32 = nd_log_delete_R.filter([this, &vid] (const R_key_value<R_key_value<int, int>,
        R_key_value<int, int>>& b1) mutable {
          {
            auto& key = b1.key;
            auto& value = b1.value;
            return key == vid;
          }
        }).peek(unit_t {});
        if (__32) {
          R_key_value<R_key_value<int, int>, R_key_value<int, int>>& x = *__32;
          __31 = x;
        } else {
          __31 = error<R_key_value<R_key_value<int, int>, R_key_value<int,
          int>>>(print("failed to find log"));
        }
        return __31.value;
      }
      _Set<R_key_value<R_key_value<int, int>, int>> nd_log_read_geq(const R_key_value<int,
      int>& vid2)  {
        return nd_log_master.filter([this, &vid2] (const R_key_value<R_key_value<int, int>,
        int>& b1) mutable {
          {
            auto& vid = b1.key;
            auto& stmt_id = b1.value;
            return vid >= vid2;
          }
        });
      }
      bool nd_check_stmt_cntr_index(const R_r1_r2_r3<R_key_value<int, int>, int, int>& b1)  {
        {
          auto vid = b1.r1;
          auto stmt_id = b1.r2;
          auto add_to_count = b1.r3;
          bool __33;
          shared_ptr<R_key_value<R_key_value<R_key_value<int, int>, int>, R_key_value<int,
          _Map<R_key_value<int, int>>>>> __34;
          __34 = nd_stmt_cntrs.filter([this, &stmt_id,
          &vid] (const R_key_value<R_key_value<R_key_value<int, int>, int>, R_key_value<int,
          _Map<R_key_value<int, int>>>>& b1) mutable {
            {
              auto& key = b1.key;
              auto& value = b1.value;
              return key == (R_key_value<R_key_value<int, int>, int> {vid, stmt_id});
            }
          }).peek(unit_t {});
          if (__34) {
            R_key_value<R_key_value<R_key_value<int, int>, int>, R_key_value<int,
            _Map<R_key_value<int, int>>>>& lookup_value = *__34;
            {
              int __35;
              __35 = add_to_count + lookup_value.value.key;
              auto& new_count = __35;
              nd_stmt_cntrs.update(lookup_value, R_key_value<R_key_value<R_key_value<int, int>,
              int>, R_key_value<int, _Map<R_key_value<int, int>>>> {R_key_value<R_key_value<int,
              int>, int> {vid, stmt_id}, R_key_value<int, _Map<R_key_value<int, int>>> {new_count,
              lookup_value.value.value}});
              __33 = 0 == new_count;
            }
          } else {
            nd_stmt_cntrs.insert(R_key_value<R_key_value<R_key_value<int, int>, int>,
            R_key_value<int, _Map<R_key_value<int, int>>>> {R_key_value<R_key_value<int, int>,
            int> {vid, stmt_id}, R_key_value<int, _Map<R_key_value<int, int>>> {add_to_count,
            _Map<R_key_value<int, int>> {}}});
            __33 = false;
          }
          vid = b1.r1;
          stmt_id = b1.r2;
          add_to_count = b1.r3;
          return __33;
        }
      }
      unit_t nd_complete_stmt_cntr_check(const R_key_value<R_key_value<int, int>, int>& b1)  {
        {
          auto vid = b1.key;
          auto stmt_id = b1.value;
          unit_t __36;
          shared_ptr<R_key_value<R_key_value<R_key_value<int, int>, int>, R_key_value<int,
          _Map<R_key_value<int, int>>>>> __37;
          __37 = nd_stmt_cntrs.filter([this, &stmt_id,
          &vid] (const R_key_value<R_key_value<R_key_value<int, int>, int>, R_key_value<int,
          _Map<R_key_value<int, int>>>>& b1) mutable {
            {
              auto& key = b1.key;
              auto& value = b1.value;
              return key == (R_key_value<R_key_value<int, int>, int> {vid, stmt_id});
            }
          }).peek(unit_t {});
          if (__37) {
            R_key_value<R_key_value<R_key_value<int, int>, int>, R_key_value<int,
            _Map<R_key_value<int, int>>>>& lookup_data = *__37;
            nd_stmt_cntrs.erase(lookup_data);
          }
          if (nd_rcvd_sys_done) {
            if ((!nd_sent_done) && nd_stmt_cntrs.size(unit_t {}) == 0) {
              auto __38 = std::make_shared<K3::ValDispatcher<bool>>(true);
              __engine.send(master_addr, __ms_rcv_node_done_tid, __38, me);
              nd_sent_done = true;
              __36 = unit_t {};
            } else {
              __36 = unit_t {};
            }
          } else {
            __36 = unit_t {};
          }
          vid = b1.key;
          stmt_id = b1.value;
          return __36;
        }
      }
      unit_t nd_update_stmt_cntr_corr_map(const R_r1_r2_r3_r4_r5_r6<R_key_value<int, int>, int, int,
      int, bool, bool>& b1)  {
        {
          auto vid = b1.r1;
          auto stmt_id = b1.r2;
          auto hop = b1.r3;
          auto count = b1.r4;
          auto root = b1.r5;
          auto create = b1.r6;
          unit_t __39;
          if (create) {
            nd_stmt_cntrs.insert(R_key_value<R_key_value<R_key_value<int, int>, int>,
            R_key_value<int, _Map<R_key_value<int, int>>>> {R_key_value<R_key_value<int, int>,
            int> {vid, stmt_id}, R_key_value<int, _Map<R_key_value<int, int>>> {0,
            _Map<R_key_value<int, int>> {}}});
          }
          shared_ptr<R_key_value<R_key_value<R_key_value<int, int>, int>, R_key_value<int,
          _Map<R_key_value<int, int>>>>> __40;
          __40 = nd_stmt_cntrs.filter([this, &stmt_id,
          &vid] (const R_key_value<R_key_value<R_key_value<int, int>, int>, R_key_value<int,
          _Map<R_key_value<int, int>>>>& b1) mutable {
            {
              auto& key = b1.key;
              auto& value = b1.value;
              return key == (R_key_value<R_key_value<int, int>, int> {vid, stmt_id});
            }
          }).peek(unit_t {});
          if (__40) {
            R_key_value<R_key_value<R_key_value<int, int>, int>, R_key_value<int,
            _Map<R_key_value<int, int>>>>& lkup = *__40;
            R_key_value<int, _Map<R_key_value<int, int>>> __41;
            {
              _Map<R_key_value<int, int>> __42;
              __42 = lkup.value.value;
              auto& sc_corr_map = __42;
              shared_ptr<R_key_value<int, int>> __43;
              __43 = sc_corr_map.filter([this, &hop] (const R_key_value<int, int>& b1) mutable {
                {
                  auto& key = b1.key;
                  auto& value = b1.value;
                  return key == hop;
                }
              }).peek(unit_t {});
              if (__43) {
                R_key_value<int, int>& lkup2 = *__43;
                sc_corr_map.update(lkup2, R_key_value<int, int> {hop, lkup2.value + count});
              } else {
                sc_corr_map.insert(R_key_value<int, int> {hop, count});
              }
              if (root) {
                
              } else {
                shared_ptr<R_key_value<int, int>> __44;
                __44 = sc_corr_map.filter([this, &hop] (const R_key_value<int, int>& b1) mutable {
                  {
                    auto& key = b1.key;
                    auto& value = b1.value;
                    return key == hop;
                  }
                }).peek(unit_t {});
                if (__44) {
                  R_key_value<int, int>& lkup2 = *__44;
                  {
                    int __45;
                    __45 = lkup2.value - 1;
                    auto& new_corr_cnt = __45;
                    if (new_corr_cnt == 0) {
                      sc_corr_map.erase(lkup2);
                    } else {
                      sc_corr_map.update(lkup2, R_key_value<int, int> {hop, new_corr_cnt});
                    }
                  }
                } else {
                  sc_corr_map.insert(R_key_value<int, int> {hop, -(1)});
                }
              }
              __41 = R_key_value<int, _Map<R_key_value<int, int>>> {lkup.value.key, sc_corr_map};
            }
            __39 = nd_stmt_cntrs.update(lkup, R_key_value<R_key_value<R_key_value<int, int>, int>,
            R_key_value<int, _Map<R_key_value<int, int>>>> {R_key_value<R_key_value<int, int>,
            int> {vid, stmt_id}, __41});
          } else {
            __39 = error<unit_t>(print("nd_update_stmt_cntr_corr_map: missing stmt_cntrs value"));
          }
          vid = b1.r1;
          stmt_id = b1.r2;
          hop = b1.r3;
          count = b1.r4;
          root = b1.r5;
          create = b1.r6;
          return __39;
        }
      }
      _Collection<R_key_value<int, _Seq<R_key_value<int,
      int>>>> nd_filter_corrective_list(const R_key_value<R_key_value<int, int>,
      _Collection<R_key_value<int, int>>>& b1)  {
        {
          auto request_vid = b1.key;
          auto trig_stmt_list = b1.value;
          _Collection<R_key_value<int, _Seq<R_key_value<int, int>>>> __46;
          auto __48 = _Seq<R_key_value<R_key_value<int, int>, int>> {};
          for (const auto& __47: nd_log_read_geq(request_vid)) {
            __48 = [this] (_Seq<R_key_value<R_key_value<int, int>, int>> acc_conv) mutable {
              return [this, acc_conv = std::move(acc_conv)] (const R_key_value<R_key_value<int,
              int>, int>& x) mutable {
                acc_conv.insert(x);
                return std::move(acc_conv);
              };
            }(std::move(__48))(__47);
          }
          auto __50 = _Collection<R_key_value<int, _Seq<R_key_value<int, int>>>> {};
          for (const auto& __49: std::move(__48).sort([this] (const R_key_value<R_key_value<int,
          int>, int>& b2) mutable {
            return [this, &b2] (const R_key_value<R_key_value<int, int>, int>& b5) mutable {
              {
                auto& vid1 = b2.key;
                auto& stmt1 = b2.value;
                {
                  auto& vid2 = b5.key;
                  auto& stmt2 = b5.value;
                  if (vid1 < vid2) {
                    return -(1);
                  } else {
                    return 1;
                  }
                }
              }
            };
          }).groupBy([this] (const R_key_value<R_key_value<int, int>, int>& b1) mutable {
            {
              auto& stmt_id = b1.value;
              return std::move(stmt_id);
            }
          }, [this] (_Seq<R_key_value<int, int>> vid_list) mutable {
            return [this, vid_list = std::move(vid_list)] (const R_key_value<R_key_value<int, int>,
            int>& b3) mutable {
              {
                auto& vid = b3.key;
                vid_list.insert(vid);
                return std::move(vid_list);
              }
            };
          }, _Seq<R_key_value<int, int>> {})) {
            __50 = [this] (_Collection<R_key_value<int, _Seq<R_key_value<int,
            int>>>> acc_conv) mutable {
              return [this, acc_conv = std::move(acc_conv)] (const R_key_value<int,
              _Seq<R_key_value<int, int>>>& x) mutable {
                acc_conv.insert(x);
                return std::move(acc_conv);
              };
            }(std::move(__50))(__49);
          }
          __46 = std::move(__50);
          request_vid = b1.key;
          trig_stmt_list = b1.value;
          return __46;
        }
      }
      unit_t add_node(const Address& addr)  {
        {
          _Seq<R_i<int>> __51;
          __51 = range<_Seq<R_i<int>>>(replicas);
          auto& rng = __51;
          {
            _Seq<R_key_value<Address, int>> __52;
            auto __54 = _Seq<R_key_value<Address, int>> {};
            for (const auto& __53: rng) {
              __54 = [this, &addr] (_Seq<R_key_value<Address, int>> _accmap) mutable {
                return [this, _accmap = std::move(_accmap), &addr] (const R_i<int>& b3) mutable {
                  {
                    auto& i = b3.i;
                    _accmap.insert(R_key_value<Address, int> {addr,
                    abs(hash(i * 2683 + hash(addr)))});
                    return std::move(_accmap);
                  }
                };
              }(std::move(__54))(__53);
            }
            __52 = std::move(__54);
            auto& new_elems = __52;
            node_ring = node_ring.combine(new_elems);
            node_ring = node_ring.sort([this] (const R_key_value<Address, int>& b2) mutable {
              return [this, &b2] (const R_key_value<Address, int>& b5) mutable {
                {
                  auto& addr = b2.key;
                  auto& hash1 = b2.value;
                  {
                    auto& addr = b5.key;
                    auto& hash2 = b5.value;
                    if (hash1 < hash2) {
                      return -(1);
                    } else {
                      return 1;
                    }
                  }
                }
              };
            });
            return unit_t {};
          }
        }
      }
      Address get_ring_node(const R_key_value<int, int>& b1)  {
        {
          auto data = b1.key;
          auto max_val = b1.value;
          Address __55;
          {
            int __56;
            __56 = truncate(real_of_int(get_max_int(unit_t {})) * real_of_int(data) / real_of_int(max_val));
            auto& scaled = __56;
            {
              _Seq<R_key_value<Address, int>> __57;
              __57 = node_ring.filter([this, &scaled] (const R_key_value<Address,
              int>& b1) mutable {
                {
                  auto& addr = b1.key;
                  auto& hash = b1.value;
                  return hash >= scaled;
                }
              });
              auto& results = __57;
              R_key_value<Address, int> __58;
              shared_ptr<R_key_value<Address, int>> __59;
              __59 = results.peek(unit_t {});
              if (__59) {
                R_key_value<Address, int>& x = *__59;
                __58 = x;
              } else {
                shared_ptr<R_key_value<Address, int>> __60;
                __60 = node_ring.peek(unit_t {});
                if (__60) {
                  R_key_value<Address, int>& x = *__60;
                  __58 = x;
                } else {
                  __58 = error<R_key_value<Address, int>>(print("empty node ring"));
                }
              }
              {
                auto& addr = __58.key;
                auto& _ = __58.value;
                __55 = addr;
              }
            }
          }
          data = b1.key;
          max_val = b1.value;
          return __55;
        }
      }
      _Set<R_r1_r2_r3<R_key_value<int, int>, int,
      int>> frontier_int_int(const R_key_value<R_key_value<int, int>,
      _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>>& b1)  {
        {
          auto vid = b1.key;
          auto input_map = b1.value;
          _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> __61;
          auto __65 = _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> {};
          for (const auto& __64: input_map.groupBy([this] (const R_r1_r2_r3<R_key_value<int, int>,
          int, int>& b1) mutable {
            {
              auto& map_vid = b1.r1;
              auto& map_0 = b1.r2;
              auto& map_val = b1.r3;
              return std::move(map_0);
            }
          }, [this, &vid] (R_key_value<_Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>,
          R_key_value<int, int>> b2) mutable {
            return [this, &b2, &vid] (const R_r1_r2_r3<R_key_value<int, int>, int,
            int>& b5) mutable {
              {
                auto& acc = b2.key;
                auto& max_vid = b2.value;
                {
                  auto& map_vid = b5.r1;
                  auto& map_0 = b5.r2;
                  auto& map_val = b5.r3;
                  if (map_vid < vid) {
                    if (map_vid == max_vid) {
                      acc.insert(R_r1_r2_r3<R_key_value<int, int>, int, int> {map_vid, map_0,
                      map_val});
                      return R_key_value<_Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>,
                      R_key_value<int, int>> {acc, max_vid};
                    } else {
                      if (map_vid > max_vid) {
                        _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> __62;
                        {
                          _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> __63;
                          __63 = _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> {};
                          auto& __collection = __63;
                          __collection.insert(R_r1_r2_r3<R_key_value<int, int>, int, int> {map_vid,
                          map_0, map_val});
                          __62 = __collection;
                        }
                        return R_key_value<_Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>,
                        R_key_value<int, int>> {__62, map_vid};
                      } else {
                        return R_key_value<_Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>,
                        R_key_value<int, int>> {acc, max_vid};
                      }
                    }
                  } else {
                    return R_key_value<_Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>,
                    R_key_value<int, int>> {acc, max_vid};
                  }
                }
              }
            };
          }, R_key_value<_Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>, R_key_value<int,
          int>> {_Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> {}, g_min_vid})) {
            __65 = [this] (const _Set<R_r1_r2_r3<R_key_value<int, int>, int,
            int>>& _accext) mutable {
              return [this, &_accext] (const R_key_value<int,
              R_key_value<_Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>, R_key_value<int,
              int>>>& b3) mutable {
                {
                  auto& b4 = b3.value;
                  {
                    auto& project = b4.key;
                    return _accext.combine(project);
                  }
                }
              };
            }(std::move(__65))(__64);
          }
          __61 = std::move(__65);
          vid = b1.key;
          input_map = b1.value;
          return __61;
        }
      }
      _Set<R_key_value<R_key_value<int, int>, int>> frontier_int(const R_key_value<R_key_value<int,
      int>, _Set<R_key_value<R_key_value<int, int>, int>>>& b1)  {
        {
          auto& vid = b1.key;
          auto& input_map = b1.value;
          auto __69 = R_key_value<_Set<R_key_value<R_key_value<int, int>, int>>, R_key_value<int,
          int>> {_Set<R_key_value<R_key_value<int, int>, int>> {}, g_min_vid};
          for (const auto& __68: input_map) {
            __69 = [this, &vid] (R_key_value<_Set<R_key_value<R_key_value<int, int>, int>>,
            R_key_value<int, int>> b2) mutable {
              return [this, &b2, &vid] (const R_key_value<R_key_value<int, int>, int>& b5) mutable {
                {
                  auto& acc = b2.key;
                  auto& max_vid = b2.value;
                  {
                    auto& map_vid = b5.key;
                    auto& map_val = b5.value;
                    if (map_vid < vid) {
                      if (map_vid == max_vid) {
                        acc.insert(R_key_value<R_key_value<int, int>, int> {map_vid, map_val});
                        return R_key_value<_Set<R_key_value<R_key_value<int, int>, int>>,
                        R_key_value<int, int>> {acc, max_vid};
                      } else {
                        if (map_vid > max_vid) {
                          _Set<R_key_value<R_key_value<int, int>, int>> __66;
                          {
                            _Set<R_key_value<R_key_value<int, int>, int>> __67;
                            __67 = _Set<R_key_value<R_key_value<int, int>, int>> {};
                            auto& __collection = __67;
                            __collection.insert(R_key_value<R_key_value<int, int>, int> {map_vid,
                            map_val});
                            __66 = __collection;
                          }
                          return R_key_value<_Set<R_key_value<R_key_value<int, int>, int>>,
                          R_key_value<int, int>> {__66, map_vid};
                        } else {
                          return R_key_value<_Set<R_key_value<R_key_value<int, int>, int>>,
                          R_key_value<int, int>> {acc, max_vid};
                        }
                      }
                    } else {
                      return R_key_value<_Set<R_key_value<R_key_value<int, int>, int>>,
                      R_key_value<int, int>> {acc, max_vid};
                    }
                  }
                }
              };
            }(std::move(__69))(__68);
          }
          return std::move(__69).key;
        }
      }
      unit_t nd_add_delta_to_int_int(const R_r1_r2_r3_r4<shared_ptr<_Set<R_r1_r2_r3<R_key_value<int,
      int>, int, int>>>, bool, R_key_value<int, int>, _Set<R_key_value<int, int>>>& b1)  {
        {
          auto target_map = b1.r1;
          auto corrective = b1.r2;
          auto min_vid = b1.r3;
          auto delta_tuples = b1.r4;
          unit_t __70;
          delta_tuples.iterate([this, &corrective, &min_vid, &target_map] (const R_key_value<int,
          int>& b1) mutable {
            {
              auto map_0 = b1.key;
              auto map_val = b1.value;
              unit_t __71;
              {
                auto& target_map_d = *target_map;
                {
                  _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> __72;
                  if (corrective) {
                    __72 = target_map_d.filter([this, &map_0,
                    &min_vid] (const R_r1_r2_r3<R_key_value<int, int>, int, int>& b1) mutable {
                      {
                        auto& r1 = b1.r1;
                        auto& r2 = b1.r2;
                        auto& r3 = b1.r3;
                        return r1 == min_vid && r2 == map_0;
                      }
                    });
                  } else {
                    __72 = _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> {};
                  }
                  auto& lookup_value = __72;
                  shared_ptr<R_r1_r2_r3<R_key_value<int, int>, int, int>> __73;
                  __73 = lookup_value.peek(unit_t {});
                  if (__73) {
                    R_r1_r2_r3<R_key_value<int, int>, int, int>& val = *__73;
                    {
                      int __74;
                      __74 = map_val + val.r3;
                      auto& update_value = __74;
                      __71 = target_map_d.update(val, R_r1_r2_r3<R_key_value<int, int>, int,
                      int> {min_vid, map_0, update_value});
                    }
                  } else {
                    {
                      _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> __75;
                      __75 = frontier_int_int(R_key_value<R_key_value<int, int>,
                      _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>> {min_vid,
                      target_map_d.filter([this, &map_0] (const R_r1_r2_r3<R_key_value<int, int>,
                      int, int>& b1) mutable {
                        {
                          auto& r1 = b1.r1;
                          auto& r2 = b1.r2;
                          auto& r3 = b1.r3;
                          return r2 == map_0;
                        }
                      })});
                      auto& lookup_value = __75;
                      {
                        int __76;
                        int __77;
                        shared_ptr<R_r1_r2_r3<R_key_value<int, int>, int, int>> __78;
                        __78 = lookup_value.peek(unit_t {});
                        if (__78) {
                          R_r1_r2_r3<R_key_value<int, int>, int, int>& val = *__78;
                          __77 = val.r3;
                        } else {
                          __77 = 0;
                        }
                        __76 = map_val + __77;
                        auto& update_value = __76;
                        __71 = target_map_d.insert(R_r1_r2_r3<R_key_value<int, int>, int,
                        int> {min_vid, map_0, update_value});
                      }
                    }
                  }
                }
              }
              map_0 = b1.key;
              map_val = b1.value;
              return __71;
            }
          });
          __70 = delta_tuples.iterate([this, &min_vid, &target_map] (const R_key_value<int,
          int>& b1) mutable {
            {
              auto map_0_delta = b1.key;
              auto map_val_delta = b1.value;
              unit_t __79;
              {
                _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> __80;
                {
                  auto& target_map_d = *target_map;
                  __80 = target_map_d.filter([this, &map_0_delta] (const R_r1_r2_r3<R_key_value<int,
                  int>, int, int>& b1) mutable {
                    {
                      auto& r1 = b1.r1;
                      auto& r2 = b1.r2;
                      auto& r3 = b1.r3;
                      return r2 == map_0_delta;
                    }
                  }).filter([this, &min_vid] (const R_r1_r2_r3<R_key_value<int, int>, int,
                  int>& b1) mutable {
                    {
                      auto& vid = b1.r1;
                      auto& map_0 = b1.r2;
                      auto& map_val = b1.r3;
                      return vid > min_vid;
                    }
                  });
                }
                auto& filtered = __80;
                __79 = filtered.iterate([this, &map_val_delta,
                &target_map] (const R_r1_r2_r3<R_key_value<int, int>, int, int>& b1) mutable {
                  {
                    auto& vid = b1.r1;
                    auto& map_0 = b1.r2;
                    auto& map_val = b1.r3;
                    {
                      auto& target_map_d = *target_map;
                      return target_map_d.update(R_r1_r2_r3<R_key_value<int, int>, int, int> {vid,
                      map_0, map_val}, R_r1_r2_r3<R_key_value<int, int>, int, int> {vid, map_0,
                      map_val + map_val_delta});
                    }
                  }
                });
              }
              map_0_delta = b1.key;
              map_val_delta = b1.value;
              return __79;
            }
          });
          target_map = b1.r1;
          corrective = b1.r2;
          min_vid = b1.r3;
          delta_tuples = b1.r4;
          return __70;
        }
      }
      unit_t nd_add_delta_to_int(const R_r1_r2_r3_r4<shared_ptr<_Set<R_key_value<R_key_value<int,
      int>, int>>>, bool, R_key_value<int, int>, _Set<R_i<int>>>& b1)  {
        {
          auto target_map = b1.r1;
          auto corrective = b1.r2;
          auto min_vid = b1.r3;
          auto delta_tuples = b1.r4;
          unit_t __81;
          delta_tuples.iterate([this, &corrective, &min_vid,
          &target_map] (const R_i<int>& b1) mutable {
            {
              auto map_val = b1.i;
              unit_t __82;
              {
                auto& target_map_d = *target_map;
                {
                  _Set<R_key_value<R_key_value<int, int>, int>> __83;
                  if (corrective) {
                    __83 = target_map_d.filter([this, &min_vid] (const R_key_value<R_key_value<int,
                    int>, int>& b1) mutable {
                      {
                        auto& key = b1.key;
                        auto& value = b1.value;
                        return key == min_vid;
                      }
                    });
                  } else {
                    __83 = _Set<R_key_value<R_key_value<int, int>, int>> {};
                  }
                  auto& lookup_value = __83;
                  shared_ptr<R_key_value<R_key_value<int, int>, int>> __84;
                  __84 = lookup_value.peek(unit_t {});
                  if (__84) {
                    R_key_value<R_key_value<int, int>, int>& val = *__84;
                    {
                      int __85;
                      __85 = map_val + val.value;
                      auto& update_value = __85;
                      __82 = target_map_d.update(val, R_key_value<R_key_value<int, int>,
                      int> {min_vid, update_value});
                    }
                  } else {
                    {
                      _Set<R_key_value<R_key_value<int, int>, int>> __86;
                      __86 = frontier_int(R_key_value<R_key_value<int, int>,
                      _Set<R_key_value<R_key_value<int, int>, int>>> {min_vid, target_map_d});
                      auto& lookup_value = __86;
                      {
                        int __87;
                        int __88;
                        shared_ptr<R_key_value<R_key_value<int, int>, int>> __89;
                        __89 = lookup_value.peek(unit_t {});
                        if (__89) {
                          R_key_value<R_key_value<int, int>, int>& val = *__89;
                          __88 = val.value;
                        } else {
                          __88 = 0;
                        }
                        __87 = map_val + __88;
                        auto& update_value = __87;
                        __82 = target_map_d.insert(R_key_value<R_key_value<int, int>, int> {min_vid,
                        update_value});
                      }
                    }
                  }
                }
              }
              map_val = b1.i;
              return __82;
            }
          });
          __81 = delta_tuples.iterate([this, &min_vid, &target_map] (const R_i<int>& b1) mutable {
            {
              auto map_val_delta = b1.i;
              unit_t __90;
              {
                _Set<R_key_value<R_key_value<int, int>, int>> __91;
                {
                  auto& target_map_d = *target_map;
                  __91 = target_map_d.filter([this, &min_vid] (const R_key_value<R_key_value<int,
                  int>, int>& b1) mutable {
                    {
                      auto& vid = b1.key;
                      auto& map_val = b1.value;
                      return vid > min_vid;
                    }
                  });
                }
                auto& filtered = __91;
                __90 = filtered.iterate([this, &map_val_delta,
                &target_map] (const R_key_value<R_key_value<int, int>, int>& b1) mutable {
                  {
                    auto& vid = b1.key;
                    auto& map_val = b1.value;
                    {
                      auto& target_map_d = *target_map;
                      return target_map_d.update(R_key_value<R_key_value<int, int>, int> {vid,
                      map_val}, R_key_value<R_key_value<int, int>, int> {vid,
                      map_val + map_val_delta});
                    }
                  }
                });
              }
              map_val_delta = b1.i;
              return __90;
            }
          });
          target_map = b1.r1;
          corrective = b1.r2;
          min_vid = b1.r3;
          delta_tuples = b1.r4;
          return __81;
        }
      }
      shared_ptr<R_key_value<int, int>> sw_gen_vid(const unit_t& _)  {
        shared_ptr<R_key_value<R_key_value<int, int>, int>> __92;
        __92 = sw_token_vid_list.peek(unit_t {});
        if (__92) {
          R_key_value<R_key_value<int, int>, int>& vid_num = *__92;
          {
            int __93;
            __93 = vid_num.value - 1;
            auto& num_new = __93;
            {
              R_key_value<int, int> __94;
              __94 = R_key_value<int, int> {vid_num.key.key, vid_num.key.value + 1};
              auto& vid_new = __94;
              if (num_new <= 0) {
                sw_token_vid_list.erase(vid_num);
              } else {
                sw_token_vid_list.update(vid_num, R_key_value<R_key_value<int, int>, int> {vid_new,
                num_new});
              }
              return make_shared<R_key_value<int, int>>(vid_num.key);
            }
          }
        } else {
          return nullptr;
        }
      }
      R_key_value<_Seq<R_key_value<int, int>>, int> calc_dim_bounds(const _Seq<R_key_value<int,
      int>>& pmap)  {
        auto __98 = R_key_value<_Seq<R_key_value<int, int>>, int> {_Seq<R_key_value<int, int>> {},
        1};
        for (const auto& __97: pmap) {
          __98 = [this] (const R_key_value<_Seq<R_key_value<int, int>>, int>& b2) mutable {
            return [this, &b2] (const R_key_value<int, int>& b5) mutable {
              {
                auto& xs = b2.key;
                auto& acc_size = b2.value;
                {
                  auto& pos = b5.key;
                  auto& bin_size = b5.value;
                  _Seq<R_key_value<int, int>> __95;
                  {
                    _Seq<R_key_value<int, int>> __96;
                    __96 = _Seq<R_key_value<int, int>> {};
                    auto& __collection = __96;
                    __collection.insert(R_key_value<int, int> {pos, acc_size});
                    __95 = __collection;
                  }
                  return R_key_value<_Seq<R_key_value<int, int>>, int> {xs.combine(__95),
                  bin_size * acc_size};
                }
              }
            };
          }(std::move(__98))(__97);
        }
        return std::move(__98);
      }
      _Collection<R_i<Address>> route_to_(const R_key_value<int, unit_t>& b1)  {
        {
          _Collection<R_i<Address>> __99;
          __99 = _Collection<R_i<Address>> {};
          auto& __collection = __99;
          __collection.insert(R_i<Address> {get_ring_node(R_key_value<int, int> {1, 1})});
          return __collection;
        }
      }
      _Collection<R_i<Address>> route_to_int(const R_key_value<int, shared_ptr<int>>& b1)  {
        {
          auto map_id = b1.key;
          auto key_0 = b1.value;
          _Collection<R_i<Address>> __100;
          {
            _Seq<R_key_value<int, int>> __101;
            R_key_value<int, _Seq<R_key_value<int, int>>> __102;
            shared_ptr<R_key_value<int, _Seq<R_key_value<int, int>>>> __103;
            __103 = pmap_data.filter([this, &map_id] (const R_key_value<int, _Seq<R_key_value<int,
            int>>>& b1) mutable {
              {
                auto& key = b1.key;
                auto& value = b1.value;
                return key == map_id;
              }
            }).peek(unit_t {});
            if (__103) {
              R_key_value<int, _Seq<R_key_value<int, int>>>& x = *__103;
              __102 = x;
            } else {
              __102 = error<R_key_value<int, _Seq<R_key_value<int,
              int>>>>(print("can't find map_id in pmap_data"));
            }
            __101 = __102.value;
            auto& pmap = __101;
            shared_ptr<R_key_value<int, int>> __104;
            __104 = pmap.peek(unit_t {});
            if (__104) {
              R_key_value<int, int>& _ = *__104;
              R_key_value<_Seq<R_key_value<int, int>>, int> __105;
              __105 = calc_dim_bounds(pmap);
              {
                auto& dim_bounds = __105.key;
                auto& max_val = __105.value;
                {
                  int __106;
                  int __107;
                  if (key_0) {
                    int& key_0_unwrap = *key_0;
                    {
                      _Seq<R_key_value<int, int>> __108;
                      __108 = pmap.filter([this] (const R_key_value<int, int>& b1) mutable {
                        {
                          auto& key = b1.key;
                          auto& value = b1.value;
                          return key == 0;
                        }
                      });
                      auto& pmap_slice = __108;
                      shared_ptr<R_key_value<int, int>> __109;
                      __109 = pmap_slice.peek(unit_t {});
                      if (__109) {
                        R_key_value<int, int>& peek_slice = *__109;
                        {
                          int __110;
                          __110 = abs(hash(key_0_unwrap)) % peek_slice.value;
                          auto& value = __110;
                          R_key_value<int, int> __111;
                          shared_ptr<R_key_value<int, int>> __112;
                          __112 = dim_bounds.filter([this] (const R_key_value<int,
                          int>& b1) mutable {
                            {
                              auto& key = b1.key;
                              auto& value = b1.value;
                              return key == 0;
                            }
                          }).peek(unit_t {});
                          if (__112) {
                            R_key_value<int, int>& x = *__112;
                            __111 = x;
                          } else {
                            __111 = error<R_key_value<int,
                            int>>(print("can't find 0 in dim_bounds"));
                          }
                          __107 = value * __111.value;
                        }
                      } else {
                        __107 = 0;
                      }
                    }
                  } else {
                    __107 = 0;
                  }
                  __106 = __107 + 0;
                  auto& bound_bucket = __106;
                  {
                    _Seq<R_key_value<int, int>> __113;
                    _Seq<R_key_value<int, int>> __114;
                    if (key_0 != nullptr) {
                      __114 = _Seq<R_key_value<int, int>> {};
                    } else {
                      __114 = pmap.filter([this] (const R_key_value<int, int>& b1) mutable {
                        {
                          auto& key = b1.key;
                          auto& value = b1.value;
                          return key == 0;
                        }
                      });
                    }
                    __113 = __114.combine(_Seq<R_key_value<int, int>> {});
                    auto& free_dims = __113;
                    {
                      _Seq<R_key_value<int, _Seq<R_i<int>>>> __115;
                      auto __117 = _Seq<R_key_value<int, _Seq<R_i<int>>>> {};
                      for (const auto& __116: free_dims) {
                        __117 = [this] (_Seq<R_key_value<int, _Seq<R_i<int>>>> _accmap) mutable {
                          return [this, _accmap = std::move(_accmap)] (const R_key_value<int,
                          int>& b3) mutable {
                            {
                              auto& i = b3.key;
                              auto& b_i = b3.value;
                              _accmap.insert(R_key_value<int, _Seq<R_i<int>>> {i,
                              range<_Seq<R_i<int>>>(b_i)});
                              return std::move(_accmap);
                            }
                          };
                        }(std::move(__117))(__116);
                      }
                      __115 = std::move(__117);
                      auto& free_domains = __115;
                      {
                        _Seq<R_i<_Seq<R_key_value<int, int>>>> __118;
                        auto __132 = _Seq<R_i<_Seq<R_key_value<int, int>>>> {};
                        for (const auto& __131: free_domains) {
                          __132 = [this] (const _Seq<R_i<_Seq<R_key_value<int,
                          int>>>>& prev_cart_prod) mutable {
                            return [this, &prev_cart_prod] (const R_key_value<int,
                            _Seq<R_i<int>>>& b3) mutable {
                              {
                                auto i = b3.key;
                                auto domain = b3.value;
                                _Seq<R_i<_Seq<R_key_value<int, int>>>> __119;
                                auto __130 = _Seq<R_i<_Seq<R_key_value<int, int>>>> {};
                                for (const auto& __129: domain) {
                                  __130 = [this, &i,
                                  &prev_cart_prod] (const _Seq<R_i<_Seq<R_key_value<int,
                                  int>>>>& _accext) mutable {
                                    return [this, &_accext, &i,
                                    &prev_cart_prod] (const R_i<int>& b3) mutable {
                                      {
                                        auto domain_element = b3.i;
                                        _Seq<R_i<_Seq<R_key_value<int, int>>>> __120;
                                        _Seq<R_i<_Seq<R_key_value<int, int>>>> __121;
                                        if (0 == prev_cart_prod.size(unit_t {})) {
                                          {
                                            _Seq<R_i<_Seq<R_key_value<int, int>>>> __122;
                                            __122 = _Seq<R_i<_Seq<R_key_value<int, int>>>> {};
                                            auto& __collection = __122;
                                            _Seq<R_key_value<int, int>> __123;
                                            {
                                              _Seq<R_key_value<int, int>> __124;
                                              __124 = _Seq<R_key_value<int, int>> {};
                                              auto& __collection = __124;
                                              __collection.insert(R_key_value<int, int> {i,
                                              domain_element});
                                              __123 = __collection;
                                            }
                                            __collection.insert(R_i<_Seq<R_key_value<int,
                                            int>>> {__123});
                                            __121 = __collection;
                                          }
                                        } else {
                                          auto __128 = _Seq<R_i<_Seq<R_key_value<int, int>>>> {};
                                          for (const auto& __127: prev_cart_prod) {
                                            __128 = [this, &domain_element,
                                            &i] (_Seq<R_i<_Seq<R_key_value<int,
                                            int>>>> _accmap) mutable {
                                              return [this, _accmap = std::move(_accmap),
                                              &domain_element, &i] (const R_i<_Seq<R_key_value<int,
                                              int>>>& b3) mutable {
                                                {
                                                  auto& rest_tup = b3.i;
                                                  _Seq<R_key_value<int, int>> __125;
                                                  {
                                                    _Seq<R_key_value<int, int>> __126;
                                                    __126 = _Seq<R_key_value<int, int>> {};
                                                    auto& __collection = __126;
                                                    __collection.insert(R_key_value<int, int> {i,
                                                    domain_element});
                                                    __125 = __collection;
                                                  }
                                                  _accmap.insert(R_i<_Seq<R_key_value<int,
                                                  int>>> {rest_tup.combine(__125)});
                                                  return std::move(_accmap);
                                                }
                                              };
                                            }(std::move(__128))(__127);
                                          }
                                          __121 = std::move(__128);
                                        }
                                        __120 = _accext.combine(__121);
                                        domain_element = b3.i;
                                        return __120;
                                      }
                                    };
                                  }(std::move(__130))(__129);
                                }
                                __119 = std::move(__130);
                                i = b3.key;
                                domain = b3.value;
                                return __119;
                              }
                            };
                          }(std::move(__132))(__131);
                        }
                        __118 = std::move(__132);
                        auto& free_cart_prod = __118;
                        {
                          _Collection<R_key_value<Address, unit_t>> __133;
                          auto __142 = _Collection<R_i<Address>> {};
                          for (const auto& __141: free_cart_prod) {
                            __142 = [this, &bound_bucket, &dim_bounds,
                            &max_val] (const _Collection<R_i<Address>>& acc_ips) mutable {
                              return [this, &acc_ips, &bound_bucket, &dim_bounds,
                              &max_val] (const R_i<_Seq<R_key_value<int, int>>>& b3) mutable {
                                {
                                  auto free_bucket = b3.i;
                                  _Collection<R_i<Address>> __134;
                                  _Collection<R_i<Address>> __135;
                                  {
                                    _Collection<R_i<Address>> __136;
                                    __136 = _Collection<R_i<Address>> {};
                                    auto& __collection = __136;
                                    auto __140 = bound_bucket;
                                    for (const auto& __139: free_bucket) {
                                      __140 = [this, &dim_bounds] (const int& acc) mutable {
                                        return [this, &acc, &dim_bounds] (const R_key_value<int,
                                        int>& b3) mutable {
                                          {
                                            auto& i = b3.key;
                                            auto& val = b3.value;
                                            R_key_value<int, int> __137;
                                            shared_ptr<R_key_value<int, int>> __138;
                                            __138 = dim_bounds.filter([this,
                                            &i] (const R_key_value<int, int>& b1) mutable {
                                              {
                                                auto& key = b1.key;
                                                auto& value = b1.value;
                                                return key == i;
                                              }
                                            }).peek(unit_t {});
                                            if (__138) {
                                              R_key_value<int, int>& x = *__138;
                                              __137 = x;
                                            } else {
                                              __137 = error<R_key_value<int,
                                              int>>(print("can't find i in dim_bounds"));
                                            }
                                            return acc + val * __137.value;
                                          }
                                        };
                                      }(std::move(__140))(__139);
                                    }
                                    __collection.insert(R_i<Address> {get_ring_node(R_key_value<int,
                                    int> {std::move(__140), max_val})});
                                    __135 = __collection;
                                  }
                                  __134 = acc_ips.combine(__135);
                                  free_bucket = b3.i;
                                  return __134;
                                }
                              };
                            }(std::move(__142))(__141);
                          }
                          __133 = std::move(__142).groupBy([this] (const R_i<Address>& b1) mutable {
                            {
                              auto& ip = b1.i;
                              return std::move(ip);
                            }
                          }, [this] (const unit_t& _) mutable {
                            return [this] (const R_i<Address>& _) mutable {
                              return unit_t {};
                            };
                          }, unit_t {});
                          auto& sorted_ip_list = __133;
                          if (0 == sorted_ip_list.size(unit_t {})) {
                            {
                              _Collection<R_i<Address>> __143;
                              __143 = _Collection<R_i<Address>> {};
                              auto& __collection = __143;
                              __collection.insert(R_i<Address> {get_ring_node(R_key_value<int,
                              int> {bound_bucket, max_val})});
                              __100 = __collection;
                            }
                          } else {
                            auto __145 = _Collection<R_i<Address>> {};
                            for (const auto& __144: sorted_ip_list) {
                              __145 = [this] (_Collection<R_i<Address>> _accmap) mutable {
                                return [this,
                                _accmap = std::move(_accmap)] (const R_key_value<Address,
                                unit_t>& x) mutable {
                                  _accmap.insert(R_i<Address> {x.key});
                                  return std::move(_accmap);
                                };
                              }(std::move(__145))(__144);
                            }
                            __100 = std::move(__145);
                          }
                        }
                      }
                    }
                  }
                }
              }
            } else {
              __100 = nodes;
            }
          }
          map_id = b1.key;
          key_0 = b1.value;
          return __100;
        }
      }
      _Collection<R_key_value<Address, _Set<R_r1_r2_r3<R_key_value<int, int>, int,
      int>>>> shuffle___SQL_SUM_AGGREGATE_1_mR1_to___SQL_SUM_AGGREGATE_2(const R_r1_r2_r3<unit_t,
      _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>, bool>& b1)  {
        {
          auto tuples = b1.r2;
          auto shuffle_on_empty = b1.r3;
          _Collection<R_key_value<Address, _Set<R_r1_r2_r3<R_key_value<int, int>, int,
          int>>>> __146;
          {
            _Collection<R_key_value<Address, _Set<R_r1_r2_r3<R_key_value<int, int>, int,
            int>>>> __147;
            if (shuffle_on_empty == true) {
              auto __149 = _Collection<R_key_value<Address, _Set<R_r1_r2_r3<R_key_value<int, int>,
              int, int>>>> {};
              for (const auto& __148: route_to_(R_key_value<int, unit_t> {4, unit_t {}})) {
                __149 = [this] (_Collection<R_key_value<Address, _Set<R_r1_r2_r3<R_key_value<int,
                int>, int, int>>>> _accmap) mutable {
                  return [this, _accmap = std::move(_accmap)] (const R_i<Address>& b3) mutable {
                    {
                      auto& ip = b3.i;
                      _accmap.insert(R_key_value<Address, _Set<R_r1_r2_r3<R_key_value<int, int>,
                      int, int>>> {ip, _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> {}});
                      return std::move(_accmap);
                    }
                  };
                }(std::move(__149))(__148);
              }
              __147 = std::move(__149);
            } else {
              __147 = _Collection<R_key_value<Address, _Set<R_r1_r2_r3<R_key_value<int, int>, int,
              int>>>> {};
            }
            auto& all_targets = __147;
            auto __156 = _Collection<R_key_value<Address, _Set<R_r1_r2_r3<R_key_value<int, int>,
            int, int>>>> {};
            for (const auto& __155: tuples) {
              __156 = [this] (const _Collection<R_key_value<Address,
              _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>>>& _accext) mutable {
                return [this, &_accext] (const R_r1_r2_r3<R_key_value<int, int>, int,
                int>& r_tuple) mutable {
                  _Collection<R_key_value<Address, _Set<R_r1_r2_r3<R_key_value<int, int>, int,
                  int>>>> __150;
                  {
                    auto rkey_0 = r_tuple.r1;
                    auto rkey_1 = r_tuple.r2;
                    auto rkey_2 = r_tuple.r3;
                    auto __154 = _Collection<R_key_value<Address, _Set<R_r1_r2_r3<R_key_value<int,
                    int>, int, int>>>> {};
                    for (const auto& __153: route_to_(R_key_value<int, unit_t> {4, unit_t {}})) {
                      __154 = [this, &r_tuple] (_Collection<R_key_value<Address,
                      _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>>> _accmap) mutable {
                        return [this, _accmap = std::move(_accmap),
                        &r_tuple] (const R_i<Address>& b3) mutable {
                          {
                            auto& ip = b3.i;
                            _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> __151;
                            {
                              _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> __152;
                              __152 = _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> {};
                              auto& __collection = __152;
                              __collection.insert(r_tuple);
                              __151 = __collection;
                            }
                            _accmap.insert(R_key_value<Address, _Set<R_r1_r2_r3<R_key_value<int,
                            int>, int, int>>> {ip, __151});
                            return std::move(_accmap);
                          }
                        };
                      }(std::move(__154))(__153);
                    }
                    __150 = std::move(__154);
                    rkey_0 = r_tuple.r1;
                    rkey_1 = r_tuple.r2;
                    rkey_2 = r_tuple.r3;
                  }
                  return _accext.combine(__150);
                };
              }(std::move(__156))(__155);
            }
            __146 = all_targets.combine(std::move(__156)).groupBy([this] (const R_key_value<Address,
            _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>>& b1) mutable {
              {
                auto& ip = b1.key;
                auto& tuple = b1.value;
                return std::move(ip);
              }
            }, [this] (const _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>& acc) mutable {
              return [this, &acc] (const R_key_value<Address, _Set<R_r1_r2_r3<R_key_value<int, int>,
              int, int>>>& b3) mutable {
                {
                  auto& ip = b3.key;
                  auto& tuple = b3.value;
                  return tuple.combine(acc);
                }
              };
            }, _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> {});
          }
          tuples = b1.r2;
          shuffle_on_empty = b1.r3;
          return __146;
        }
      }
      _Collection<R_key_value<Address, _Set<R_r1_r2_r3<R_key_value<int, int>, int,
      int>>>> shuffle___SQL_SUM_AGGREGATE_2_mR1_to___SQL_SUM_AGGREGATE_2(const R_r1_r2_r3<unit_t,
      _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>, bool>& b1)  {
        {
          auto tuples = b1.r2;
          auto shuffle_on_empty = b1.r3;
          _Collection<R_key_value<Address, _Set<R_r1_r2_r3<R_key_value<int, int>, int,
          int>>>> __157;
          {
            _Collection<R_key_value<Address, _Set<R_r1_r2_r3<R_key_value<int, int>, int,
            int>>>> __158;
            if (shuffle_on_empty == true) {
              auto __160 = _Collection<R_key_value<Address, _Set<R_r1_r2_r3<R_key_value<int, int>,
              int, int>>>> {};
              for (const auto& __159: route_to_(R_key_value<int, unit_t> {4, unit_t {}})) {
                __160 = [this] (_Collection<R_key_value<Address, _Set<R_r1_r2_r3<R_key_value<int,
                int>, int, int>>>> _accmap) mutable {
                  return [this, _accmap = std::move(_accmap)] (const R_i<Address>& b3) mutable {
                    {
                      auto& ip = b3.i;
                      _accmap.insert(R_key_value<Address, _Set<R_r1_r2_r3<R_key_value<int, int>,
                      int, int>>> {ip, _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> {}});
                      return std::move(_accmap);
                    }
                  };
                }(std::move(__160))(__159);
              }
              __158 = std::move(__160);
            } else {
              __158 = _Collection<R_key_value<Address, _Set<R_r1_r2_r3<R_key_value<int, int>, int,
              int>>>> {};
            }
            auto& all_targets = __158;
            auto __167 = _Collection<R_key_value<Address, _Set<R_r1_r2_r3<R_key_value<int, int>,
            int, int>>>> {};
            for (const auto& __166: tuples) {
              __167 = [this] (const _Collection<R_key_value<Address,
              _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>>>& _accext) mutable {
                return [this, &_accext] (const R_r1_r2_r3<R_key_value<int, int>, int,
                int>& r_tuple) mutable {
                  _Collection<R_key_value<Address, _Set<R_r1_r2_r3<R_key_value<int, int>, int,
                  int>>>> __161;
                  {
                    auto rkey_0 = r_tuple.r1;
                    auto rkey_1 = r_tuple.r2;
                    auto rkey_2 = r_tuple.r3;
                    auto __165 = _Collection<R_key_value<Address, _Set<R_r1_r2_r3<R_key_value<int,
                    int>, int, int>>>> {};
                    for (const auto& __164: route_to_(R_key_value<int, unit_t> {4, unit_t {}})) {
                      __165 = [this, &r_tuple] (_Collection<R_key_value<Address,
                      _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>>> _accmap) mutable {
                        return [this, _accmap = std::move(_accmap),
                        &r_tuple] (const R_i<Address>& b3) mutable {
                          {
                            auto& ip = b3.i;
                            _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> __162;
                            {
                              _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> __163;
                              __163 = _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> {};
                              auto& __collection = __163;
                              __collection.insert(r_tuple);
                              __162 = __collection;
                            }
                            _accmap.insert(R_key_value<Address, _Set<R_r1_r2_r3<R_key_value<int,
                            int>, int, int>>> {ip, __162});
                            return std::move(_accmap);
                          }
                        };
                      }(std::move(__165))(__164);
                    }
                    __161 = std::move(__165);
                    rkey_0 = r_tuple.r1;
                    rkey_1 = r_tuple.r2;
                    rkey_2 = r_tuple.r3;
                  }
                  return _accext.combine(__161);
                };
              }(std::move(__167))(__166);
            }
            __157 = all_targets.combine(std::move(__167)).groupBy([this] (const R_key_value<Address,
            _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>>& b1) mutable {
              {
                auto& ip = b1.key;
                auto& tuple = b1.value;
                return std::move(ip);
              }
            }, [this] (const _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>& acc) mutable {
              return [this, &acc] (const R_key_value<Address, _Set<R_r1_r2_r3<R_key_value<int, int>,
              int, int>>>& b3) mutable {
                {
                  auto& ip = b3.key;
                  auto& tuple = b3.value;
                  return tuple.combine(acc);
                }
              };
            }, _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> {});
          }
          tuples = b1.r2;
          shuffle_on_empty = b1.r3;
          return __157;
        }
      }
      _Collection<R_key_value<Address, _Set<R_r1_r2_r3<R_key_value<int, int>, int,
      int>>>> shuffle___SQL_SUM_AGGREGATE_1_mR1_to___SQL_SUM_AGGREGATE_1(const R_r1_r2_r3<unit_t,
      _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>, bool>& b1)  {
        {
          auto tuples = b1.r2;
          auto shuffle_on_empty = b1.r3;
          _Collection<R_key_value<Address, _Set<R_r1_r2_r3<R_key_value<int, int>, int,
          int>>>> __168;
          {
            _Collection<R_key_value<Address, _Set<R_r1_r2_r3<R_key_value<int, int>, int,
            int>>>> __169;
            if (shuffle_on_empty == true) {
              auto __171 = _Collection<R_key_value<Address, _Set<R_r1_r2_r3<R_key_value<int, int>,
              int, int>>>> {};
              for (const auto& __170: route_to_(R_key_value<int, unit_t> {1, unit_t {}})) {
                __171 = [this] (_Collection<R_key_value<Address, _Set<R_r1_r2_r3<R_key_value<int,
                int>, int, int>>>> _accmap) mutable {
                  return [this, _accmap = std::move(_accmap)] (const R_i<Address>& b3) mutable {
                    {
                      auto& ip = b3.i;
                      _accmap.insert(R_key_value<Address, _Set<R_r1_r2_r3<R_key_value<int, int>,
                      int, int>>> {ip, _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> {}});
                      return std::move(_accmap);
                    }
                  };
                }(std::move(__171))(__170);
              }
              __169 = std::move(__171);
            } else {
              __169 = _Collection<R_key_value<Address, _Set<R_r1_r2_r3<R_key_value<int, int>, int,
              int>>>> {};
            }
            auto& all_targets = __169;
            auto __178 = _Collection<R_key_value<Address, _Set<R_r1_r2_r3<R_key_value<int, int>,
            int, int>>>> {};
            for (const auto& __177: tuples) {
              __178 = [this] (const _Collection<R_key_value<Address,
              _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>>>& _accext) mutable {
                return [this, &_accext] (const R_r1_r2_r3<R_key_value<int, int>, int,
                int>& r_tuple) mutable {
                  _Collection<R_key_value<Address, _Set<R_r1_r2_r3<R_key_value<int, int>, int,
                  int>>>> __172;
                  {
                    auto rkey_0 = r_tuple.r1;
                    auto rkey_1 = r_tuple.r2;
                    auto rkey_2 = r_tuple.r3;
                    auto __176 = _Collection<R_key_value<Address, _Set<R_r1_r2_r3<R_key_value<int,
                    int>, int, int>>>> {};
                    for (const auto& __175: route_to_(R_key_value<int, unit_t> {1, unit_t {}})) {
                      __176 = [this, &r_tuple] (_Collection<R_key_value<Address,
                      _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>>> _accmap) mutable {
                        return [this, _accmap = std::move(_accmap),
                        &r_tuple] (const R_i<Address>& b3) mutable {
                          {
                            auto& ip = b3.i;
                            _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> __173;
                            {
                              _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> __174;
                              __174 = _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> {};
                              auto& __collection = __174;
                              __collection.insert(r_tuple);
                              __173 = __collection;
                            }
                            _accmap.insert(R_key_value<Address, _Set<R_r1_r2_r3<R_key_value<int,
                            int>, int, int>>> {ip, __173});
                            return std::move(_accmap);
                          }
                        };
                      }(std::move(__176))(__175);
                    }
                    __172 = std::move(__176);
                    rkey_0 = r_tuple.r1;
                    rkey_1 = r_tuple.r2;
                    rkey_2 = r_tuple.r3;
                  }
                  return _accext.combine(__172);
                };
              }(std::move(__178))(__177);
            }
            __168 = all_targets.combine(std::move(__178)).groupBy([this] (const R_key_value<Address,
            _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>>& b1) mutable {
              {
                auto& ip = b1.key;
                auto& tuple = b1.value;
                return std::move(ip);
              }
            }, [this] (const _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>& acc) mutable {
              return [this, &acc] (const R_key_value<Address, _Set<R_r1_r2_r3<R_key_value<int, int>,
              int, int>>>& b3) mutable {
                {
                  auto& ip = b3.key;
                  auto& tuple = b3.value;
                  return tuple.combine(acc);
                }
              };
            }, _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> {});
          }
          tuples = b1.r2;
          shuffle_on_empty = b1.r3;
          return __168;
        }
      }
      _Collection<R_key_value<Address, _Set<R_r1_r2_r3<R_key_value<int, int>, int,
      int>>>> shuffle___SQL_SUM_AGGREGATE_2_mS3_to___SQL_SUM_AGGREGATE_2(const R_r1_r2_r3<unit_t,
      _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>, bool>& b1)  {
        {
          auto tuples = b1.r2;
          auto shuffle_on_empty = b1.r3;
          _Collection<R_key_value<Address, _Set<R_r1_r2_r3<R_key_value<int, int>, int,
          int>>>> __179;
          {
            _Collection<R_key_value<Address, _Set<R_r1_r2_r3<R_key_value<int, int>, int,
            int>>>> __180;
            if (shuffle_on_empty == true) {
              auto __182 = _Collection<R_key_value<Address, _Set<R_r1_r2_r3<R_key_value<int, int>,
              int, int>>>> {};
              for (const auto& __181: route_to_(R_key_value<int, unit_t> {4, unit_t {}})) {
                __182 = [this] (_Collection<R_key_value<Address, _Set<R_r1_r2_r3<R_key_value<int,
                int>, int, int>>>> _accmap) mutable {
                  return [this, _accmap = std::move(_accmap)] (const R_i<Address>& b3) mutable {
                    {
                      auto& ip = b3.i;
                      _accmap.insert(R_key_value<Address, _Set<R_r1_r2_r3<R_key_value<int, int>,
                      int, int>>> {ip, _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> {}});
                      return std::move(_accmap);
                    }
                  };
                }(std::move(__182))(__181);
              }
              __180 = std::move(__182);
            } else {
              __180 = _Collection<R_key_value<Address, _Set<R_r1_r2_r3<R_key_value<int, int>, int,
              int>>>> {};
            }
            auto& all_targets = __180;
            auto __189 = _Collection<R_key_value<Address, _Set<R_r1_r2_r3<R_key_value<int, int>,
            int, int>>>> {};
            for (const auto& __188: tuples) {
              __189 = [this] (const _Collection<R_key_value<Address,
              _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>>>& _accext) mutable {
                return [this, &_accext] (const R_r1_r2_r3<R_key_value<int, int>, int,
                int>& r_tuple) mutable {
                  _Collection<R_key_value<Address, _Set<R_r1_r2_r3<R_key_value<int, int>, int,
                  int>>>> __183;
                  {
                    auto rkey_0 = r_tuple.r1;
                    auto rkey_1 = r_tuple.r2;
                    auto rkey_2 = r_tuple.r3;
                    auto __187 = _Collection<R_key_value<Address, _Set<R_r1_r2_r3<R_key_value<int,
                    int>, int, int>>>> {};
                    for (const auto& __186: route_to_(R_key_value<int, unit_t> {4, unit_t {}})) {
                      __187 = [this, &r_tuple] (_Collection<R_key_value<Address,
                      _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>>> _accmap) mutable {
                        return [this, _accmap = std::move(_accmap),
                        &r_tuple] (const R_i<Address>& b3) mutable {
                          {
                            auto& ip = b3.i;
                            _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> __184;
                            {
                              _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> __185;
                              __185 = _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> {};
                              auto& __collection = __185;
                              __collection.insert(r_tuple);
                              __184 = __collection;
                            }
                            _accmap.insert(R_key_value<Address, _Set<R_r1_r2_r3<R_key_value<int,
                            int>, int, int>>> {ip, __184});
                            return std::move(_accmap);
                          }
                        };
                      }(std::move(__187))(__186);
                    }
                    __183 = std::move(__187);
                    rkey_0 = r_tuple.r1;
                    rkey_1 = r_tuple.r2;
                    rkey_2 = r_tuple.r3;
                  }
                  return _accext.combine(__183);
                };
              }(std::move(__189))(__188);
            }
            __179 = all_targets.combine(std::move(__189)).groupBy([this] (const R_key_value<Address,
            _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>>& b1) mutable {
              {
                auto& ip = b1.key;
                auto& tuple = b1.value;
                return std::move(ip);
              }
            }, [this] (const _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>& acc) mutable {
              return [this, &acc] (const R_key_value<Address, _Set<R_r1_r2_r3<R_key_value<int, int>,
              int, int>>>& b3) mutable {
                {
                  auto& ip = b3.key;
                  auto& tuple = b3.value;
                  return tuple.combine(acc);
                }
              };
            }, _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> {});
          }
          tuples = b1.r2;
          shuffle_on_empty = b1.r3;
          return __179;
        }
      }
      _Collection<R_key_value<Address, _Set<R_r1_r2_r3<R_key_value<int, int>, int,
      int>>>> shuffle___SQL_SUM_AGGREGATE_1_mS1_to___SQL_SUM_AGGREGATE_2(const R_r1_r2_r3<unit_t,
      _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>, bool>& b1)  {
        {
          auto tuples = b1.r2;
          auto shuffle_on_empty = b1.r3;
          _Collection<R_key_value<Address, _Set<R_r1_r2_r3<R_key_value<int, int>, int,
          int>>>> __190;
          {
            _Collection<R_key_value<Address, _Set<R_r1_r2_r3<R_key_value<int, int>, int,
            int>>>> __191;
            if (shuffle_on_empty == true) {
              auto __193 = _Collection<R_key_value<Address, _Set<R_r1_r2_r3<R_key_value<int, int>,
              int, int>>>> {};
              for (const auto& __192: route_to_(R_key_value<int, unit_t> {4, unit_t {}})) {
                __193 = [this] (_Collection<R_key_value<Address, _Set<R_r1_r2_r3<R_key_value<int,
                int>, int, int>>>> _accmap) mutable {
                  return [this, _accmap = std::move(_accmap)] (const R_i<Address>& b3) mutable {
                    {
                      auto& ip = b3.i;
                      _accmap.insert(R_key_value<Address, _Set<R_r1_r2_r3<R_key_value<int, int>,
                      int, int>>> {ip, _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> {}});
                      return std::move(_accmap);
                    }
                  };
                }(std::move(__193))(__192);
              }
              __191 = std::move(__193);
            } else {
              __191 = _Collection<R_key_value<Address, _Set<R_r1_r2_r3<R_key_value<int, int>, int,
              int>>>> {};
            }
            auto& all_targets = __191;
            auto __200 = _Collection<R_key_value<Address, _Set<R_r1_r2_r3<R_key_value<int, int>,
            int, int>>>> {};
            for (const auto& __199: tuples) {
              __200 = [this] (const _Collection<R_key_value<Address,
              _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>>>& _accext) mutable {
                return [this, &_accext] (const R_r1_r2_r3<R_key_value<int, int>, int,
                int>& r_tuple) mutable {
                  _Collection<R_key_value<Address, _Set<R_r1_r2_r3<R_key_value<int, int>, int,
                  int>>>> __194;
                  {
                    auto rkey_0 = r_tuple.r1;
                    auto rkey_1 = r_tuple.r2;
                    auto rkey_2 = r_tuple.r3;
                    auto __198 = _Collection<R_key_value<Address, _Set<R_r1_r2_r3<R_key_value<int,
                    int>, int, int>>>> {};
                    for (const auto& __197: route_to_(R_key_value<int, unit_t> {4, unit_t {}})) {
                      __198 = [this, &r_tuple] (_Collection<R_key_value<Address,
                      _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>>> _accmap) mutable {
                        return [this, _accmap = std::move(_accmap),
                        &r_tuple] (const R_i<Address>& b3) mutable {
                          {
                            auto& ip = b3.i;
                            _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> __195;
                            {
                              _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> __196;
                              __196 = _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> {};
                              auto& __collection = __196;
                              __collection.insert(r_tuple);
                              __195 = __collection;
                            }
                            _accmap.insert(R_key_value<Address, _Set<R_r1_r2_r3<R_key_value<int,
                            int>, int, int>>> {ip, __195});
                            return std::move(_accmap);
                          }
                        };
                      }(std::move(__198))(__197);
                    }
                    __194 = std::move(__198);
                    rkey_0 = r_tuple.r1;
                    rkey_1 = r_tuple.r2;
                    rkey_2 = r_tuple.r3;
                  }
                  return _accext.combine(__194);
                };
              }(std::move(__200))(__199);
            }
            __190 = all_targets.combine(std::move(__200)).groupBy([this] (const R_key_value<Address,
            _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>>& b1) mutable {
              {
                auto& ip = b1.key;
                auto& tuple = b1.value;
                return std::move(ip);
              }
            }, [this] (const _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>& acc) mutable {
              return [this, &acc] (const R_key_value<Address, _Set<R_r1_r2_r3<R_key_value<int, int>,
              int, int>>>& b3) mutable {
                {
                  auto& ip = b3.key;
                  auto& tuple = b3.value;
                  return tuple.combine(acc);
                }
              };
            }, _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> {});
          }
          tuples = b1.r2;
          shuffle_on_empty = b1.r3;
          return __190;
        }
      }
      _Collection<R_key_value<Address, _Set<R_r1_r2_r3<R_key_value<int, int>, int,
      int>>>> shuffle___SQL_SUM_AGGREGATE_1_mS1_to___SQL_SUM_AGGREGATE_1(const R_r1_r2_r3<unit_t,
      _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>, bool>& b1)  {
        {
          auto tuples = b1.r2;
          auto shuffle_on_empty = b1.r3;
          _Collection<R_key_value<Address, _Set<R_r1_r2_r3<R_key_value<int, int>, int,
          int>>>> __201;
          {
            _Collection<R_key_value<Address, _Set<R_r1_r2_r3<R_key_value<int, int>, int,
            int>>>> __202;
            if (shuffle_on_empty == true) {
              auto __204 = _Collection<R_key_value<Address, _Set<R_r1_r2_r3<R_key_value<int, int>,
              int, int>>>> {};
              for (const auto& __203: route_to_(R_key_value<int, unit_t> {1, unit_t {}})) {
                __204 = [this] (_Collection<R_key_value<Address, _Set<R_r1_r2_r3<R_key_value<int,
                int>, int, int>>>> _accmap) mutable {
                  return [this, _accmap = std::move(_accmap)] (const R_i<Address>& b3) mutable {
                    {
                      auto& ip = b3.i;
                      _accmap.insert(R_key_value<Address, _Set<R_r1_r2_r3<R_key_value<int, int>,
                      int, int>>> {ip, _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> {}});
                      return std::move(_accmap);
                    }
                  };
                }(std::move(__204))(__203);
              }
              __202 = std::move(__204);
            } else {
              __202 = _Collection<R_key_value<Address, _Set<R_r1_r2_r3<R_key_value<int, int>, int,
              int>>>> {};
            }
            auto& all_targets = __202;
            auto __211 = _Collection<R_key_value<Address, _Set<R_r1_r2_r3<R_key_value<int, int>,
            int, int>>>> {};
            for (const auto& __210: tuples) {
              __211 = [this] (const _Collection<R_key_value<Address,
              _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>>>& _accext) mutable {
                return [this, &_accext] (const R_r1_r2_r3<R_key_value<int, int>, int,
                int>& r_tuple) mutable {
                  _Collection<R_key_value<Address, _Set<R_r1_r2_r3<R_key_value<int, int>, int,
                  int>>>> __205;
                  {
                    auto rkey_0 = r_tuple.r1;
                    auto rkey_1 = r_tuple.r2;
                    auto rkey_2 = r_tuple.r3;
                    auto __209 = _Collection<R_key_value<Address, _Set<R_r1_r2_r3<R_key_value<int,
                    int>, int, int>>>> {};
                    for (const auto& __208: route_to_(R_key_value<int, unit_t> {1, unit_t {}})) {
                      __209 = [this, &r_tuple] (_Collection<R_key_value<Address,
                      _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>>> _accmap) mutable {
                        return [this, _accmap = std::move(_accmap),
                        &r_tuple] (const R_i<Address>& b3) mutable {
                          {
                            auto& ip = b3.i;
                            _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> __206;
                            {
                              _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> __207;
                              __207 = _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> {};
                              auto& __collection = __207;
                              __collection.insert(r_tuple);
                              __206 = __collection;
                            }
                            _accmap.insert(R_key_value<Address, _Set<R_r1_r2_r3<R_key_value<int,
                            int>, int, int>>> {ip, __206});
                            return std::move(_accmap);
                          }
                        };
                      }(std::move(__209))(__208);
                    }
                    __205 = std::move(__209);
                    rkey_0 = r_tuple.r1;
                    rkey_1 = r_tuple.r2;
                    rkey_2 = r_tuple.r3;
                  }
                  return _accext.combine(__205);
                };
              }(std::move(__211))(__210);
            }
            __201 = all_targets.combine(std::move(__211)).groupBy([this] (const R_key_value<Address,
            _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>>& b1) mutable {
              {
                auto& ip = b1.key;
                auto& tuple = b1.value;
                return std::move(ip);
              }
            }, [this] (const _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>& acc) mutable {
              return [this, &acc] (const R_key_value<Address, _Set<R_r1_r2_r3<R_key_value<int, int>,
              int, int>>>& b3) mutable {
                {
                  auto& ip = b3.key;
                  auto& tuple = b3.value;
                  return tuple.combine(acc);
                }
              };
            }, _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> {});
          }
          tuples = b1.r2;
          shuffle_on_empty = b1.r3;
          return __201;
        }
      }
      int nd___SQL_SUM_AGGREGATE_2_mS3_send_correctives(const R_r1_r2_r3_r4_r5_r6<Address, int,
      R_key_value<int, int>, int, R_key_value<int, int>, _Set<R_key_value<int, int>>>& b1)  {
        {
          auto orig_addr = b1.r1;
          auto orig_stmt_id = b1.r2;
          auto orig_vid = b1.r3;
          auto hop = b1.r4;
          auto corrective_vid = b1.r5;
          auto delta_tuples = b1.r6;
          int __212;
          {
            _Collection<R_key_value<int, _Seq<R_key_value<int, int>>>> __213;
            _Collection<R_key_value<int, int>> __214;
            {
              _Collection<R_key_value<int, int>> __215;
              __215 = _Collection<R_key_value<int, int>> {};
              auto& __collection = __215;
              __collection.insert(R_key_value<int, int> {0, 2});
              __collection.insert(R_key_value<int, int> {1, 6});
              __214 = __collection;
            }
            __213 = nd_filter_corrective_list(R_key_value<R_key_value<int, int>,
            _Collection<R_key_value<int, int>>> {corrective_vid, __214});
            auto& corrective_list = __213;
            if (0 == corrective_list.size(unit_t {})) {
              __212 = 0;
            } else {
              {
                _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> __216;
                auto __218 = _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> {};
                for (const auto& __217: delta_tuples) {
                  __218 = [this] (_Set<R_r1_r2_r3<R_key_value<int, int>, int,
                  int>> _accmap) mutable {
                    return [this, _accmap = std::move(_accmap)] (const R_key_value<int,
                    int>& b3) mutable {
                      {
                        auto& map_0 = b3.key;
                        auto& map_val = b3.value;
                        _accmap.insert(R_r1_r2_r3<R_key_value<int, int>, int, int> {g_min_vid,
                        map_0, map_val});
                        return std::move(_accmap);
                      }
                    };
                  }(std::move(__218))(__217);
                }
                __216 = std::move(__218);
                auto& delta_tuples2 = __216;
                auto __251 = 0;
                for (const auto& __250: corrective_list) {
                  __251 = [this, &corrective_vid, &delta_tuples2, &hop, &orig_addr, &orig_stmt_id,
                  &orig_vid] (const int& acc_count) mutable {
                    return [this, &acc_count, &corrective_vid, &delta_tuples2, &hop, &orig_addr,
                    &orig_stmt_id, &orig_vid] (const R_key_value<int, _Seq<R_key_value<int,
                    int>>>& b3) mutable {
                      {
                        auto stmt_id = b3.key;
                        auto vid_list = b3.value;
                        int __219;
                        if (stmt_id == 6) {
                          {
                            _Collection<R_key_value<Address, R_key_value<_Seq<R_key_value<int,
                            int>>, _Set<R_key_value<int, int>>>>> __220;
                            auto __229 = _Collection<R_r1_r2_r3<Address, R_key_value<int, int>,
                            _Set<R_key_value<int, int>>>> {};
                            for (const auto& __228: vid_list) {
                              __229 = [this, &delta_tuples2] (const _Collection<R_r1_r2_r3<Address,
                              R_key_value<int, int>, _Set<R_key_value<int,
                              int>>>>& _accext) mutable {
                                return [this, &_accext, &delta_tuples2] (const R_key_value<int,
                                int>& vid) mutable {
                                  _Collection<R_r1_r2_r3<Address, R_key_value<int, int>,
                                  _Set<R_key_value<int, int>>>> __221;
                                  R_key_value<int, int> __222;
                                  __222 = nd_log_get_bound_delete_S(vid);
                                  {
                                    auto& S_B = __222.key;
                                    auto& S_C = __222.value;
                                    auto __227 = _Collection<R_r1_r2_r3<Address, R_key_value<int,
                                    int>, _Set<R_key_value<int, int>>>> {};
                                    for (const auto& __226: shuffle___SQL_SUM_AGGREGATE_2_mS3_to___SQL_SUM_AGGREGATE_2(R_r1_r2_r3<unit_t,
                                    _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>,
                                    bool> {unit_t {}, delta_tuples2, false})) {
                                      __227 = [this, &vid] (_Collection<R_r1_r2_r3<Address,
                                      R_key_value<int, int>, _Set<R_key_value<int,
                                      int>>>> _accmap) mutable {
                                        return [this, _accmap = std::move(_accmap),
                                        &vid] (const R_key_value<Address,
                                        _Set<R_r1_r2_r3<R_key_value<int, int>, int,
                                        int>>>& b3) mutable {
                                          {
                                            auto ip = b3.key;
                                            auto tuples = b3.value;
                                            _Collection<R_r1_r2_r3<Address, R_key_value<int, int>,
                                            _Set<R_key_value<int, int>>>> __223;
                                            auto __225 = _Set<R_key_value<int, int>> {};
                                            for (const auto& __224: tuples) {
                                              __225 = [this] (_Set<R_key_value<int,
                                              int>> _accmap) mutable {
                                                return [this,
                                                _accmap = std::move(_accmap)] (const R_r1_r2_r3<R_key_value<int,
                                                int>, int, int>& b3) mutable {
                                                  {
                                                    auto& vid = b3.r1;
                                                    auto& map_0 = b3.r2;
                                                    auto& map_val = b3.r3;
                                                    _accmap.insert(R_key_value<int, int> {map_0,
                                                    map_val});
                                                    return std::move(_accmap);
                                                  }
                                                };
                                              }(std::move(__225))(__224);
                                            }
                                            _accmap.insert(R_r1_r2_r3<Address, R_key_value<int,
                                            int>, _Set<R_key_value<int, int>>> {ip, vid,
                                            std::move(__225)});
                                            __223 = _accmap;
                                            ip = b3.key;
                                            tuples = b3.value;
                                            return std::move(__223);
                                          }
                                        };
                                      }(std::move(__227))(__226);
                                    }
                                    __221 = std::move(__227);
                                  }
                                  return _accext.combine(__221);
                                };
                              }(std::move(__229))(__228);
                            }
                            __220 = std::move(__229).groupBy([this] (const R_r1_r2_r3<Address,
                            R_key_value<int, int>, _Set<R_key_value<int, int>>>& b1) mutable {
                              {
                                auto& ip = b1.r1;
                                auto& vid = b1.r2;
                                auto& tuples = b1.r3;
                                return std::move(ip);
                              }
                            }, [this] (R_key_value<_Seq<R_key_value<int, int>>,
                            _Set<R_key_value<int, int>>> b2) mutable {
                              return [this, &b2] (const R_r1_r2_r3<Address, R_key_value<int, int>,
                              _Set<R_key_value<int, int>>>& b5) mutable {
                                {
                                  auto& acc_vid = b2.key;
                                  auto& acc_tuples = b2.value;
                                  {
                                    auto& ip = b5.r1;
                                    auto& vid = b5.r2;
                                    auto& tuples = b5.r3;
                                    acc_vid.insert(vid);
                                    auto __231 = _Set<R_key_value<int, int>> {};
                                    for (const auto& __230: acc_tuples.combine(tuples).groupBy([this] (const R_key_value<int,
                                    int>& tuple) mutable {
                                      return std::move(tuple);
                                    }, [this] (const unit_t& _) mutable {
                                      return [this] (const R_key_value<int, int>& _) mutable {
                                        return unit_t {};
                                      };
                                    }, unit_t {})) {
                                      __231 = [this] (_Set<R_key_value<int, int>> _accmap) mutable {
                                        return [this,
                                        _accmap = std::move(_accmap)] (const R_key_value<R_key_value<int,
                                        int>, unit_t>& x) mutable {
                                          _accmap.insert(x.key);
                                          return std::move(_accmap);
                                        };
                                      }(std::move(__231))(__230);
                                    }
                                    return R_key_value<_Seq<R_key_value<int, int>>,
                                    _Set<R_key_value<int, int>>> {acc_vid, std::move(__231)};
                                  }
                                }
                              };
                            }, R_key_value<_Seq<R_key_value<int, int>>, _Set<R_key_value<int,
                            int>>> {_Seq<R_key_value<int, int>> {}, _Set<R_key_value<int,
                            int>> {}});
                            auto& ips_vids = __220;
                            auto __234 = acc_count;
                            for (const auto& __233: ips_vids) {
                              __234 = [this, &corrective_vid, &hop, &orig_addr, &orig_stmt_id,
                              &orig_vid] (const int& acc_count) mutable {
                                return [this, &acc_count, &corrective_vid, &hop, &orig_addr,
                                &orig_stmt_id, &orig_vid] (const R_key_value<Address,
                                R_key_value<_Seq<R_key_value<int, int>>, _Set<R_key_value<int,
                                int>>>>& b3) mutable {
                                  {
                                    auto& ip = b3.key;
                                    auto& vid_send_list_tup = b3.value;
                                    auto __232 = std::make_shared<K3::ValDispatcher<R_r1_r2_r3_r4_r5_r6_r7<Address,
                                    int, R_key_value<int, int>, int, R_key_value<int, int>,
                                    _Seq<R_key_value<int, int>>, _Set<R_key_value<int,
                                    int>>>>>(R_r1_r2_r3_r4_r5_r6_r7<Address, int, R_key_value<int,
                                    int>, int, R_key_value<int, int>, _Seq<R_key_value<int, int>>,
                                    _Set<R_key_value<int, int>>> {orig_addr, orig_stmt_id, orig_vid,
                                    hop, corrective_vid, vid_send_list_tup.key,
                                    vid_send_list_tup.value});
                                    __engine.send(ip,
                                    __delete_S_rcv_corrective_s6_m___SQL_SUM_AGGREGATE_2_mS3_tid,
                                    __232, me);
                                    return acc_count + 1;
                                  }
                                };
                              }(std::move(__234))(__233);
                            }
                            __219 = std::move(__234);
                          }
                        } else {
                          if (stmt_id == 2) {
                            {
                              _Collection<R_key_value<Address, R_key_value<_Seq<R_key_value<int,
                              int>>, _Set<R_key_value<int, int>>>>> __235;
                              auto __244 = _Collection<R_r1_r2_r3<Address, R_key_value<int, int>,
                              _Set<R_key_value<int, int>>>> {};
                              for (const auto& __243: vid_list) {
                                __244 = [this,
                                &delta_tuples2] (const _Collection<R_r1_r2_r3<Address,
                                R_key_value<int, int>, _Set<R_key_value<int,
                                int>>>>& _accext) mutable {
                                  return [this, &_accext, &delta_tuples2] (const R_key_value<int,
                                  int>& vid) mutable {
                                    _Collection<R_r1_r2_r3<Address, R_key_value<int, int>,
                                    _Set<R_key_value<int, int>>>> __236;
                                    R_key_value<int, int> __237;
                                    __237 = nd_log_get_bound_insert_S(vid);
                                    {
                                      auto& S_B = __237.key;
                                      auto& S_C = __237.value;
                                      auto __242 = _Collection<R_r1_r2_r3<Address, R_key_value<int,
                                      int>, _Set<R_key_value<int, int>>>> {};
                                      for (const auto& __241: shuffle___SQL_SUM_AGGREGATE_2_mS3_to___SQL_SUM_AGGREGATE_2(R_r1_r2_r3<unit_t,
                                      _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>,
                                      bool> {unit_t {}, delta_tuples2, false})) {
                                        __242 = [this, &vid] (_Collection<R_r1_r2_r3<Address,
                                        R_key_value<int, int>, _Set<R_key_value<int,
                                        int>>>> _accmap) mutable {
                                          return [this, _accmap = std::move(_accmap),
                                          &vid] (const R_key_value<Address,
                                          _Set<R_r1_r2_r3<R_key_value<int, int>, int,
                                          int>>>& b3) mutable {
                                            {
                                              auto ip = b3.key;
                                              auto tuples = b3.value;
                                              _Collection<R_r1_r2_r3<Address, R_key_value<int, int>,
                                              _Set<R_key_value<int, int>>>> __238;
                                              auto __240 = _Set<R_key_value<int, int>> {};
                                              for (const auto& __239: tuples) {
                                                __240 = [this] (_Set<R_key_value<int,
                                                int>> _accmap) mutable {
                                                  return [this,
                                                  _accmap = std::move(_accmap)] (const R_r1_r2_r3<R_key_value<int,
                                                  int>, int, int>& b3) mutable {
                                                    {
                                                      auto& vid = b3.r1;
                                                      auto& map_0 = b3.r2;
                                                      auto& map_val = b3.r3;
                                                      _accmap.insert(R_key_value<int, int> {map_0,
                                                      map_val});
                                                      return std::move(_accmap);
                                                    }
                                                  };
                                                }(std::move(__240))(__239);
                                              }
                                              _accmap.insert(R_r1_r2_r3<Address, R_key_value<int,
                                              int>, _Set<R_key_value<int, int>>> {ip, vid,
                                              std::move(__240)});
                                              __238 = _accmap;
                                              ip = b3.key;
                                              tuples = b3.value;
                                              return std::move(__238);
                                            }
                                          };
                                        }(std::move(__242))(__241);
                                      }
                                      __236 = std::move(__242);
                                    }
                                    return _accext.combine(__236);
                                  };
                                }(std::move(__244))(__243);
                              }
                              __235 = std::move(__244).groupBy([this] (const R_r1_r2_r3<Address,
                              R_key_value<int, int>, _Set<R_key_value<int, int>>>& b1) mutable {
                                {
                                  auto& ip = b1.r1;
                                  auto& vid = b1.r2;
                                  auto& tuples = b1.r3;
                                  return std::move(ip);
                                }
                              }, [this] (R_key_value<_Seq<R_key_value<int, int>>,
                              _Set<R_key_value<int, int>>> b2) mutable {
                                return [this, &b2] (const R_r1_r2_r3<Address, R_key_value<int, int>,
                                _Set<R_key_value<int, int>>>& b5) mutable {
                                  {
                                    auto& acc_vid = b2.key;
                                    auto& acc_tuples = b2.value;
                                    {
                                      auto& ip = b5.r1;
                                      auto& vid = b5.r2;
                                      auto& tuples = b5.r3;
                                      acc_vid.insert(vid);
                                      auto __246 = _Set<R_key_value<int, int>> {};
                                      for (const auto& __245: acc_tuples.combine(tuples).groupBy([this] (const R_key_value<int,
                                      int>& tuple) mutable {
                                        return std::move(tuple);
                                      }, [this] (const unit_t& _) mutable {
                                        return [this] (const R_key_value<int, int>& _) mutable {
                                          return unit_t {};
                                        };
                                      }, unit_t {})) {
                                        __246 = [this] (_Set<R_key_value<int,
                                        int>> _accmap) mutable {
                                          return [this,
                                          _accmap = std::move(_accmap)] (const R_key_value<R_key_value<int,
                                          int>, unit_t>& x) mutable {
                                            _accmap.insert(x.key);
                                            return std::move(_accmap);
                                          };
                                        }(std::move(__246))(__245);
                                      }
                                      return R_key_value<_Seq<R_key_value<int, int>>,
                                      _Set<R_key_value<int, int>>> {acc_vid, std::move(__246)};
                                    }
                                  }
                                };
                              }, R_key_value<_Seq<R_key_value<int, int>>, _Set<R_key_value<int,
                              int>>> {_Seq<R_key_value<int, int>> {}, _Set<R_key_value<int,
                              int>> {}});
                              auto& ips_vids = __235;
                              auto __249 = acc_count;
                              for (const auto& __248: ips_vids) {
                                __249 = [this, &corrective_vid, &hop, &orig_addr, &orig_stmt_id,
                                &orig_vid] (const int& acc_count) mutable {
                                  return [this, &acc_count, &corrective_vid, &hop, &orig_addr,
                                  &orig_stmt_id, &orig_vid] (const R_key_value<Address,
                                  R_key_value<_Seq<R_key_value<int, int>>, _Set<R_key_value<int,
                                  int>>>>& b3) mutable {
                                    {
                                      auto& ip = b3.key;
                                      auto& vid_send_list_tup = b3.value;
                                      auto __247 = std::make_shared<K3::ValDispatcher<R_r1_r2_r3_r4_r5_r6_r7<Address,
                                      int, R_key_value<int, int>, int, R_key_value<int, int>,
                                      _Seq<R_key_value<int, int>>, _Set<R_key_value<int,
                                      int>>>>>(R_r1_r2_r3_r4_r5_r6_r7<Address, int, R_key_value<int,
                                      int>, int, R_key_value<int, int>, _Seq<R_key_value<int, int>>,
                                      _Set<R_key_value<int, int>>> {orig_addr, orig_stmt_id,
                                      orig_vid, hop, corrective_vid, vid_send_list_tup.key,
                                      vid_send_list_tup.value});
                                      __engine.send(ip,
                                      __insert_S_rcv_corrective_s2_m___SQL_SUM_AGGREGATE_2_mS3_tid,
                                      __247, me);
                                      return acc_count + 1;
                                    }
                                  };
                                }(std::move(__249))(__248);
                              }
                              __219 = std::move(__249);
                            }
                          } else {
                            __219 = acc_count;
                          }
                        }
                        stmt_id = b3.key;
                        vid_list = b3.value;
                        return std::move(__219);
                      }
                    };
                  }(std::move(__251))(__250);
                }
                __212 = std::move(__251);
              }
            }
          }
          orig_addr = b1.r1;
          orig_stmt_id = b1.r2;
          orig_vid = b1.r3;
          hop = b1.r4;
          corrective_vid = b1.r5;
          delta_tuples = b1.r6;
          return __212;
        }
      }
      int nd___SQL_SUM_AGGREGATE_1_mS1_send_correctives(const R_r1_r2_r3_r4_r5_r6<Address, int,
      R_key_value<int, int>, int, R_key_value<int, int>, _Set<R_key_value<int, int>>>& b1)  {
        {
          auto orig_addr = b1.r1;
          auto orig_stmt_id = b1.r2;
          auto orig_vid = b1.r3;
          auto hop = b1.r4;
          auto corrective_vid = b1.r5;
          auto delta_tuples = b1.r6;
          int __252;
          {
            _Collection<R_key_value<int, _Seq<R_key_value<int, int>>>> __253;
            _Collection<R_key_value<int, int>> __254;
            {
              _Collection<R_key_value<int, int>> __255;
              __255 = _Collection<R_key_value<int, int>> {};
              auto& __collection = __255;
              __collection.insert(R_key_value<int, int> {0, 0});
              __collection.insert(R_key_value<int, int> {0, 2});
              __collection.insert(R_key_value<int, int> {1, 4});
              __collection.insert(R_key_value<int, int> {1, 6});
              __254 = __collection;
            }
            __253 = nd_filter_corrective_list(R_key_value<R_key_value<int, int>,
            _Collection<R_key_value<int, int>>> {corrective_vid, __254});
            auto& corrective_list = __253;
            if (0 == corrective_list.size(unit_t {})) {
              __252 = 0;
            } else {
              {
                _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> __256;
                auto __258 = _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> {};
                for (const auto& __257: delta_tuples) {
                  __258 = [this] (_Set<R_r1_r2_r3<R_key_value<int, int>, int,
                  int>> _accmap) mutable {
                    return [this, _accmap = std::move(_accmap)] (const R_key_value<int,
                    int>& b3) mutable {
                      {
                        auto& map_0 = b3.key;
                        auto& map_val = b3.value;
                        _accmap.insert(R_r1_r2_r3<R_key_value<int, int>, int, int> {g_min_vid,
                        map_0, map_val});
                        return std::move(_accmap);
                      }
                    };
                  }(std::move(__258))(__257);
                }
                __256 = std::move(__258);
                auto& delta_tuples2 = __256;
                auto __321 = 0;
                for (const auto& __320: corrective_list) {
                  __321 = [this, &corrective_vid, &delta_tuples2, &hop, &orig_addr, &orig_stmt_id,
                  &orig_vid] (const int& acc_count) mutable {
                    return [this, &acc_count, &corrective_vid, &delta_tuples2, &hop, &orig_addr,
                    &orig_stmt_id, &orig_vid] (const R_key_value<int, _Seq<R_key_value<int,
                    int>>>& b3) mutable {
                      {
                        auto stmt_id = b3.key;
                        auto vid_list = b3.value;
                        int __259;
                        if (stmt_id == 6) {
                          {
                            _Collection<R_key_value<Address, R_key_value<_Seq<R_key_value<int,
                            int>>, _Set<R_key_value<int, int>>>>> __260;
                            auto __269 = _Collection<R_r1_r2_r3<Address, R_key_value<int, int>,
                            _Set<R_key_value<int, int>>>> {};
                            for (const auto& __268: vid_list) {
                              __269 = [this, &delta_tuples2] (const _Collection<R_r1_r2_r3<Address,
                              R_key_value<int, int>, _Set<R_key_value<int,
                              int>>>>& _accext) mutable {
                                return [this, &_accext, &delta_tuples2] (const R_key_value<int,
                                int>& vid) mutable {
                                  _Collection<R_r1_r2_r3<Address, R_key_value<int, int>,
                                  _Set<R_key_value<int, int>>>> __261;
                                  R_key_value<int, int> __262;
                                  __262 = nd_log_get_bound_delete_S(vid);
                                  {
                                    auto& S_B = __262.key;
                                    auto& S_C = __262.value;
                                    auto __267 = _Collection<R_r1_r2_r3<Address, R_key_value<int,
                                    int>, _Set<R_key_value<int, int>>>> {};
                                    for (const auto& __266: shuffle___SQL_SUM_AGGREGATE_1_mS1_to___SQL_SUM_AGGREGATE_2(R_r1_r2_r3<unit_t,
                                    _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>,
                                    bool> {unit_t {}, delta_tuples2, false})) {
                                      __267 = [this, &vid] (_Collection<R_r1_r2_r3<Address,
                                      R_key_value<int, int>, _Set<R_key_value<int,
                                      int>>>> _accmap) mutable {
                                        return [this, _accmap = std::move(_accmap),
                                        &vid] (const R_key_value<Address,
                                        _Set<R_r1_r2_r3<R_key_value<int, int>, int,
                                        int>>>& b3) mutable {
                                          {
                                            auto ip = b3.key;
                                            auto tuples = b3.value;
                                            _Collection<R_r1_r2_r3<Address, R_key_value<int, int>,
                                            _Set<R_key_value<int, int>>>> __263;
                                            auto __265 = _Set<R_key_value<int, int>> {};
                                            for (const auto& __264: tuples) {
                                              __265 = [this] (_Set<R_key_value<int,
                                              int>> _accmap) mutable {
                                                return [this,
                                                _accmap = std::move(_accmap)] (const R_r1_r2_r3<R_key_value<int,
                                                int>, int, int>& b3) mutable {
                                                  {
                                                    auto& vid = b3.r1;
                                                    auto& map_0 = b3.r2;
                                                    auto& map_val = b3.r3;
                                                    _accmap.insert(R_key_value<int, int> {map_0,
                                                    map_val});
                                                    return std::move(_accmap);
                                                  }
                                                };
                                              }(std::move(__265))(__264);
                                            }
                                            _accmap.insert(R_r1_r2_r3<Address, R_key_value<int,
                                            int>, _Set<R_key_value<int, int>>> {ip, vid,
                                            std::move(__265)});
                                            __263 = _accmap;
                                            ip = b3.key;
                                            tuples = b3.value;
                                            return std::move(__263);
                                          }
                                        };
                                      }(std::move(__267))(__266);
                                    }
                                    __261 = std::move(__267);
                                  }
                                  return _accext.combine(__261);
                                };
                              }(std::move(__269))(__268);
                            }
                            __260 = std::move(__269).groupBy([this] (const R_r1_r2_r3<Address,
                            R_key_value<int, int>, _Set<R_key_value<int, int>>>& b1) mutable {
                              {
                                auto& ip = b1.r1;
                                auto& vid = b1.r2;
                                auto& tuples = b1.r3;
                                return std::move(ip);
                              }
                            }, [this] (R_key_value<_Seq<R_key_value<int, int>>,
                            _Set<R_key_value<int, int>>> b2) mutable {
                              return [this, &b2] (const R_r1_r2_r3<Address, R_key_value<int, int>,
                              _Set<R_key_value<int, int>>>& b5) mutable {
                                {
                                  auto& acc_vid = b2.key;
                                  auto& acc_tuples = b2.value;
                                  {
                                    auto& ip = b5.r1;
                                    auto& vid = b5.r2;
                                    auto& tuples = b5.r3;
                                    acc_vid.insert(vid);
                                    auto __271 = _Set<R_key_value<int, int>> {};
                                    for (const auto& __270: acc_tuples.combine(tuples).groupBy([this] (const R_key_value<int,
                                    int>& tuple) mutable {
                                      return std::move(tuple);
                                    }, [this] (const unit_t& _) mutable {
                                      return [this] (const R_key_value<int, int>& _) mutable {
                                        return unit_t {};
                                      };
                                    }, unit_t {})) {
                                      __271 = [this] (_Set<R_key_value<int, int>> _accmap) mutable {
                                        return [this,
                                        _accmap = std::move(_accmap)] (const R_key_value<R_key_value<int,
                                        int>, unit_t>& x) mutable {
                                          _accmap.insert(x.key);
                                          return std::move(_accmap);
                                        };
                                      }(std::move(__271))(__270);
                                    }
                                    return R_key_value<_Seq<R_key_value<int, int>>,
                                    _Set<R_key_value<int, int>>> {acc_vid, std::move(__271)};
                                  }
                                }
                              };
                            }, R_key_value<_Seq<R_key_value<int, int>>, _Set<R_key_value<int,
                            int>>> {_Seq<R_key_value<int, int>> {}, _Set<R_key_value<int,
                            int>> {}});
                            auto& ips_vids = __260;
                            auto __274 = acc_count;
                            for (const auto& __273: ips_vids) {
                              __274 = [this, &corrective_vid, &hop, &orig_addr, &orig_stmt_id,
                              &orig_vid] (const int& acc_count) mutable {
                                return [this, &acc_count, &corrective_vid, &hop, &orig_addr,
                                &orig_stmt_id, &orig_vid] (const R_key_value<Address,
                                R_key_value<_Seq<R_key_value<int, int>>, _Set<R_key_value<int,
                                int>>>>& b3) mutable {
                                  {
                                    auto& ip = b3.key;
                                    auto& vid_send_list_tup = b3.value;
                                    auto __272 = std::make_shared<K3::ValDispatcher<R_r1_r2_r3_r4_r5_r6_r7<Address,
                                    int, R_key_value<int, int>, int, R_key_value<int, int>,
                                    _Seq<R_key_value<int, int>>, _Set<R_key_value<int,
                                    int>>>>>(R_r1_r2_r3_r4_r5_r6_r7<Address, int, R_key_value<int,
                                    int>, int, R_key_value<int, int>, _Seq<R_key_value<int, int>>,
                                    _Set<R_key_value<int, int>>> {orig_addr, orig_stmt_id, orig_vid,
                                    hop, corrective_vid, vid_send_list_tup.key,
                                    vid_send_list_tup.value});
                                    __engine.send(ip,
                                    __delete_S_rcv_corrective_s6_m___SQL_SUM_AGGREGATE_1_mS1_tid,
                                    __272, me);
                                    return acc_count + 1;
                                  }
                                };
                              }(std::move(__274))(__273);
                            }
                            __259 = std::move(__274);
                          }
                        } else {
                          if (stmt_id == 4) {
                            {
                              _Collection<R_key_value<Address, R_key_value<_Seq<R_key_value<int,
                              int>>, _Set<R_key_value<int, int>>>>> __275;
                              auto __284 = _Collection<R_r1_r2_r3<Address, R_key_value<int, int>,
                              _Set<R_key_value<int, int>>>> {};
                              for (const auto& __283: vid_list) {
                                __284 = [this,
                                &delta_tuples2] (const _Collection<R_r1_r2_r3<Address,
                                R_key_value<int, int>, _Set<R_key_value<int,
                                int>>>>& _accext) mutable {
                                  return [this, &_accext, &delta_tuples2] (const R_key_value<int,
                                  int>& vid) mutable {
                                    _Collection<R_r1_r2_r3<Address, R_key_value<int, int>,
                                    _Set<R_key_value<int, int>>>> __276;
                                    R_key_value<int, int> __277;
                                    __277 = nd_log_get_bound_delete_S(vid);
                                    {
                                      auto& S_B = __277.key;
                                      auto& S_C = __277.value;
                                      auto __282 = _Collection<R_r1_r2_r3<Address, R_key_value<int,
                                      int>, _Set<R_key_value<int, int>>>> {};
                                      for (const auto& __281: shuffle___SQL_SUM_AGGREGATE_1_mS1_to___SQL_SUM_AGGREGATE_1(R_r1_r2_r3<unit_t,
                                      _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>,
                                      bool> {unit_t {}, delta_tuples2, false})) {
                                        __282 = [this, &vid] (_Collection<R_r1_r2_r3<Address,
                                        R_key_value<int, int>, _Set<R_key_value<int,
                                        int>>>> _accmap) mutable {
                                          return [this, _accmap = std::move(_accmap),
                                          &vid] (const R_key_value<Address,
                                          _Set<R_r1_r2_r3<R_key_value<int, int>, int,
                                          int>>>& b3) mutable {
                                            {
                                              auto ip = b3.key;
                                              auto tuples = b3.value;
                                              _Collection<R_r1_r2_r3<Address, R_key_value<int, int>,
                                              _Set<R_key_value<int, int>>>> __278;
                                              auto __280 = _Set<R_key_value<int, int>> {};
                                              for (const auto& __279: tuples) {
                                                __280 = [this] (_Set<R_key_value<int,
                                                int>> _accmap) mutable {
                                                  return [this,
                                                  _accmap = std::move(_accmap)] (const R_r1_r2_r3<R_key_value<int,
                                                  int>, int, int>& b3) mutable {
                                                    {
                                                      auto& vid = b3.r1;
                                                      auto& map_0 = b3.r2;
                                                      auto& map_val = b3.r3;
                                                      _accmap.insert(R_key_value<int, int> {map_0,
                                                      map_val});
                                                      return std::move(_accmap);
                                                    }
                                                  };
                                                }(std::move(__280))(__279);
                                              }
                                              _accmap.insert(R_r1_r2_r3<Address, R_key_value<int,
                                              int>, _Set<R_key_value<int, int>>> {ip, vid,
                                              std::move(__280)});
                                              __278 = _accmap;
                                              ip = b3.key;
                                              tuples = b3.value;
                                              return std::move(__278);
                                            }
                                          };
                                        }(std::move(__282))(__281);
                                      }
                                      __276 = std::move(__282);
                                    }
                                    return _accext.combine(__276);
                                  };
                                }(std::move(__284))(__283);
                              }
                              __275 = std::move(__284).groupBy([this] (const R_r1_r2_r3<Address,
                              R_key_value<int, int>, _Set<R_key_value<int, int>>>& b1) mutable {
                                {
                                  auto& ip = b1.r1;
                                  auto& vid = b1.r2;
                                  auto& tuples = b1.r3;
                                  return std::move(ip);
                                }
                              }, [this] (R_key_value<_Seq<R_key_value<int, int>>,
                              _Set<R_key_value<int, int>>> b2) mutable {
                                return [this, &b2] (const R_r1_r2_r3<Address, R_key_value<int, int>,
                                _Set<R_key_value<int, int>>>& b5) mutable {
                                  {
                                    auto& acc_vid = b2.key;
                                    auto& acc_tuples = b2.value;
                                    {
                                      auto& ip = b5.r1;
                                      auto& vid = b5.r2;
                                      auto& tuples = b5.r3;
                                      acc_vid.insert(vid);
                                      auto __286 = _Set<R_key_value<int, int>> {};
                                      for (const auto& __285: acc_tuples.combine(tuples).groupBy([this] (const R_key_value<int,
                                      int>& tuple) mutable {
                                        return std::move(tuple);
                                      }, [this] (const unit_t& _) mutable {
                                        return [this] (const R_key_value<int, int>& _) mutable {
                                          return unit_t {};
                                        };
                                      }, unit_t {})) {
                                        __286 = [this] (_Set<R_key_value<int,
                                        int>> _accmap) mutable {
                                          return [this,
                                          _accmap = std::move(_accmap)] (const R_key_value<R_key_value<int,
                                          int>, unit_t>& x) mutable {
                                            _accmap.insert(x.key);
                                            return std::move(_accmap);
                                          };
                                        }(std::move(__286))(__285);
                                      }
                                      return R_key_value<_Seq<R_key_value<int, int>>,
                                      _Set<R_key_value<int, int>>> {acc_vid, std::move(__286)};
                                    }
                                  }
                                };
                              }, R_key_value<_Seq<R_key_value<int, int>>, _Set<R_key_value<int,
                              int>>> {_Seq<R_key_value<int, int>> {}, _Set<R_key_value<int,
                              int>> {}});
                              auto& ips_vids = __275;
                              auto __289 = acc_count;
                              for (const auto& __288: ips_vids) {
                                __289 = [this, &corrective_vid, &hop, &orig_addr, &orig_stmt_id,
                                &orig_vid] (const int& acc_count) mutable {
                                  return [this, &acc_count, &corrective_vid, &hop, &orig_addr,
                                  &orig_stmt_id, &orig_vid] (const R_key_value<Address,
                                  R_key_value<_Seq<R_key_value<int, int>>, _Set<R_key_value<int,
                                  int>>>>& b3) mutable {
                                    {
                                      auto& ip = b3.key;
                                      auto& vid_send_list_tup = b3.value;
                                      auto __287 = std::make_shared<K3::ValDispatcher<R_r1_r2_r3_r4_r5_r6_r7<Address,
                                      int, R_key_value<int, int>, int, R_key_value<int, int>,
                                      _Seq<R_key_value<int, int>>, _Set<R_key_value<int,
                                      int>>>>>(R_r1_r2_r3_r4_r5_r6_r7<Address, int, R_key_value<int,
                                      int>, int, R_key_value<int, int>, _Seq<R_key_value<int, int>>,
                                      _Set<R_key_value<int, int>>> {orig_addr, orig_stmt_id,
                                      orig_vid, hop, corrective_vid, vid_send_list_tup.key,
                                      vid_send_list_tup.value});
                                      __engine.send(ip,
                                      __delete_S_rcv_corrective_s4_m___SQL_SUM_AGGREGATE_1_mS1_tid,
                                      __287, me);
                                      return acc_count + 1;
                                    }
                                  };
                                }(std::move(__289))(__288);
                              }
                              __259 = std::move(__289);
                            }
                          } else {
                            if (stmt_id == 2) {
                              {
                                _Collection<R_key_value<Address, R_key_value<_Seq<R_key_value<int,
                                int>>, _Set<R_key_value<int, int>>>>> __290;
                                auto __299 = _Collection<R_r1_r2_r3<Address, R_key_value<int, int>,
                                _Set<R_key_value<int, int>>>> {};
                                for (const auto& __298: vid_list) {
                                  __299 = [this,
                                  &delta_tuples2] (const _Collection<R_r1_r2_r3<Address,
                                  R_key_value<int, int>, _Set<R_key_value<int,
                                  int>>>>& _accext) mutable {
                                    return [this, &_accext, &delta_tuples2] (const R_key_value<int,
                                    int>& vid) mutable {
                                      _Collection<R_r1_r2_r3<Address, R_key_value<int, int>,
                                      _Set<R_key_value<int, int>>>> __291;
                                      R_key_value<int, int> __292;
                                      __292 = nd_log_get_bound_insert_S(vid);
                                      {
                                        auto& S_B = __292.key;
                                        auto& S_C = __292.value;
                                        auto __297 = _Collection<R_r1_r2_r3<Address,
                                        R_key_value<int, int>, _Set<R_key_value<int, int>>>> {};
                                        for (const auto& __296: shuffle___SQL_SUM_AGGREGATE_1_mS1_to___SQL_SUM_AGGREGATE_2(R_r1_r2_r3<unit_t,
                                        _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>,
                                        bool> {unit_t {}, delta_tuples2, false})) {
                                          __297 = [this, &vid] (_Collection<R_r1_r2_r3<Address,
                                          R_key_value<int, int>, _Set<R_key_value<int,
                                          int>>>> _accmap) mutable {
                                            return [this, _accmap = std::move(_accmap),
                                            &vid] (const R_key_value<Address,
                                            _Set<R_r1_r2_r3<R_key_value<int, int>, int,
                                            int>>>& b3) mutable {
                                              {
                                                auto ip = b3.key;
                                                auto tuples = b3.value;
                                                _Collection<R_r1_r2_r3<Address, R_key_value<int,
                                                int>, _Set<R_key_value<int, int>>>> __293;
                                                auto __295 = _Set<R_key_value<int, int>> {};
                                                for (const auto& __294: tuples) {
                                                  __295 = [this] (_Set<R_key_value<int,
                                                  int>> _accmap) mutable {
                                                    return [this,
                                                    _accmap = std::move(_accmap)] (const R_r1_r2_r3<R_key_value<int,
                                                    int>, int, int>& b3) mutable {
                                                      {
                                                        auto& vid = b3.r1;
                                                        auto& map_0 = b3.r2;
                                                        auto& map_val = b3.r3;
                                                        _accmap.insert(R_key_value<int, int> {map_0,
                                                        map_val});
                                                        return std::move(_accmap);
                                                      }
                                                    };
                                                  }(std::move(__295))(__294);
                                                }
                                                _accmap.insert(R_r1_r2_r3<Address, R_key_value<int,
                                                int>, _Set<R_key_value<int, int>>> {ip, vid,
                                                std::move(__295)});
                                                __293 = _accmap;
                                                ip = b3.key;
                                                tuples = b3.value;
                                                return std::move(__293);
                                              }
                                            };
                                          }(std::move(__297))(__296);
                                        }
                                        __291 = std::move(__297);
                                      }
                                      return _accext.combine(__291);
                                    };
                                  }(std::move(__299))(__298);
                                }
                                __290 = std::move(__299).groupBy([this] (const R_r1_r2_r3<Address,
                                R_key_value<int, int>, _Set<R_key_value<int, int>>>& b1) mutable {
                                  {
                                    auto& ip = b1.r1;
                                    auto& vid = b1.r2;
                                    auto& tuples = b1.r3;
                                    return std::move(ip);
                                  }
                                }, [this] (R_key_value<_Seq<R_key_value<int, int>>,
                                _Set<R_key_value<int, int>>> b2) mutable {
                                  return [this, &b2] (const R_r1_r2_r3<Address, R_key_value<int,
                                  int>, _Set<R_key_value<int, int>>>& b5) mutable {
                                    {
                                      auto& acc_vid = b2.key;
                                      auto& acc_tuples = b2.value;
                                      {
                                        auto& ip = b5.r1;
                                        auto& vid = b5.r2;
                                        auto& tuples = b5.r3;
                                        acc_vid.insert(vid);
                                        auto __301 = _Set<R_key_value<int, int>> {};
                                        for (const auto& __300: acc_tuples.combine(tuples).groupBy([this] (const R_key_value<int,
                                        int>& tuple) mutable {
                                          return std::move(tuple);
                                        }, [this] (const unit_t& _) mutable {
                                          return [this] (const R_key_value<int, int>& _) mutable {
                                            return unit_t {};
                                          };
                                        }, unit_t {})) {
                                          __301 = [this] (_Set<R_key_value<int,
                                          int>> _accmap) mutable {
                                            return [this,
                                            _accmap = std::move(_accmap)] (const R_key_value<R_key_value<int,
                                            int>, unit_t>& x) mutable {
                                              _accmap.insert(x.key);
                                              return std::move(_accmap);
                                            };
                                          }(std::move(__301))(__300);
                                        }
                                        return R_key_value<_Seq<R_key_value<int, int>>,
                                        _Set<R_key_value<int, int>>> {acc_vid, std::move(__301)};
                                      }
                                    }
                                  };
                                }, R_key_value<_Seq<R_key_value<int, int>>, _Set<R_key_value<int,
                                int>>> {_Seq<R_key_value<int, int>> {}, _Set<R_key_value<int,
                                int>> {}});
                                auto& ips_vids = __290;
                                auto __304 = acc_count;
                                for (const auto& __303: ips_vids) {
                                  __304 = [this, &corrective_vid, &hop, &orig_addr, &orig_stmt_id,
                                  &orig_vid] (const int& acc_count) mutable {
                                    return [this, &acc_count, &corrective_vid, &hop, &orig_addr,
                                    &orig_stmt_id, &orig_vid] (const R_key_value<Address,
                                    R_key_value<_Seq<R_key_value<int, int>>, _Set<R_key_value<int,
                                    int>>>>& b3) mutable {
                                      {
                                        auto& ip = b3.key;
                                        auto& vid_send_list_tup = b3.value;
                                        auto __302 = std::make_shared<K3::ValDispatcher<R_r1_r2_r3_r4_r5_r6_r7<Address,
                                        int, R_key_value<int, int>, int, R_key_value<int, int>,
                                        _Seq<R_key_value<int, int>>, _Set<R_key_value<int,
                                        int>>>>>(R_r1_r2_r3_r4_r5_r6_r7<Address, int,
                                        R_key_value<int, int>, int, R_key_value<int, int>,
                                        _Seq<R_key_value<int, int>>, _Set<R_key_value<int,
                                        int>>> {orig_addr, orig_stmt_id, orig_vid, hop,
                                        corrective_vid, vid_send_list_tup.key,
                                        vid_send_list_tup.value});
                                        __engine.send(ip,
                                        __insert_S_rcv_corrective_s2_m___SQL_SUM_AGGREGATE_1_mS1_tid,
                                        __302, me);
                                        return acc_count + 1;
                                      }
                                    };
                                  }(std::move(__304))(__303);
                                }
                                __259 = std::move(__304);
                              }
                            } else {
                              if (stmt_id == 0) {
                                {
                                  _Collection<R_key_value<Address, R_key_value<_Seq<R_key_value<int,
                                  int>>, _Set<R_key_value<int, int>>>>> __305;
                                  auto __314 = _Collection<R_r1_r2_r3<Address, R_key_value<int,
                                  int>, _Set<R_key_value<int, int>>>> {};
                                  for (const auto& __313: vid_list) {
                                    __314 = [this,
                                    &delta_tuples2] (const _Collection<R_r1_r2_r3<Address,
                                    R_key_value<int, int>, _Set<R_key_value<int,
                                    int>>>>& _accext) mutable {
                                      return [this, &_accext,
                                      &delta_tuples2] (const R_key_value<int, int>& vid) mutable {
                                        _Collection<R_r1_r2_r3<Address, R_key_value<int, int>,
                                        _Set<R_key_value<int, int>>>> __306;
                                        R_key_value<int, int> __307;
                                        __307 = nd_log_get_bound_insert_S(vid);
                                        {
                                          auto& S_B = __307.key;
                                          auto& S_C = __307.value;
                                          auto __312 = _Collection<R_r1_r2_r3<Address,
                                          R_key_value<int, int>, _Set<R_key_value<int, int>>>> {};
                                          for (const auto& __311: shuffle___SQL_SUM_AGGREGATE_1_mS1_to___SQL_SUM_AGGREGATE_1(R_r1_r2_r3<unit_t,
                                          _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>,
                                          bool> {unit_t {}, delta_tuples2, false})) {
                                            __312 = [this, &vid] (_Collection<R_r1_r2_r3<Address,
                                            R_key_value<int, int>, _Set<R_key_value<int,
                                            int>>>> _accmap) mutable {
                                              return [this, _accmap = std::move(_accmap),
                                              &vid] (const R_key_value<Address,
                                              _Set<R_r1_r2_r3<R_key_value<int, int>, int,
                                              int>>>& b3) mutable {
                                                {
                                                  auto ip = b3.key;
                                                  auto tuples = b3.value;
                                                  _Collection<R_r1_r2_r3<Address, R_key_value<int,
                                                  int>, _Set<R_key_value<int, int>>>> __308;
                                                  auto __310 = _Set<R_key_value<int, int>> {};
                                                  for (const auto& __309: tuples) {
                                                    __310 = [this] (_Set<R_key_value<int,
                                                    int>> _accmap) mutable {
                                                      return [this,
                                                      _accmap = std::move(_accmap)] (const R_r1_r2_r3<R_key_value<int,
                                                      int>, int, int>& b3) mutable {
                                                        {
                                                          auto& vid = b3.r1;
                                                          auto& map_0 = b3.r2;
                                                          auto& map_val = b3.r3;
                                                          _accmap.insert(R_key_value<int,
                                                          int> {map_0, map_val});
                                                          return std::move(_accmap);
                                                        }
                                                      };
                                                    }(std::move(__310))(__309);
                                                  }
                                                  _accmap.insert(R_r1_r2_r3<Address,
                                                  R_key_value<int, int>, _Set<R_key_value<int,
                                                  int>>> {ip, vid, std::move(__310)});
                                                  __308 = _accmap;
                                                  ip = b3.key;
                                                  tuples = b3.value;
                                                  return std::move(__308);
                                                }
                                              };
                                            }(std::move(__312))(__311);
                                          }
                                          __306 = std::move(__312);
                                        }
                                        return _accext.combine(__306);
                                      };
                                    }(std::move(__314))(__313);
                                  }
                                  __305 = std::move(__314).groupBy([this] (const R_r1_r2_r3<Address,
                                  R_key_value<int, int>, _Set<R_key_value<int, int>>>& b1) mutable {
                                    {
                                      auto& ip = b1.r1;
                                      auto& vid = b1.r2;
                                      auto& tuples = b1.r3;
                                      return std::move(ip);
                                    }
                                  }, [this] (R_key_value<_Seq<R_key_value<int, int>>,
                                  _Set<R_key_value<int, int>>> b2) mutable {
                                    return [this, &b2] (const R_r1_r2_r3<Address, R_key_value<int,
                                    int>, _Set<R_key_value<int, int>>>& b5) mutable {
                                      {
                                        auto& acc_vid = b2.key;
                                        auto& acc_tuples = b2.value;
                                        {
                                          auto& ip = b5.r1;
                                          auto& vid = b5.r2;
                                          auto& tuples = b5.r3;
                                          acc_vid.insert(vid);
                                          auto __316 = _Set<R_key_value<int, int>> {};
                                          for (const auto& __315: acc_tuples.combine(tuples).groupBy([this] (const R_key_value<int,
                                          int>& tuple) mutable {
                                            return std::move(tuple);
                                          }, [this] (const unit_t& _) mutable {
                                            return [this] (const R_key_value<int, int>& _) mutable {
                                              return unit_t {};
                                            };
                                          }, unit_t {})) {
                                            __316 = [this] (_Set<R_key_value<int,
                                            int>> _accmap) mutable {
                                              return [this,
                                              _accmap = std::move(_accmap)] (const R_key_value<R_key_value<int,
                                              int>, unit_t>& x) mutable {
                                                _accmap.insert(x.key);
                                                return std::move(_accmap);
                                              };
                                            }(std::move(__316))(__315);
                                          }
                                          return R_key_value<_Seq<R_key_value<int, int>>,
                                          _Set<R_key_value<int, int>>> {acc_vid, std::move(__316)};
                                        }
                                      }
                                    };
                                  }, R_key_value<_Seq<R_key_value<int, int>>, _Set<R_key_value<int,
                                  int>>> {_Seq<R_key_value<int, int>> {}, _Set<R_key_value<int,
                                  int>> {}});
                                  auto& ips_vids = __305;
                                  auto __319 = acc_count;
                                  for (const auto& __318: ips_vids) {
                                    __319 = [this, &corrective_vid, &hop, &orig_addr, &orig_stmt_id,
                                    &orig_vid] (const int& acc_count) mutable {
                                      return [this, &acc_count, &corrective_vid, &hop, &orig_addr,
                                      &orig_stmt_id, &orig_vid] (const R_key_value<Address,
                                      R_key_value<_Seq<R_key_value<int, int>>, _Set<R_key_value<int,
                                      int>>>>& b3) mutable {
                                        {
                                          auto& ip = b3.key;
                                          auto& vid_send_list_tup = b3.value;
                                          auto __317 = std::make_shared<K3::ValDispatcher<R_r1_r2_r3_r4_r5_r6_r7<Address,
                                          int, R_key_value<int, int>, int, R_key_value<int, int>,
                                          _Seq<R_key_value<int, int>>, _Set<R_key_value<int,
                                          int>>>>>(R_r1_r2_r3_r4_r5_r6_r7<Address, int,
                                          R_key_value<int, int>, int, R_key_value<int, int>,
                                          _Seq<R_key_value<int, int>>, _Set<R_key_value<int,
                                          int>>> {orig_addr, orig_stmt_id, orig_vid, hop,
                                          corrective_vid, vid_send_list_tup.key,
                                          vid_send_list_tup.value});
                                          __engine.send(ip,
                                          __insert_S_rcv_corrective_s0_m___SQL_SUM_AGGREGATE_1_mS1_tid,
                                          __317, me);
                                          return acc_count + 1;
                                        }
                                      };
                                    }(std::move(__319))(__318);
                                  }
                                  __259 = std::move(__319);
                                }
                              } else {
                                __259 = acc_count;
                              }
                            }
                          }
                        }
                        stmt_id = b3.key;
                        vid_list = b3.value;
                        return std::move(__259);
                      }
                    };
                  }(std::move(__321))(__320);
                }
                __252 = std::move(__321);
              }
            }
          }
          orig_addr = b1.r1;
          orig_stmt_id = b1.r2;
          orig_vid = b1.r3;
          hop = b1.r4;
          corrective_vid = b1.r5;
          delta_tuples = b1.r6;
          return __252;
        }
      }
      int nd___SQL_SUM_AGGREGATE_2_mR1_send_correctives(const R_r1_r2_r3_r4_r5_r6<Address, int,
      R_key_value<int, int>, int, R_key_value<int, int>, _Set<R_key_value<int, int>>>& b1)  {
        {
          auto orig_addr = b1.r1;
          auto orig_stmt_id = b1.r2;
          auto orig_vid = b1.r3;
          auto hop = b1.r4;
          auto corrective_vid = b1.r5;
          auto delta_tuples = b1.r6;
          int __322;
          {
            _Collection<R_key_value<int, _Seq<R_key_value<int, int>>>> __323;
            _Collection<R_key_value<int, int>> __324;
            {
              _Collection<R_key_value<int, int>> __325;
              __325 = _Collection<R_key_value<int, int>> {};
              auto& __collection = __325;
              __collection.insert(R_key_value<int, int> {2, 10});
              __collection.insert(R_key_value<int, int> {3, 14});
              __324 = __collection;
            }
            __323 = nd_filter_corrective_list(R_key_value<R_key_value<int, int>,
            _Collection<R_key_value<int, int>>> {corrective_vid, __324});
            auto& corrective_list = __323;
            if (0 == corrective_list.size(unit_t {})) {
              __322 = 0;
            } else {
              {
                _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> __326;
                auto __328 = _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> {};
                for (const auto& __327: delta_tuples) {
                  __328 = [this] (_Set<R_r1_r2_r3<R_key_value<int, int>, int,
                  int>> _accmap) mutable {
                    return [this, _accmap = std::move(_accmap)] (const R_key_value<int,
                    int>& b3) mutable {
                      {
                        auto& map_0 = b3.key;
                        auto& map_val = b3.value;
                        _accmap.insert(R_r1_r2_r3<R_key_value<int, int>, int, int> {g_min_vid,
                        map_0, map_val});
                        return std::move(_accmap);
                      }
                    };
                  }(std::move(__328))(__327);
                }
                __326 = std::move(__328);
                auto& delta_tuples2 = __326;
                auto __361 = 0;
                for (const auto& __360: corrective_list) {
                  __361 = [this, &corrective_vid, &delta_tuples2, &hop, &orig_addr, &orig_stmt_id,
                  &orig_vid] (const int& acc_count) mutable {
                    return [this, &acc_count, &corrective_vid, &delta_tuples2, &hop, &orig_addr,
                    &orig_stmt_id, &orig_vid] (const R_key_value<int, _Seq<R_key_value<int,
                    int>>>& b3) mutable {
                      {
                        auto stmt_id = b3.key;
                        auto vid_list = b3.value;
                        int __329;
                        if (stmt_id == 14) {
                          {
                            _Collection<R_key_value<Address, R_key_value<_Seq<R_key_value<int,
                            int>>, _Set<R_key_value<int, int>>>>> __330;
                            auto __339 = _Collection<R_r1_r2_r3<Address, R_key_value<int, int>,
                            _Set<R_key_value<int, int>>>> {};
                            for (const auto& __338: vid_list) {
                              __339 = [this, &delta_tuples2] (const _Collection<R_r1_r2_r3<Address,
                              R_key_value<int, int>, _Set<R_key_value<int,
                              int>>>>& _accext) mutable {
                                return [this, &_accext, &delta_tuples2] (const R_key_value<int,
                                int>& vid) mutable {
                                  _Collection<R_r1_r2_r3<Address, R_key_value<int, int>,
                                  _Set<R_key_value<int, int>>>> __331;
                                  R_key_value<int, int> __332;
                                  __332 = nd_log_get_bound_delete_R(vid);
                                  {
                                    auto& R_A = __332.key;
                                    auto& R_B = __332.value;
                                    auto __337 = _Collection<R_r1_r2_r3<Address, R_key_value<int,
                                    int>, _Set<R_key_value<int, int>>>> {};
                                    for (const auto& __336: shuffle___SQL_SUM_AGGREGATE_2_mR1_to___SQL_SUM_AGGREGATE_2(R_r1_r2_r3<unit_t,
                                    _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>,
                                    bool> {unit_t {}, delta_tuples2, false})) {
                                      __337 = [this, &vid] (_Collection<R_r1_r2_r3<Address,
                                      R_key_value<int, int>, _Set<R_key_value<int,
                                      int>>>> _accmap) mutable {
                                        return [this, _accmap = std::move(_accmap),
                                        &vid] (const R_key_value<Address,
                                        _Set<R_r1_r2_r3<R_key_value<int, int>, int,
                                        int>>>& b3) mutable {
                                          {
                                            auto ip = b3.key;
                                            auto tuples = b3.value;
                                            _Collection<R_r1_r2_r3<Address, R_key_value<int, int>,
                                            _Set<R_key_value<int, int>>>> __333;
                                            auto __335 = _Set<R_key_value<int, int>> {};
                                            for (const auto& __334: tuples) {
                                              __335 = [this] (_Set<R_key_value<int,
                                              int>> _accmap) mutable {
                                                return [this,
                                                _accmap = std::move(_accmap)] (const R_r1_r2_r3<R_key_value<int,
                                                int>, int, int>& b3) mutable {
                                                  {
                                                    auto& vid = b3.r1;
                                                    auto& map_0 = b3.r2;
                                                    auto& map_val = b3.r3;
                                                    _accmap.insert(R_key_value<int, int> {map_0,
                                                    map_val});
                                                    return std::move(_accmap);
                                                  }
                                                };
                                              }(std::move(__335))(__334);
                                            }
                                            _accmap.insert(R_r1_r2_r3<Address, R_key_value<int,
                                            int>, _Set<R_key_value<int, int>>> {ip, vid,
                                            std::move(__335)});
                                            __333 = _accmap;
                                            ip = b3.key;
                                            tuples = b3.value;
                                            return std::move(__333);
                                          }
                                        };
                                      }(std::move(__337))(__336);
                                    }
                                    __331 = std::move(__337);
                                  }
                                  return _accext.combine(__331);
                                };
                              }(std::move(__339))(__338);
                            }
                            __330 = std::move(__339).groupBy([this] (const R_r1_r2_r3<Address,
                            R_key_value<int, int>, _Set<R_key_value<int, int>>>& b1) mutable {
                              {
                                auto& ip = b1.r1;
                                auto& vid = b1.r2;
                                auto& tuples = b1.r3;
                                return std::move(ip);
                              }
                            }, [this] (R_key_value<_Seq<R_key_value<int, int>>,
                            _Set<R_key_value<int, int>>> b2) mutable {
                              return [this, &b2] (const R_r1_r2_r3<Address, R_key_value<int, int>,
                              _Set<R_key_value<int, int>>>& b5) mutable {
                                {
                                  auto& acc_vid = b2.key;
                                  auto& acc_tuples = b2.value;
                                  {
                                    auto& ip = b5.r1;
                                    auto& vid = b5.r2;
                                    auto& tuples = b5.r3;
                                    acc_vid.insert(vid);
                                    auto __341 = _Set<R_key_value<int, int>> {};
                                    for (const auto& __340: acc_tuples.combine(tuples).groupBy([this] (const R_key_value<int,
                                    int>& tuple) mutable {
                                      return std::move(tuple);
                                    }, [this] (const unit_t& _) mutable {
                                      return [this] (const R_key_value<int, int>& _) mutable {
                                        return unit_t {};
                                      };
                                    }, unit_t {})) {
                                      __341 = [this] (_Set<R_key_value<int, int>> _accmap) mutable {
                                        return [this,
                                        _accmap = std::move(_accmap)] (const R_key_value<R_key_value<int,
                                        int>, unit_t>& x) mutable {
                                          _accmap.insert(x.key);
                                          return std::move(_accmap);
                                        };
                                      }(std::move(__341))(__340);
                                    }
                                    return R_key_value<_Seq<R_key_value<int, int>>,
                                    _Set<R_key_value<int, int>>> {acc_vid, std::move(__341)};
                                  }
                                }
                              };
                            }, R_key_value<_Seq<R_key_value<int, int>>, _Set<R_key_value<int,
                            int>>> {_Seq<R_key_value<int, int>> {}, _Set<R_key_value<int,
                            int>> {}});
                            auto& ips_vids = __330;
                            auto __344 = acc_count;
                            for (const auto& __343: ips_vids) {
                              __344 = [this, &corrective_vid, &hop, &orig_addr, &orig_stmt_id,
                              &orig_vid] (const int& acc_count) mutable {
                                return [this, &acc_count, &corrective_vid, &hop, &orig_addr,
                                &orig_stmt_id, &orig_vid] (const R_key_value<Address,
                                R_key_value<_Seq<R_key_value<int, int>>, _Set<R_key_value<int,
                                int>>>>& b3) mutable {
                                  {
                                    auto& ip = b3.key;
                                    auto& vid_send_list_tup = b3.value;
                                    auto __342 = std::make_shared<K3::ValDispatcher<R_r1_r2_r3_r4_r5_r6_r7<Address,
                                    int, R_key_value<int, int>, int, R_key_value<int, int>,
                                    _Seq<R_key_value<int, int>>, _Set<R_key_value<int,
                                    int>>>>>(R_r1_r2_r3_r4_r5_r6_r7<Address, int, R_key_value<int,
                                    int>, int, R_key_value<int, int>, _Seq<R_key_value<int, int>>,
                                    _Set<R_key_value<int, int>>> {orig_addr, orig_stmt_id, orig_vid,
                                    hop, corrective_vid, vid_send_list_tup.key,
                                    vid_send_list_tup.value});
                                    __engine.send(ip,
                                    __delete_R_rcv_corrective_s14_m___SQL_SUM_AGGREGATE_2_mR1_tid,
                                    __342, me);
                                    return acc_count + 1;
                                  }
                                };
                              }(std::move(__344))(__343);
                            }
                            __329 = std::move(__344);
                          }
                        } else {
                          if (stmt_id == 10) {
                            {
                              _Collection<R_key_value<Address, R_key_value<_Seq<R_key_value<int,
                              int>>, _Set<R_key_value<int, int>>>>> __345;
                              auto __354 = _Collection<R_r1_r2_r3<Address, R_key_value<int, int>,
                              _Set<R_key_value<int, int>>>> {};
                              for (const auto& __353: vid_list) {
                                __354 = [this,
                                &delta_tuples2] (const _Collection<R_r1_r2_r3<Address,
                                R_key_value<int, int>, _Set<R_key_value<int,
                                int>>>>& _accext) mutable {
                                  return [this, &_accext, &delta_tuples2] (const R_key_value<int,
                                  int>& vid) mutable {
                                    _Collection<R_r1_r2_r3<Address, R_key_value<int, int>,
                                    _Set<R_key_value<int, int>>>> __346;
                                    R_key_value<int, int> __347;
                                    __347 = nd_log_get_bound_insert_R(vid);
                                    {
                                      auto& R_A = __347.key;
                                      auto& R_B = __347.value;
                                      auto __352 = _Collection<R_r1_r2_r3<Address, R_key_value<int,
                                      int>, _Set<R_key_value<int, int>>>> {};
                                      for (const auto& __351: shuffle___SQL_SUM_AGGREGATE_2_mR1_to___SQL_SUM_AGGREGATE_2(R_r1_r2_r3<unit_t,
                                      _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>,
                                      bool> {unit_t {}, delta_tuples2, false})) {
                                        __352 = [this, &vid] (_Collection<R_r1_r2_r3<Address,
                                        R_key_value<int, int>, _Set<R_key_value<int,
                                        int>>>> _accmap) mutable {
                                          return [this, _accmap = std::move(_accmap),
                                          &vid] (const R_key_value<Address,
                                          _Set<R_r1_r2_r3<R_key_value<int, int>, int,
                                          int>>>& b3) mutable {
                                            {
                                              auto ip = b3.key;
                                              auto tuples = b3.value;
                                              _Collection<R_r1_r2_r3<Address, R_key_value<int, int>,
                                              _Set<R_key_value<int, int>>>> __348;
                                              auto __350 = _Set<R_key_value<int, int>> {};
                                              for (const auto& __349: tuples) {
                                                __350 = [this] (_Set<R_key_value<int,
                                                int>> _accmap) mutable {
                                                  return [this,
                                                  _accmap = std::move(_accmap)] (const R_r1_r2_r3<R_key_value<int,
                                                  int>, int, int>& b3) mutable {
                                                    {
                                                      auto& vid = b3.r1;
                                                      auto& map_0 = b3.r2;
                                                      auto& map_val = b3.r3;
                                                      _accmap.insert(R_key_value<int, int> {map_0,
                                                      map_val});
                                                      return std::move(_accmap);
                                                    }
                                                  };
                                                }(std::move(__350))(__349);
                                              }
                                              _accmap.insert(R_r1_r2_r3<Address, R_key_value<int,
                                              int>, _Set<R_key_value<int, int>>> {ip, vid,
                                              std::move(__350)});
                                              __348 = _accmap;
                                              ip = b3.key;
                                              tuples = b3.value;
                                              return std::move(__348);
                                            }
                                          };
                                        }(std::move(__352))(__351);
                                      }
                                      __346 = std::move(__352);
                                    }
                                    return _accext.combine(__346);
                                  };
                                }(std::move(__354))(__353);
                              }
                              __345 = std::move(__354).groupBy([this] (const R_r1_r2_r3<Address,
                              R_key_value<int, int>, _Set<R_key_value<int, int>>>& b1) mutable {
                                {
                                  auto& ip = b1.r1;
                                  auto& vid = b1.r2;
                                  auto& tuples = b1.r3;
                                  return std::move(ip);
                                }
                              }, [this] (R_key_value<_Seq<R_key_value<int, int>>,
                              _Set<R_key_value<int, int>>> b2) mutable {
                                return [this, &b2] (const R_r1_r2_r3<Address, R_key_value<int, int>,
                                _Set<R_key_value<int, int>>>& b5) mutable {
                                  {
                                    auto& acc_vid = b2.key;
                                    auto& acc_tuples = b2.value;
                                    {
                                      auto& ip = b5.r1;
                                      auto& vid = b5.r2;
                                      auto& tuples = b5.r3;
                                      acc_vid.insert(vid);
                                      auto __356 = _Set<R_key_value<int, int>> {};
                                      for (const auto& __355: acc_tuples.combine(tuples).groupBy([this] (const R_key_value<int,
                                      int>& tuple) mutable {
                                        return std::move(tuple);
                                      }, [this] (const unit_t& _) mutable {
                                        return [this] (const R_key_value<int, int>& _) mutable {
                                          return unit_t {};
                                        };
                                      }, unit_t {})) {
                                        __356 = [this] (_Set<R_key_value<int,
                                        int>> _accmap) mutable {
                                          return [this,
                                          _accmap = std::move(_accmap)] (const R_key_value<R_key_value<int,
                                          int>, unit_t>& x) mutable {
                                            _accmap.insert(x.key);
                                            return std::move(_accmap);
                                          };
                                        }(std::move(__356))(__355);
                                      }
                                      return R_key_value<_Seq<R_key_value<int, int>>,
                                      _Set<R_key_value<int, int>>> {acc_vid, std::move(__356)};
                                    }
                                  }
                                };
                              }, R_key_value<_Seq<R_key_value<int, int>>, _Set<R_key_value<int,
                              int>>> {_Seq<R_key_value<int, int>> {}, _Set<R_key_value<int,
                              int>> {}});
                              auto& ips_vids = __345;
                              auto __359 = acc_count;
                              for (const auto& __358: ips_vids) {
                                __359 = [this, &corrective_vid, &hop, &orig_addr, &orig_stmt_id,
                                &orig_vid] (const int& acc_count) mutable {
                                  return [this, &acc_count, &corrective_vid, &hop, &orig_addr,
                                  &orig_stmt_id, &orig_vid] (const R_key_value<Address,
                                  R_key_value<_Seq<R_key_value<int, int>>, _Set<R_key_value<int,
                                  int>>>>& b3) mutable {
                                    {
                                      auto& ip = b3.key;
                                      auto& vid_send_list_tup = b3.value;
                                      auto __357 = std::make_shared<K3::ValDispatcher<R_r1_r2_r3_r4_r5_r6_r7<Address,
                                      int, R_key_value<int, int>, int, R_key_value<int, int>,
                                      _Seq<R_key_value<int, int>>, _Set<R_key_value<int,
                                      int>>>>>(R_r1_r2_r3_r4_r5_r6_r7<Address, int, R_key_value<int,
                                      int>, int, R_key_value<int, int>, _Seq<R_key_value<int, int>>,
                                      _Set<R_key_value<int, int>>> {orig_addr, orig_stmt_id,
                                      orig_vid, hop, corrective_vid, vid_send_list_tup.key,
                                      vid_send_list_tup.value});
                                      __engine.send(ip,
                                      __insert_R_rcv_corrective_s10_m___SQL_SUM_AGGREGATE_2_mR1_tid,
                                      __357, me);
                                      return acc_count + 1;
                                    }
                                  };
                                }(std::move(__359))(__358);
                              }
                              __329 = std::move(__359);
                            }
                          } else {
                            __329 = acc_count;
                          }
                        }
                        stmt_id = b3.key;
                        vid_list = b3.value;
                        return std::move(__329);
                      }
                    };
                  }(std::move(__361))(__360);
                }
                __322 = std::move(__361);
              }
            }
          }
          orig_addr = b1.r1;
          orig_stmt_id = b1.r2;
          orig_vid = b1.r3;
          hop = b1.r4;
          corrective_vid = b1.r5;
          delta_tuples = b1.r6;
          return __322;
        }
      }
      int nd___SQL_SUM_AGGREGATE_1_mR1_send_correctives(const R_r1_r2_r3_r4_r5_r6<Address, int,
      R_key_value<int, int>, int, R_key_value<int, int>, _Set<R_key_value<int, int>>>& b1)  {
        {
          auto orig_addr = b1.r1;
          auto orig_stmt_id = b1.r2;
          auto orig_vid = b1.r3;
          auto hop = b1.r4;
          auto corrective_vid = b1.r5;
          auto delta_tuples = b1.r6;
          int __362;
          {
            _Collection<R_key_value<int, _Seq<R_key_value<int, int>>>> __363;
            _Collection<R_key_value<int, int>> __364;
            {
              _Collection<R_key_value<int, int>> __365;
              __365 = _Collection<R_key_value<int, int>> {};
              auto& __collection = __365;
              __collection.insert(R_key_value<int, int> {2, 8});
              __collection.insert(R_key_value<int, int> {2, 10});
              __collection.insert(R_key_value<int, int> {3, 12});
              __collection.insert(R_key_value<int, int> {3, 14});
              __364 = __collection;
            }
            __363 = nd_filter_corrective_list(R_key_value<R_key_value<int, int>,
            _Collection<R_key_value<int, int>>> {corrective_vid, __364});
            auto& corrective_list = __363;
            if (0 == corrective_list.size(unit_t {})) {
              __362 = 0;
            } else {
              {
                _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> __366;
                auto __368 = _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> {};
                for (const auto& __367: delta_tuples) {
                  __368 = [this] (_Set<R_r1_r2_r3<R_key_value<int, int>, int,
                  int>> _accmap) mutable {
                    return [this, _accmap = std::move(_accmap)] (const R_key_value<int,
                    int>& b3) mutable {
                      {
                        auto& map_0 = b3.key;
                        auto& map_val = b3.value;
                        _accmap.insert(R_r1_r2_r3<R_key_value<int, int>, int, int> {g_min_vid,
                        map_0, map_val});
                        return std::move(_accmap);
                      }
                    };
                  }(std::move(__368))(__367);
                }
                __366 = std::move(__368);
                auto& delta_tuples2 = __366;
                auto __431 = 0;
                for (const auto& __430: corrective_list) {
                  __431 = [this, &corrective_vid, &delta_tuples2, &hop, &orig_addr, &orig_stmt_id,
                  &orig_vid] (const int& acc_count) mutable {
                    return [this, &acc_count, &corrective_vid, &delta_tuples2, &hop, &orig_addr,
                    &orig_stmt_id, &orig_vid] (const R_key_value<int, _Seq<R_key_value<int,
                    int>>>& b3) mutable {
                      {
                        auto stmt_id = b3.key;
                        auto vid_list = b3.value;
                        int __369;
                        if (stmt_id == 14) {
                          {
                            _Collection<R_key_value<Address, R_key_value<_Seq<R_key_value<int,
                            int>>, _Set<R_key_value<int, int>>>>> __370;
                            auto __379 = _Collection<R_r1_r2_r3<Address, R_key_value<int, int>,
                            _Set<R_key_value<int, int>>>> {};
                            for (const auto& __378: vid_list) {
                              __379 = [this, &delta_tuples2] (const _Collection<R_r1_r2_r3<Address,
                              R_key_value<int, int>, _Set<R_key_value<int,
                              int>>>>& _accext) mutable {
                                return [this, &_accext, &delta_tuples2] (const R_key_value<int,
                                int>& vid) mutable {
                                  _Collection<R_r1_r2_r3<Address, R_key_value<int, int>,
                                  _Set<R_key_value<int, int>>>> __371;
                                  R_key_value<int, int> __372;
                                  __372 = nd_log_get_bound_delete_R(vid);
                                  {
                                    auto& R_A = __372.key;
                                    auto& R_B = __372.value;
                                    auto __377 = _Collection<R_r1_r2_r3<Address, R_key_value<int,
                                    int>, _Set<R_key_value<int, int>>>> {};
                                    for (const auto& __376: shuffle___SQL_SUM_AGGREGATE_1_mR1_to___SQL_SUM_AGGREGATE_2(R_r1_r2_r3<unit_t,
                                    _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>,
                                    bool> {unit_t {}, delta_tuples2, false})) {
                                      __377 = [this, &vid] (_Collection<R_r1_r2_r3<Address,
                                      R_key_value<int, int>, _Set<R_key_value<int,
                                      int>>>> _accmap) mutable {
                                        return [this, _accmap = std::move(_accmap),
                                        &vid] (const R_key_value<Address,
                                        _Set<R_r1_r2_r3<R_key_value<int, int>, int,
                                        int>>>& b3) mutable {
                                          {
                                            auto ip = b3.key;
                                            auto tuples = b3.value;
                                            _Collection<R_r1_r2_r3<Address, R_key_value<int, int>,
                                            _Set<R_key_value<int, int>>>> __373;
                                            auto __375 = _Set<R_key_value<int, int>> {};
                                            for (const auto& __374: tuples) {
                                              __375 = [this] (_Set<R_key_value<int,
                                              int>> _accmap) mutable {
                                                return [this,
                                                _accmap = std::move(_accmap)] (const R_r1_r2_r3<R_key_value<int,
                                                int>, int, int>& b3) mutable {
                                                  {
                                                    auto& vid = b3.r1;
                                                    auto& map_0 = b3.r2;
                                                    auto& map_val = b3.r3;
                                                    _accmap.insert(R_key_value<int, int> {map_0,
                                                    map_val});
                                                    return std::move(_accmap);
                                                  }
                                                };
                                              }(std::move(__375))(__374);
                                            }
                                            _accmap.insert(R_r1_r2_r3<Address, R_key_value<int,
                                            int>, _Set<R_key_value<int, int>>> {ip, vid,
                                            std::move(__375)});
                                            __373 = _accmap;
                                            ip = b3.key;
                                            tuples = b3.value;
                                            return std::move(__373);
                                          }
                                        };
                                      }(std::move(__377))(__376);
                                    }
                                    __371 = std::move(__377);
                                  }
                                  return _accext.combine(__371);
                                };
                              }(std::move(__379))(__378);
                            }
                            __370 = std::move(__379).groupBy([this] (const R_r1_r2_r3<Address,
                            R_key_value<int, int>, _Set<R_key_value<int, int>>>& b1) mutable {
                              {
                                auto& ip = b1.r1;
                                auto& vid = b1.r2;
                                auto& tuples = b1.r3;
                                return std::move(ip);
                              }
                            }, [this] (R_key_value<_Seq<R_key_value<int, int>>,
                            _Set<R_key_value<int, int>>> b2) mutable {
                              return [this, &b2] (const R_r1_r2_r3<Address, R_key_value<int, int>,
                              _Set<R_key_value<int, int>>>& b5) mutable {
                                {
                                  auto& acc_vid = b2.key;
                                  auto& acc_tuples = b2.value;
                                  {
                                    auto& ip = b5.r1;
                                    auto& vid = b5.r2;
                                    auto& tuples = b5.r3;
                                    acc_vid.insert(vid);
                                    auto __381 = _Set<R_key_value<int, int>> {};
                                    for (const auto& __380: acc_tuples.combine(tuples).groupBy([this] (const R_key_value<int,
                                    int>& tuple) mutable {
                                      return std::move(tuple);
                                    }, [this] (const unit_t& _) mutable {
                                      return [this] (const R_key_value<int, int>& _) mutable {
                                        return unit_t {};
                                      };
                                    }, unit_t {})) {
                                      __381 = [this] (_Set<R_key_value<int, int>> _accmap) mutable {
                                        return [this,
                                        _accmap = std::move(_accmap)] (const R_key_value<R_key_value<int,
                                        int>, unit_t>& x) mutable {
                                          _accmap.insert(x.key);
                                          return std::move(_accmap);
                                        };
                                      }(std::move(__381))(__380);
                                    }
                                    return R_key_value<_Seq<R_key_value<int, int>>,
                                    _Set<R_key_value<int, int>>> {acc_vid, std::move(__381)};
                                  }
                                }
                              };
                            }, R_key_value<_Seq<R_key_value<int, int>>, _Set<R_key_value<int,
                            int>>> {_Seq<R_key_value<int, int>> {}, _Set<R_key_value<int,
                            int>> {}});
                            auto& ips_vids = __370;
                            auto __384 = acc_count;
                            for (const auto& __383: ips_vids) {
                              __384 = [this, &corrective_vid, &hop, &orig_addr, &orig_stmt_id,
                              &orig_vid] (const int& acc_count) mutable {
                                return [this, &acc_count, &corrective_vid, &hop, &orig_addr,
                                &orig_stmt_id, &orig_vid] (const R_key_value<Address,
                                R_key_value<_Seq<R_key_value<int, int>>, _Set<R_key_value<int,
                                int>>>>& b3) mutable {
                                  {
                                    auto& ip = b3.key;
                                    auto& vid_send_list_tup = b3.value;
                                    auto __382 = std::make_shared<K3::ValDispatcher<R_r1_r2_r3_r4_r5_r6_r7<Address,
                                    int, R_key_value<int, int>, int, R_key_value<int, int>,
                                    _Seq<R_key_value<int, int>>, _Set<R_key_value<int,
                                    int>>>>>(R_r1_r2_r3_r4_r5_r6_r7<Address, int, R_key_value<int,
                                    int>, int, R_key_value<int, int>, _Seq<R_key_value<int, int>>,
                                    _Set<R_key_value<int, int>>> {orig_addr, orig_stmt_id, orig_vid,
                                    hop, corrective_vid, vid_send_list_tup.key,
                                    vid_send_list_tup.value});
                                    __engine.send(ip,
                                    __delete_R_rcv_corrective_s14_m___SQL_SUM_AGGREGATE_1_mR1_tid,
                                    __382, me);
                                    return acc_count + 1;
                                  }
                                };
                              }(std::move(__384))(__383);
                            }
                            __369 = std::move(__384);
                          }
                        } else {
                          if (stmt_id == 12) {
                            {
                              _Collection<R_key_value<Address, R_key_value<_Seq<R_key_value<int,
                              int>>, _Set<R_key_value<int, int>>>>> __385;
                              auto __394 = _Collection<R_r1_r2_r3<Address, R_key_value<int, int>,
                              _Set<R_key_value<int, int>>>> {};
                              for (const auto& __393: vid_list) {
                                __394 = [this,
                                &delta_tuples2] (const _Collection<R_r1_r2_r3<Address,
                                R_key_value<int, int>, _Set<R_key_value<int,
                                int>>>>& _accext) mutable {
                                  return [this, &_accext, &delta_tuples2] (const R_key_value<int,
                                  int>& vid) mutable {
                                    _Collection<R_r1_r2_r3<Address, R_key_value<int, int>,
                                    _Set<R_key_value<int, int>>>> __386;
                                    R_key_value<int, int> __387;
                                    __387 = nd_log_get_bound_delete_R(vid);
                                    {
                                      auto& R_A = __387.key;
                                      auto& R_B = __387.value;
                                      auto __392 = _Collection<R_r1_r2_r3<Address, R_key_value<int,
                                      int>, _Set<R_key_value<int, int>>>> {};
                                      for (const auto& __391: shuffle___SQL_SUM_AGGREGATE_1_mR1_to___SQL_SUM_AGGREGATE_1(R_r1_r2_r3<unit_t,
                                      _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>,
                                      bool> {unit_t {}, delta_tuples2, false})) {
                                        __392 = [this, &vid] (_Collection<R_r1_r2_r3<Address,
                                        R_key_value<int, int>, _Set<R_key_value<int,
                                        int>>>> _accmap) mutable {
                                          return [this, _accmap = std::move(_accmap),
                                          &vid] (const R_key_value<Address,
                                          _Set<R_r1_r2_r3<R_key_value<int, int>, int,
                                          int>>>& b3) mutable {
                                            {
                                              auto ip = b3.key;
                                              auto tuples = b3.value;
                                              _Collection<R_r1_r2_r3<Address, R_key_value<int, int>,
                                              _Set<R_key_value<int, int>>>> __388;
                                              auto __390 = _Set<R_key_value<int, int>> {};
                                              for (const auto& __389: tuples) {
                                                __390 = [this] (_Set<R_key_value<int,
                                                int>> _accmap) mutable {
                                                  return [this,
                                                  _accmap = std::move(_accmap)] (const R_r1_r2_r3<R_key_value<int,
                                                  int>, int, int>& b3) mutable {
                                                    {
                                                      auto& vid = b3.r1;
                                                      auto& map_0 = b3.r2;
                                                      auto& map_val = b3.r3;
                                                      _accmap.insert(R_key_value<int, int> {map_0,
                                                      map_val});
                                                      return std::move(_accmap);
                                                    }
                                                  };
                                                }(std::move(__390))(__389);
                                              }
                                              _accmap.insert(R_r1_r2_r3<Address, R_key_value<int,
                                              int>, _Set<R_key_value<int, int>>> {ip, vid,
                                              std::move(__390)});
                                              __388 = _accmap;
                                              ip = b3.key;
                                              tuples = b3.value;
                                              return std::move(__388);
                                            }
                                          };
                                        }(std::move(__392))(__391);
                                      }
                                      __386 = std::move(__392);
                                    }
                                    return _accext.combine(__386);
                                  };
                                }(std::move(__394))(__393);
                              }
                              __385 = std::move(__394).groupBy([this] (const R_r1_r2_r3<Address,
                              R_key_value<int, int>, _Set<R_key_value<int, int>>>& b1) mutable {
                                {
                                  auto& ip = b1.r1;
                                  auto& vid = b1.r2;
                                  auto& tuples = b1.r3;
                                  return std::move(ip);
                                }
                              }, [this] (R_key_value<_Seq<R_key_value<int, int>>,
                              _Set<R_key_value<int, int>>> b2) mutable {
                                return [this, &b2] (const R_r1_r2_r3<Address, R_key_value<int, int>,
                                _Set<R_key_value<int, int>>>& b5) mutable {
                                  {
                                    auto& acc_vid = b2.key;
                                    auto& acc_tuples = b2.value;
                                    {
                                      auto& ip = b5.r1;
                                      auto& vid = b5.r2;
                                      auto& tuples = b5.r3;
                                      acc_vid.insert(vid);
                                      auto __396 = _Set<R_key_value<int, int>> {};
                                      for (const auto& __395: acc_tuples.combine(tuples).groupBy([this] (const R_key_value<int,
                                      int>& tuple) mutable {
                                        return std::move(tuple);
                                      }, [this] (const unit_t& _) mutable {
                                        return [this] (const R_key_value<int, int>& _) mutable {
                                          return unit_t {};
                                        };
                                      }, unit_t {})) {
                                        __396 = [this] (_Set<R_key_value<int,
                                        int>> _accmap) mutable {
                                          return [this,
                                          _accmap = std::move(_accmap)] (const R_key_value<R_key_value<int,
                                          int>, unit_t>& x) mutable {
                                            _accmap.insert(x.key);
                                            return std::move(_accmap);
                                          };
                                        }(std::move(__396))(__395);
                                      }
                                      return R_key_value<_Seq<R_key_value<int, int>>,
                                      _Set<R_key_value<int, int>>> {acc_vid, std::move(__396)};
                                    }
                                  }
                                };
                              }, R_key_value<_Seq<R_key_value<int, int>>, _Set<R_key_value<int,
                              int>>> {_Seq<R_key_value<int, int>> {}, _Set<R_key_value<int,
                              int>> {}});
                              auto& ips_vids = __385;
                              auto __399 = acc_count;
                              for (const auto& __398: ips_vids) {
                                __399 = [this, &corrective_vid, &hop, &orig_addr, &orig_stmt_id,
                                &orig_vid] (const int& acc_count) mutable {
                                  return [this, &acc_count, &corrective_vid, &hop, &orig_addr,
                                  &orig_stmt_id, &orig_vid] (const R_key_value<Address,
                                  R_key_value<_Seq<R_key_value<int, int>>, _Set<R_key_value<int,
                                  int>>>>& b3) mutable {
                                    {
                                      auto& ip = b3.key;
                                      auto& vid_send_list_tup = b3.value;
                                      auto __397 = std::make_shared<K3::ValDispatcher<R_r1_r2_r3_r4_r5_r6_r7<Address,
                                      int, R_key_value<int, int>, int, R_key_value<int, int>,
                                      _Seq<R_key_value<int, int>>, _Set<R_key_value<int,
                                      int>>>>>(R_r1_r2_r3_r4_r5_r6_r7<Address, int, R_key_value<int,
                                      int>, int, R_key_value<int, int>, _Seq<R_key_value<int, int>>,
                                      _Set<R_key_value<int, int>>> {orig_addr, orig_stmt_id,
                                      orig_vid, hop, corrective_vid, vid_send_list_tup.key,
                                      vid_send_list_tup.value});
                                      __engine.send(ip,
                                      __delete_R_rcv_corrective_s12_m___SQL_SUM_AGGREGATE_1_mR1_tid,
                                      __397, me);
                                      return acc_count + 1;
                                    }
                                  };
                                }(std::move(__399))(__398);
                              }
                              __369 = std::move(__399);
                            }
                          } else {
                            if (stmt_id == 10) {
                              {
                                _Collection<R_key_value<Address, R_key_value<_Seq<R_key_value<int,
                                int>>, _Set<R_key_value<int, int>>>>> __400;
                                auto __409 = _Collection<R_r1_r2_r3<Address, R_key_value<int, int>,
                                _Set<R_key_value<int, int>>>> {};
                                for (const auto& __408: vid_list) {
                                  __409 = [this,
                                  &delta_tuples2] (const _Collection<R_r1_r2_r3<Address,
                                  R_key_value<int, int>, _Set<R_key_value<int,
                                  int>>>>& _accext) mutable {
                                    return [this, &_accext, &delta_tuples2] (const R_key_value<int,
                                    int>& vid) mutable {
                                      _Collection<R_r1_r2_r3<Address, R_key_value<int, int>,
                                      _Set<R_key_value<int, int>>>> __401;
                                      R_key_value<int, int> __402;
                                      __402 = nd_log_get_bound_insert_R(vid);
                                      {
                                        auto& R_A = __402.key;
                                        auto& R_B = __402.value;
                                        auto __407 = _Collection<R_r1_r2_r3<Address,
                                        R_key_value<int, int>, _Set<R_key_value<int, int>>>> {};
                                        for (const auto& __406: shuffle___SQL_SUM_AGGREGATE_1_mR1_to___SQL_SUM_AGGREGATE_2(R_r1_r2_r3<unit_t,
                                        _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>,
                                        bool> {unit_t {}, delta_tuples2, false})) {
                                          __407 = [this, &vid] (_Collection<R_r1_r2_r3<Address,
                                          R_key_value<int, int>, _Set<R_key_value<int,
                                          int>>>> _accmap) mutable {
                                            return [this, _accmap = std::move(_accmap),
                                            &vid] (const R_key_value<Address,
                                            _Set<R_r1_r2_r3<R_key_value<int, int>, int,
                                            int>>>& b3) mutable {
                                              {
                                                auto ip = b3.key;
                                                auto tuples = b3.value;
                                                _Collection<R_r1_r2_r3<Address, R_key_value<int,
                                                int>, _Set<R_key_value<int, int>>>> __403;
                                                auto __405 = _Set<R_key_value<int, int>> {};
                                                for (const auto& __404: tuples) {
                                                  __405 = [this] (_Set<R_key_value<int,
                                                  int>> _accmap) mutable {
                                                    return [this,
                                                    _accmap = std::move(_accmap)] (const R_r1_r2_r3<R_key_value<int,
                                                    int>, int, int>& b3) mutable {
                                                      {
                                                        auto& vid = b3.r1;
                                                        auto& map_0 = b3.r2;
                                                        auto& map_val = b3.r3;
                                                        _accmap.insert(R_key_value<int, int> {map_0,
                                                        map_val});
                                                        return std::move(_accmap);
                                                      }
                                                    };
                                                  }(std::move(__405))(__404);
                                                }
                                                _accmap.insert(R_r1_r2_r3<Address, R_key_value<int,
                                                int>, _Set<R_key_value<int, int>>> {ip, vid,
                                                std::move(__405)});
                                                __403 = _accmap;
                                                ip = b3.key;
                                                tuples = b3.value;
                                                return std::move(__403);
                                              }
                                            };
                                          }(std::move(__407))(__406);
                                        }
                                        __401 = std::move(__407);
                                      }
                                      return _accext.combine(__401);
                                    };
                                  }(std::move(__409))(__408);
                                }
                                __400 = std::move(__409).groupBy([this] (const R_r1_r2_r3<Address,
                                R_key_value<int, int>, _Set<R_key_value<int, int>>>& b1) mutable {
                                  {
                                    auto& ip = b1.r1;
                                    auto& vid = b1.r2;
                                    auto& tuples = b1.r3;
                                    return std::move(ip);
                                  }
                                }, [this] (R_key_value<_Seq<R_key_value<int, int>>,
                                _Set<R_key_value<int, int>>> b2) mutable {
                                  return [this, &b2] (const R_r1_r2_r3<Address, R_key_value<int,
                                  int>, _Set<R_key_value<int, int>>>& b5) mutable {
                                    {
                                      auto& acc_vid = b2.key;
                                      auto& acc_tuples = b2.value;
                                      {
                                        auto& ip = b5.r1;
                                        auto& vid = b5.r2;
                                        auto& tuples = b5.r3;
                                        acc_vid.insert(vid);
                                        auto __411 = _Set<R_key_value<int, int>> {};
                                        for (const auto& __410: acc_tuples.combine(tuples).groupBy([this] (const R_key_value<int,
                                        int>& tuple) mutable {
                                          return std::move(tuple);
                                        }, [this] (const unit_t& _) mutable {
                                          return [this] (const R_key_value<int, int>& _) mutable {
                                            return unit_t {};
                                          };
                                        }, unit_t {})) {
                                          __411 = [this] (_Set<R_key_value<int,
                                          int>> _accmap) mutable {
                                            return [this,
                                            _accmap = std::move(_accmap)] (const R_key_value<R_key_value<int,
                                            int>, unit_t>& x) mutable {
                                              _accmap.insert(x.key);
                                              return std::move(_accmap);
                                            };
                                          }(std::move(__411))(__410);
                                        }
                                        return R_key_value<_Seq<R_key_value<int, int>>,
                                        _Set<R_key_value<int, int>>> {acc_vid, std::move(__411)};
                                      }
                                    }
                                  };
                                }, R_key_value<_Seq<R_key_value<int, int>>, _Set<R_key_value<int,
                                int>>> {_Seq<R_key_value<int, int>> {}, _Set<R_key_value<int,
                                int>> {}});
                                auto& ips_vids = __400;
                                auto __414 = acc_count;
                                for (const auto& __413: ips_vids) {
                                  __414 = [this, &corrective_vid, &hop, &orig_addr, &orig_stmt_id,
                                  &orig_vid] (const int& acc_count) mutable {
                                    return [this, &acc_count, &corrective_vid, &hop, &orig_addr,
                                    &orig_stmt_id, &orig_vid] (const R_key_value<Address,
                                    R_key_value<_Seq<R_key_value<int, int>>, _Set<R_key_value<int,
                                    int>>>>& b3) mutable {
                                      {
                                        auto& ip = b3.key;
                                        auto& vid_send_list_tup = b3.value;
                                        auto __412 = std::make_shared<K3::ValDispatcher<R_r1_r2_r3_r4_r5_r6_r7<Address,
                                        int, R_key_value<int, int>, int, R_key_value<int, int>,
                                        _Seq<R_key_value<int, int>>, _Set<R_key_value<int,
                                        int>>>>>(R_r1_r2_r3_r4_r5_r6_r7<Address, int,
                                        R_key_value<int, int>, int, R_key_value<int, int>,
                                        _Seq<R_key_value<int, int>>, _Set<R_key_value<int,
                                        int>>> {orig_addr, orig_stmt_id, orig_vid, hop,
                                        corrective_vid, vid_send_list_tup.key,
                                        vid_send_list_tup.value});
                                        __engine.send(ip,
                                        __insert_R_rcv_corrective_s10_m___SQL_SUM_AGGREGATE_1_mR1_tid,
                                        __412, me);
                                        return acc_count + 1;
                                      }
                                    };
                                  }(std::move(__414))(__413);
                                }
                                __369 = std::move(__414);
                              }
                            } else {
                              if (stmt_id == 8) {
                                {
                                  _Collection<R_key_value<Address, R_key_value<_Seq<R_key_value<int,
                                  int>>, _Set<R_key_value<int, int>>>>> __415;
                                  auto __424 = _Collection<R_r1_r2_r3<Address, R_key_value<int,
                                  int>, _Set<R_key_value<int, int>>>> {};
                                  for (const auto& __423: vid_list) {
                                    __424 = [this,
                                    &delta_tuples2] (const _Collection<R_r1_r2_r3<Address,
                                    R_key_value<int, int>, _Set<R_key_value<int,
                                    int>>>>& _accext) mutable {
                                      return [this, &_accext,
                                      &delta_tuples2] (const R_key_value<int, int>& vid) mutable {
                                        _Collection<R_r1_r2_r3<Address, R_key_value<int, int>,
                                        _Set<R_key_value<int, int>>>> __416;
                                        R_key_value<int, int> __417;
                                        __417 = nd_log_get_bound_insert_R(vid);
                                        {
                                          auto& R_A = __417.key;
                                          auto& R_B = __417.value;
                                          auto __422 = _Collection<R_r1_r2_r3<Address,
                                          R_key_value<int, int>, _Set<R_key_value<int, int>>>> {};
                                          for (const auto& __421: shuffle___SQL_SUM_AGGREGATE_1_mR1_to___SQL_SUM_AGGREGATE_1(R_r1_r2_r3<unit_t,
                                          _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>,
                                          bool> {unit_t {}, delta_tuples2, false})) {
                                            __422 = [this, &vid] (_Collection<R_r1_r2_r3<Address,
                                            R_key_value<int, int>, _Set<R_key_value<int,
                                            int>>>> _accmap) mutable {
                                              return [this, _accmap = std::move(_accmap),
                                              &vid] (const R_key_value<Address,
                                              _Set<R_r1_r2_r3<R_key_value<int, int>, int,
                                              int>>>& b3) mutable {
                                                {
                                                  auto ip = b3.key;
                                                  auto tuples = b3.value;
                                                  _Collection<R_r1_r2_r3<Address, R_key_value<int,
                                                  int>, _Set<R_key_value<int, int>>>> __418;
                                                  auto __420 = _Set<R_key_value<int, int>> {};
                                                  for (const auto& __419: tuples) {
                                                    __420 = [this] (_Set<R_key_value<int,
                                                    int>> _accmap) mutable {
                                                      return [this,
                                                      _accmap = std::move(_accmap)] (const R_r1_r2_r3<R_key_value<int,
                                                      int>, int, int>& b3) mutable {
                                                        {
                                                          auto& vid = b3.r1;
                                                          auto& map_0 = b3.r2;
                                                          auto& map_val = b3.r3;
                                                          _accmap.insert(R_key_value<int,
                                                          int> {map_0, map_val});
                                                          return std::move(_accmap);
                                                        }
                                                      };
                                                    }(std::move(__420))(__419);
                                                  }
                                                  _accmap.insert(R_r1_r2_r3<Address,
                                                  R_key_value<int, int>, _Set<R_key_value<int,
                                                  int>>> {ip, vid, std::move(__420)});
                                                  __418 = _accmap;
                                                  ip = b3.key;
                                                  tuples = b3.value;
                                                  return std::move(__418);
                                                }
                                              };
                                            }(std::move(__422))(__421);
                                          }
                                          __416 = std::move(__422);
                                        }
                                        return _accext.combine(__416);
                                      };
                                    }(std::move(__424))(__423);
                                  }
                                  __415 = std::move(__424).groupBy([this] (const R_r1_r2_r3<Address,
                                  R_key_value<int, int>, _Set<R_key_value<int, int>>>& b1) mutable {
                                    {
                                      auto& ip = b1.r1;
                                      auto& vid = b1.r2;
                                      auto& tuples = b1.r3;
                                      return std::move(ip);
                                    }
                                  }, [this] (R_key_value<_Seq<R_key_value<int, int>>,
                                  _Set<R_key_value<int, int>>> b2) mutable {
                                    return [this, &b2] (const R_r1_r2_r3<Address, R_key_value<int,
                                    int>, _Set<R_key_value<int, int>>>& b5) mutable {
                                      {
                                        auto& acc_vid = b2.key;
                                        auto& acc_tuples = b2.value;
                                        {
                                          auto& ip = b5.r1;
                                          auto& vid = b5.r2;
                                          auto& tuples = b5.r3;
                                          acc_vid.insert(vid);
                                          auto __426 = _Set<R_key_value<int, int>> {};
                                          for (const auto& __425: acc_tuples.combine(tuples).groupBy([this] (const R_key_value<int,
                                          int>& tuple) mutable {
                                            return std::move(tuple);
                                          }, [this] (const unit_t& _) mutable {
                                            return [this] (const R_key_value<int, int>& _) mutable {
                                              return unit_t {};
                                            };
                                          }, unit_t {})) {
                                            __426 = [this] (_Set<R_key_value<int,
                                            int>> _accmap) mutable {
                                              return [this,
                                              _accmap = std::move(_accmap)] (const R_key_value<R_key_value<int,
                                              int>, unit_t>& x) mutable {
                                                _accmap.insert(x.key);
                                                return std::move(_accmap);
                                              };
                                            }(std::move(__426))(__425);
                                          }
                                          return R_key_value<_Seq<R_key_value<int, int>>,
                                          _Set<R_key_value<int, int>>> {acc_vid, std::move(__426)};
                                        }
                                      }
                                    };
                                  }, R_key_value<_Seq<R_key_value<int, int>>, _Set<R_key_value<int,
                                  int>>> {_Seq<R_key_value<int, int>> {}, _Set<R_key_value<int,
                                  int>> {}});
                                  auto& ips_vids = __415;
                                  auto __429 = acc_count;
                                  for (const auto& __428: ips_vids) {
                                    __429 = [this, &corrective_vid, &hop, &orig_addr, &orig_stmt_id,
                                    &orig_vid] (const int& acc_count) mutable {
                                      return [this, &acc_count, &corrective_vid, &hop, &orig_addr,
                                      &orig_stmt_id, &orig_vid] (const R_key_value<Address,
                                      R_key_value<_Seq<R_key_value<int, int>>, _Set<R_key_value<int,
                                      int>>>>& b3) mutable {
                                        {
                                          auto& ip = b3.key;
                                          auto& vid_send_list_tup = b3.value;
                                          auto __427 = std::make_shared<K3::ValDispatcher<R_r1_r2_r3_r4_r5_r6_r7<Address,
                                          int, R_key_value<int, int>, int, R_key_value<int, int>,
                                          _Seq<R_key_value<int, int>>, _Set<R_key_value<int,
                                          int>>>>>(R_r1_r2_r3_r4_r5_r6_r7<Address, int,
                                          R_key_value<int, int>, int, R_key_value<int, int>,
                                          _Seq<R_key_value<int, int>>, _Set<R_key_value<int,
                                          int>>> {orig_addr, orig_stmt_id, orig_vid, hop,
                                          corrective_vid, vid_send_list_tup.key,
                                          vid_send_list_tup.value});
                                          __engine.send(ip,
                                          __insert_R_rcv_corrective_s8_m___SQL_SUM_AGGREGATE_1_mR1_tid,
                                          __427, me);
                                          return acc_count + 1;
                                        }
                                      };
                                    }(std::move(__429))(__428);
                                  }
                                  __369 = std::move(__429);
                                }
                              } else {
                                __369 = acc_count;
                              }
                            }
                          }
                        }
                        stmt_id = b3.key;
                        vid_list = b3.value;
                        return std::move(__369);
                      }
                    };
                  }(std::move(__431))(__430);
                }
                __362 = std::move(__431);
              }
            }
          }
          orig_addr = b1.r1;
          orig_stmt_id = b1.r2;
          orig_vid = b1.r3;
          hop = b1.r4;
          corrective_vid = b1.r5;
          delta_tuples = b1.r6;
          return __362;
        }
      }
      unit_t sw_insert_S(const R_key_value<int, int>& args)  {
        sw_buf_insert_S.insert(args);
        sw_trig_buf_idx.insert(R_i<int> {0});
        sw_need_vid_cntr = sw_need_vid_cntr + 1;
        return unit_t {};
      }
      unit_t sw_insert_S_send_fetch(const R_key_value<int, int>& vid)  {
        shared_ptr<R_key_value<int, int>> __432;
        __432 = sw_buf_insert_S.peek(unit_t {});
        if (__432) {
          R_key_value<int, int>& args = *__432;
          sw_buf_insert_S.erase(args);
          {
            auto& S_B = args.key;
            auto& S_C = args.value;
            route_to_int(R_key_value<int, shared_ptr<int>> {3,
            make_shared<int>(S_B)}).iterate([this, &S_B, &S_C,
            &vid] (const R_i<Address>& b1) mutable {
              {
                auto& ip = b1.i;
                auto __433 = std::make_shared<K3::ValDispatcher<R_r1_r2_r3<R_key_value<int, int>,
                int, int>>>(R_r1_r2_r3<R_key_value<int, int>, int, int> {vid, S_B, S_C});
                __engine.send(ip, __nd_insert_S_do_complete_s1_trig_tid, __433, me);
                return unit_t {};
              }
            });
            route_to_int(R_key_value<int, shared_ptr<int>> {6,
            make_shared<int>(S_B)}).iterate([this, &S_B, &S_C,
            &vid] (const R_i<Address>& b1) mutable {
              {
                auto& ip = b1.i;
                auto __434 = std::make_shared<K3::ValDispatcher<R_r1_r2_r3<R_key_value<int, int>,
                int, int>>>(R_r1_r2_r3<R_key_value<int, int>, int, int> {vid, S_B, S_C});
                __engine.send(ip, __nd_insert_S_do_complete_s3_trig_tid, __434, me);
                return unit_t {};
              }
            });
            _Collection<R_r1_r2_r3<Address, int, int>> __435;
            {
              int __436;
              auto __438 = 0;
              for (const auto& __437: route_to_int(R_key_value<int, shared_ptr<int>> {2,
              make_shared<int>(S_B)})) {
                __438 = [this] (const int& count) mutable {
                  return [this, &count] (const R_i<Address>& b3) mutable {
                    {
                      auto& ip = b3.i;
                      return count + 1;
                    }
                  };
                }(std::move(__438))(__437);
              }
              __436 = std::move(__438);
              auto& sender_count = __436;
              auto __440 = _Collection<R_r1_r2_r3<Address, int, int>> {};
              for (const auto& __439: shuffle___SQL_SUM_AGGREGATE_1_mS1_to___SQL_SUM_AGGREGATE_1(R_r1_r2_r3<unit_t,
              _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>, bool> {unit_t {},
              _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> {}, true})) {
                __440 = [this, &sender_count] (_Collection<R_r1_r2_r3<Address, int,
                int>> _accmap) mutable {
                  return [this, _accmap = std::move(_accmap),
                  &sender_count] (const R_key_value<Address, _Set<R_r1_r2_r3<R_key_value<int, int>,
                  int, int>>>& b3) mutable {
                    {
                      auto& ip = b3.key;
                      auto& tuples = b3.value;
                      _accmap.insert(R_r1_r2_r3<Address, int, int> {ip, 0, sender_count});
                      return std::move(_accmap);
                    }
                  };
                }(std::move(__440))(__439);
              }
              _Collection<R_r1_r2_r3<Address, int, int>> __441;
              {
                int __442;
                auto __444 = 0;
                for (const auto& __443: route_to_int(R_key_value<int, shared_ptr<int>> {2,
                make_shared<int>(S_B)})) {
                  __444 = [this] (const int& count) mutable {
                    return [this, &count] (const R_i<Address>& b3) mutable {
                      {
                        auto& ip = b3.i;
                        return count + 1;
                      }
                    };
                  }(std::move(__444))(__443);
                }
                __442 = std::move(__444);
                auto& sender_count = __442;
                auto __446 = _Collection<R_r1_r2_r3<Address, int, int>> {};
                for (const auto& __445: shuffle___SQL_SUM_AGGREGATE_1_mS1_to___SQL_SUM_AGGREGATE_2(R_r1_r2_r3<unit_t,
                _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>, bool> {unit_t {},
                _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> {}, true})) {
                  __446 = [this, &sender_count] (_Collection<R_r1_r2_r3<Address, int,
                  int>> _accmap) mutable {
                    return [this, _accmap = std::move(_accmap),
                    &sender_count] (const R_key_value<Address, _Set<R_r1_r2_r3<R_key_value<int,
                    int>, int, int>>>& b3) mutable {
                      {
                        auto& ip = b3.key;
                        auto& tuples = b3.value;
                        _accmap.insert(R_r1_r2_r3<Address, int, int> {ip, 2, sender_count});
                        return std::move(_accmap);
                      }
                    };
                  }(std::move(__446))(__445);
                }
                _Collection<R_r1_r2_r3<Address, int, int>> __447;
                {
                  int __448;
                  auto __450 = 0;
                  for (const auto& __449: route_to_int(R_key_value<int, shared_ptr<int>> {5,
                  make_shared<int>(S_B)})) {
                    __450 = [this] (const int& count) mutable {
                      return [this, &count] (const R_i<Address>& b3) mutable {
                        {
                          auto& ip = b3.i;
                          return count + 1;
                        }
                      };
                    }(std::move(__450))(__449);
                  }
                  __448 = std::move(__450);
                  auto& sender_count = __448;
                  auto __452 = _Collection<R_r1_r2_r3<Address, int, int>> {};
                  for (const auto& __451: shuffle___SQL_SUM_AGGREGATE_2_mS3_to___SQL_SUM_AGGREGATE_2(R_r1_r2_r3<unit_t,
                  _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>, bool> {unit_t {},
                  _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> {}, true})) {
                    __452 = [this, &sender_count] (_Collection<R_r1_r2_r3<Address, int,
                    int>> _accmap) mutable {
                      return [this, _accmap = std::move(_accmap),
                      &sender_count] (const R_key_value<Address, _Set<R_r1_r2_r3<R_key_value<int,
                      int>, int, int>>>& b3) mutable {
                        {
                          auto& ip = b3.key;
                          auto& tuples = b3.value;
                          _accmap.insert(R_r1_r2_r3<Address, int, int> {ip, 2, sender_count});
                          return std::move(_accmap);
                        }
                      };
                    }(std::move(__452))(__451);
                  }
                  __447 = std::move(__452);
                }
                __441 = std::move(__446).combine(__447);
              }
              __435 = std::move(__440).combine(__441);
            }
            (_Collection<R_r1_r2_r3<Address, int,
            int>> {}).combine(__435).groupBy([this] (const R_r1_r2_r3<Address, int,
            int>& b1) mutable {
              {
                auto& ip = b1.r1;
                auto& stmt_id = b1.r2;
                auto& count = b1.r3;
                return R_key_value<Address, int> {ip, stmt_id};
              }
            }, [this] (const int& acc) mutable {
              return [this, &acc] (const R_r1_r2_r3<Address, int, int>& b3) mutable {
                {
                  auto& ip = b3.r1;
                  auto& stmt_id = b3.r2;
                  auto& count = b3.r3;
                  return acc + count;
                }
              };
            }, 0).groupBy([this] (const R_key_value<R_key_value<Address, int>, int>& b1) mutable {
              {
                auto& b2 = b1.key;
                auto& count = b1.value;
                {
                  auto& ip = b2.key;
                  auto& stmt_id = b2.value;
                  return std::move(ip);
                }
              }
            }, [this] (const _Collection<R_key_value<int, int>>& acc) mutable {
              return [this, &acc] (const R_key_value<R_key_value<Address, int>, int>& b3) mutable {
                {
                  auto& ip_and_stmt_id = b3.key;
                  auto& count = b3.value;
                  {
                    auto& ip = ip_and_stmt_id.key;
                    auto& stmt_id = ip_and_stmt_id.value;
                    _Collection<R_key_value<int, int>> __453;
                    {
                      _Collection<R_key_value<int, int>> __454;
                      __454 = _Collection<R_key_value<int, int>> {};
                      auto& __collection = __454;
                      __collection.insert(R_key_value<int, int> {stmt_id, count});
                      __453 = __collection;
                    }
                    return acc.combine(__453);
                  }
                }
              };
            }, _Collection<R_key_value<int, int>> {}).iterate([this, &S_B, &S_C,
            &vid] (const R_key_value<Address, _Collection<R_key_value<int, int>>>& b1) mutable {
              {
                auto addr = b1.key;
                auto stmt_cnt_list = b1.value;
                unit_t __455;
                auto __456 = std::make_shared<K3::ValDispatcher<R_r1_r2_r3_r4_r5<Address,
                _Collection<R_key_value<int, int>>, R_key_value<int, int>, int,
                int>>>(R_r1_r2_r3_r4_r5<Address, _Collection<R_key_value<int, int>>,
                R_key_value<int, int>, int, int> {me, stmt_cnt_list, vid, S_B, S_C});
                __engine.send(addr, __nd_insert_S_rcv_put_tid, __456, me);
                sw_num_sent = sw_num_sent + 1;
                shared_ptr<R_key_value<R_key_value<int, int>, int>> __457;
                __457 = sw_ack_log.filter([this, &vid] (const R_key_value<R_key_value<int, int>,
                int>& b1) mutable {
                  {
                    auto& key = b1.key;
                    auto& value = b1.value;
                    return key == vid;
                  }
                }).peek(unit_t {});
                if (__457) {
                  R_key_value<R_key_value<int, int>, int>& x = *__457;
                  __455 = sw_ack_log.update(x, R_key_value<R_key_value<int, int>, int> {vid,
                  x.value + 1});
                } else {
                  __455 = sw_ack_log.insert(R_key_value<R_key_value<int, int>, int> {vid, 1});
                }
                addr = b1.key;
                stmt_cnt_list = b1.value;
                return __455;
              }
            });
            auto __459 = _Collection<R_r1_r2_r3<int, int, Address>> {};
            for (const auto& __458: route_to_int(R_key_value<int, shared_ptr<int>> {5,
            make_shared<int>(S_B)})) {
              __459 = [this] (_Collection<R_r1_r2_r3<int, int, Address>> _accmap) mutable {
                return [this, _accmap = std::move(_accmap)] (const R_i<Address>& b3) mutable {
                  {
                    auto& ip = b3.i;
                    _accmap.insert(R_r1_r2_r3<int, int, Address> {2, 5, ip});
                    return std::move(_accmap);
                  }
                };
              }(std::move(__459))(__458);
            }
            auto __461 = _Collection<R_r1_r2_r3<int, int, Address>> {};
            for (const auto& __460: route_to_int(R_key_value<int, shared_ptr<int>> {2,
            make_shared<int>(S_B)})) {
              __461 = [this] (_Collection<R_r1_r2_r3<int, int, Address>> _accmap) mutable {
                return [this, _accmap = std::move(_accmap)] (const R_i<Address>& b3) mutable {
                  {
                    auto& ip = b3.i;
                    _accmap.insert(R_r1_r2_r3<int, int, Address> {2, 2, ip});
                    return std::move(_accmap);
                  }
                };
              }(std::move(__461))(__460);
            }
            auto __463 = _Collection<R_r1_r2_r3<int, int, Address>> {};
            for (const auto& __462: route_to_int(R_key_value<int, shared_ptr<int>> {2,
            make_shared<int>(S_B)})) {
              __463 = [this] (_Collection<R_r1_r2_r3<int, int, Address>> _accmap) mutable {
                return [this, _accmap = std::move(_accmap)] (const R_i<Address>& b3) mutable {
                  {
                    auto& ip = b3.i;
                    _accmap.insert(R_r1_r2_r3<int, int, Address> {0, 2, ip});
                    return std::move(_accmap);
                  }
                };
              }(std::move(__463))(__462);
            }
            return std::move(__459).combine(std::move(__461).combine(std::move(__463).combine(_Collection<R_r1_r2_r3<int,
            int, Address>> {}))).groupBy([this] (const R_r1_r2_r3<int, int, Address>& b1) mutable {
              {
                auto& stmt_id = b1.r1;
                auto& map_id = b1.r2;
                auto& ip = b1.r3;
                return std::move(ip);
              }
            }, [this] (_Collection<R_key_value<int, int>> acc) mutable {
              return [this, acc = std::move(acc)] (const R_r1_r2_r3<int, int,
              Address>& b3) mutable {
                {
                  auto& stmt_id = b3.r1;
                  auto& map_id = b3.r2;
                  auto& ip = b3.r3;
                  acc.insert(R_key_value<int, int> {stmt_id, map_id});
                  return std::move(acc);
                }
              };
            }, _Collection<R_key_value<int, int>> {}).iterate([this, &S_B, &S_C,
            &vid] (const R_key_value<Address, _Collection<R_key_value<int, int>>>& b1) mutable {
              {
                auto& ip = b1.key;
                auto& stmt_map_ids = b1.value;
                auto __464 = std::make_shared<K3::ValDispatcher<R_r1_r2_r3_r4<_Collection<R_key_value<int,
                int>>, R_key_value<int, int>, int, int>>>(R_r1_r2_r3_r4<_Collection<R_key_value<int,
                int>>, R_key_value<int, int>, int, int> {stmt_map_ids, vid, S_B, S_C});
                __engine.send(ip, __nd_insert_S_rcv_fetch_tid, __464, me);
                return unit_t {};
              }
            });
          }
        } else {
          return error<unit_t>(print("unexpected missing arguments in sw_buf_insert_S"));
        }
      }
      unit_t nd_insert_S_do_complete_s0(const R_r1_r2_r3<R_key_value<int, int>, int, int>& b1)  {
        {
          auto vid = b1.r1;
          auto S_B = b1.r2;
          auto S_C = b1.r3;
          unit_t __465;
          {
            int __466;
            int __467;
            {
              _Collection<R_key_value<int, int>> __468;
              {
                auto& __x = *map___SQL_SUM_AGGREGATE_1_mS1_s0_buf;
                auto __470 = _Collection<R_key_value<int, int>> {};
                for (const auto& __469: frontier_int_int(R_key_value<R_key_value<int, int>,
                _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>> {vid, __x.filter([this,
                &S_B] (const R_r1_r2_r3<R_key_value<int, int>, int, int>& b1) mutable {
                  {
                    auto& r1 = b1.r1;
                    auto& r2 = b1.r2;
                    auto& r3 = b1.r3;
                    return r2 == S_B;
                  }
                })})) {
                  __470 = [this] (_Collection<R_key_value<int, int>> acc) mutable {
                    return [this, acc = std::move(acc)] (const R_r1_r2_r3<R_key_value<int, int>,
                    int, int>& b3) mutable {
                      {
                        auto& vid = b3.r1;
                        auto& map_0 = b3.r2;
                        auto& map_val = b3.r3;
                        acc.insert(R_key_value<int, int> {map_0, map_val});
                        return std::move(acc);
                      }
                    };
                  }(std::move(__470))(__469);
                }
                __468 = std::move(__470);
              }
              auto& wrapped_lookup_value = __468;
              shared_ptr<R_key_value<int, int>> __471;
              __471 = wrapped_lookup_value.peek(unit_t {});
              if (__471) {
                R_key_value<int, int>& unwrapped_value = *__471;
                {
                  auto& _ = unwrapped_value.key;
                  auto& projected_field = unwrapped_value.value;
                  __467 = projected_field;
                }
              } else {
                __467 = 0;
              }
            }
            __466 = __467 * S_C;
            auto& __prod_ret__1 = __466;
            _Set<R_i<int>> __472;
            {
              _Set<R_i<int>> __473;
              __473 = _Set<R_i<int>> {};
              auto& __collection = __473;
              __collection.insert(R_i<int> {__prod_ret__1});
              __472 = __collection;
            }
            nd_add_delta_to_int(R_r1_r2_r3_r4<shared_ptr<_Set<R_key_value<R_key_value<int, int>,
            int>>>, bool, R_key_value<int, int>, _Set<R_i<int>>> {__SQL_SUM_AGGREGATE_1, false, vid,
            __472});
            __465 = nd_complete_stmt_cntr_check(R_key_value<R_key_value<int, int>, int> {vid, 0});
          }
          vid = b1.r1;
          S_B = b1.r2;
          S_C = b1.r3;
          return __465;
        }
      }
      unit_t nd_insert_S_do_complete_s2(const R_r1_r2_r3<R_key_value<int, int>, int, int>& b1)  {
        {
          auto vid = b1.r1;
          auto S_B = b1.r2;
          auto S_C = b1.r3;
          unit_t __474;
          {
            int __475;
            int __476;
            {
              _Collection<R_key_value<int, int>> __477;
              {
                auto& __x = *map___SQL_SUM_AGGREGATE_1_mS1_s2_buf;
                auto __479 = _Collection<R_key_value<int, int>> {};
                for (const auto& __478: frontier_int_int(R_key_value<R_key_value<int, int>,
                _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>> {vid, __x.filter([this,
                &S_B] (const R_r1_r2_r3<R_key_value<int, int>, int, int>& b1) mutable {
                  {
                    auto& r1 = b1.r1;
                    auto& r2 = b1.r2;
                    auto& r3 = b1.r3;
                    return r2 == S_B;
                  }
                })})) {
                  __479 = [this] (_Collection<R_key_value<int, int>> acc) mutable {
                    return [this, acc = std::move(acc)] (const R_r1_r2_r3<R_key_value<int, int>,
                    int, int>& b3) mutable {
                      {
                        auto& vid = b3.r1;
                        auto& map_0 = b3.r2;
                        auto& map_val = b3.r3;
                        acc.insert(R_key_value<int, int> {map_0, map_val});
                        return std::move(acc);
                      }
                    };
                  }(std::move(__479))(__478);
                }
                __477 = std::move(__479);
              }
              auto& wrapped_lookup_value = __477;
              shared_ptr<R_key_value<int, int>> __480;
              __480 = wrapped_lookup_value.peek(unit_t {});
              if (__480) {
                R_key_value<int, int>& unwrapped_value = *__480;
                {
                  auto& _ = unwrapped_value.key;
                  auto& projected_field = unwrapped_value.value;
                  __476 = projected_field;
                }
              } else {
                __476 = 0;
              }
            }
            int __481;
            {
              _Collection<R_key_value<int, int>> __482;
              {
                auto& __x = *map___SQL_SUM_AGGREGATE_2_mS3_s2_buf;
                auto __484 = _Collection<R_key_value<int, int>> {};
                for (const auto& __483: frontier_int_int(R_key_value<R_key_value<int, int>,
                _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>> {vid, __x.filter([this,
                &S_B] (const R_r1_r2_r3<R_key_value<int, int>, int, int>& b1) mutable {
                  {
                    auto& r1 = b1.r1;
                    auto& r2 = b1.r2;
                    auto& r3 = b1.r3;
                    return r2 == S_B;
                  }
                })})) {
                  __484 = [this] (_Collection<R_key_value<int, int>> acc) mutable {
                    return [this, acc = std::move(acc)] (const R_r1_r2_r3<R_key_value<int, int>,
                    int, int>& b3) mutable {
                      {
                        auto& vid = b3.r1;
                        auto& map_0 = b3.r2;
                        auto& map_val = b3.r3;
                        acc.insert(R_key_value<int, int> {map_0, map_val});
                        return std::move(acc);
                      }
                    };
                  }(std::move(__484))(__483);
                }
                __482 = std::move(__484);
              }
              auto& wrapped_lookup_value = __482;
              shared_ptr<R_key_value<int, int>> __485;
              __485 = wrapped_lookup_value.peek(unit_t {});
              if (__485) {
                R_key_value<int, int>& unwrapped_value = *__485;
                {
                  auto& _ = unwrapped_value.key;
                  auto& projected_field = unwrapped_value.value;
                  __481 = projected_field;
                }
              } else {
                __481 = 0;
              }
            }
            __475 = __476 + __481 * S_C;
            auto& __sum_ret__1 = __475;
            _Set<R_i<int>> __486;
            {
              _Set<R_i<int>> __487;
              __487 = _Set<R_i<int>> {};
              auto& __collection = __487;
              __collection.insert(R_i<int> {__sum_ret__1});
              __486 = __collection;
            }
            nd_add_delta_to_int(R_r1_r2_r3_r4<shared_ptr<_Set<R_key_value<R_key_value<int, int>,
            int>>>, bool, R_key_value<int, int>, _Set<R_i<int>>> {__SQL_SUM_AGGREGATE_2, false, vid,
            __486});
            __474 = nd_complete_stmt_cntr_check(R_key_value<R_key_value<int, int>, int> {vid, 2});
          }
          vid = b1.r1;
          S_B = b1.r2;
          S_C = b1.r3;
          return __474;
        }
      }
      unit_t nd_insert_S_do_complete_s1(const R_r1_r2_r3<R_key_value<int, int>, int, int>& b1)  {
        {
          auto& vid = b1.r1;
          auto& S_B = b1.r2;
          auto& S_C = b1.r3;
          {
            int __488;
            __488 = S_C;
            auto& __val_ret__2 = __488;
            _Set<R_key_value<int, int>> __489;
            {
              _Set<R_key_value<int, int>> __490;
              __490 = _Set<R_key_value<int, int>> {};
              auto& __collection = __490;
              __collection.insert(R_key_value<int, int> {S_B, __val_ret__2});
              __489 = __collection;
            }
            nd_add_delta_to_int_int(R_r1_r2_r3_r4<shared_ptr<_Set<R_r1_r2_r3<R_key_value<int, int>,
            int, int>>>, bool, R_key_value<int, int>, _Set<R_key_value<int,
            int>>> {__SQL_SUM_AGGREGATE_1_mR1, false, vid, __489});
            {
              int __491;
              _Set<R_key_value<int, int>> __492;
              {
                _Set<R_key_value<int, int>> __493;
                __493 = _Set<R_key_value<int, int>> {};
                auto& __collection = __493;
                __collection.insert(R_key_value<int, int> {S_B, __val_ret__2});
                __492 = __collection;
              }
              __491 = nd___SQL_SUM_AGGREGATE_1_mR1_send_correctives(R_r1_r2_r3_r4_r5_r6<Address,
              int, R_key_value<int, int>, int, R_key_value<int, int>, _Set<R_key_value<int,
              int>>> {me, 1, vid, 1, vid, __492});
              auto& sent_msgs = __491;
              if (sent_msgs == 0) {
                return unit_t {};
              } else {
                return nd_update_stmt_cntr_corr_map(R_r1_r2_r3_r4_r5_r6<R_key_value<int, int>, int,
                int, int, bool, bool> {vid, 1, 1, sent_msgs, true, true});
              }
            }
          }
        }
      }
      unit_t nd_insert_S_do_complete_s3(const R_r1_r2_r3<R_key_value<int, int>, int, int>& b1)  {
        {
          auto& vid = b1.r1;
          auto& S_B = b1.r2;
          auto& S_C = b1.r3;
          {
            int __494;
            __494 = 1;
            auto& __val_ret__4 = __494;
            _Set<R_key_value<int, int>> __495;
            {
              _Set<R_key_value<int, int>> __496;
              __496 = _Set<R_key_value<int, int>> {};
              auto& __collection = __496;
              __collection.insert(R_key_value<int, int> {S_B, __val_ret__4});
              __495 = __collection;
            }
            nd_add_delta_to_int_int(R_r1_r2_r3_r4<shared_ptr<_Set<R_r1_r2_r3<R_key_value<int, int>,
            int, int>>>, bool, R_key_value<int, int>, _Set<R_key_value<int,
            int>>> {__SQL_SUM_AGGREGATE_2_mR1, false, vid, __495});
            {
              int __497;
              _Set<R_key_value<int, int>> __498;
              {
                _Set<R_key_value<int, int>> __499;
                __499 = _Set<R_key_value<int, int>> {};
                auto& __collection = __499;
                __collection.insert(R_key_value<int, int> {S_B, __val_ret__4});
                __498 = __collection;
              }
              __497 = nd___SQL_SUM_AGGREGATE_2_mR1_send_correctives(R_r1_r2_r3_r4_r5_r6<Address,
              int, R_key_value<int, int>, int, R_key_value<int, int>, _Set<R_key_value<int,
              int>>> {me, 3, vid, 1, vid, __498});
              auto& sent_msgs = __497;
              if (sent_msgs == 0) {
                return unit_t {};
              } else {
                return nd_update_stmt_cntr_corr_map(R_r1_r2_r3_r4_r5_r6<R_key_value<int, int>, int,
                int, int, bool, bool> {vid, 3, 1, sent_msgs, true, true});
              }
            }
          }
        }
      }
      int insert_S_do_corrective_s0_m___SQL_SUM_AGGREGATE_1_mS1(const R_r1_r2_r3_r4_r5_r6_r7_r8<Address,
      int, R_key_value<int, int>, int, R_key_value<int, int>, int, int, _Set<R_key_value<int,
      int>>>& b1)  {
        {
          auto orig_addr = b1.r1;
          auto orig_stmt_id = b1.r2;
          auto orig_vid = b1.r3;
          auto hop = b1.r4;
          auto vid = b1.r5;
          auto S_B = b1.r6;
          auto S_C = b1.r7;
          auto delta_tuples = b1.r8;
          int __500;
          {
            _Set<R_i<int>> __501;
            auto __507 = _Set<R_i<int>> {};
            for (const auto& __506: delta_tuples) {
              __507 = [this, &S_B, &S_C] (const _Set<R_i<int>>& _accext) mutable {
                return [this, &S_B, &S_C, &_accext] (const R_key_value<int, int>& b3) mutable {
                  {
                    auto& delta___SQL_SUM_AGGREGATE_1_mSS_B = b3.key;
                    auto& delta___SQL_SUM_AGGREGATE_1_mS1 = b3.value;
                    _Set<R_i<int>> __502;
                    {
                      int __503;
                      int __504;
                      if (S_B == delta___SQL_SUM_AGGREGATE_1_mSS_B) {
                        __504 = 1;
                      } else {
                        __504 = 0;
                      }
                      __503 = __504 * delta___SQL_SUM_AGGREGATE_1_mS1 * S_C;
                      auto& __prod_ret__37 = __503;
                      {
                        _Set<R_i<int>> __505;
                        __505 = _Set<R_i<int>> {};
                        auto& __collection = __505;
                        __collection.insert(R_i<int> {__prod_ret__37});
                        __502 = __collection;
                      }
                    }
                    return _accext.combine(__502);
                  }
                };
              }(std::move(__507))(__506);
            }
            __501 = std::move(__507).filter([this] (const R_i<int>& b1) mutable {
              {
                auto& map_val = b1.i;
                return 0 != map_val;
              }
            });
            auto& new_tuples = __501;
            nd_add_delta_to_int(R_r1_r2_r3_r4<shared_ptr<_Set<R_key_value<R_key_value<int, int>,
            int>>>, bool, R_key_value<int, int>, _Set<R_i<int>>> {__SQL_SUM_AGGREGATE_1, true, vid,
            new_tuples});
            __500 = 0;
          }
          orig_addr = b1.r1;
          orig_stmt_id = b1.r2;
          orig_vid = b1.r3;
          hop = b1.r4;
          vid = b1.r5;
          S_B = b1.r6;
          S_C = b1.r7;
          delta_tuples = b1.r8;
          return __500;
        }
      }
      int insert_S_do_corrective_s2_m___SQL_SUM_AGGREGATE_1_mS1(const R_r1_r2_r3_r4_r5_r6_r7_r8<Address,
      int, R_key_value<int, int>, int, R_key_value<int, int>, int, int, _Set<R_key_value<int,
      int>>>& b1)  {
        {
          auto orig_addr = b1.r1;
          auto orig_stmt_id = b1.r2;
          auto orig_vid = b1.r3;
          auto hop = b1.r4;
          auto vid = b1.r5;
          auto S_B = b1.r6;
          auto S_C = b1.r7;
          auto delta_tuples = b1.r8;
          int __508;
          {
            _Set<R_i<int>> __509;
            auto __515 = _Set<R_i<int>> {};
            for (const auto& __514: delta_tuples) {
              __515 = [this, &S_B] (const _Set<R_i<int>>& _accext) mutable {
                return [this, &S_B, &_accext] (const R_key_value<int, int>& b3) mutable {
                  {
                    auto& delta___SQL_SUM_AGGREGATE_1_mSS_B = b3.key;
                    auto& delta___SQL_SUM_AGGREGATE_1_mS1 = b3.value;
                    _Set<R_i<int>> __510;
                    {
                      int __511;
                      int __512;
                      if (S_B == delta___SQL_SUM_AGGREGATE_1_mSS_B) {
                        __512 = 1;
                      } else {
                        __512 = 0;
                      }
                      __511 = __512 * delta___SQL_SUM_AGGREGATE_1_mS1;
                      auto& __prod_ret__38 = __511;
                      {
                        _Set<R_i<int>> __513;
                        __513 = _Set<R_i<int>> {};
                        auto& __collection = __513;
                        __collection.insert(R_i<int> {__prod_ret__38});
                        __510 = __collection;
                      }
                    }
                    return _accext.combine(__510);
                  }
                };
              }(std::move(__515))(__514);
            }
            __509 = std::move(__515).filter([this] (const R_i<int>& b1) mutable {
              {
                auto& map_val = b1.i;
                return 0 != map_val;
              }
            });
            auto& new_tuples = __509;
            nd_add_delta_to_int(R_r1_r2_r3_r4<shared_ptr<_Set<R_key_value<R_key_value<int, int>,
            int>>>, bool, R_key_value<int, int>, _Set<R_i<int>>> {__SQL_SUM_AGGREGATE_2, true, vid,
            new_tuples});
            __508 = 0;
          }
          orig_addr = b1.r1;
          orig_stmt_id = b1.r2;
          orig_vid = b1.r3;
          hop = b1.r4;
          vid = b1.r5;
          S_B = b1.r6;
          S_C = b1.r7;
          delta_tuples = b1.r8;
          return __508;
        }
      }
      int insert_S_do_corrective_s2_m___SQL_SUM_AGGREGATE_2_mS3(const R_r1_r2_r3_r4_r5_r6_r7_r8<Address,
      int, R_key_value<int, int>, int, R_key_value<int, int>, int, int, _Set<R_key_value<int,
      int>>>& b1)  {
        {
          auto orig_addr = b1.r1;
          auto orig_stmt_id = b1.r2;
          auto orig_vid = b1.r3;
          auto hop = b1.r4;
          auto vid = b1.r5;
          auto S_B = b1.r6;
          auto S_C = b1.r7;
          auto delta_tuples = b1.r8;
          int __516;
          {
            _Set<R_i<int>> __517;
            auto __523 = _Set<R_i<int>> {};
            for (const auto& __522: delta_tuples) {
              __523 = [this, &S_B, &S_C] (const _Set<R_i<int>>& _accext) mutable {
                return [this, &S_B, &S_C, &_accext] (const R_key_value<int, int>& b3) mutable {
                  {
                    auto& delta___SQL_SUM_AGGREGATE_2_mSS_B = b3.key;
                    auto& delta___SQL_SUM_AGGREGATE_2_mS3 = b3.value;
                    _Set<R_i<int>> __518;
                    {
                      int __519;
                      int __520;
                      if (S_B == delta___SQL_SUM_AGGREGATE_2_mSS_B) {
                        __520 = 1;
                      } else {
                        __520 = 0;
                      }
                      __519 = __520 * delta___SQL_SUM_AGGREGATE_2_mS3 * S_C;
                      auto& __prod_ret__43 = __519;
                      {
                        _Set<R_i<int>> __521;
                        __521 = _Set<R_i<int>> {};
                        auto& __collection = __521;
                        __collection.insert(R_i<int> {__prod_ret__43});
                        __518 = __collection;
                      }
                    }
                    return _accext.combine(__518);
                  }
                };
              }(std::move(__523))(__522);
            }
            __517 = std::move(__523).filter([this] (const R_i<int>& b1) mutable {
              {
                auto& map_val = b1.i;
                return 0 != map_val;
              }
            });
            auto& new_tuples = __517;
            nd_add_delta_to_int(R_r1_r2_r3_r4<shared_ptr<_Set<R_key_value<R_key_value<int, int>,
            int>>>, bool, R_key_value<int, int>, _Set<R_i<int>>> {__SQL_SUM_AGGREGATE_2, true, vid,
            new_tuples});
            __516 = 0;
          }
          orig_addr = b1.r1;
          orig_stmt_id = b1.r2;
          orig_vid = b1.r3;
          hop = b1.r4;
          vid = b1.r5;
          S_B = b1.r6;
          S_C = b1.r7;
          delta_tuples = b1.r8;
          return __516;
        }
      }
      unit_t sw_delete_S(const R_key_value<int, int>& args)  {
        sw_buf_delete_S.insert(args);
        sw_trig_buf_idx.insert(R_i<int> {1});
        sw_need_vid_cntr = sw_need_vid_cntr + 1;
        return unit_t {};
      }
      unit_t sw_delete_S_send_fetch(const R_key_value<int, int>& vid)  {
        shared_ptr<R_key_value<int, int>> __524;
        __524 = sw_buf_delete_S.peek(unit_t {});
        if (__524) {
          R_key_value<int, int>& args = *__524;
          sw_buf_delete_S.erase(args);
          {
            auto& S_B = args.key;
            auto& S_C = args.value;
            route_to_int(R_key_value<int, shared_ptr<int>> {3,
            make_shared<int>(S_B)}).iterate([this, &S_B, &S_C,
            &vid] (const R_i<Address>& b1) mutable {
              {
                auto& ip = b1.i;
                auto __525 = std::make_shared<K3::ValDispatcher<R_r1_r2_r3<R_key_value<int, int>,
                int, int>>>(R_r1_r2_r3<R_key_value<int, int>, int, int> {vid, S_B, S_C});
                __engine.send(ip, __nd_delete_S_do_complete_s5_trig_tid, __525, me);
                return unit_t {};
              }
            });
            route_to_int(R_key_value<int, shared_ptr<int>> {6,
            make_shared<int>(S_B)}).iterate([this, &S_B, &S_C,
            &vid] (const R_i<Address>& b1) mutable {
              {
                auto& ip = b1.i;
                auto __526 = std::make_shared<K3::ValDispatcher<R_r1_r2_r3<R_key_value<int, int>,
                int, int>>>(R_r1_r2_r3<R_key_value<int, int>, int, int> {vid, S_B, S_C});
                __engine.send(ip, __nd_delete_S_do_complete_s7_trig_tid, __526, me);
                return unit_t {};
              }
            });
            _Collection<R_r1_r2_r3<Address, int, int>> __527;
            {
              int __528;
              auto __530 = 0;
              for (const auto& __529: route_to_int(R_key_value<int, shared_ptr<int>> {2,
              make_shared<int>(S_B)})) {
                __530 = [this] (const int& count) mutable {
                  return [this, &count] (const R_i<Address>& b3) mutable {
                    {
                      auto& ip = b3.i;
                      return count + 1;
                    }
                  };
                }(std::move(__530))(__529);
              }
              __528 = std::move(__530);
              auto& sender_count = __528;
              auto __532 = _Collection<R_r1_r2_r3<Address, int, int>> {};
              for (const auto& __531: shuffle___SQL_SUM_AGGREGATE_1_mS1_to___SQL_SUM_AGGREGATE_1(R_r1_r2_r3<unit_t,
              _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>, bool> {unit_t {},
              _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> {}, true})) {
                __532 = [this, &sender_count] (_Collection<R_r1_r2_r3<Address, int,
                int>> _accmap) mutable {
                  return [this, _accmap = std::move(_accmap),
                  &sender_count] (const R_key_value<Address, _Set<R_r1_r2_r3<R_key_value<int, int>,
                  int, int>>>& b3) mutable {
                    {
                      auto& ip = b3.key;
                      auto& tuples = b3.value;
                      _accmap.insert(R_r1_r2_r3<Address, int, int> {ip, 4, sender_count});
                      return std::move(_accmap);
                    }
                  };
                }(std::move(__532))(__531);
              }
              _Collection<R_r1_r2_r3<Address, int, int>> __533;
              {
                int __534;
                auto __536 = 0;
                for (const auto& __535: route_to_int(R_key_value<int, shared_ptr<int>> {2,
                make_shared<int>(S_B)})) {
                  __536 = [this] (const int& count) mutable {
                    return [this, &count] (const R_i<Address>& b3) mutable {
                      {
                        auto& ip = b3.i;
                        return count + 1;
                      }
                    };
                  }(std::move(__536))(__535);
                }
                __534 = std::move(__536);
                auto& sender_count = __534;
                auto __538 = _Collection<R_r1_r2_r3<Address, int, int>> {};
                for (const auto& __537: shuffle___SQL_SUM_AGGREGATE_1_mS1_to___SQL_SUM_AGGREGATE_2(R_r1_r2_r3<unit_t,
                _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>, bool> {unit_t {},
                _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> {}, true})) {
                  __538 = [this, &sender_count] (_Collection<R_r1_r2_r3<Address, int,
                  int>> _accmap) mutable {
                    return [this, _accmap = std::move(_accmap),
                    &sender_count] (const R_key_value<Address, _Set<R_r1_r2_r3<R_key_value<int,
                    int>, int, int>>>& b3) mutable {
                      {
                        auto& ip = b3.key;
                        auto& tuples = b3.value;
                        _accmap.insert(R_r1_r2_r3<Address, int, int> {ip, 6, sender_count});
                        return std::move(_accmap);
                      }
                    };
                  }(std::move(__538))(__537);
                }
                _Collection<R_r1_r2_r3<Address, int, int>> __539;
                {
                  int __540;
                  auto __542 = 0;
                  for (const auto& __541: route_to_int(R_key_value<int, shared_ptr<int>> {5,
                  make_shared<int>(S_B)})) {
                    __542 = [this] (const int& count) mutable {
                      return [this, &count] (const R_i<Address>& b3) mutable {
                        {
                          auto& ip = b3.i;
                          return count + 1;
                        }
                      };
                    }(std::move(__542))(__541);
                  }
                  __540 = std::move(__542);
                  auto& sender_count = __540;
                  auto __544 = _Collection<R_r1_r2_r3<Address, int, int>> {};
                  for (const auto& __543: shuffle___SQL_SUM_AGGREGATE_2_mS3_to___SQL_SUM_AGGREGATE_2(R_r1_r2_r3<unit_t,
                  _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>, bool> {unit_t {},
                  _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> {}, true})) {
                    __544 = [this, &sender_count] (_Collection<R_r1_r2_r3<Address, int,
                    int>> _accmap) mutable {
                      return [this, _accmap = std::move(_accmap),
                      &sender_count] (const R_key_value<Address, _Set<R_r1_r2_r3<R_key_value<int,
                      int>, int, int>>>& b3) mutable {
                        {
                          auto& ip = b3.key;
                          auto& tuples = b3.value;
                          _accmap.insert(R_r1_r2_r3<Address, int, int> {ip, 6, sender_count});
                          return std::move(_accmap);
                        }
                      };
                    }(std::move(__544))(__543);
                  }
                  __539 = std::move(__544);
                }
                __533 = std::move(__538).combine(__539);
              }
              __527 = std::move(__532).combine(__533);
            }
            (_Collection<R_r1_r2_r3<Address, int,
            int>> {}).combine(__527).groupBy([this] (const R_r1_r2_r3<Address, int,
            int>& b1) mutable {
              {
                auto& ip = b1.r1;
                auto& stmt_id = b1.r2;
                auto& count = b1.r3;
                return R_key_value<Address, int> {ip, stmt_id};
              }
            }, [this] (const int& acc) mutable {
              return [this, &acc] (const R_r1_r2_r3<Address, int, int>& b3) mutable {
                {
                  auto& ip = b3.r1;
                  auto& stmt_id = b3.r2;
                  auto& count = b3.r3;
                  return acc + count;
                }
              };
            }, 0).groupBy([this] (const R_key_value<R_key_value<Address, int>, int>& b1) mutable {
              {
                auto& b2 = b1.key;
                auto& count = b1.value;
                {
                  auto& ip = b2.key;
                  auto& stmt_id = b2.value;
                  return std::move(ip);
                }
              }
            }, [this] (const _Collection<R_key_value<int, int>>& acc) mutable {
              return [this, &acc] (const R_key_value<R_key_value<Address, int>, int>& b3) mutable {
                {
                  auto& ip_and_stmt_id = b3.key;
                  auto& count = b3.value;
                  {
                    auto& ip = ip_and_stmt_id.key;
                    auto& stmt_id = ip_and_stmt_id.value;
                    _Collection<R_key_value<int, int>> __545;
                    {
                      _Collection<R_key_value<int, int>> __546;
                      __546 = _Collection<R_key_value<int, int>> {};
                      auto& __collection = __546;
                      __collection.insert(R_key_value<int, int> {stmt_id, count});
                      __545 = __collection;
                    }
                    return acc.combine(__545);
                  }
                }
              };
            }, _Collection<R_key_value<int, int>> {}).iterate([this, &S_B, &S_C,
            &vid] (const R_key_value<Address, _Collection<R_key_value<int, int>>>& b1) mutable {
              {
                auto addr = b1.key;
                auto stmt_cnt_list = b1.value;
                unit_t __547;
                auto __548 = std::make_shared<K3::ValDispatcher<R_r1_r2_r3_r4_r5<Address,
                _Collection<R_key_value<int, int>>, R_key_value<int, int>, int,
                int>>>(R_r1_r2_r3_r4_r5<Address, _Collection<R_key_value<int, int>>,
                R_key_value<int, int>, int, int> {me, stmt_cnt_list, vid, S_B, S_C});
                __engine.send(addr, __nd_delete_S_rcv_put_tid, __548, me);
                sw_num_sent = sw_num_sent + 1;
                shared_ptr<R_key_value<R_key_value<int, int>, int>> __549;
                __549 = sw_ack_log.filter([this, &vid] (const R_key_value<R_key_value<int, int>,
                int>& b1) mutable {
                  {
                    auto& key = b1.key;
                    auto& value = b1.value;
                    return key == vid;
                  }
                }).peek(unit_t {});
                if (__549) {
                  R_key_value<R_key_value<int, int>, int>& x = *__549;
                  __547 = sw_ack_log.update(x, R_key_value<R_key_value<int, int>, int> {vid,
                  x.value + 1});
                } else {
                  __547 = sw_ack_log.insert(R_key_value<R_key_value<int, int>, int> {vid, 1});
                }
                addr = b1.key;
                stmt_cnt_list = b1.value;
                return __547;
              }
            });
            auto __551 = _Collection<R_r1_r2_r3<int, int, Address>> {};
            for (const auto& __550: route_to_int(R_key_value<int, shared_ptr<int>> {5,
            make_shared<int>(S_B)})) {
              __551 = [this] (_Collection<R_r1_r2_r3<int, int, Address>> _accmap) mutable {
                return [this, _accmap = std::move(_accmap)] (const R_i<Address>& b3) mutable {
                  {
                    auto& ip = b3.i;
                    _accmap.insert(R_r1_r2_r3<int, int, Address> {6, 5, ip});
                    return std::move(_accmap);
                  }
                };
              }(std::move(__551))(__550);
            }
            auto __553 = _Collection<R_r1_r2_r3<int, int, Address>> {};
            for (const auto& __552: route_to_int(R_key_value<int, shared_ptr<int>> {2,
            make_shared<int>(S_B)})) {
              __553 = [this] (_Collection<R_r1_r2_r3<int, int, Address>> _accmap) mutable {
                return [this, _accmap = std::move(_accmap)] (const R_i<Address>& b3) mutable {
                  {
                    auto& ip = b3.i;
                    _accmap.insert(R_r1_r2_r3<int, int, Address> {6, 2, ip});
                    return std::move(_accmap);
                  }
                };
              }(std::move(__553))(__552);
            }
            auto __555 = _Collection<R_r1_r2_r3<int, int, Address>> {};
            for (const auto& __554: route_to_int(R_key_value<int, shared_ptr<int>> {2,
            make_shared<int>(S_B)})) {
              __555 = [this] (_Collection<R_r1_r2_r3<int, int, Address>> _accmap) mutable {
                return [this, _accmap = std::move(_accmap)] (const R_i<Address>& b3) mutable {
                  {
                    auto& ip = b3.i;
                    _accmap.insert(R_r1_r2_r3<int, int, Address> {4, 2, ip});
                    return std::move(_accmap);
                  }
                };
              }(std::move(__555))(__554);
            }
            return std::move(__551).combine(std::move(__553).combine(std::move(__555).combine(_Collection<R_r1_r2_r3<int,
            int, Address>> {}))).groupBy([this] (const R_r1_r2_r3<int, int, Address>& b1) mutable {
              {
                auto& stmt_id = b1.r1;
                auto& map_id = b1.r2;
                auto& ip = b1.r3;
                return std::move(ip);
              }
            }, [this] (_Collection<R_key_value<int, int>> acc) mutable {
              return [this, acc = std::move(acc)] (const R_r1_r2_r3<int, int,
              Address>& b3) mutable {
                {
                  auto& stmt_id = b3.r1;
                  auto& map_id = b3.r2;
                  auto& ip = b3.r3;
                  acc.insert(R_key_value<int, int> {stmt_id, map_id});
                  return std::move(acc);
                }
              };
            }, _Collection<R_key_value<int, int>> {}).iterate([this, &S_B, &S_C,
            &vid] (const R_key_value<Address, _Collection<R_key_value<int, int>>>& b1) mutable {
              {
                auto& ip = b1.key;
                auto& stmt_map_ids = b1.value;
                auto __556 = std::make_shared<K3::ValDispatcher<R_r1_r2_r3_r4<_Collection<R_key_value<int,
                int>>, R_key_value<int, int>, int, int>>>(R_r1_r2_r3_r4<_Collection<R_key_value<int,
                int>>, R_key_value<int, int>, int, int> {stmt_map_ids, vid, S_B, S_C});
                __engine.send(ip, __nd_delete_S_rcv_fetch_tid, __556, me);
                return unit_t {};
              }
            });
          }
        } else {
          return error<unit_t>(print("unexpected missing arguments in sw_buf_delete_S"));
        }
      }
      unit_t nd_delete_S_do_complete_s4(const R_r1_r2_r3<R_key_value<int, int>, int, int>& b1)  {
        {
          auto vid = b1.r1;
          auto S_B = b1.r2;
          auto S_C = b1.r3;
          unit_t __557;
          {
            int __558;
            int __559;
            {
              _Collection<R_key_value<int, int>> __560;
              {
                auto& __x = *map___SQL_SUM_AGGREGATE_1_mS1_s4_buf;
                auto __562 = _Collection<R_key_value<int, int>> {};
                for (const auto& __561: frontier_int_int(R_key_value<R_key_value<int, int>,
                _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>> {vid, __x.filter([this,
                &S_B] (const R_r1_r2_r3<R_key_value<int, int>, int, int>& b1) mutable {
                  {
                    auto& r1 = b1.r1;
                    auto& r2 = b1.r2;
                    auto& r3 = b1.r3;
                    return r2 == S_B;
                  }
                })})) {
                  __562 = [this] (_Collection<R_key_value<int, int>> acc) mutable {
                    return [this, acc = std::move(acc)] (const R_r1_r2_r3<R_key_value<int, int>,
                    int, int>& b3) mutable {
                      {
                        auto& vid = b3.r1;
                        auto& map_0 = b3.r2;
                        auto& map_val = b3.r3;
                        acc.insert(R_key_value<int, int> {map_0, map_val});
                        return std::move(acc);
                      }
                    };
                  }(std::move(__562))(__561);
                }
                __560 = std::move(__562);
              }
              auto& wrapped_lookup_value = __560;
              shared_ptr<R_key_value<int, int>> __563;
              __563 = wrapped_lookup_value.peek(unit_t {});
              if (__563) {
                R_key_value<int, int>& unwrapped_value = *__563;
                {
                  auto& _ = unwrapped_value.key;
                  auto& projected_field = unwrapped_value.value;
                  __559 = projected_field;
                }
              } else {
                __559 = 0;
              }
            }
            __558 = __559 * (-S_C);
            auto& __prod_ret__4 = __558;
            _Set<R_i<int>> __564;
            {
              _Set<R_i<int>> __565;
              __565 = _Set<R_i<int>> {};
              auto& __collection = __565;
              __collection.insert(R_i<int> {__prod_ret__4});
              __564 = __collection;
            }
            nd_add_delta_to_int(R_r1_r2_r3_r4<shared_ptr<_Set<R_key_value<R_key_value<int, int>,
            int>>>, bool, R_key_value<int, int>, _Set<R_i<int>>> {__SQL_SUM_AGGREGATE_1, false, vid,
            __564});
            __557 = nd_complete_stmt_cntr_check(R_key_value<R_key_value<int, int>, int> {vid, 4});
          }
          vid = b1.r1;
          S_B = b1.r2;
          S_C = b1.r3;
          return __557;
        }
      }
      unit_t nd_delete_S_do_complete_s6(const R_r1_r2_r3<R_key_value<int, int>, int, int>& b1)  {
        {
          auto vid = b1.r1;
          auto S_B = b1.r2;
          auto S_C = b1.r3;
          unit_t __566;
          {
            int __567;
            int __568;
            {
              _Collection<R_key_value<int, int>> __569;
              {
                auto& __x = *map___SQL_SUM_AGGREGATE_1_mS1_s6_buf;
                auto __571 = _Collection<R_key_value<int, int>> {};
                for (const auto& __570: frontier_int_int(R_key_value<R_key_value<int, int>,
                _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>> {vid, __x.filter([this,
                &S_B] (const R_r1_r2_r3<R_key_value<int, int>, int, int>& b1) mutable {
                  {
                    auto& r1 = b1.r1;
                    auto& r2 = b1.r2;
                    auto& r3 = b1.r3;
                    return r2 == S_B;
                  }
                })})) {
                  __571 = [this] (_Collection<R_key_value<int, int>> acc) mutable {
                    return [this, acc = std::move(acc)] (const R_r1_r2_r3<R_key_value<int, int>,
                    int, int>& b3) mutable {
                      {
                        auto& vid = b3.r1;
                        auto& map_0 = b3.r2;
                        auto& map_val = b3.r3;
                        acc.insert(R_key_value<int, int> {map_0, map_val});
                        return std::move(acc);
                      }
                    };
                  }(std::move(__571))(__570);
                }
                __569 = std::move(__571);
              }
              auto& wrapped_lookup_value = __569;
              shared_ptr<R_key_value<int, int>> __572;
              __572 = wrapped_lookup_value.peek(unit_t {});
              if (__572) {
                R_key_value<int, int>& unwrapped_value = *__572;
                {
                  auto& _ = unwrapped_value.key;
                  auto& projected_field = unwrapped_value.value;
                  __568 = projected_field;
                }
              } else {
                __568 = 0;
              }
            }
            int __573;
            {
              _Collection<R_key_value<int, int>> __574;
              {
                auto& __x = *map___SQL_SUM_AGGREGATE_2_mS3_s6_buf;
                auto __576 = _Collection<R_key_value<int, int>> {};
                for (const auto& __575: frontier_int_int(R_key_value<R_key_value<int, int>,
                _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>> {vid, __x.filter([this,
                &S_B] (const R_r1_r2_r3<R_key_value<int, int>, int, int>& b1) mutable {
                  {
                    auto& r1 = b1.r1;
                    auto& r2 = b1.r2;
                    auto& r3 = b1.r3;
                    return r2 == S_B;
                  }
                })})) {
                  __576 = [this] (_Collection<R_key_value<int, int>> acc) mutable {
                    return [this, acc = std::move(acc)] (const R_r1_r2_r3<R_key_value<int, int>,
                    int, int>& b3) mutable {
                      {
                        auto& vid = b3.r1;
                        auto& map_0 = b3.r2;
                        auto& map_val = b3.r3;
                        acc.insert(R_key_value<int, int> {map_0, map_val});
                        return std::move(acc);
                      }
                    };
                  }(std::move(__576))(__575);
                }
                __574 = std::move(__576);
              }
              auto& wrapped_lookup_value = __574;
              shared_ptr<R_key_value<int, int>> __577;
              __577 = wrapped_lookup_value.peek(unit_t {});
              if (__577) {
                R_key_value<int, int>& unwrapped_value = *__577;
                {
                  auto& _ = unwrapped_value.key;
                  auto& projected_field = unwrapped_value.value;
                  __573 = projected_field;
                }
              } else {
                __573 = 0;
              }
            }
            __567 = (__568 + __573 * S_C) * (-(1));
            auto& __prod_ret__7 = __567;
            _Set<R_i<int>> __578;
            {
              _Set<R_i<int>> __579;
              __579 = _Set<R_i<int>> {};
              auto& __collection = __579;
              __collection.insert(R_i<int> {__prod_ret__7});
              __578 = __collection;
            }
            nd_add_delta_to_int(R_r1_r2_r3_r4<shared_ptr<_Set<R_key_value<R_key_value<int, int>,
            int>>>, bool, R_key_value<int, int>, _Set<R_i<int>>> {__SQL_SUM_AGGREGATE_2, false, vid,
            __578});
            __566 = nd_complete_stmt_cntr_check(R_key_value<R_key_value<int, int>, int> {vid, 6});
          }
          vid = b1.r1;
          S_B = b1.r2;
          S_C = b1.r3;
          return __566;
        }
      }
      unit_t nd_delete_S_do_complete_s5(const R_r1_r2_r3<R_key_value<int, int>, int, int>& b1)  {
        {
          auto& vid = b1.r1;
          auto& S_B = b1.r2;
          auto& S_C = b1.r3;
          {
            int __580;
            __580 = -S_C;
            auto& __prod_ret__5 = __580;
            _Set<R_key_value<int, int>> __581;
            {
              _Set<R_key_value<int, int>> __582;
              __582 = _Set<R_key_value<int, int>> {};
              auto& __collection = __582;
              __collection.insert(R_key_value<int, int> {S_B, __prod_ret__5});
              __581 = __collection;
            }
            nd_add_delta_to_int_int(R_r1_r2_r3_r4<shared_ptr<_Set<R_r1_r2_r3<R_key_value<int, int>,
            int, int>>>, bool, R_key_value<int, int>, _Set<R_key_value<int,
            int>>> {__SQL_SUM_AGGREGATE_1_mR1, false, vid, __581});
            {
              int __583;
              _Set<R_key_value<int, int>> __584;
              {
                _Set<R_key_value<int, int>> __585;
                __585 = _Set<R_key_value<int, int>> {};
                auto& __collection = __585;
                __collection.insert(R_key_value<int, int> {S_B, __prod_ret__5});
                __584 = __collection;
              }
              __583 = nd___SQL_SUM_AGGREGATE_1_mR1_send_correctives(R_r1_r2_r3_r4_r5_r6<Address,
              int, R_key_value<int, int>, int, R_key_value<int, int>, _Set<R_key_value<int,
              int>>> {me, 5, vid, 1, vid, __584});
              auto& sent_msgs = __583;
              if (sent_msgs == 0) {
                return unit_t {};
              } else {
                return nd_update_stmt_cntr_corr_map(R_r1_r2_r3_r4_r5_r6<R_key_value<int, int>, int,
                int, int, bool, bool> {vid, 5, 1, sent_msgs, true, true});
              }
            }
          }
        }
      }
      unit_t nd_delete_S_do_complete_s7(const R_r1_r2_r3<R_key_value<int, int>, int, int>& b1)  {
        {
          auto& vid = b1.r1;
          auto& S_B = b1.r2;
          auto& S_C = b1.r3;
          {
            int __586;
            __586 = -(1);
            auto& __val_ret__11 = __586;
            _Set<R_key_value<int, int>> __587;
            {
              _Set<R_key_value<int, int>> __588;
              __588 = _Set<R_key_value<int, int>> {};
              auto& __collection = __588;
              __collection.insert(R_key_value<int, int> {S_B, __val_ret__11});
              __587 = __collection;
            }
            nd_add_delta_to_int_int(R_r1_r2_r3_r4<shared_ptr<_Set<R_r1_r2_r3<R_key_value<int, int>,
            int, int>>>, bool, R_key_value<int, int>, _Set<R_key_value<int,
            int>>> {__SQL_SUM_AGGREGATE_2_mR1, false, vid, __587});
            {
              int __589;
              _Set<R_key_value<int, int>> __590;
              {
                _Set<R_key_value<int, int>> __591;
                __591 = _Set<R_key_value<int, int>> {};
                auto& __collection = __591;
                __collection.insert(R_key_value<int, int> {S_B, __val_ret__11});
                __590 = __collection;
              }
              __589 = nd___SQL_SUM_AGGREGATE_2_mR1_send_correctives(R_r1_r2_r3_r4_r5_r6<Address,
              int, R_key_value<int, int>, int, R_key_value<int, int>, _Set<R_key_value<int,
              int>>> {me, 7, vid, 1, vid, __590});
              auto& sent_msgs = __589;
              if (sent_msgs == 0) {
                return unit_t {};
              } else {
                return nd_update_stmt_cntr_corr_map(R_r1_r2_r3_r4_r5_r6<R_key_value<int, int>, int,
                int, int, bool, bool> {vid, 7, 1, sent_msgs, true, true});
              }
            }
          }
        }
      }
      int delete_S_do_corrective_s4_m___SQL_SUM_AGGREGATE_1_mS1(const R_r1_r2_r3_r4_r5_r6_r7_r8<Address,
      int, R_key_value<int, int>, int, R_key_value<int, int>, int, int, _Set<R_key_value<int,
      int>>>& b1)  {
        {
          auto orig_addr = b1.r1;
          auto orig_stmt_id = b1.r2;
          auto orig_vid = b1.r3;
          auto hop = b1.r4;
          auto vid = b1.r5;
          auto S_B = b1.r6;
          auto S_C = b1.r7;
          auto delta_tuples = b1.r8;
          int __592;
          {
            _Set<R_i<int>> __593;
            auto __599 = _Set<R_i<int>> {};
            for (const auto& __598: delta_tuples) {
              __599 = [this, &S_B, &S_C] (const _Set<R_i<int>>& _accext) mutable {
                return [this, &S_B, &S_C, &_accext] (const R_key_value<int, int>& b3) mutable {
                  {
                    auto& delta___SQL_SUM_AGGREGATE_1_mSS_B = b3.key;
                    auto& delta___SQL_SUM_AGGREGATE_1_mS1 = b3.value;
                    _Set<R_i<int>> __594;
                    {
                      int __595;
                      int __596;
                      if (S_B == delta___SQL_SUM_AGGREGATE_1_mSS_B) {
                        __596 = 1;
                      } else {
                        __596 = 0;
                      }
                      __595 = __596 * (-delta___SQL_SUM_AGGREGATE_1_mS1) * S_C;
                      auto& __prod_ret__46 = __595;
                      {
                        _Set<R_i<int>> __597;
                        __597 = _Set<R_i<int>> {};
                        auto& __collection = __597;
                        __collection.insert(R_i<int> {__prod_ret__46});
                        __594 = __collection;
                      }
                    }
                    return _accext.combine(__594);
                  }
                };
              }(std::move(__599))(__598);
            }
            __593 = std::move(__599).filter([this] (const R_i<int>& b1) mutable {
              {
                auto& map_val = b1.i;
                return 0 != map_val;
              }
            });
            auto& new_tuples = __593;
            nd_add_delta_to_int(R_r1_r2_r3_r4<shared_ptr<_Set<R_key_value<R_key_value<int, int>,
            int>>>, bool, R_key_value<int, int>, _Set<R_i<int>>> {__SQL_SUM_AGGREGATE_1, true, vid,
            new_tuples});
            __592 = 0;
          }
          orig_addr = b1.r1;
          orig_stmt_id = b1.r2;
          orig_vid = b1.r3;
          hop = b1.r4;
          vid = b1.r5;
          S_B = b1.r6;
          S_C = b1.r7;
          delta_tuples = b1.r8;
          return __592;
        }
      }
      int delete_S_do_corrective_s6_m___SQL_SUM_AGGREGATE_1_mS1(const R_r1_r2_r3_r4_r5_r6_r7_r8<Address,
      int, R_key_value<int, int>, int, R_key_value<int, int>, int, int, _Set<R_key_value<int,
      int>>>& b1)  {
        {
          auto orig_addr = b1.r1;
          auto orig_stmt_id = b1.r2;
          auto orig_vid = b1.r3;
          auto hop = b1.r4;
          auto vid = b1.r5;
          auto S_B = b1.r6;
          auto S_C = b1.r7;
          auto delta_tuples = b1.r8;
          int __600;
          {
            _Set<R_i<int>> __601;
            auto __607 = _Set<R_i<int>> {};
            for (const auto& __606: delta_tuples) {
              __607 = [this, &S_B] (const _Set<R_i<int>>& _accext) mutable {
                return [this, &S_B, &_accext] (const R_key_value<int, int>& b3) mutable {
                  {
                    auto& delta___SQL_SUM_AGGREGATE_1_mSS_B = b3.key;
                    auto& delta___SQL_SUM_AGGREGATE_1_mS1 = b3.value;
                    _Set<R_i<int>> __602;
                    {
                      int __603;
                      int __604;
                      if (S_B == delta___SQL_SUM_AGGREGATE_1_mSS_B) {
                        __604 = 1;
                      } else {
                        __604 = 0;
                      }
                      __603 = __604 * (-delta___SQL_SUM_AGGREGATE_1_mS1);
                      auto& __prod_ret__48 = __603;
                      {
                        _Set<R_i<int>> __605;
                        __605 = _Set<R_i<int>> {};
                        auto& __collection = __605;
                        __collection.insert(R_i<int> {__prod_ret__48});
                        __602 = __collection;
                      }
                    }
                    return _accext.combine(__602);
                  }
                };
              }(std::move(__607))(__606);
            }
            __601 = std::move(__607).filter([this] (const R_i<int>& b1) mutable {
              {
                auto& map_val = b1.i;
                return 0 != map_val;
              }
            });
            auto& new_tuples = __601;
            nd_add_delta_to_int(R_r1_r2_r3_r4<shared_ptr<_Set<R_key_value<R_key_value<int, int>,
            int>>>, bool, R_key_value<int, int>, _Set<R_i<int>>> {__SQL_SUM_AGGREGATE_2, true, vid,
            new_tuples});
            __600 = 0;
          }
          orig_addr = b1.r1;
          orig_stmt_id = b1.r2;
          orig_vid = b1.r3;
          hop = b1.r4;
          vid = b1.r5;
          S_B = b1.r6;
          S_C = b1.r7;
          delta_tuples = b1.r8;
          return __600;
        }
      }
      int delete_S_do_corrective_s6_m___SQL_SUM_AGGREGATE_2_mS3(const R_r1_r2_r3_r4_r5_r6_r7_r8<Address,
      int, R_key_value<int, int>, int, R_key_value<int, int>, int, int, _Set<R_key_value<int,
      int>>>& b1)  {
        {
          auto orig_addr = b1.r1;
          auto orig_stmt_id = b1.r2;
          auto orig_vid = b1.r3;
          auto hop = b1.r4;
          auto vid = b1.r5;
          auto S_B = b1.r6;
          auto S_C = b1.r7;
          auto delta_tuples = b1.r8;
          int __608;
          {
            _Set<R_i<int>> __609;
            auto __615 = _Set<R_i<int>> {};
            for (const auto& __614: delta_tuples) {
              __615 = [this, &S_B, &S_C] (const _Set<R_i<int>>& _accext) mutable {
                return [this, &S_B, &S_C, &_accext] (const R_key_value<int, int>& b3) mutable {
                  {
                    auto& delta___SQL_SUM_AGGREGATE_2_mSS_B = b3.key;
                    auto& delta___SQL_SUM_AGGREGATE_2_mS3 = b3.value;
                    _Set<R_i<int>> __610;
                    {
                      int __611;
                      int __612;
                      if (S_B == delta___SQL_SUM_AGGREGATE_2_mSS_B) {
                        __612 = 1;
                      } else {
                        __612 = 0;
                      }
                      __611 = __612 * (-delta___SQL_SUM_AGGREGATE_2_mS3) * S_C;
                      auto& __prod_ret__56 = __611;
                      {
                        _Set<R_i<int>> __613;
                        __613 = _Set<R_i<int>> {};
                        auto& __collection = __613;
                        __collection.insert(R_i<int> {__prod_ret__56});
                        __610 = __collection;
                      }
                    }
                    return _accext.combine(__610);
                  }
                };
              }(std::move(__615))(__614);
            }
            __609 = std::move(__615).filter([this] (const R_i<int>& b1) mutable {
              {
                auto& map_val = b1.i;
                return 0 != map_val;
              }
            });
            auto& new_tuples = __609;
            nd_add_delta_to_int(R_r1_r2_r3_r4<shared_ptr<_Set<R_key_value<R_key_value<int, int>,
            int>>>, bool, R_key_value<int, int>, _Set<R_i<int>>> {__SQL_SUM_AGGREGATE_2, true, vid,
            new_tuples});
            __608 = 0;
          }
          orig_addr = b1.r1;
          orig_stmt_id = b1.r2;
          orig_vid = b1.r3;
          hop = b1.r4;
          vid = b1.r5;
          S_B = b1.r6;
          S_C = b1.r7;
          delta_tuples = b1.r8;
          return __608;
        }
      }
      unit_t sw_insert_R(const R_key_value<int, int>& args)  {
        sw_buf_insert_R.insert(args);
        sw_trig_buf_idx.insert(R_i<int> {2});
        sw_need_vid_cntr = sw_need_vid_cntr + 1;
        return unit_t {};
      }
      unit_t sw_insert_R_send_fetch(const R_key_value<int, int>& vid)  {
        shared_ptr<R_key_value<int, int>> __616;
        __616 = sw_buf_insert_R.peek(unit_t {});
        if (__616) {
          R_key_value<int, int>& args = *__616;
          sw_buf_insert_R.erase(args);
          {
            auto& R_A = args.key;
            auto& R_B = args.value;
            route_to_int(R_key_value<int, shared_ptr<int>> {2,
            make_shared<int>(R_B)}).iterate([this, &R_A, &R_B,
            &vid] (const R_i<Address>& b1) mutable {
              {
                auto& ip = b1.i;
                auto __617 = std::make_shared<K3::ValDispatcher<R_r1_r2_r3<R_key_value<int, int>,
                int, int>>>(R_r1_r2_r3<R_key_value<int, int>, int, int> {vid, R_A, R_B});
                __engine.send(ip, __nd_insert_R_do_complete_s9_trig_tid, __617, me);
                return unit_t {};
              }
            });
            route_to_int(R_key_value<int, shared_ptr<int>> {5,
            make_shared<int>(R_B)}).iterate([this, &R_A, &R_B,
            &vid] (const R_i<Address>& b1) mutable {
              {
                auto& ip = b1.i;
                auto __618 = std::make_shared<K3::ValDispatcher<R_r1_r2_r3<R_key_value<int, int>,
                int, int>>>(R_r1_r2_r3<R_key_value<int, int>, int, int> {vid, R_A, R_B});
                __engine.send(ip, __nd_insert_R_do_complete_s11_trig_tid, __618, me);
                return unit_t {};
              }
            });
            _Collection<R_r1_r2_r3<Address, int, int>> __619;
            {
              int __620;
              auto __622 = 0;
              for (const auto& __621: route_to_int(R_key_value<int, shared_ptr<int>> {3,
              make_shared<int>(R_B)})) {
                __622 = [this] (const int& count) mutable {
                  return [this, &count] (const R_i<Address>& b3) mutable {
                    {
                      auto& ip = b3.i;
                      return count + 1;
                    }
                  };
                }(std::move(__622))(__621);
              }
              __620 = std::move(__622);
              auto& sender_count = __620;
              auto __624 = _Collection<R_r1_r2_r3<Address, int, int>> {};
              for (const auto& __623: shuffle___SQL_SUM_AGGREGATE_1_mR1_to___SQL_SUM_AGGREGATE_1(R_r1_r2_r3<unit_t,
              _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>, bool> {unit_t {},
              _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> {}, true})) {
                __624 = [this, &sender_count] (_Collection<R_r1_r2_r3<Address, int,
                int>> _accmap) mutable {
                  return [this, _accmap = std::move(_accmap),
                  &sender_count] (const R_key_value<Address, _Set<R_r1_r2_r3<R_key_value<int, int>,
                  int, int>>>& b3) mutable {
                    {
                      auto& ip = b3.key;
                      auto& tuples = b3.value;
                      _accmap.insert(R_r1_r2_r3<Address, int, int> {ip, 8, sender_count});
                      return std::move(_accmap);
                    }
                  };
                }(std::move(__624))(__623);
              }
              _Collection<R_r1_r2_r3<Address, int, int>> __625;
              {
                int __626;
                auto __628 = 0;
                for (const auto& __627: route_to_int(R_key_value<int, shared_ptr<int>> {6,
                make_shared<int>(R_B)})) {
                  __628 = [this] (const int& count) mutable {
                    return [this, &count] (const R_i<Address>& b3) mutable {
                      {
                        auto& ip = b3.i;
                        return count + 1;
                      }
                    };
                  }(std::move(__628))(__627);
                }
                __626 = std::move(__628);
                auto& sender_count = __626;
                auto __630 = _Collection<R_r1_r2_r3<Address, int, int>> {};
                for (const auto& __629: shuffle___SQL_SUM_AGGREGATE_2_mR1_to___SQL_SUM_AGGREGATE_2(R_r1_r2_r3<unit_t,
                _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>, bool> {unit_t {},
                _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> {}, true})) {
                  __630 = [this, &sender_count] (_Collection<R_r1_r2_r3<Address, int,
                  int>> _accmap) mutable {
                    return [this, _accmap = std::move(_accmap),
                    &sender_count] (const R_key_value<Address, _Set<R_r1_r2_r3<R_key_value<int,
                    int>, int, int>>>& b3) mutable {
                      {
                        auto& ip = b3.key;
                        auto& tuples = b3.value;
                        _accmap.insert(R_r1_r2_r3<Address, int, int> {ip, 10, sender_count});
                        return std::move(_accmap);
                      }
                    };
                  }(std::move(__630))(__629);
                }
                _Collection<R_r1_r2_r3<Address, int, int>> __631;
                {
                  int __632;
                  auto __634 = 0;
                  for (const auto& __633: route_to_int(R_key_value<int, shared_ptr<int>> {3,
                  make_shared<int>(R_B)})) {
                    __634 = [this] (const int& count) mutable {
                      return [this, &count] (const R_i<Address>& b3) mutable {
                        {
                          auto& ip = b3.i;
                          return count + 1;
                        }
                      };
                    }(std::move(__634))(__633);
                  }
                  __632 = std::move(__634);
                  auto& sender_count = __632;
                  auto __636 = _Collection<R_r1_r2_r3<Address, int, int>> {};
                  for (const auto& __635: shuffle___SQL_SUM_AGGREGATE_1_mR1_to___SQL_SUM_AGGREGATE_2(R_r1_r2_r3<unit_t,
                  _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>, bool> {unit_t {},
                  _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> {}, true})) {
                    __636 = [this, &sender_count] (_Collection<R_r1_r2_r3<Address, int,
                    int>> _accmap) mutable {
                      return [this, _accmap = std::move(_accmap),
                      &sender_count] (const R_key_value<Address, _Set<R_r1_r2_r3<R_key_value<int,
                      int>, int, int>>>& b3) mutable {
                        {
                          auto& ip = b3.key;
                          auto& tuples = b3.value;
                          _accmap.insert(R_r1_r2_r3<Address, int, int> {ip, 10, sender_count});
                          return std::move(_accmap);
                        }
                      };
                    }(std::move(__636))(__635);
                  }
                  __631 = std::move(__636);
                }
                __625 = std::move(__630).combine(__631);
              }
              __619 = std::move(__624).combine(__625);
            }
            (_Collection<R_r1_r2_r3<Address, int,
            int>> {}).combine(__619).groupBy([this] (const R_r1_r2_r3<Address, int,
            int>& b1) mutable {
              {
                auto& ip = b1.r1;
                auto& stmt_id = b1.r2;
                auto& count = b1.r3;
                return R_key_value<Address, int> {ip, stmt_id};
              }
            }, [this] (const int& acc) mutable {
              return [this, &acc] (const R_r1_r2_r3<Address, int, int>& b3) mutable {
                {
                  auto& ip = b3.r1;
                  auto& stmt_id = b3.r2;
                  auto& count = b3.r3;
                  return acc + count;
                }
              };
            }, 0).groupBy([this] (const R_key_value<R_key_value<Address, int>, int>& b1) mutable {
              {
                auto& b2 = b1.key;
                auto& count = b1.value;
                {
                  auto& ip = b2.key;
                  auto& stmt_id = b2.value;
                  return std::move(ip);
                }
              }
            }, [this] (const _Collection<R_key_value<int, int>>& acc) mutable {
              return [this, &acc] (const R_key_value<R_key_value<Address, int>, int>& b3) mutable {
                {
                  auto& ip_and_stmt_id = b3.key;
                  auto& count = b3.value;
                  {
                    auto& ip = ip_and_stmt_id.key;
                    auto& stmt_id = ip_and_stmt_id.value;
                    _Collection<R_key_value<int, int>> __637;
                    {
                      _Collection<R_key_value<int, int>> __638;
                      __638 = _Collection<R_key_value<int, int>> {};
                      auto& __collection = __638;
                      __collection.insert(R_key_value<int, int> {stmt_id, count});
                      __637 = __collection;
                    }
                    return acc.combine(__637);
                  }
                }
              };
            }, _Collection<R_key_value<int, int>> {}).iterate([this, &R_A, &R_B,
            &vid] (const R_key_value<Address, _Collection<R_key_value<int, int>>>& b1) mutable {
              {
                auto addr = b1.key;
                auto stmt_cnt_list = b1.value;
                unit_t __639;
                auto __640 = std::make_shared<K3::ValDispatcher<R_r1_r2_r3_r4_r5<Address,
                _Collection<R_key_value<int, int>>, R_key_value<int, int>, int,
                int>>>(R_r1_r2_r3_r4_r5<Address, _Collection<R_key_value<int, int>>,
                R_key_value<int, int>, int, int> {me, stmt_cnt_list, vid, R_A, R_B});
                __engine.send(addr, __nd_insert_R_rcv_put_tid, __640, me);
                sw_num_sent = sw_num_sent + 1;
                shared_ptr<R_key_value<R_key_value<int, int>, int>> __641;
                __641 = sw_ack_log.filter([this, &vid] (const R_key_value<R_key_value<int, int>,
                int>& b1) mutable {
                  {
                    auto& key = b1.key;
                    auto& value = b1.value;
                    return key == vid;
                  }
                }).peek(unit_t {});
                if (__641) {
                  R_key_value<R_key_value<int, int>, int>& x = *__641;
                  __639 = sw_ack_log.update(x, R_key_value<R_key_value<int, int>, int> {vid,
                  x.value + 1});
                } else {
                  __639 = sw_ack_log.insert(R_key_value<R_key_value<int, int>, int> {vid, 1});
                }
                addr = b1.key;
                stmt_cnt_list = b1.value;
                return __639;
              }
            });
            auto __643 = _Collection<R_r1_r2_r3<int, int, Address>> {};
            for (const auto& __642: route_to_int(R_key_value<int, shared_ptr<int>> {3,
            make_shared<int>(R_B)})) {
              __643 = [this] (_Collection<R_r1_r2_r3<int, int, Address>> _accmap) mutable {
                return [this, _accmap = std::move(_accmap)] (const R_i<Address>& b3) mutable {
                  {
                    auto& ip = b3.i;
                    _accmap.insert(R_r1_r2_r3<int, int, Address> {10, 3, ip});
                    return std::move(_accmap);
                  }
                };
              }(std::move(__643))(__642);
            }
            auto __645 = _Collection<R_r1_r2_r3<int, int, Address>> {};
            for (const auto& __644: route_to_int(R_key_value<int, shared_ptr<int>> {6,
            make_shared<int>(R_B)})) {
              __645 = [this] (_Collection<R_r1_r2_r3<int, int, Address>> _accmap) mutable {
                return [this, _accmap = std::move(_accmap)] (const R_i<Address>& b3) mutable {
                  {
                    auto& ip = b3.i;
                    _accmap.insert(R_r1_r2_r3<int, int, Address> {10, 6, ip});
                    return std::move(_accmap);
                  }
                };
              }(std::move(__645))(__644);
            }
            auto __647 = _Collection<R_r1_r2_r3<int, int, Address>> {};
            for (const auto& __646: route_to_int(R_key_value<int, shared_ptr<int>> {3,
            make_shared<int>(R_B)})) {
              __647 = [this] (_Collection<R_r1_r2_r3<int, int, Address>> _accmap) mutable {
                return [this, _accmap = std::move(_accmap)] (const R_i<Address>& b3) mutable {
                  {
                    auto& ip = b3.i;
                    _accmap.insert(R_r1_r2_r3<int, int, Address> {8, 3, ip});
                    return std::move(_accmap);
                  }
                };
              }(std::move(__647))(__646);
            }
            return std::move(__643).combine(std::move(__645).combine(std::move(__647).combine(_Collection<R_r1_r2_r3<int,
            int, Address>> {}))).groupBy([this] (const R_r1_r2_r3<int, int, Address>& b1) mutable {
              {
                auto& stmt_id = b1.r1;
                auto& map_id = b1.r2;
                auto& ip = b1.r3;
                return std::move(ip);
              }
            }, [this] (_Collection<R_key_value<int, int>> acc) mutable {
              return [this, acc = std::move(acc)] (const R_r1_r2_r3<int, int,
              Address>& b3) mutable {
                {
                  auto& stmt_id = b3.r1;
                  auto& map_id = b3.r2;
                  auto& ip = b3.r3;
                  acc.insert(R_key_value<int, int> {stmt_id, map_id});
                  return std::move(acc);
                }
              };
            }, _Collection<R_key_value<int, int>> {}).iterate([this, &R_A, &R_B,
            &vid] (const R_key_value<Address, _Collection<R_key_value<int, int>>>& b1) mutable {
              {
                auto& ip = b1.key;
                auto& stmt_map_ids = b1.value;
                auto __648 = std::make_shared<K3::ValDispatcher<R_r1_r2_r3_r4<_Collection<R_key_value<int,
                int>>, R_key_value<int, int>, int, int>>>(R_r1_r2_r3_r4<_Collection<R_key_value<int,
                int>>, R_key_value<int, int>, int, int> {stmt_map_ids, vid, R_A, R_B});
                __engine.send(ip, __nd_insert_R_rcv_fetch_tid, __648, me);
                return unit_t {};
              }
            });
          }
        } else {
          return error<unit_t>(print("unexpected missing arguments in sw_buf_insert_R"));
        }
      }
      unit_t nd_insert_R_do_complete_s8(const R_r1_r2_r3<R_key_value<int, int>, int, int>& b1)  {
        {
          auto vid = b1.r1;
          auto R_A = b1.r2;
          auto R_B = b1.r3;
          unit_t __649;
          {
            int __650;
            int __651;
            {
              _Collection<R_key_value<int, int>> __652;
              {
                auto& __x = *map___SQL_SUM_AGGREGATE_1_mR1_s8_buf;
                auto __654 = _Collection<R_key_value<int, int>> {};
                for (const auto& __653: frontier_int_int(R_key_value<R_key_value<int, int>,
                _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>> {vid, __x.filter([this,
                &R_B] (const R_r1_r2_r3<R_key_value<int, int>, int, int>& b1) mutable {
                  {
                    auto& r1 = b1.r1;
                    auto& r2 = b1.r2;
                    auto& r3 = b1.r3;
                    return r2 == R_B;
                  }
                })})) {
                  __654 = [this] (_Collection<R_key_value<int, int>> acc) mutable {
                    return [this, acc = std::move(acc)] (const R_r1_r2_r3<R_key_value<int, int>,
                    int, int>& b3) mutable {
                      {
                        auto& vid = b3.r1;
                        auto& map_0 = b3.r2;
                        auto& map_val = b3.r3;
                        acc.insert(R_key_value<int, int> {map_0, map_val});
                        return std::move(acc);
                      }
                    };
                  }(std::move(__654))(__653);
                }
                __652 = std::move(__654);
              }
              auto& wrapped_lookup_value = __652;
              shared_ptr<R_key_value<int, int>> __655;
              __655 = wrapped_lookup_value.peek(unit_t {});
              if (__655) {
                R_key_value<int, int>& unwrapped_value = *__655;
                {
                  auto& _ = unwrapped_value.key;
                  auto& projected_field = unwrapped_value.value;
                  __651 = projected_field;
                }
              } else {
                __651 = 0;
              }
            }
            __650 = __651 * R_A;
            auto& __prod_ret__8 = __650;
            _Set<R_i<int>> __656;
            {
              _Set<R_i<int>> __657;
              __657 = _Set<R_i<int>> {};
              auto& __collection = __657;
              __collection.insert(R_i<int> {__prod_ret__8});
              __656 = __collection;
            }
            nd_add_delta_to_int(R_r1_r2_r3_r4<shared_ptr<_Set<R_key_value<R_key_value<int, int>,
            int>>>, bool, R_key_value<int, int>, _Set<R_i<int>>> {__SQL_SUM_AGGREGATE_1, false, vid,
            __656});
            __649 = nd_complete_stmt_cntr_check(R_key_value<R_key_value<int, int>, int> {vid, 8});
          }
          vid = b1.r1;
          R_A = b1.r2;
          R_B = b1.r3;
          return __649;
        }
      }
      unit_t nd_insert_R_do_complete_s10(const R_r1_r2_r3<R_key_value<int, int>, int, int>& b1)  {
        {
          auto vid = b1.r1;
          auto R_A = b1.r2;
          auto R_B = b1.r3;
          unit_t __658;
          {
            int __659;
            int __660;
            {
              _Collection<R_key_value<int, int>> __661;
              {
                auto& __x = *map___SQL_SUM_AGGREGATE_2_mR1_s10_buf;
                auto __663 = _Collection<R_key_value<int, int>> {};
                for (const auto& __662: frontier_int_int(R_key_value<R_key_value<int, int>,
                _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>> {vid, __x.filter([this,
                &R_B] (const R_r1_r2_r3<R_key_value<int, int>, int, int>& b1) mutable {
                  {
                    auto& r1 = b1.r1;
                    auto& r2 = b1.r2;
                    auto& r3 = b1.r3;
                    return r2 == R_B;
                  }
                })})) {
                  __663 = [this] (_Collection<R_key_value<int, int>> acc) mutable {
                    return [this, acc = std::move(acc)] (const R_r1_r2_r3<R_key_value<int, int>,
                    int, int>& b3) mutable {
                      {
                        auto& vid = b3.r1;
                        auto& map_0 = b3.r2;
                        auto& map_val = b3.r3;
                        acc.insert(R_key_value<int, int> {map_0, map_val});
                        return std::move(acc);
                      }
                    };
                  }(std::move(__663))(__662);
                }
                __661 = std::move(__663);
              }
              auto& wrapped_lookup_value = __661;
              shared_ptr<R_key_value<int, int>> __664;
              __664 = wrapped_lookup_value.peek(unit_t {});
              if (__664) {
                R_key_value<int, int>& unwrapped_value = *__664;
                {
                  auto& _ = unwrapped_value.key;
                  auto& projected_field = unwrapped_value.value;
                  __660 = projected_field;
                }
              } else {
                __660 = 0;
              }
            }
            int __665;
            {
              _Collection<R_key_value<int, int>> __666;
              {
                auto& __x = *map___SQL_SUM_AGGREGATE_1_mR1_s10_buf;
                auto __668 = _Collection<R_key_value<int, int>> {};
                for (const auto& __667: frontier_int_int(R_key_value<R_key_value<int, int>,
                _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>> {vid, __x.filter([this,
                &R_B] (const R_r1_r2_r3<R_key_value<int, int>, int, int>& b1) mutable {
                  {
                    auto& r1 = b1.r1;
                    auto& r2 = b1.r2;
                    auto& r3 = b1.r3;
                    return r2 == R_B;
                  }
                })})) {
                  __668 = [this] (_Collection<R_key_value<int, int>> acc) mutable {
                    return [this, acc = std::move(acc)] (const R_r1_r2_r3<R_key_value<int, int>,
                    int, int>& b3) mutable {
                      {
                        auto& vid = b3.r1;
                        auto& map_0 = b3.r2;
                        auto& map_val = b3.r3;
                        acc.insert(R_key_value<int, int> {map_0, map_val});
                        return std::move(acc);
                      }
                    };
                  }(std::move(__668))(__667);
                }
                __666 = std::move(__668);
              }
              auto& wrapped_lookup_value = __666;
              shared_ptr<R_key_value<int, int>> __669;
              __669 = wrapped_lookup_value.peek(unit_t {});
              if (__669) {
                R_key_value<int, int>& unwrapped_value = *__669;
                {
                  auto& _ = unwrapped_value.key;
                  auto& projected_field = unwrapped_value.value;
                  __665 = projected_field;
                }
              } else {
                __665 = 0;
              }
            }
            __659 = __660 * R_A + __665;
            auto& __sum_ret__3 = __659;
            _Set<R_i<int>> __670;
            {
              _Set<R_i<int>> __671;
              __671 = _Set<R_i<int>> {};
              auto& __collection = __671;
              __collection.insert(R_i<int> {__sum_ret__3});
              __670 = __collection;
            }
            nd_add_delta_to_int(R_r1_r2_r3_r4<shared_ptr<_Set<R_key_value<R_key_value<int, int>,
            int>>>, bool, R_key_value<int, int>, _Set<R_i<int>>> {__SQL_SUM_AGGREGATE_2, false, vid,
            __670});
            __658 = nd_complete_stmt_cntr_check(R_key_value<R_key_value<int, int>, int> {vid, 10});
          }
          vid = b1.r1;
          R_A = b1.r2;
          R_B = b1.r3;
          return __658;
        }
      }
      unit_t nd_insert_R_do_complete_s9(const R_r1_r2_r3<R_key_value<int, int>, int, int>& b1)  {
        {
          auto& vid = b1.r1;
          auto& R_A = b1.r2;
          auto& R_B = b1.r3;
          {
            int __672;
            __672 = R_A;
            auto& __val_ret__13 = __672;
            _Set<R_key_value<int, int>> __673;
            {
              _Set<R_key_value<int, int>> __674;
              __674 = _Set<R_key_value<int, int>> {};
              auto& __collection = __674;
              __collection.insert(R_key_value<int, int> {R_B, __val_ret__13});
              __673 = __collection;
            }
            nd_add_delta_to_int_int(R_r1_r2_r3_r4<shared_ptr<_Set<R_r1_r2_r3<R_key_value<int, int>,
            int, int>>>, bool, R_key_value<int, int>, _Set<R_key_value<int,
            int>>> {__SQL_SUM_AGGREGATE_1_mS1, false, vid, __673});
            {
              int __675;
              _Set<R_key_value<int, int>> __676;
              {
                _Set<R_key_value<int, int>> __677;
                __677 = _Set<R_key_value<int, int>> {};
                auto& __collection = __677;
                __collection.insert(R_key_value<int, int> {R_B, __val_ret__13});
                __676 = __collection;
              }
              __675 = nd___SQL_SUM_AGGREGATE_1_mS1_send_correctives(R_r1_r2_r3_r4_r5_r6<Address,
              int, R_key_value<int, int>, int, R_key_value<int, int>, _Set<R_key_value<int,
              int>>> {me, 9, vid, 1, vid, __676});
              auto& sent_msgs = __675;
              if (sent_msgs == 0) {
                return unit_t {};
              } else {
                return nd_update_stmt_cntr_corr_map(R_r1_r2_r3_r4_r5_r6<R_key_value<int, int>, int,
                int, int, bool, bool> {vid, 9, 1, sent_msgs, true, true});
              }
            }
          }
        }
      }
      unit_t nd_insert_R_do_complete_s11(const R_r1_r2_r3<R_key_value<int, int>, int, int>& b1)  {
        {
          auto& vid = b1.r1;
          auto& R_A = b1.r2;
          auto& R_B = b1.r3;
          {
            int __678;
            __678 = 1;
            auto& __val_ret__15 = __678;
            _Set<R_key_value<int, int>> __679;
            {
              _Set<R_key_value<int, int>> __680;
              __680 = _Set<R_key_value<int, int>> {};
              auto& __collection = __680;
              __collection.insert(R_key_value<int, int> {R_B, __val_ret__15});
              __679 = __collection;
            }
            nd_add_delta_to_int_int(R_r1_r2_r3_r4<shared_ptr<_Set<R_r1_r2_r3<R_key_value<int, int>,
            int, int>>>, bool, R_key_value<int, int>, _Set<R_key_value<int,
            int>>> {__SQL_SUM_AGGREGATE_2_mS3, false, vid, __679});
            {
              int __681;
              _Set<R_key_value<int, int>> __682;
              {
                _Set<R_key_value<int, int>> __683;
                __683 = _Set<R_key_value<int, int>> {};
                auto& __collection = __683;
                __collection.insert(R_key_value<int, int> {R_B, __val_ret__15});
                __682 = __collection;
              }
              __681 = nd___SQL_SUM_AGGREGATE_2_mS3_send_correctives(R_r1_r2_r3_r4_r5_r6<Address,
              int, R_key_value<int, int>, int, R_key_value<int, int>, _Set<R_key_value<int,
              int>>> {me, 11, vid, 1, vid, __682});
              auto& sent_msgs = __681;
              if (sent_msgs == 0) {
                return unit_t {};
              } else {
                return nd_update_stmt_cntr_corr_map(R_r1_r2_r3_r4_r5_r6<R_key_value<int, int>, int,
                int, int, bool, bool> {vid, 11, 1, sent_msgs, true, true});
              }
            }
          }
        }
      }
      int insert_R_do_corrective_s8_m___SQL_SUM_AGGREGATE_1_mR1(const R_r1_r2_r3_r4_r5_r6_r7_r8<Address,
      int, R_key_value<int, int>, int, R_key_value<int, int>, int, int, _Set<R_key_value<int,
      int>>>& b1)  {
        {
          auto orig_addr = b1.r1;
          auto orig_stmt_id = b1.r2;
          auto orig_vid = b1.r3;
          auto hop = b1.r4;
          auto vid = b1.r5;
          auto R_A = b1.r6;
          auto R_B = b1.r7;
          auto delta_tuples = b1.r8;
          int __684;
          {
            _Set<R_i<int>> __685;
            auto __691 = _Set<R_i<int>> {};
            for (const auto& __690: delta_tuples) {
              __691 = [this, &R_A, &R_B] (const _Set<R_i<int>>& _accext) mutable {
                return [this, &R_A, &R_B, &_accext] (const R_key_value<int, int>& b3) mutable {
                  {
                    auto& delta___SQL_SUM_AGGREGATE_1_mRR_B = b3.key;
                    auto& delta___SQL_SUM_AGGREGATE_1_mR1 = b3.value;
                    _Set<R_i<int>> __686;
                    {
                      int __687;
                      int __688;
                      if (R_B == delta___SQL_SUM_AGGREGATE_1_mRR_B) {
                        __688 = 1;
                      } else {
                        __688 = 0;
                      }
                      __687 = __688 * delta___SQL_SUM_AGGREGATE_1_mR1 * R_A;
                      auto& __prod_ret__16 = __687;
                      {
                        _Set<R_i<int>> __689;
                        __689 = _Set<R_i<int>> {};
                        auto& __collection = __689;
                        __collection.insert(R_i<int> {__prod_ret__16});
                        __686 = __collection;
                      }
                    }
                    return _accext.combine(__686);
                  }
                };
              }(std::move(__691))(__690);
            }
            __685 = std::move(__691).filter([this] (const R_i<int>& b1) mutable {
              {
                auto& map_val = b1.i;
                return 0 != map_val;
              }
            });
            auto& new_tuples = __685;
            nd_add_delta_to_int(R_r1_r2_r3_r4<shared_ptr<_Set<R_key_value<R_key_value<int, int>,
            int>>>, bool, R_key_value<int, int>, _Set<R_i<int>>> {__SQL_SUM_AGGREGATE_1, true, vid,
            new_tuples});
            __684 = 0;
          }
          orig_addr = b1.r1;
          orig_stmt_id = b1.r2;
          orig_vid = b1.r3;
          hop = b1.r4;
          vid = b1.r5;
          R_A = b1.r6;
          R_B = b1.r7;
          delta_tuples = b1.r8;
          return __684;
        }
      }
      int insert_R_do_corrective_s10_m___SQL_SUM_AGGREGATE_2_mR1(const R_r1_r2_r3_r4_r5_r6_r7_r8<Address,
      int, R_key_value<int, int>, int, R_key_value<int, int>, int, int, _Set<R_key_value<int,
      int>>>& b1)  {
        {
          auto orig_addr = b1.r1;
          auto orig_stmt_id = b1.r2;
          auto orig_vid = b1.r3;
          auto hop = b1.r4;
          auto vid = b1.r5;
          auto R_A = b1.r6;
          auto R_B = b1.r7;
          auto delta_tuples = b1.r8;
          int __692;
          {
            _Set<R_i<int>> __693;
            auto __699 = _Set<R_i<int>> {};
            for (const auto& __698: delta_tuples) {
              __699 = [this, &R_A, &R_B] (const _Set<R_i<int>>& _accext) mutable {
                return [this, &R_A, &R_B, &_accext] (const R_key_value<int, int>& b3) mutable {
                  {
                    auto& delta___SQL_SUM_AGGREGATE_2_mRR_B = b3.key;
                    auto& delta___SQL_SUM_AGGREGATE_2_mR1 = b3.value;
                    _Set<R_i<int>> __694;
                    {
                      int __695;
                      int __696;
                      if (R_B == delta___SQL_SUM_AGGREGATE_2_mRR_B) {
                        __696 = 1;
                      } else {
                        __696 = 0;
                      }
                      __695 = __696 * delta___SQL_SUM_AGGREGATE_2_mR1 * R_A;
                      auto& __prod_ret__22 = __695;
                      {
                        _Set<R_i<int>> __697;
                        __697 = _Set<R_i<int>> {};
                        auto& __collection = __697;
                        __collection.insert(R_i<int> {__prod_ret__22});
                        __694 = __collection;
                      }
                    }
                    return _accext.combine(__694);
                  }
                };
              }(std::move(__699))(__698);
            }
            __693 = std::move(__699).filter([this] (const R_i<int>& b1) mutable {
              {
                auto& map_val = b1.i;
                return 0 != map_val;
              }
            });
            auto& new_tuples = __693;
            nd_add_delta_to_int(R_r1_r2_r3_r4<shared_ptr<_Set<R_key_value<R_key_value<int, int>,
            int>>>, bool, R_key_value<int, int>, _Set<R_i<int>>> {__SQL_SUM_AGGREGATE_2, true, vid,
            new_tuples});
            __692 = 0;
          }
          orig_addr = b1.r1;
          orig_stmt_id = b1.r2;
          orig_vid = b1.r3;
          hop = b1.r4;
          vid = b1.r5;
          R_A = b1.r6;
          R_B = b1.r7;
          delta_tuples = b1.r8;
          return __692;
        }
      }
      int insert_R_do_corrective_s10_m___SQL_SUM_AGGREGATE_1_mR1(const R_r1_r2_r3_r4_r5_r6_r7_r8<Address,
      int, R_key_value<int, int>, int, R_key_value<int, int>, int, int, _Set<R_key_value<int,
      int>>>& b1)  {
        {
          auto orig_addr = b1.r1;
          auto orig_stmt_id = b1.r2;
          auto orig_vid = b1.r3;
          auto hop = b1.r4;
          auto vid = b1.r5;
          auto R_A = b1.r6;
          auto R_B = b1.r7;
          auto delta_tuples = b1.r8;
          int __700;
          {
            _Set<R_i<int>> __701;
            auto __707 = _Set<R_i<int>> {};
            for (const auto& __706: delta_tuples) {
              __707 = [this, &R_B] (const _Set<R_i<int>>& _accext) mutable {
                return [this, &R_B, &_accext] (const R_key_value<int, int>& b3) mutable {
                  {
                    auto& delta___SQL_SUM_AGGREGATE_1_mRR_B = b3.key;
                    auto& delta___SQL_SUM_AGGREGATE_1_mR1 = b3.value;
                    _Set<R_i<int>> __702;
                    {
                      int __703;
                      int __704;
                      if (R_B == delta___SQL_SUM_AGGREGATE_1_mRR_B) {
                        __704 = 1;
                      } else {
                        __704 = 0;
                      }
                      __703 = __704 * delta___SQL_SUM_AGGREGATE_1_mR1;
                      auto& __prod_ret__17 = __703;
                      {
                        _Set<R_i<int>> __705;
                        __705 = _Set<R_i<int>> {};
                        auto& __collection = __705;
                        __collection.insert(R_i<int> {__prod_ret__17});
                        __702 = __collection;
                      }
                    }
                    return _accext.combine(__702);
                  }
                };
              }(std::move(__707))(__706);
            }
            __701 = std::move(__707).filter([this] (const R_i<int>& b1) mutable {
              {
                auto& map_val = b1.i;
                return 0 != map_val;
              }
            });
            auto& new_tuples = __701;
            nd_add_delta_to_int(R_r1_r2_r3_r4<shared_ptr<_Set<R_key_value<R_key_value<int, int>,
            int>>>, bool, R_key_value<int, int>, _Set<R_i<int>>> {__SQL_SUM_AGGREGATE_2, true, vid,
            new_tuples});
            __700 = 0;
          }
          orig_addr = b1.r1;
          orig_stmt_id = b1.r2;
          orig_vid = b1.r3;
          hop = b1.r4;
          vid = b1.r5;
          R_A = b1.r6;
          R_B = b1.r7;
          delta_tuples = b1.r8;
          return __700;
        }
      }
      unit_t sw_delete_R(const R_key_value<int, int>& args)  {
        sw_buf_delete_R.insert(args);
        sw_trig_buf_idx.insert(R_i<int> {3});
        sw_need_vid_cntr = sw_need_vid_cntr + 1;
        return unit_t {};
      }
      unit_t sw_delete_R_send_fetch(const R_key_value<int, int>& vid)  {
        shared_ptr<R_key_value<int, int>> __708;
        __708 = sw_buf_delete_R.peek(unit_t {});
        if (__708) {
          R_key_value<int, int>& args = *__708;
          sw_buf_delete_R.erase(args);
          {
            auto& R_A = args.key;
            auto& R_B = args.value;
            route_to_int(R_key_value<int, shared_ptr<int>> {2,
            make_shared<int>(R_B)}).iterate([this, &R_A, &R_B,
            &vid] (const R_i<Address>& b1) mutable {
              {
                auto& ip = b1.i;
                auto __709 = std::make_shared<K3::ValDispatcher<R_r1_r2_r3<R_key_value<int, int>,
                int, int>>>(R_r1_r2_r3<R_key_value<int, int>, int, int> {vid, R_A, R_B});
                __engine.send(ip, __nd_delete_R_do_complete_s13_trig_tid, __709, me);
                return unit_t {};
              }
            });
            route_to_int(R_key_value<int, shared_ptr<int>> {5,
            make_shared<int>(R_B)}).iterate([this, &R_A, &R_B,
            &vid] (const R_i<Address>& b1) mutable {
              {
                auto& ip = b1.i;
                auto __710 = std::make_shared<K3::ValDispatcher<R_r1_r2_r3<R_key_value<int, int>,
                int, int>>>(R_r1_r2_r3<R_key_value<int, int>, int, int> {vid, R_A, R_B});
                __engine.send(ip, __nd_delete_R_do_complete_s15_trig_tid, __710, me);
                return unit_t {};
              }
            });
            _Collection<R_r1_r2_r3<Address, int, int>> __711;
            {
              int __712;
              auto __714 = 0;
              for (const auto& __713: route_to_int(R_key_value<int, shared_ptr<int>> {3,
              make_shared<int>(R_B)})) {
                __714 = [this] (const int& count) mutable {
                  return [this, &count] (const R_i<Address>& b3) mutable {
                    {
                      auto& ip = b3.i;
                      return count + 1;
                    }
                  };
                }(std::move(__714))(__713);
              }
              __712 = std::move(__714);
              auto& sender_count = __712;
              auto __716 = _Collection<R_r1_r2_r3<Address, int, int>> {};
              for (const auto& __715: shuffle___SQL_SUM_AGGREGATE_1_mR1_to___SQL_SUM_AGGREGATE_1(R_r1_r2_r3<unit_t,
              _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>, bool> {unit_t {},
              _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> {}, true})) {
                __716 = [this, &sender_count] (_Collection<R_r1_r2_r3<Address, int,
                int>> _accmap) mutable {
                  return [this, _accmap = std::move(_accmap),
                  &sender_count] (const R_key_value<Address, _Set<R_r1_r2_r3<R_key_value<int, int>,
                  int, int>>>& b3) mutable {
                    {
                      auto& ip = b3.key;
                      auto& tuples = b3.value;
                      _accmap.insert(R_r1_r2_r3<Address, int, int> {ip, 12, sender_count});
                      return std::move(_accmap);
                    }
                  };
                }(std::move(__716))(__715);
              }
              _Collection<R_r1_r2_r3<Address, int, int>> __717;
              {
                int __718;
                auto __720 = 0;
                for (const auto& __719: route_to_int(R_key_value<int, shared_ptr<int>> {6,
                make_shared<int>(R_B)})) {
                  __720 = [this] (const int& count) mutable {
                    return [this, &count] (const R_i<Address>& b3) mutable {
                      {
                        auto& ip = b3.i;
                        return count + 1;
                      }
                    };
                  }(std::move(__720))(__719);
                }
                __718 = std::move(__720);
                auto& sender_count = __718;
                auto __722 = _Collection<R_r1_r2_r3<Address, int, int>> {};
                for (const auto& __721: shuffle___SQL_SUM_AGGREGATE_2_mR1_to___SQL_SUM_AGGREGATE_2(R_r1_r2_r3<unit_t,
                _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>, bool> {unit_t {},
                _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> {}, true})) {
                  __722 = [this, &sender_count] (_Collection<R_r1_r2_r3<Address, int,
                  int>> _accmap) mutable {
                    return [this, _accmap = std::move(_accmap),
                    &sender_count] (const R_key_value<Address, _Set<R_r1_r2_r3<R_key_value<int,
                    int>, int, int>>>& b3) mutable {
                      {
                        auto& ip = b3.key;
                        auto& tuples = b3.value;
                        _accmap.insert(R_r1_r2_r3<Address, int, int> {ip, 14, sender_count});
                        return std::move(_accmap);
                      }
                    };
                  }(std::move(__722))(__721);
                }
                _Collection<R_r1_r2_r3<Address, int, int>> __723;
                {
                  int __724;
                  auto __726 = 0;
                  for (const auto& __725: route_to_int(R_key_value<int, shared_ptr<int>> {3,
                  make_shared<int>(R_B)})) {
                    __726 = [this] (const int& count) mutable {
                      return [this, &count] (const R_i<Address>& b3) mutable {
                        {
                          auto& ip = b3.i;
                          return count + 1;
                        }
                      };
                    }(std::move(__726))(__725);
                  }
                  __724 = std::move(__726);
                  auto& sender_count = __724;
                  auto __728 = _Collection<R_r1_r2_r3<Address, int, int>> {};
                  for (const auto& __727: shuffle___SQL_SUM_AGGREGATE_1_mR1_to___SQL_SUM_AGGREGATE_2(R_r1_r2_r3<unit_t,
                  _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>, bool> {unit_t {},
                  _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> {}, true})) {
                    __728 = [this, &sender_count] (_Collection<R_r1_r2_r3<Address, int,
                    int>> _accmap) mutable {
                      return [this, _accmap = std::move(_accmap),
                      &sender_count] (const R_key_value<Address, _Set<R_r1_r2_r3<R_key_value<int,
                      int>, int, int>>>& b3) mutable {
                        {
                          auto& ip = b3.key;
                          auto& tuples = b3.value;
                          _accmap.insert(R_r1_r2_r3<Address, int, int> {ip, 14, sender_count});
                          return std::move(_accmap);
                        }
                      };
                    }(std::move(__728))(__727);
                  }
                  __723 = std::move(__728);
                }
                __717 = std::move(__722).combine(__723);
              }
              __711 = std::move(__716).combine(__717);
            }
            (_Collection<R_r1_r2_r3<Address, int,
            int>> {}).combine(__711).groupBy([this] (const R_r1_r2_r3<Address, int,
            int>& b1) mutable {
              {
                auto& ip = b1.r1;
                auto& stmt_id = b1.r2;
                auto& count = b1.r3;
                return R_key_value<Address, int> {ip, stmt_id};
              }
            }, [this] (const int& acc) mutable {
              return [this, &acc] (const R_r1_r2_r3<Address, int, int>& b3) mutable {
                {
                  auto& ip = b3.r1;
                  auto& stmt_id = b3.r2;
                  auto& count = b3.r3;
                  return acc + count;
                }
              };
            }, 0).groupBy([this] (const R_key_value<R_key_value<Address, int>, int>& b1) mutable {
              {
                auto& b2 = b1.key;
                auto& count = b1.value;
                {
                  auto& ip = b2.key;
                  auto& stmt_id = b2.value;
                  return std::move(ip);
                }
              }
            }, [this] (const _Collection<R_key_value<int, int>>& acc) mutable {
              return [this, &acc] (const R_key_value<R_key_value<Address, int>, int>& b3) mutable {
                {
                  auto& ip_and_stmt_id = b3.key;
                  auto& count = b3.value;
                  {
                    auto& ip = ip_and_stmt_id.key;
                    auto& stmt_id = ip_and_stmt_id.value;
                    _Collection<R_key_value<int, int>> __729;
                    {
                      _Collection<R_key_value<int, int>> __730;
                      __730 = _Collection<R_key_value<int, int>> {};
                      auto& __collection = __730;
                      __collection.insert(R_key_value<int, int> {stmt_id, count});
                      __729 = __collection;
                    }
                    return acc.combine(__729);
                  }
                }
              };
            }, _Collection<R_key_value<int, int>> {}).iterate([this, &R_A, &R_B,
            &vid] (const R_key_value<Address, _Collection<R_key_value<int, int>>>& b1) mutable {
              {
                auto addr = b1.key;
                auto stmt_cnt_list = b1.value;
                unit_t __731;
                auto __732 = std::make_shared<K3::ValDispatcher<R_r1_r2_r3_r4_r5<Address,
                _Collection<R_key_value<int, int>>, R_key_value<int, int>, int,
                int>>>(R_r1_r2_r3_r4_r5<Address, _Collection<R_key_value<int, int>>,
                R_key_value<int, int>, int, int> {me, stmt_cnt_list, vid, R_A, R_B});
                __engine.send(addr, __nd_delete_R_rcv_put_tid, __732, me);
                sw_num_sent = sw_num_sent + 1;
                shared_ptr<R_key_value<R_key_value<int, int>, int>> __733;
                __733 = sw_ack_log.filter([this, &vid] (const R_key_value<R_key_value<int, int>,
                int>& b1) mutable {
                  {
                    auto& key = b1.key;
                    auto& value = b1.value;
                    return key == vid;
                  }
                }).peek(unit_t {});
                if (__733) {
                  R_key_value<R_key_value<int, int>, int>& x = *__733;
                  __731 = sw_ack_log.update(x, R_key_value<R_key_value<int, int>, int> {vid,
                  x.value + 1});
                } else {
                  __731 = sw_ack_log.insert(R_key_value<R_key_value<int, int>, int> {vid, 1});
                }
                addr = b1.key;
                stmt_cnt_list = b1.value;
                return __731;
              }
            });
            auto __735 = _Collection<R_r1_r2_r3<int, int, Address>> {};
            for (const auto& __734: route_to_int(R_key_value<int, shared_ptr<int>> {3,
            make_shared<int>(R_B)})) {
              __735 = [this] (_Collection<R_r1_r2_r3<int, int, Address>> _accmap) mutable {
                return [this, _accmap = std::move(_accmap)] (const R_i<Address>& b3) mutable {
                  {
                    auto& ip = b3.i;
                    _accmap.insert(R_r1_r2_r3<int, int, Address> {14, 3, ip});
                    return std::move(_accmap);
                  }
                };
              }(std::move(__735))(__734);
            }
            auto __737 = _Collection<R_r1_r2_r3<int, int, Address>> {};
            for (const auto& __736: route_to_int(R_key_value<int, shared_ptr<int>> {6,
            make_shared<int>(R_B)})) {
              __737 = [this] (_Collection<R_r1_r2_r3<int, int, Address>> _accmap) mutable {
                return [this, _accmap = std::move(_accmap)] (const R_i<Address>& b3) mutable {
                  {
                    auto& ip = b3.i;
                    _accmap.insert(R_r1_r2_r3<int, int, Address> {14, 6, ip});
                    return std::move(_accmap);
                  }
                };
              }(std::move(__737))(__736);
            }
            auto __739 = _Collection<R_r1_r2_r3<int, int, Address>> {};
            for (const auto& __738: route_to_int(R_key_value<int, shared_ptr<int>> {3,
            make_shared<int>(R_B)})) {
              __739 = [this] (_Collection<R_r1_r2_r3<int, int, Address>> _accmap) mutable {
                return [this, _accmap = std::move(_accmap)] (const R_i<Address>& b3) mutable {
                  {
                    auto& ip = b3.i;
                    _accmap.insert(R_r1_r2_r3<int, int, Address> {12, 3, ip});
                    return std::move(_accmap);
                  }
                };
              }(std::move(__739))(__738);
            }
            return std::move(__735).combine(std::move(__737).combine(std::move(__739).combine(_Collection<R_r1_r2_r3<int,
            int, Address>> {}))).groupBy([this] (const R_r1_r2_r3<int, int, Address>& b1) mutable {
              {
                auto& stmt_id = b1.r1;
                auto& map_id = b1.r2;
                auto& ip = b1.r3;
                return std::move(ip);
              }
            }, [this] (_Collection<R_key_value<int, int>> acc) mutable {
              return [this, acc = std::move(acc)] (const R_r1_r2_r3<int, int,
              Address>& b3) mutable {
                {
                  auto& stmt_id = b3.r1;
                  auto& map_id = b3.r2;
                  auto& ip = b3.r3;
                  acc.insert(R_key_value<int, int> {stmt_id, map_id});
                  return std::move(acc);
                }
              };
            }, _Collection<R_key_value<int, int>> {}).iterate([this, &R_A, &R_B,
            &vid] (const R_key_value<Address, _Collection<R_key_value<int, int>>>& b1) mutable {
              {
                auto& ip = b1.key;
                auto& stmt_map_ids = b1.value;
                auto __740 = std::make_shared<K3::ValDispatcher<R_r1_r2_r3_r4<_Collection<R_key_value<int,
                int>>, R_key_value<int, int>, int, int>>>(R_r1_r2_r3_r4<_Collection<R_key_value<int,
                int>>, R_key_value<int, int>, int, int> {stmt_map_ids, vid, R_A, R_B});
                __engine.send(ip, __nd_delete_R_rcv_fetch_tid, __740, me);
                return unit_t {};
              }
            });
          }
        } else {
          return error<unit_t>(print("unexpected missing arguments in sw_buf_delete_R"));
        }
      }
      unit_t nd_delete_R_do_complete_s12(const R_r1_r2_r3<R_key_value<int, int>, int, int>& b1)  {
        {
          auto vid = b1.r1;
          auto R_A = b1.r2;
          auto R_B = b1.r3;
          unit_t __741;
          {
            int __742;
            int __743;
            {
              _Collection<R_key_value<int, int>> __744;
              {
                auto& __x = *map___SQL_SUM_AGGREGATE_1_mR1_s12_buf;
                auto __746 = _Collection<R_key_value<int, int>> {};
                for (const auto& __745: frontier_int_int(R_key_value<R_key_value<int, int>,
                _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>> {vid, __x.filter([this,
                &R_B] (const R_r1_r2_r3<R_key_value<int, int>, int, int>& b1) mutable {
                  {
                    auto& r1 = b1.r1;
                    auto& r2 = b1.r2;
                    auto& r3 = b1.r3;
                    return r2 == R_B;
                  }
                })})) {
                  __746 = [this] (_Collection<R_key_value<int, int>> acc) mutable {
                    return [this, acc = std::move(acc)] (const R_r1_r2_r3<R_key_value<int, int>,
                    int, int>& b3) mutable {
                      {
                        auto& vid = b3.r1;
                        auto& map_0 = b3.r2;
                        auto& map_val = b3.r3;
                        acc.insert(R_key_value<int, int> {map_0, map_val});
                        return std::move(acc);
                      }
                    };
                  }(std::move(__746))(__745);
                }
                __744 = std::move(__746);
              }
              auto& wrapped_lookup_value = __744;
              shared_ptr<R_key_value<int, int>> __747;
              __747 = wrapped_lookup_value.peek(unit_t {});
              if (__747) {
                R_key_value<int, int>& unwrapped_value = *__747;
                {
                  auto& _ = unwrapped_value.key;
                  auto& projected_field = unwrapped_value.value;
                  __743 = projected_field;
                }
              } else {
                __743 = 0;
              }
            }
            __742 = __743 * (-R_A);
            auto& __prod_ret__11 = __742;
            _Set<R_i<int>> __748;
            {
              _Set<R_i<int>> __749;
              __749 = _Set<R_i<int>> {};
              auto& __collection = __749;
              __collection.insert(R_i<int> {__prod_ret__11});
              __748 = __collection;
            }
            nd_add_delta_to_int(R_r1_r2_r3_r4<shared_ptr<_Set<R_key_value<R_key_value<int, int>,
            int>>>, bool, R_key_value<int, int>, _Set<R_i<int>>> {__SQL_SUM_AGGREGATE_1, false, vid,
            __748});
            __741 = nd_complete_stmt_cntr_check(R_key_value<R_key_value<int, int>, int> {vid, 12});
          }
          vid = b1.r1;
          R_A = b1.r2;
          R_B = b1.r3;
          return __741;
        }
      }
      unit_t nd_delete_R_do_complete_s14(const R_r1_r2_r3<R_key_value<int, int>, int, int>& b1)  {
        {
          auto vid = b1.r1;
          auto R_A = b1.r2;
          auto R_B = b1.r3;
          unit_t __750;
          {
            int __751;
            int __752;
            {
              _Collection<R_key_value<int, int>> __753;
              {
                auto& __x = *map___SQL_SUM_AGGREGATE_2_mR1_s14_buf;
                auto __755 = _Collection<R_key_value<int, int>> {};
                for (const auto& __754: frontier_int_int(R_key_value<R_key_value<int, int>,
                _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>> {vid, __x.filter([this,
                &R_B] (const R_r1_r2_r3<R_key_value<int, int>, int, int>& b1) mutable {
                  {
                    auto& r1 = b1.r1;
                    auto& r2 = b1.r2;
                    auto& r3 = b1.r3;
                    return r2 == R_B;
                  }
                })})) {
                  __755 = [this] (_Collection<R_key_value<int, int>> acc) mutable {
                    return [this, acc = std::move(acc)] (const R_r1_r2_r3<R_key_value<int, int>,
                    int, int>& b3) mutable {
                      {
                        auto& vid = b3.r1;
                        auto& map_0 = b3.r2;
                        auto& map_val = b3.r3;
                        acc.insert(R_key_value<int, int> {map_0, map_val});
                        return std::move(acc);
                      }
                    };
                  }(std::move(__755))(__754);
                }
                __753 = std::move(__755);
              }
              auto& wrapped_lookup_value = __753;
              shared_ptr<R_key_value<int, int>> __756;
              __756 = wrapped_lookup_value.peek(unit_t {});
              if (__756) {
                R_key_value<int, int>& unwrapped_value = *__756;
                {
                  auto& _ = unwrapped_value.key;
                  auto& projected_field = unwrapped_value.value;
                  __752 = projected_field;
                }
              } else {
                __752 = 0;
              }
            }
            int __757;
            {
              _Collection<R_key_value<int, int>> __758;
              {
                auto& __x = *map___SQL_SUM_AGGREGATE_1_mR1_s14_buf;
                auto __760 = _Collection<R_key_value<int, int>> {};
                for (const auto& __759: frontier_int_int(R_key_value<R_key_value<int, int>,
                _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>> {vid, __x.filter([this,
                &R_B] (const R_r1_r2_r3<R_key_value<int, int>, int, int>& b1) mutable {
                  {
                    auto& r1 = b1.r1;
                    auto& r2 = b1.r2;
                    auto& r3 = b1.r3;
                    return r2 == R_B;
                  }
                })})) {
                  __760 = [this] (_Collection<R_key_value<int, int>> acc) mutable {
                    return [this, acc = std::move(acc)] (const R_r1_r2_r3<R_key_value<int, int>,
                    int, int>& b3) mutable {
                      {
                        auto& vid = b3.r1;
                        auto& map_0 = b3.r2;
                        auto& map_val = b3.r3;
                        acc.insert(R_key_value<int, int> {map_0, map_val});
                        return std::move(acc);
                      }
                    };
                  }(std::move(__760))(__759);
                }
                __758 = std::move(__760);
              }
              auto& wrapped_lookup_value = __758;
              shared_ptr<R_key_value<int, int>> __761;
              __761 = wrapped_lookup_value.peek(unit_t {});
              if (__761) {
                R_key_value<int, int>& unwrapped_value = *__761;
                {
                  auto& _ = unwrapped_value.key;
                  auto& projected_field = unwrapped_value.value;
                  __757 = projected_field;
                }
              } else {
                __757 = 0;
              }
            }
            __751 = (__752 * R_A + __757) * (-(1));
            auto& __prod_ret__14 = __751;
            _Set<R_i<int>> __762;
            {
              _Set<R_i<int>> __763;
              __763 = _Set<R_i<int>> {};
              auto& __collection = __763;
              __collection.insert(R_i<int> {__prod_ret__14});
              __762 = __collection;
            }
            nd_add_delta_to_int(R_r1_r2_r3_r4<shared_ptr<_Set<R_key_value<R_key_value<int, int>,
            int>>>, bool, R_key_value<int, int>, _Set<R_i<int>>> {__SQL_SUM_AGGREGATE_2, false, vid,
            __762});
            __750 = nd_complete_stmt_cntr_check(R_key_value<R_key_value<int, int>, int> {vid, 14});
          }
          vid = b1.r1;
          R_A = b1.r2;
          R_B = b1.r3;
          return __750;
        }
      }
      unit_t nd_delete_R_do_complete_s13(const R_r1_r2_r3<R_key_value<int, int>, int, int>& b1)  {
        {
          auto& vid = b1.r1;
          auto& R_A = b1.r2;
          auto& R_B = b1.r3;
          {
            int __764;
            __764 = -R_A;
            auto& __prod_ret__12 = __764;
            _Set<R_key_value<int, int>> __765;
            {
              _Set<R_key_value<int, int>> __766;
              __766 = _Set<R_key_value<int, int>> {};
              auto& __collection = __766;
              __collection.insert(R_key_value<int, int> {R_B, __prod_ret__12});
              __765 = __collection;
            }
            nd_add_delta_to_int_int(R_r1_r2_r3_r4<shared_ptr<_Set<R_r1_r2_r3<R_key_value<int, int>,
            int, int>>>, bool, R_key_value<int, int>, _Set<R_key_value<int,
            int>>> {__SQL_SUM_AGGREGATE_1_mS1, false, vid, __765});
            {
              int __767;
              _Set<R_key_value<int, int>> __768;
              {
                _Set<R_key_value<int, int>> __769;
                __769 = _Set<R_key_value<int, int>> {};
                auto& __collection = __769;
                __collection.insert(R_key_value<int, int> {R_B, __prod_ret__12});
                __768 = __collection;
              }
              __767 = nd___SQL_SUM_AGGREGATE_1_mS1_send_correctives(R_r1_r2_r3_r4_r5_r6<Address,
              int, R_key_value<int, int>, int, R_key_value<int, int>, _Set<R_key_value<int,
              int>>> {me, 13, vid, 1, vid, __768});
              auto& sent_msgs = __767;
              if (sent_msgs == 0) {
                return unit_t {};
              } else {
                return nd_update_stmt_cntr_corr_map(R_r1_r2_r3_r4_r5_r6<R_key_value<int, int>, int,
                int, int, bool, bool> {vid, 13, 1, sent_msgs, true, true});
              }
            }
          }
        }
      }
      unit_t nd_delete_R_do_complete_s15(const R_r1_r2_r3<R_key_value<int, int>, int, int>& b1)  {
        {
          auto& vid = b1.r1;
          auto& R_A = b1.r2;
          auto& R_B = b1.r3;
          {
            int __770;
            __770 = -(1);
            auto& __val_ret__22 = __770;
            _Set<R_key_value<int, int>> __771;
            {
              _Set<R_key_value<int, int>> __772;
              __772 = _Set<R_key_value<int, int>> {};
              auto& __collection = __772;
              __collection.insert(R_key_value<int, int> {R_B, __val_ret__22});
              __771 = __collection;
            }
            nd_add_delta_to_int_int(R_r1_r2_r3_r4<shared_ptr<_Set<R_r1_r2_r3<R_key_value<int, int>,
            int, int>>>, bool, R_key_value<int, int>, _Set<R_key_value<int,
            int>>> {__SQL_SUM_AGGREGATE_2_mS3, false, vid, __771});
            {
              int __773;
              _Set<R_key_value<int, int>> __774;
              {
                _Set<R_key_value<int, int>> __775;
                __775 = _Set<R_key_value<int, int>> {};
                auto& __collection = __775;
                __collection.insert(R_key_value<int, int> {R_B, __val_ret__22});
                __774 = __collection;
              }
              __773 = nd___SQL_SUM_AGGREGATE_2_mS3_send_correctives(R_r1_r2_r3_r4_r5_r6<Address,
              int, R_key_value<int, int>, int, R_key_value<int, int>, _Set<R_key_value<int,
              int>>> {me, 15, vid, 1, vid, __774});
              auto& sent_msgs = __773;
              if (sent_msgs == 0) {
                return unit_t {};
              } else {
                return nd_update_stmt_cntr_corr_map(R_r1_r2_r3_r4_r5_r6<R_key_value<int, int>, int,
                int, int, bool, bool> {vid, 15, 1, sent_msgs, true, true});
              }
            }
          }
        }
      }
      int delete_R_do_corrective_s12_m___SQL_SUM_AGGREGATE_1_mR1(const R_r1_r2_r3_r4_r5_r6_r7_r8<Address,
      int, R_key_value<int, int>, int, R_key_value<int, int>, int, int, _Set<R_key_value<int,
      int>>>& b1)  {
        {
          auto orig_addr = b1.r1;
          auto orig_stmt_id = b1.r2;
          auto orig_vid = b1.r3;
          auto hop = b1.r4;
          auto vid = b1.r5;
          auto R_A = b1.r6;
          auto R_B = b1.r7;
          auto delta_tuples = b1.r8;
          int __776;
          {
            _Set<R_i<int>> __777;
            auto __783 = _Set<R_i<int>> {};
            for (const auto& __782: delta_tuples) {
              __783 = [this, &R_A, &R_B] (const _Set<R_i<int>>& _accext) mutable {
                return [this, &R_A, &R_B, &_accext] (const R_key_value<int, int>& b3) mutable {
                  {
                    auto& delta___SQL_SUM_AGGREGATE_1_mRR_B = b3.key;
                    auto& delta___SQL_SUM_AGGREGATE_1_mR1 = b3.value;
                    _Set<R_i<int>> __778;
                    {
                      int __779;
                      int __780;
                      if (R_B == delta___SQL_SUM_AGGREGATE_1_mRR_B) {
                        __780 = 1;
                      } else {
                        __780 = 0;
                      }
                      __779 = __780 * (-delta___SQL_SUM_AGGREGATE_1_mR1) * R_A;
                      auto& __prod_ret__25 = __779;
                      {
                        _Set<R_i<int>> __781;
                        __781 = _Set<R_i<int>> {};
                        auto& __collection = __781;
                        __collection.insert(R_i<int> {__prod_ret__25});
                        __778 = __collection;
                      }
                    }
                    return _accext.combine(__778);
                  }
                };
              }(std::move(__783))(__782);
            }
            __777 = std::move(__783).filter([this] (const R_i<int>& b1) mutable {
              {
                auto& map_val = b1.i;
                return 0 != map_val;
              }
            });
            auto& new_tuples = __777;
            nd_add_delta_to_int(R_r1_r2_r3_r4<shared_ptr<_Set<R_key_value<R_key_value<int, int>,
            int>>>, bool, R_key_value<int, int>, _Set<R_i<int>>> {__SQL_SUM_AGGREGATE_1, true, vid,
            new_tuples});
            __776 = 0;
          }
          orig_addr = b1.r1;
          orig_stmt_id = b1.r2;
          orig_vid = b1.r3;
          hop = b1.r4;
          vid = b1.r5;
          R_A = b1.r6;
          R_B = b1.r7;
          delta_tuples = b1.r8;
          return __776;
        }
      }
      int delete_R_do_corrective_s14_m___SQL_SUM_AGGREGATE_2_mR1(const R_r1_r2_r3_r4_r5_r6_r7_r8<Address,
      int, R_key_value<int, int>, int, R_key_value<int, int>, int, int, _Set<R_key_value<int,
      int>>>& b1)  {
        {
          auto orig_addr = b1.r1;
          auto orig_stmt_id = b1.r2;
          auto orig_vid = b1.r3;
          auto hop = b1.r4;
          auto vid = b1.r5;
          auto R_A = b1.r6;
          auto R_B = b1.r7;
          auto delta_tuples = b1.r8;
          int __784;
          {
            _Set<R_i<int>> __785;
            auto __791 = _Set<R_i<int>> {};
            for (const auto& __790: delta_tuples) {
              __791 = [this, &R_A, &R_B] (const _Set<R_i<int>>& _accext) mutable {
                return [this, &R_A, &R_B, &_accext] (const R_key_value<int, int>& b3) mutable {
                  {
                    auto& delta___SQL_SUM_AGGREGATE_2_mRR_B = b3.key;
                    auto& delta___SQL_SUM_AGGREGATE_2_mR1 = b3.value;
                    _Set<R_i<int>> __786;
                    {
                      int __787;
                      int __788;
                      if (R_B == delta___SQL_SUM_AGGREGATE_2_mRR_B) {
                        __788 = 1;
                      } else {
                        __788 = 0;
                      }
                      __787 = __788 * (-delta___SQL_SUM_AGGREGATE_2_mR1) * R_A;
                      auto& __prod_ret__35 = __787;
                      {
                        _Set<R_i<int>> __789;
                        __789 = _Set<R_i<int>> {};
                        auto& __collection = __789;
                        __collection.insert(R_i<int> {__prod_ret__35});
                        __786 = __collection;
                      }
                    }
                    return _accext.combine(__786);
                  }
                };
              }(std::move(__791))(__790);
            }
            __785 = std::move(__791).filter([this] (const R_i<int>& b1) mutable {
              {
                auto& map_val = b1.i;
                return 0 != map_val;
              }
            });
            auto& new_tuples = __785;
            nd_add_delta_to_int(R_r1_r2_r3_r4<shared_ptr<_Set<R_key_value<R_key_value<int, int>,
            int>>>, bool, R_key_value<int, int>, _Set<R_i<int>>> {__SQL_SUM_AGGREGATE_2, true, vid,
            new_tuples});
            __784 = 0;
          }
          orig_addr = b1.r1;
          orig_stmt_id = b1.r2;
          orig_vid = b1.r3;
          hop = b1.r4;
          vid = b1.r5;
          R_A = b1.r6;
          R_B = b1.r7;
          delta_tuples = b1.r8;
          return __784;
        }
      }
      int delete_R_do_corrective_s14_m___SQL_SUM_AGGREGATE_1_mR1(const R_r1_r2_r3_r4_r5_r6_r7_r8<Address,
      int, R_key_value<int, int>, int, R_key_value<int, int>, int, int, _Set<R_key_value<int,
      int>>>& b1)  {
        {
          auto orig_addr = b1.r1;
          auto orig_stmt_id = b1.r2;
          auto orig_vid = b1.r3;
          auto hop = b1.r4;
          auto vid = b1.r5;
          auto R_A = b1.r6;
          auto R_B = b1.r7;
          auto delta_tuples = b1.r8;
          int __792;
          {
            _Set<R_i<int>> __793;
            auto __799 = _Set<R_i<int>> {};
            for (const auto& __798: delta_tuples) {
              __799 = [this, &R_B] (const _Set<R_i<int>>& _accext) mutable {
                return [this, &R_B, &_accext] (const R_key_value<int, int>& b3) mutable {
                  {
                    auto& delta___SQL_SUM_AGGREGATE_1_mRR_B = b3.key;
                    auto& delta___SQL_SUM_AGGREGATE_1_mR1 = b3.value;
                    _Set<R_i<int>> __794;
                    {
                      int __795;
                      int __796;
                      if (R_B == delta___SQL_SUM_AGGREGATE_1_mRR_B) {
                        __796 = 1;
                      } else {
                        __796 = 0;
                      }
                      __795 = __796 * (-delta___SQL_SUM_AGGREGATE_1_mR1);
                      auto& __prod_ret__27 = __795;
                      {
                        _Set<R_i<int>> __797;
                        __797 = _Set<R_i<int>> {};
                        auto& __collection = __797;
                        __collection.insert(R_i<int> {__prod_ret__27});
                        __794 = __collection;
                      }
                    }
                    return _accext.combine(__794);
                  }
                };
              }(std::move(__799))(__798);
            }
            __793 = std::move(__799).filter([this] (const R_i<int>& b1) mutable {
              {
                auto& map_val = b1.i;
                return 0 != map_val;
              }
            });
            auto& new_tuples = __793;
            nd_add_delta_to_int(R_r1_r2_r3_r4<shared_ptr<_Set<R_key_value<R_key_value<int, int>,
            int>>>, bool, R_key_value<int, int>, _Set<R_i<int>>> {__SQL_SUM_AGGREGATE_2, true, vid,
            new_tuples});
            __792 = 0;
          }
          orig_addr = b1.r1;
          orig_stmt_id = b1.r2;
          orig_vid = b1.r3;
          hop = b1.r4;
          vid = b1.r5;
          R_A = b1.r6;
          R_B = b1.r7;
          delta_tuples = b1.r8;
          return __792;
        }
      }
      unit_t ms_rcv_sw_init_ack(const unit_t& _u)  {
        ms_rcv_sw_init_ack_cnt = ms_rcv_sw_init_ack_cnt + 1;
        if (ms_rcv_sw_init_ack_cnt == num_switches) {
          ms_start_time = now_int(unit_t {});
          auto __800 = std::make_shared<K3::ValDispatcher<R_key_value<int, int>>>(g_start_vid);
          __engine.send(sw_next_switch_addr, __sw_rcv_token_tid, __800, me);
          auto __801 = std::make_shared<K3::ValDispatcher<R_r1_r2_r3<int, int,
          Address>>>(R_r1_r2_r3<int, int, Address> {ms_gc_interval, 0, me});
          __engine.send(timer_addr, __tm_insert_timer_tid, __801, me);
          return unit_t {};
        } else {
          return unit_t {};
        }
      }
      unit_t sw_rcv_init(const unit_t& _u)  {
        sw_init = true;
        auto __802 = std::make_shared<K3::ValDispatcher<unit_t>>(unit_t {});
        __engine.send(me, __sw_driver_trig_tid, __802, me);
        auto __803 = std::make_shared<K3::ValDispatcher<unit_t>>(unit_t {});
        __engine.send(master_addr, __ms_rcv_sw_init_ack_tid, __803, me);
        return unit_t {};
      }
      unit_t ms_rcv_jobs_ack(const unit_t& _u)  {
        ms_rcv_jobs_ack_cnt = ms_rcv_jobs_ack_cnt + 1;
        if (ms_rcv_jobs_ack_cnt == num_peers) {
          return switches.iterate([this] (const R_i<Address>& b1) mutable {
            {
              auto& addr = b1.i;
              auto __804 = std::make_shared<K3::ValDispatcher<unit_t>>(unit_t {});
              __engine.send(addr, __sw_rcv_init_tid, __804, me);
              return unit_t {};
            }
          });
        } else {
          return unit_t {};
        }
      }
      unit_t rcv_jobs(const _Map<R_key_value<Address, int>>& jobs_in)  {
        jobs = jobs_in;
        shared_ptr<R_key_value<Address, int>> __805;
        __805 = jobs.filter([this] (const R_key_value<Address, int>& b1) mutable {
          {
            auto& addr = b1.key;
            auto& job = b1.value;
            return job == job_timer;
          }
        }).peek(unit_t {});
        if (__805) {
          R_key_value<Address, int>& timer = *__805;
          timer_addr = timer.key;
        } else {
          timer_addr = error<Address>(print("no timer peer found"));
        }
        auto __807 = _Collection<R_i<Address>> {};
        for (const auto& __806: jobs.filter([this] (const R_key_value<Address, int>& b1) mutable {
          {
            auto& addr = b1.key;
            auto& job = b1.value;
            return job == job_node;
          }
        })) {
          __807 = [this] (_Collection<R_i<Address>> _accmap) mutable {
            return [this, _accmap = std::move(_accmap)] (const R_key_value<Address,
            int>& x) mutable {
              _accmap.insert(R_i<Address> {x.key});
              return std::move(_accmap);
            };
          }(std::move(__807))(__806);
        }
        nodes = std::move(__807);
        num_nodes = nodes.size(unit_t {});
        auto __809 = _Collection<R_i<Address>> {};
        for (const auto& __808: jobs.filter([this] (const R_key_value<Address, int>& b1) mutable {
          {
            auto& addr = b1.key;
            auto& job = b1.value;
            return job == job_switch;
          }
        })) {
          __809 = [this] (_Collection<R_i<Address>> _accmap) mutable {
            return [this, _accmap = std::move(_accmap)] (const R_key_value<Address,
            int>& x) mutable {
              _accmap.insert(R_i<Address> {x.key});
              return std::move(_accmap);
            };
          }(std::move(__809))(__808);
        }
        switches = std::move(__809);
        num_switches = switches.size(unit_t {});
        if (job <= job_switch) {
          {
            _Seq<R_i<Address>> __810;
            auto __812 = _Seq<R_i<Address>> {};
            for (const auto& __811: switches) {
              __812 = [this] (_Seq<R_i<Address>> acc_conv) mutable {
                return [this, acc_conv = std::move(acc_conv)] (const R_i<Address>& b3) mutable {
                  {
                    auto& x = b3.i;
                    acc_conv.insert(R_i<Address> {x});
                    return std::move(acc_conv);
                  }
                };
              }(std::move(__812))(__811);
            }
            __810 = std::move(__812).sort([this] (const R_i<Address>& b2) mutable {
              return [this, &b2] (const R_i<Address>& b3) mutable {
                {
                  auto& addr1 = b2.i;
                  {
                    auto& addr2 = b3.i;
                    if (addr1 < addr2) {
                      return -(1);
                    } else {
                      return 1;
                    }
                  }
                }
              };
            });
            auto& addr_list = __810;
            shared_ptr<R_i<Address>> __813;
            __813 = addr_list.peek(unit_t {});
            if (__813) {
              R_i<Address>& first_addr = *__813;
              auto __815 = R_key_value<bool, Address> {false, first_addr.i};
              for (const auto& __814: addr_list) {
                __815 = [this] (const R_key_value<bool, Address>& b2) mutable {
                  return [this, &b2] (const R_i<Address>& b5) mutable {
                    {
                      auto& take = b2.key;
                      auto& result = b2.value;
                      {
                        auto& x = b5.i;
                        if (take) {
                          return R_key_value<bool, Address> {false, x};
                        } else {
                          if (x == me) {
                            return R_key_value<bool, Address> {true, result};
                          } else {
                            return R_key_value<bool, Address> {take, result};
                          }
                        }
                      }
                    }
                  };
                }(std::move(__815))(__814);
              }
              sw_next_switch_addr = std::move(__815).value;
            } else {
              sw_next_switch_addr = error<Address>(print("no addresses in addr_list"));
            }
          }
        }
        nodes.iterate([this] (const R_i<Address>& b1) mutable {
          {
            auto& addr = b1.i;
            return add_node(addr);
          }
        });
        auto __816 = std::make_shared<K3::ValDispatcher<unit_t>>(unit_t {});
        __engine.send(master_addr, __ms_rcv_jobs_ack_tid, __816, me);
        return unit_t {};
      }
      unit_t ms_rcv_job(const R_key_value<Address, int>& b1)  {
        {
          auto addr = b1.key;
          auto job = b1.value;
          unit_t __817;
          jobs.insert(R_key_value<Address, int> {addr, job});
          ms_rcv_job_cnt = ms_rcv_job_cnt + 1;
          if (ms_rcv_job_cnt == num_peers) {
            __817 = my_peers.iterate([this] (const R_i<Address>& b1) mutable {
              {
                auto& addr = b1.i;
                auto __818 = std::make_shared<K3::ValDispatcher<_Map<R_key_value<Address,
                int>>>>(jobs);
                __engine.send(addr, __rcv_jobs_tid, __818, me);
                return unit_t {};
              }
            });
          } else {
            __817 = unit_t {};
          }
          addr = b1.key;
          job = b1.value;
          return __817;
        }
      }
      unit_t rcv_master_addr(const Address& addr)  {
        master_addr = addr;
        auto __819 = std::make_shared<K3::ValDispatcher<R_key_value<Address,
        int>>>(R_key_value<Address, int> {me, job});
        __engine.send(addr, __ms_rcv_job_tid, __819, me);
        return unit_t {};
      }
      unit_t ms_send_addr_self(const unit_t& _u)  {
        return my_peers.iterate([this] (const R_i<Address>& b1) mutable {
          {
            auto& addr = b1.i;
            auto __820 = std::make_shared<K3::ValDispatcher<Address>>(me);
            __engine.send(addr, __rcv_master_addr_tid, __820, me);
            return unit_t {};
          }
        });
      }
      unit_t shutdown_trig(const unit_t& _u)  {
        return haltEngine(unit_t {});
      }
      unit_t ms_shutdown(const unit_t& _u)  {
        if (ms_rcv_node_done_cnt == num_nodes) {
          return my_peers.iterate([this] (const R_i<Address>& b1) mutable {
            {
              auto& addr = b1.i;
              auto __821 = std::make_shared<K3::ValDispatcher<unit_t>>(unit_t {});
              __engine.send(addr, __shutdown_trig_tid, __821, me);
              return unit_t {};
            }
          });
        } else {
          return unit_t {};
        }
      }
      unit_t ms_rcv_node_done(const bool& done)  {
        if (done) {
          ms_rcv_node_done_cnt = ms_rcv_node_done_cnt + 1;
          if (ms_rcv_node_done_cnt == num_nodes) {
            ms_end_time = now_int(unit_t {});
            auto __822 = std::make_shared<K3::ValDispatcher<unit_t>>(unit_t {});
            __engine.send(me, __ms_shutdown_tid, __822, me);
            return unit_t {};
          } else {
            return unit_t {};
          }
        } else {
          ms_rcv_node_done_cnt = ms_rcv_node_done_cnt - 1;
          return unit_t {};
        }
      }
      unit_t nd_rcv_done(const unit_t& _u)  {
        nd_rcvd_sys_done = true;
        if ((!nd_sent_done) && nd_stmt_cntrs.size(unit_t {}) == 0) {
          auto __823 = std::make_shared<K3::ValDispatcher<bool>>(true);
          __engine.send(master_addr, __ms_rcv_node_done_tid, __823, me);
          nd_sent_done = true;
          return unit_t {};
        } else {
          return unit_t {};
        }
      }
      unit_t ms_rcv_switch_done(const unit_t& _u)  {
        ms_rcv_switch_done_cnt = ms_rcv_switch_done_cnt + 1;
        if (ms_rcv_switch_done_cnt == num_switches) {
          return nodes.iterate([this] (const R_i<Address>& b1) mutable {
            {
              auto& addr = b1.i;
              auto __824 = std::make_shared<K3::ValDispatcher<unit_t>>(unit_t {});
              __engine.send(addr, __nd_rcv_done_tid, __824, me);
              return unit_t {};
            }
          });
        } else {
          return unit_t {};
        }
      }
      unit_t sw_ack_rcv(const R_key_value<Address, R_key_value<int, int>>& b1)  {
        {
          auto addr = b1.key;
          auto vid = b1.value;
          unit_t __825;
          sw_num_ack = sw_num_ack + 1;
          shared_ptr<R_key_value<R_key_value<int, int>, int>> __826;
          __826 = sw_ack_log.filter([this, &vid] (const R_key_value<R_key_value<int, int>,
          int>& b1) mutable {
            {
              auto& key = b1.key;
              auto& value = b1.value;
              return key == vid;
            }
          }).peek(unit_t {});
          if (__826) {
            R_key_value<R_key_value<int, int>, int>& x = *__826;
            if (x.value == 0) {
              sw_ack_log.erase(x);
            } else {
              sw_ack_log.update(x, R_key_value<R_key_value<int, int>, int> {vid, x.value - 1});
            }
          }
          if ((!sw_sent_done) && sw_trig_buf_idx.size(unit_t {}) == 0 && sw_num_ack == sw_num_sent && sw_seen_sentry == true) {
            auto __827 = std::make_shared<K3::ValDispatcher<unit_t>>(unit_t {});
            __engine.send(master_addr, __ms_rcv_switch_done_tid, __827, me);
            sw_sent_done = true;
            __825 = unit_t {};
          } else {
            __825 = unit_t {};
          }
          addr = b1.key;
          vid = b1.value;
          return __825;
        }
      }
      unit_t ms_rcv_gc_vid(const R_key_value<Address, R_key_value<int, int>>& data)  {
        ms_gc_vid_map.insert(data);
        ms_gc_vid_ctr = ms_gc_vid_ctr + 1;
        if (ms_gc_vid_ctr >= ms_num_gc_expected) {
          {
            R_key_value<int, int> __828;
            auto __830 = g_min_vid;
            for (const auto& __829: ms_gc_vid_map) {
              __830 = [this] (const R_key_value<int, int>& min_vid) mutable {
                return [this, &min_vid] (const R_key_value<Address, R_key_value<int,
                int>>& b3) mutable {
                  {
                    auto& addr = b3.key;
                    auto& vid = b3.value;
                    if (min_vid < vid) {
                      return std::move(min_vid);
                    } else {
                      return std::move(vid);
                    }
                  }
                };
              }(std::move(__830))(__829);
            }
            __828 = std::move(__830);
            auto& min_vid = __828;
            ms_gc_vid_ctr = 0;
            ms_gc_vid_map = _Map<R_key_value<Address, R_key_value<int, int>>> {};
            my_peers.iterate([this, &min_vid] (const R_i<Address>& b1) mutable {
              {
                auto& addr = b1.i;
                auto __831 = std::make_shared<K3::ValDispatcher<R_key_value<int, int>>>(min_vid);
                __engine.send(addr, __do_gc_tid, __831, me);
                return unit_t {};
              }
            });
            auto __832 = std::make_shared<K3::ValDispatcher<R_r1_r2_r3<int, int,
            Address>>>(R_r1_r2_r3<int, int, Address> {ms_gc_interval, 0, me});
            __engine.send(timer_addr, __tm_insert_timer_tid, __832, me);
            return unit_t {};
          }
        } else {
          return unit_t {};
        }
      }
      unit_t rcv_req_gc_vid(const unit_t& _u)  {
        if (job == job_switch || job == job_master) {
          auto __835 = sw_highest_vid;
          for (const auto& __834: sw_ack_log) {
            __835 = [this] (const R_key_value<int, int>& min_vid) mutable {
              return [this, &min_vid] (const R_key_value<R_key_value<int, int>, int>& b3) mutable {
                {
                  auto& vid = b3.key;
                  auto& count = b3.value;
                  if (min_vid < vid) {
                    return std::move(min_vid);
                  } else {
                    return std::move(vid);
                  }
                }
              };
            }(std::move(__835))(__834);
          }
          auto __833 = std::make_shared<K3::ValDispatcher<R_key_value<Address, R_key_value<int,
          int>>>>(R_key_value<Address, R_key_value<int, int>> {me, std::move(__835)});
          __engine.send(master_addr, __ms_rcv_gc_vid_tid, __833, me);
          return unit_t {};
        } else {
          if (job == job_node) {
            auto __838 = g_max_vid;
            for (const auto& __837: nd_stmt_cntrs) {
              __838 = [this] (const R_key_value<int, int>& min_vid) mutable {
                return [this, &min_vid] (const R_key_value<R_key_value<R_key_value<int, int>, int>,
                R_key_value<int, _Map<R_key_value<int, int>>>>& b3) mutable {
                  {
                    auto& vid_stmt_id = b3.key;
                    auto& ctr_corrs = b3.value;
                    if (min_vid < vid_stmt_id.key) {
                      return std::move(min_vid);
                    } else {
                      return std::move(vid_stmt_id.key);
                    }
                  }
                };
              }(std::move(__838))(__837);
            }
            auto __836 = std::make_shared<K3::ValDispatcher<R_key_value<Address, R_key_value<int,
            int>>>>(R_key_value<Address, R_key_value<int, int>> {me, std::move(__838)});
            __engine.send(master_addr, __ms_rcv_gc_vid_tid, __836, me);
            return unit_t {};
          } else {
            return unit_t {};
          }
        }
      }
      unit_t ms_send_gc_req(const unit_t& _u)  {
        return my_peers.iterate([this] (const R_i<Address>& b1) mutable {
          {
            auto& addr = b1.i;
            auto __839 = std::make_shared<K3::ValDispatcher<unit_t>>(unit_t {});
            __engine.send(addr, __rcv_req_gc_vid_tid, __839, me);
            return unit_t {};
          }
        });
      }
      unit_t do_gc(const R_key_value<int, int>& min_gc_vid)  {
        {
          _Set<R_key_value<R_key_value<int, int>, int>> __840;
          __840 = _Set<R_key_value<R_key_value<int, int>, int>> {};
          auto& temp = __840;
          nd_log_master.iterate([this, &min_gc_vid,
          temp = std::move(temp)] (const R_key_value<R_key_value<int, int>, int>& b1) mutable {
            {
              auto& vid = b1.key;
              auto& stmt_id = b1.value;
              if (vid < min_gc_vid) {
                return temp.insert(R_key_value<R_key_value<int, int>, int> {vid, stmt_id});
              } else {
                return unit_t {};
              }
            }
          });
          temp.iterate([this] (const R_key_value<R_key_value<int, int>, int>& val) mutable {
            return nd_log_master.erase(val);
          });
          {
            _Map<R_key_value<R_key_value<int, int>, R_key_value<int, int>>> __841;
            __841 = _Map<R_key_value<R_key_value<int, int>, R_key_value<int, int>>> {};
            auto& temp = __841;
            nd_log_insert_S.iterate([this, &min_gc_vid,
            temp = std::move(temp)] (const R_key_value<R_key_value<int, int>, R_key_value<int,
            int>>& b1) mutable {
              {
                auto& vid = b1.key;
                auto& args = b1.value;
                if (vid < min_gc_vid) {
                  return temp.insert(R_key_value<R_key_value<int, int>, R_key_value<int, int>> {vid,
                  args});
                } else {
                  return unit_t {};
                }
              }
            });
            temp.iterate([this] (const R_key_value<R_key_value<int, int>, R_key_value<int,
            int>>& val) mutable {
              return nd_log_insert_S.erase(val);
            });
            {
              _Map<R_key_value<R_key_value<int, int>, R_key_value<int, int>>> __842;
              __842 = _Map<R_key_value<R_key_value<int, int>, R_key_value<int, int>>> {};
              auto& temp = __842;
              nd_log_delete_S.iterate([this, &min_gc_vid,
              temp = std::move(temp)] (const R_key_value<R_key_value<int, int>, R_key_value<int,
              int>>& b1) mutable {
                {
                  auto& vid = b1.key;
                  auto& args = b1.value;
                  if (vid < min_gc_vid) {
                    return temp.insert(R_key_value<R_key_value<int, int>, R_key_value<int,
                    int>> {vid, args});
                  } else {
                    return unit_t {};
                  }
                }
              });
              temp.iterate([this] (const R_key_value<R_key_value<int, int>, R_key_value<int,
              int>>& val) mutable {
                return nd_log_delete_S.erase(val);
              });
              {
                _Map<R_key_value<R_key_value<int, int>, R_key_value<int, int>>> __843;
                __843 = _Map<R_key_value<R_key_value<int, int>, R_key_value<int, int>>> {};
                auto& temp = __843;
                nd_log_insert_R.iterate([this, &min_gc_vid,
                temp = std::move(temp)] (const R_key_value<R_key_value<int, int>, R_key_value<int,
                int>>& b1) mutable {
                  {
                    auto& vid = b1.key;
                    auto& args = b1.value;
                    if (vid < min_gc_vid) {
                      return temp.insert(R_key_value<R_key_value<int, int>, R_key_value<int,
                      int>> {vid, args});
                    } else {
                      return unit_t {};
                    }
                  }
                });
                temp.iterate([this] (const R_key_value<R_key_value<int, int>, R_key_value<int,
                int>>& val) mutable {
                  return nd_log_insert_R.erase(val);
                });
                {
                  _Map<R_key_value<R_key_value<int, int>, R_key_value<int, int>>> __844;
                  __844 = _Map<R_key_value<R_key_value<int, int>, R_key_value<int, int>>> {};
                  auto& temp = __844;
                  nd_log_delete_R.iterate([this, &min_gc_vid,
                  temp = std::move(temp)] (const R_key_value<R_key_value<int, int>, R_key_value<int,
                  int>>& b1) mutable {
                    {
                      auto& vid = b1.key;
                      auto& args = b1.value;
                      if (vid < min_gc_vid) {
                        return temp.insert(R_key_value<R_key_value<int, int>, R_key_value<int,
                        int>> {vid, args});
                      } else {
                        return unit_t {};
                      }
                    }
                  });
                  temp.iterate([this] (const R_key_value<R_key_value<int, int>, R_key_value<int,
                  int>>& val) mutable {
                    return nd_log_delete_R.erase(val);
                  });
                  {
                    _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> __845;
                    __845 = _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> {};
                    auto& temp = __845;
                    {
                      auto& map___SQL_SUM_AGGREGATE_1_mS1_s0_buf_unwrap = *map___SQL_SUM_AGGREGATE_1_mS1_s0_buf;
                      {
                        _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> __846;
                        __846 = frontier_int_int(R_key_value<R_key_value<int, int>,
                        _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>> {min_gc_vid,
                        map___SQL_SUM_AGGREGATE_1_mS1_s0_buf_unwrap});
                        auto& frontier = __846;
                        map___SQL_SUM_AGGREGATE_1_mS1_s0_buf_unwrap.iterate([this, &min_gc_vid,
                        temp = std::move(temp)] (const R_r1_r2_r3<R_key_value<int, int>, int,
                        int>& b1) mutable {
                          {
                            auto& vid = b1.r1;
                            auto& map_0 = b1.r2;
                            auto& map_val = b1.r3;
                            if (vid < min_gc_vid) {
                              return temp.insert(R_r1_r2_r3<R_key_value<int, int>, int, int> {vid,
                              map_0, map_val});
                            } else {
                              return unit_t {};
                            }
                          }
                        });
                        temp.iterate([this,
                        map___SQL_SUM_AGGREGATE_1_mS1_s0_buf_unwrap = std::move(map___SQL_SUM_AGGREGATE_1_mS1_s0_buf_unwrap)] (const R_r1_r2_r3<R_key_value<int,
                        int>, int, int>& val) mutable {
                          return map___SQL_SUM_AGGREGATE_1_mS1_s0_buf_unwrap.erase(val);
                        });
                        frontier.iterate([this,
                        map___SQL_SUM_AGGREGATE_1_mS1_s0_buf_unwrap = std::move(map___SQL_SUM_AGGREGATE_1_mS1_s0_buf_unwrap)] (const R_r1_r2_r3<R_key_value<int,
                        int>, int, int>& val) mutable {
                          return map___SQL_SUM_AGGREGATE_1_mS1_s0_buf_unwrap.insert(val);
                        });
                        {
                          _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> __847;
                          __847 = _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> {};
                          auto& temp = __847;
                          {
                            auto& map___SQL_SUM_AGGREGATE_1_mS1_s2_buf_unwrap = *map___SQL_SUM_AGGREGATE_1_mS1_s2_buf;
                            {
                              _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> __848;
                              __848 = frontier_int_int(R_key_value<R_key_value<int, int>,
                              _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>> {min_gc_vid,
                              map___SQL_SUM_AGGREGATE_1_mS1_s2_buf_unwrap});
                              auto& frontier = __848;
                              map___SQL_SUM_AGGREGATE_1_mS1_s2_buf_unwrap.iterate([this,
                              &min_gc_vid,
                              temp = std::move(temp)] (const R_r1_r2_r3<R_key_value<int, int>, int,
                              int>& b1) mutable {
                                {
                                  auto& vid = b1.r1;
                                  auto& map_0 = b1.r2;
                                  auto& map_val = b1.r3;
                                  if (vid < min_gc_vid) {
                                    return temp.insert(R_r1_r2_r3<R_key_value<int, int>, int,
                                    int> {vid, map_0, map_val});
                                  } else {
                                    return unit_t {};
                                  }
                                }
                              });
                              temp.iterate([this,
                              map___SQL_SUM_AGGREGATE_1_mS1_s2_buf_unwrap = std::move(map___SQL_SUM_AGGREGATE_1_mS1_s2_buf_unwrap)] (const R_r1_r2_r3<R_key_value<int,
                              int>, int, int>& val) mutable {
                                return map___SQL_SUM_AGGREGATE_1_mS1_s2_buf_unwrap.erase(val);
                              });
                              frontier.iterate([this,
                              map___SQL_SUM_AGGREGATE_1_mS1_s2_buf_unwrap = std::move(map___SQL_SUM_AGGREGATE_1_mS1_s2_buf_unwrap)] (const R_r1_r2_r3<R_key_value<int,
                              int>, int, int>& val) mutable {
                                return map___SQL_SUM_AGGREGATE_1_mS1_s2_buf_unwrap.insert(val);
                              });
                              {
                                _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> __849;
                                __849 = _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> {};
                                auto& temp = __849;
                                {
                                  auto& map___SQL_SUM_AGGREGATE_2_mS3_s2_buf_unwrap = *map___SQL_SUM_AGGREGATE_2_mS3_s2_buf;
                                  {
                                    _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> __850;
                                    __850 = frontier_int_int(R_key_value<R_key_value<int, int>,
                                    _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>> {min_gc_vid,
                                    map___SQL_SUM_AGGREGATE_2_mS3_s2_buf_unwrap});
                                    auto& frontier = __850;
                                    map___SQL_SUM_AGGREGATE_2_mS3_s2_buf_unwrap.iterate([this,
                                    &min_gc_vid,
                                    temp = std::move(temp)] (const R_r1_r2_r3<R_key_value<int, int>,
                                    int, int>& b1) mutable {
                                      {
                                        auto& vid = b1.r1;
                                        auto& map_0 = b1.r2;
                                        auto& map_val = b1.r3;
                                        if (vid < min_gc_vid) {
                                          return temp.insert(R_r1_r2_r3<R_key_value<int, int>, int,
                                          int> {vid, map_0, map_val});
                                        } else {
                                          return unit_t {};
                                        }
                                      }
                                    });
                                    temp.iterate([this,
                                    map___SQL_SUM_AGGREGATE_2_mS3_s2_buf_unwrap = std::move(map___SQL_SUM_AGGREGATE_2_mS3_s2_buf_unwrap)] (const R_r1_r2_r3<R_key_value<int,
                                    int>, int, int>& val) mutable {
                                      return map___SQL_SUM_AGGREGATE_2_mS3_s2_buf_unwrap.erase(val);
                                    });
                                    frontier.iterate([this,
                                    map___SQL_SUM_AGGREGATE_2_mS3_s2_buf_unwrap = std::move(map___SQL_SUM_AGGREGATE_2_mS3_s2_buf_unwrap)] (const R_r1_r2_r3<R_key_value<int,
                                    int>, int, int>& val) mutable {
                                      return map___SQL_SUM_AGGREGATE_2_mS3_s2_buf_unwrap.insert(val);
                                    });
                                    {
                                      _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> __851;
                                      __851 = _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> {};
                                      auto& temp = __851;
                                      {
                                        auto& map___SQL_SUM_AGGREGATE_1_mS1_s4_buf_unwrap = *map___SQL_SUM_AGGREGATE_1_mS1_s4_buf;
                                        {
                                          _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> __852;
                                          __852 = frontier_int_int(R_key_value<R_key_value<int,
                                          int>, _Set<R_r1_r2_r3<R_key_value<int, int>, int,
                                          int>>> {min_gc_vid,
                                          map___SQL_SUM_AGGREGATE_1_mS1_s4_buf_unwrap});
                                          auto& frontier = __852;
                                          map___SQL_SUM_AGGREGATE_1_mS1_s4_buf_unwrap.iterate([this,
                                          &min_gc_vid,
                                          temp = std::move(temp)] (const R_r1_r2_r3<R_key_value<int,
                                          int>, int, int>& b1) mutable {
                                            {
                                              auto& vid = b1.r1;
                                              auto& map_0 = b1.r2;
                                              auto& map_val = b1.r3;
                                              if (vid < min_gc_vid) {
                                                return temp.insert(R_r1_r2_r3<R_key_value<int, int>,
                                                int, int> {vid, map_0, map_val});
                                              } else {
                                                return unit_t {};
                                              }
                                            }
                                          });
                                          temp.iterate([this,
                                          map___SQL_SUM_AGGREGATE_1_mS1_s4_buf_unwrap = std::move(map___SQL_SUM_AGGREGATE_1_mS1_s4_buf_unwrap)] (const R_r1_r2_r3<R_key_value<int,
                                          int>, int, int>& val) mutable {
                                            return map___SQL_SUM_AGGREGATE_1_mS1_s4_buf_unwrap.erase(val);
                                          });
                                          frontier.iterate([this,
                                          map___SQL_SUM_AGGREGATE_1_mS1_s4_buf_unwrap = std::move(map___SQL_SUM_AGGREGATE_1_mS1_s4_buf_unwrap)] (const R_r1_r2_r3<R_key_value<int,
                                          int>, int, int>& val) mutable {
                                            return map___SQL_SUM_AGGREGATE_1_mS1_s4_buf_unwrap.insert(val);
                                          });
                                          {
                                            _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> __853;
                                            __853 = _Set<R_r1_r2_r3<R_key_value<int, int>, int,
                                            int>> {};
                                            auto& temp = __853;
                                            {
                                              auto& map___SQL_SUM_AGGREGATE_1_mS1_s6_buf_unwrap = *map___SQL_SUM_AGGREGATE_1_mS1_s6_buf;
                                              {
                                                _Set<R_r1_r2_r3<R_key_value<int, int>, int,
                                                int>> __854;
                                                __854 = frontier_int_int(R_key_value<R_key_value<int,
                                                int>, _Set<R_r1_r2_r3<R_key_value<int, int>, int,
                                                int>>> {min_gc_vid,
                                                map___SQL_SUM_AGGREGATE_1_mS1_s6_buf_unwrap});
                                                auto& frontier = __854;
                                                map___SQL_SUM_AGGREGATE_1_mS1_s6_buf_unwrap.iterate([this,
                                                &min_gc_vid,
                                                temp = std::move(temp)] (const R_r1_r2_r3<R_key_value<int,
                                                int>, int, int>& b1) mutable {
                                                  {
                                                    auto& vid = b1.r1;
                                                    auto& map_0 = b1.r2;
                                                    auto& map_val = b1.r3;
                                                    if (vid < min_gc_vid) {
                                                      return temp.insert(R_r1_r2_r3<R_key_value<int,
                                                      int>, int, int> {vid, map_0, map_val});
                                                    } else {
                                                      return unit_t {};
                                                    }
                                                  }
                                                });
                                                temp.iterate([this,
                                                map___SQL_SUM_AGGREGATE_1_mS1_s6_buf_unwrap = std::move(map___SQL_SUM_AGGREGATE_1_mS1_s6_buf_unwrap)] (const R_r1_r2_r3<R_key_value<int,
                                                int>, int, int>& val) mutable {
                                                  return map___SQL_SUM_AGGREGATE_1_mS1_s6_buf_unwrap.erase(val);
                                                });
                                                frontier.iterate([this,
                                                map___SQL_SUM_AGGREGATE_1_mS1_s6_buf_unwrap = std::move(map___SQL_SUM_AGGREGATE_1_mS1_s6_buf_unwrap)] (const R_r1_r2_r3<R_key_value<int,
                                                int>, int, int>& val) mutable {
                                                  return map___SQL_SUM_AGGREGATE_1_mS1_s6_buf_unwrap.insert(val);
                                                });
                                                {
                                                  _Set<R_r1_r2_r3<R_key_value<int, int>, int,
                                                  int>> __855;
                                                  __855 = _Set<R_r1_r2_r3<R_key_value<int, int>,
                                                  int, int>> {};
                                                  auto& temp = __855;
                                                  {
                                                    auto& map___SQL_SUM_AGGREGATE_2_mS3_s6_buf_unwrap = *map___SQL_SUM_AGGREGATE_2_mS3_s6_buf;
                                                    {
                                                      _Set<R_r1_r2_r3<R_key_value<int, int>, int,
                                                      int>> __856;
                                                      __856 = frontier_int_int(R_key_value<R_key_value<int,
                                                      int>, _Set<R_r1_r2_r3<R_key_value<int, int>,
                                                      int, int>>> {min_gc_vid,
                                                      map___SQL_SUM_AGGREGATE_2_mS3_s6_buf_unwrap});
                                                      auto& frontier = __856;
                                                      map___SQL_SUM_AGGREGATE_2_mS3_s6_buf_unwrap.iterate([this,
                                                      &min_gc_vid,
                                                      temp = std::move(temp)] (const R_r1_r2_r3<R_key_value<int,
                                                      int>, int, int>& b1) mutable {
                                                        {
                                                          auto& vid = b1.r1;
                                                          auto& map_0 = b1.r2;
                                                          auto& map_val = b1.r3;
                                                          if (vid < min_gc_vid) {
                                                            return temp.insert(R_r1_r2_r3<R_key_value<int,
                                                            int>, int, int> {vid, map_0, map_val});
                                                          } else {
                                                            return unit_t {};
                                                          }
                                                        }
                                                      });
                                                      temp.iterate([this,
                                                      map___SQL_SUM_AGGREGATE_2_mS3_s6_buf_unwrap = std::move(map___SQL_SUM_AGGREGATE_2_mS3_s6_buf_unwrap)] (const R_r1_r2_r3<R_key_value<int,
                                                      int>, int, int>& val) mutable {
                                                        return map___SQL_SUM_AGGREGATE_2_mS3_s6_buf_unwrap.erase(val);
                                                      });
                                                      frontier.iterate([this,
                                                      map___SQL_SUM_AGGREGATE_2_mS3_s6_buf_unwrap = std::move(map___SQL_SUM_AGGREGATE_2_mS3_s6_buf_unwrap)] (const R_r1_r2_r3<R_key_value<int,
                                                      int>, int, int>& val) mutable {
                                                        return map___SQL_SUM_AGGREGATE_2_mS3_s6_buf_unwrap.insert(val);
                                                      });
                                                      {
                                                        _Set<R_r1_r2_r3<R_key_value<int, int>, int,
                                                        int>> __857;
                                                        __857 = _Set<R_r1_r2_r3<R_key_value<int,
                                                        int>, int, int>> {};
                                                        auto& temp = __857;
                                                        {
                                                          auto& map___SQL_SUM_AGGREGATE_1_mR1_s8_buf_unwrap = *map___SQL_SUM_AGGREGATE_1_mR1_s8_buf;
                                                          {
                                                            _Set<R_r1_r2_r3<R_key_value<int, int>,
                                                            int, int>> __858;
                                                            __858 = frontier_int_int(R_key_value<R_key_value<int,
                                                            int>, _Set<R_r1_r2_r3<R_key_value<int,
                                                            int>, int, int>>> {min_gc_vid,
                                                            map___SQL_SUM_AGGREGATE_1_mR1_s8_buf_unwrap});
                                                            auto& frontier = __858;
                                                            map___SQL_SUM_AGGREGATE_1_mR1_s8_buf_unwrap.iterate([this,
                                                            &min_gc_vid,
                                                            temp = std::move(temp)] (const R_r1_r2_r3<R_key_value<int,
                                                            int>, int, int>& b1) mutable {
                                                              {
                                                                auto& vid = b1.r1;
                                                                auto& map_0 = b1.r2;
                                                                auto& map_val = b1.r3;
                                                                if (vid < min_gc_vid) {
                                                                  return temp.insert(R_r1_r2_r3<R_key_value<int,
                                                                  int>, int, int> {vid, map_0,
                                                                  map_val});
                                                                } else {
                                                                  return unit_t {};
                                                                }
                                                              }
                                                            });
                                                            temp.iterate([this,
                                                            map___SQL_SUM_AGGREGATE_1_mR1_s8_buf_unwrap = std::move(map___SQL_SUM_AGGREGATE_1_mR1_s8_buf_unwrap)] (const R_r1_r2_r3<R_key_value<int,
                                                            int>, int, int>& val) mutable {
                                                              return map___SQL_SUM_AGGREGATE_1_mR1_s8_buf_unwrap.erase(val);
                                                            });
                                                            frontier.iterate([this,
                                                            map___SQL_SUM_AGGREGATE_1_mR1_s8_buf_unwrap = std::move(map___SQL_SUM_AGGREGATE_1_mR1_s8_buf_unwrap)] (const R_r1_r2_r3<R_key_value<int,
                                                            int>, int, int>& val) mutable {
                                                              return map___SQL_SUM_AGGREGATE_1_mR1_s8_buf_unwrap.insert(val);
                                                            });
                                                            {
                                                              _Set<R_r1_r2_r3<R_key_value<int, int>,
                                                              int, int>> __859;
                                                              __859 = _Set<R_r1_r2_r3<R_key_value<int,
                                                              int>, int, int>> {};
                                                              auto& temp = __859;
                                                              {
                                                                auto& map___SQL_SUM_AGGREGATE_2_mR1_s10_buf_unwrap = *map___SQL_SUM_AGGREGATE_2_mR1_s10_buf;
                                                                {
                                                                  _Set<R_r1_r2_r3<R_key_value<int,
                                                                  int>, int, int>> __860;
                                                                  __860 = frontier_int_int(R_key_value<R_key_value<int,
                                                                  int>,
                                                                  _Set<R_r1_r2_r3<R_key_value<int,
                                                                  int>, int, int>>> {min_gc_vid,
                                                                  map___SQL_SUM_AGGREGATE_2_mR1_s10_buf_unwrap});
                                                                  auto& frontier = __860;
                                                                  map___SQL_SUM_AGGREGATE_2_mR1_s10_buf_unwrap.iterate([this,
                                                                  &min_gc_vid,
                                                                  temp = std::move(temp)] (const R_r1_r2_r3<R_key_value<int,
                                                                  int>, int, int>& b1) mutable {
                                                                    {
                                                                      auto& vid = b1.r1;
                                                                      auto& map_0 = b1.r2;
                                                                      auto& map_val = b1.r3;
                                                                      if (vid < min_gc_vid) {
                                                                        return temp.insert(R_r1_r2_r3<R_key_value<int,
                                                                        int>, int, int> {vid, map_0,
                                                                        map_val});
                                                                      } else {
                                                                        return unit_t {};
                                                                      }
                                                                    }
                                                                  });
                                                                  temp.iterate([this,
                                                                  map___SQL_SUM_AGGREGATE_2_mR1_s10_buf_unwrap = std::move(map___SQL_SUM_AGGREGATE_2_mR1_s10_buf_unwrap)] (const R_r1_r2_r3<R_key_value<int,
                                                                  int>, int, int>& val) mutable {
                                                                    return map___SQL_SUM_AGGREGATE_2_mR1_s10_buf_unwrap.erase(val);
                                                                  });
                                                                  frontier.iterate([this,
                                                                  map___SQL_SUM_AGGREGATE_2_mR1_s10_buf_unwrap = std::move(map___SQL_SUM_AGGREGATE_2_mR1_s10_buf_unwrap)] (const R_r1_r2_r3<R_key_value<int,
                                                                  int>, int, int>& val) mutable {
                                                                    return map___SQL_SUM_AGGREGATE_2_mR1_s10_buf_unwrap.insert(val);
                                                                  });
                                                                  {
                                                                    _Set<R_r1_r2_r3<R_key_value<int,
                                                                    int>, int, int>> __861;
                                                                    __861 = _Set<R_r1_r2_r3<R_key_value<int,
                                                                    int>, int, int>> {};
                                                                    auto& temp = __861;
                                                                    {
                                                                      auto& map___SQL_SUM_AGGREGATE_1_mR1_s10_buf_unwrap = *map___SQL_SUM_AGGREGATE_1_mR1_s10_buf;
                                                                      {
                                                                        _Set<R_r1_r2_r3<R_key_value<int,
                                                                        int>, int, int>> __862;
                                                                        __862 = frontier_int_int(R_key_value<R_key_value<int,
                                                                        int>,
                                                                        _Set<R_r1_r2_r3<R_key_value<int,
                                                                        int>, int,
                                                                        int>>> {min_gc_vid,
                                                                        map___SQL_SUM_AGGREGATE_1_mR1_s10_buf_unwrap});
                                                                        auto& frontier = __862;
                                                                        map___SQL_SUM_AGGREGATE_1_mR1_s10_buf_unwrap.iterate([this,
                                                                        &min_gc_vid,
                                                                        temp = std::move(temp)] (const R_r1_r2_r3<R_key_value<int,
                                                                        int>, int,
                                                                        int>& b1) mutable {
                                                                          {
                                                                            auto& vid = b1.r1;
                                                                            auto& map_0 = b1.r2;
                                                                            auto& map_val = b1.r3;
                                                                            if (vid < min_gc_vid) {
                                                                              return temp.insert(R_r1_r2_r3<R_key_value<int,
                                                                              int>, int, int> {vid,
                                                                              map_0, map_val});
                                                                            } else {
                                                                              return unit_t {};
                                                                            }
                                                                          }
                                                                        });
                                                                        temp.iterate([this,
                                                                        map___SQL_SUM_AGGREGATE_1_mR1_s10_buf_unwrap = std::move(map___SQL_SUM_AGGREGATE_1_mR1_s10_buf_unwrap)] (const R_r1_r2_r3<R_key_value<int,
                                                                        int>, int,
                                                                        int>& val) mutable {
                                                                          return map___SQL_SUM_AGGREGATE_1_mR1_s10_buf_unwrap.erase(val);
                                                                        });
                                                                        frontier.iterate([this,
                                                                        map___SQL_SUM_AGGREGATE_1_mR1_s10_buf_unwrap = std::move(map___SQL_SUM_AGGREGATE_1_mR1_s10_buf_unwrap)] (const R_r1_r2_r3<R_key_value<int,
                                                                        int>, int,
                                                                        int>& val) mutable {
                                                                          return map___SQL_SUM_AGGREGATE_1_mR1_s10_buf_unwrap.insert(val);
                                                                        });
                                                                        {
                                                                          _Set<R_r1_r2_r3<R_key_value<int,
                                                                          int>, int, int>> __863;
                                                                          __863 = _Set<R_r1_r2_r3<R_key_value<int,
                                                                          int>, int, int>> {};
                                                                          auto& temp = __863;
                                                                          {
                                                                            auto& map___SQL_SUM_AGGREGATE_1_mR1_s12_buf_unwrap = *map___SQL_SUM_AGGREGATE_1_mR1_s12_buf;
                                                                            {
                                                                              _Set<R_r1_r2_r3<R_key_value<int,
                                                                              int>, int,
                                                                              int>> __864;
                                                                              __864 = frontier_int_int(R_key_value<R_key_value<int,
                                                                              int>,
                                                                              _Set<R_r1_r2_r3<R_key_value<int,
                                                                              int>, int,
                                                                              int>>> {min_gc_vid,
                                                                              map___SQL_SUM_AGGREGATE_1_mR1_s12_buf_unwrap});
                                                                              auto& frontier = __864;
                                                                              map___SQL_SUM_AGGREGATE_1_mR1_s12_buf_unwrap.iterate([this,
                                                                              &min_gc_vid,
                                                                              temp = std::move(temp)] (const R_r1_r2_r3<R_key_value<int,
                                                                              int>, int,
                                                                              int>& b1) mutable {
                                                                                {
                                                                                  auto& vid = b1.r1;
                                                                                  auto& map_0 = b1.r2;
                                                                                  auto& map_val = b1.r3;
                                                                                  if (vid < min_gc_vid) {
                                                                                    return temp.insert(R_r1_r2_r3<R_key_value<int,
                                                                                    int>, int,
                                                                                    int> {vid,
                                                                                    map_0,
                                                                                    map_val});
                                                                                  } else {
                                                                                    return unit_t {};
                                                                                  }
                                                                                }
                                                                              });
                                                                              temp.iterate([this,
                                                                              map___SQL_SUM_AGGREGATE_1_mR1_s12_buf_unwrap = std::move(map___SQL_SUM_AGGREGATE_1_mR1_s12_buf_unwrap)] (const R_r1_r2_r3<R_key_value<int,
                                                                              int>, int,
                                                                              int>& val) mutable {
                                                                                return map___SQL_SUM_AGGREGATE_1_mR1_s12_buf_unwrap.erase(val);
                                                                              });
                                                                              frontier.iterate([this,
                                                                              map___SQL_SUM_AGGREGATE_1_mR1_s12_buf_unwrap = std::move(map___SQL_SUM_AGGREGATE_1_mR1_s12_buf_unwrap)] (const R_r1_r2_r3<R_key_value<int,
                                                                              int>, int,
                                                                              int>& val) mutable {
                                                                                return map___SQL_SUM_AGGREGATE_1_mR1_s12_buf_unwrap.insert(val);
                                                                              });
                                                                              {
                                                                                _Set<R_r1_r2_r3<R_key_value<int,
                                                                                int>, int,
                                                                                int>> __865;
                                                                                __865 = _Set<R_r1_r2_r3<R_key_value<int,
                                                                                int>, int, int>> {};
                                                                                auto& temp = __865;
                                                                                {
                                                                                  auto& map___SQL_SUM_AGGREGATE_2_mR1_s14_buf_unwrap = *map___SQL_SUM_AGGREGATE_2_mR1_s14_buf;
                                                                                  {
                                                                                    _Set<R_r1_r2_r3<R_key_value<int,
                                                                                    int>, int,
                                                                                    int>> __866;
                                                                                    __866 = frontier_int_int(R_key_value<R_key_value<int,
                                                                                    int>,
                                                                                    _Set<R_r1_r2_r3<R_key_value<int,
                                                                                    int>, int,
                                                                                    int>>> {min_gc_vid,
                                                                                    map___SQL_SUM_AGGREGATE_2_mR1_s14_buf_unwrap});
                                                                                    auto& frontier = __866;
                                                                                    map___SQL_SUM_AGGREGATE_2_mR1_s14_buf_unwrap.iterate([this,
                                                                                    &min_gc_vid,
                                                                                    temp = std::move(temp)] (const R_r1_r2_r3<R_key_value<int,
                                                                                    int>, int,
                                                                                    int>& b1) mutable {
                                                                                      {
                                                                                        auto& vid = b1.r1;
                                                                                        auto& map_0 = b1.r2;
                                                                                        auto& map_val = b1.r3;
                                                                                        if (vid < min_gc_vid) {
                                                                                          return temp.insert(R_r1_r2_r3<R_key_value<int,
                                                                                          int>, int,
                                                                                          int> {vid,
                                                                                          map_0,
                                                                                          map_val});
                                                                                        } else {
                                                                                          return unit_t {};
                                                                                        }
                                                                                      }
                                                                                    });
                                                                                    temp.iterate([this,
                                                                                    map___SQL_SUM_AGGREGATE_2_mR1_s14_buf_unwrap = std::move(map___SQL_SUM_AGGREGATE_2_mR1_s14_buf_unwrap)] (const R_r1_r2_r3<R_key_value<int,
                                                                                    int>, int,
                                                                                    int>& val) mutable {
                                                                                      return map___SQL_SUM_AGGREGATE_2_mR1_s14_buf_unwrap.erase(val);
                                                                                    });
                                                                                    frontier.iterate([this,
                                                                                    map___SQL_SUM_AGGREGATE_2_mR1_s14_buf_unwrap = std::move(map___SQL_SUM_AGGREGATE_2_mR1_s14_buf_unwrap)] (const R_r1_r2_r3<R_key_value<int,
                                                                                    int>, int,
                                                                                    int>& val) mutable {
                                                                                      return map___SQL_SUM_AGGREGATE_2_mR1_s14_buf_unwrap.insert(val);
                                                                                    });
                                                                                    {
                                                                                      _Set<R_r1_r2_r3<R_key_value<int,
                                                                                      int>, int,
                                                                                      int>> __867;
                                                                                      __867 = _Set<R_r1_r2_r3<R_key_value<int,
                                                                                      int>, int,
                                                                                      int>> {};
                                                                                      auto& temp = __867;
                                                                                      {
                                                                                        auto& map___SQL_SUM_AGGREGATE_1_mR1_s14_buf_unwrap = *map___SQL_SUM_AGGREGATE_1_mR1_s14_buf;
                                                                                        {
                                                                                          _Set<R_r1_r2_r3<R_key_value<int,
                                                                                          int>, int,
                                                                                          int>> __868;
                                                                                          __868 = frontier_int_int(R_key_value<R_key_value<int,
                                                                                          int>,
                                                                                          _Set<R_r1_r2_r3<R_key_value<int,
                                                                                          int>, int,
                                                                                          int>>> {min_gc_vid,
                                                                                          map___SQL_SUM_AGGREGATE_1_mR1_s14_buf_unwrap});
                                                                                          auto& frontier = __868;
                                                                                          map___SQL_SUM_AGGREGATE_1_mR1_s14_buf_unwrap.iterate([this,
                                                                                          &min_gc_vid,
                                                                                          temp = std::move(temp)] (const R_r1_r2_r3<R_key_value<int,
                                                                                          int>, int,
                                                                                          int>& b1) mutable {
                                                                                            {
                                                                                              auto& vid = b1.r1;
                                                                                              auto& map_0 = b1.r2;
                                                                                              auto& map_val = b1.r3;
                                                                                              if (vid < min_gc_vid) {
                                                                                                return temp.insert(R_r1_r2_r3<R_key_value<int,
                                                                                                int>,
                                                                                                int,
                                                                                                int> {vid,
                                                                                                map_0,
                                                                                                map_val});
                                                                                              } else {
                                                                                                return unit_t {};
                                                                                              }
                                                                                            }
                                                                                          });
                                                                                          temp.iterate([this,
                                                                                          map___SQL_SUM_AGGREGATE_1_mR1_s14_buf_unwrap = std::move(map___SQL_SUM_AGGREGATE_1_mR1_s14_buf_unwrap)] (const R_r1_r2_r3<R_key_value<int,
                                                                                          int>, int,
                                                                                          int>& val) mutable {
                                                                                            return map___SQL_SUM_AGGREGATE_1_mR1_s14_buf_unwrap.erase(val);
                                                                                          });
                                                                                          frontier.iterate([this,
                                                                                          map___SQL_SUM_AGGREGATE_1_mR1_s14_buf_unwrap = std::move(map___SQL_SUM_AGGREGATE_1_mR1_s14_buf_unwrap)] (const R_r1_r2_r3<R_key_value<int,
                                                                                          int>, int,
                                                                                          int>& val) mutable {
                                                                                            return map___SQL_SUM_AGGREGATE_1_mR1_s14_buf_unwrap.insert(val);
                                                                                          });
                                                                                          {
                                                                                            _Set<R_key_value<R_key_value<int,
                                                                                            int>,
                                                                                            int>> __869;
                                                                                            __869 = _Set<R_key_value<R_key_value<int,
                                                                                            int>,
                                                                                            int>> {};
                                                                                            auto& temp = __869;
                                                                                            {
                                                                                              auto& __SQL_SUM_AGGREGATE_1_unwrap = *__SQL_SUM_AGGREGATE_1;
                                                                                              {
                                                                                                _Set<R_key_value<R_key_value<int,
                                                                                                int>,
                                                                                                int>> __870;
                                                                                                __870 = frontier_int(R_key_value<R_key_value<int,
                                                                                                int>,
                                                                                                _Set<R_key_value<R_key_value<int,
                                                                                                int>,
                                                                                                int>>> {min_gc_vid,
                                                                                                __SQL_SUM_AGGREGATE_1_unwrap});
                                                                                                auto& frontier = __870;
                                                                                                __SQL_SUM_AGGREGATE_1_unwrap.iterate([this,
                                                                                                &min_gc_vid,
                                                                                                temp = std::move(temp)] (const R_key_value<R_key_value<int,
                                                                                                int>,
                                                                                                int>& b1) mutable {
                                                                                                  {
                                                                                                    auto& vid = b1.key;
                                                                                                    auto& map_val = b1.value;
                                                                                                    if (vid < min_gc_vid) {
                                                                                                      return temp.insert(R_key_value<R_key_value<int,
                                                                                                      int>,
                                                                                                      int> {vid,
                                                                                                      map_val});
                                                                                                    } else {
                                                                                                      return unit_t {};
                                                                                                    }
                                                                                                  }
                                                                                                });
                                                                                                temp.iterate([this,
                                                                                                __SQL_SUM_AGGREGATE_1_unwrap = std::move(__SQL_SUM_AGGREGATE_1_unwrap)] (const R_key_value<R_key_value<int,
                                                                                                int>,
                                                                                                int>& val) mutable {
                                                                                                  return __SQL_SUM_AGGREGATE_1_unwrap.erase(val);
                                                                                                });
                                                                                                frontier.iterate([this,
                                                                                                __SQL_SUM_AGGREGATE_1_unwrap = std::move(__SQL_SUM_AGGREGATE_1_unwrap)] (const R_key_value<R_key_value<int,
                                                                                                int>,
                                                                                                int>& val) mutable {
                                                                                                  return __SQL_SUM_AGGREGATE_1_unwrap.insert(val);
                                                                                                });
                                                                                                {
                                                                                                  _Set<R_r1_r2_r3<R_key_value<int,
                                                                                                  int>,
                                                                                                  int,
                                                                                                  int>> __871;
                                                                                                  __871 = _Set<R_r1_r2_r3<R_key_value<int,
                                                                                                  int>,
                                                                                                  int,
                                                                                                  int>> {};
                                                                                                  auto& temp = __871;
                                                                                                  {
                                                                                                    auto& __SQL_SUM_AGGREGATE_1_mS1_unwrap = *__SQL_SUM_AGGREGATE_1_mS1;
                                                                                                    {
                                                                                                      _Set<R_r1_r2_r3<R_key_value<int,
                                                                                                      int>,
                                                                                                      int,
                                                                                                      int>> __872;
                                                                                                      __872 = frontier_int_int(R_key_value<R_key_value<int,
                                                                                                      int>,
                                                                                                      _Set<R_r1_r2_r3<R_key_value<int,
                                                                                                      int>,
                                                                                                      int,
                                                                                                      int>>> {min_gc_vid,
                                                                                                      __SQL_SUM_AGGREGATE_1_mS1_unwrap});
                                                                                                      auto& frontier = __872;
                                                                                                      __SQL_SUM_AGGREGATE_1_mS1_unwrap.iterate([this,
                                                                                                      &min_gc_vid,
                                                                                                      temp = std::move(temp)] (const R_r1_r2_r3<R_key_value<int,
                                                                                                      int>,
                                                                                                      int,
                                                                                                      int>& b1) mutable {
                                                                                                        {
                                                                                                          auto& vid = b1.r1;
                                                                                                          auto& map_0 = b1.r2;
                                                                                                          auto& map_val = b1.r3;
                                                                                                          if (vid < min_gc_vid) {
                                                                                                            return temp.insert(R_r1_r2_r3<R_key_value<int,
                                                                                                            int>,
                                                                                                            int,
                                                                                                            int> {vid,
                                                                                                            map_0,
                                                                                                            map_val});
                                                                                                          } else {
                                                                                                            return unit_t {};
                                                                                                          }
                                                                                                        }
                                                                                                      });
                                                                                                      temp.iterate([this,
                                                                                                      __SQL_SUM_AGGREGATE_1_mS1_unwrap = std::move(__SQL_SUM_AGGREGATE_1_mS1_unwrap)] (const R_r1_r2_r3<R_key_value<int,
                                                                                                      int>,
                                                                                                      int,
                                                                                                      int>& val) mutable {
                                                                                                        return __SQL_SUM_AGGREGATE_1_mS1_unwrap.erase(val);
                                                                                                      });
                                                                                                      frontier.iterate([this,
                                                                                                      __SQL_SUM_AGGREGATE_1_mS1_unwrap = std::move(__SQL_SUM_AGGREGATE_1_mS1_unwrap)] (const R_r1_r2_r3<R_key_value<int,
                                                                                                      int>,
                                                                                                      int,
                                                                                                      int>& val) mutable {
                                                                                                        return __SQL_SUM_AGGREGATE_1_mS1_unwrap.insert(val);
                                                                                                      });
                                                                                                      {
                                                                                                        _Set<R_r1_r2_r3<R_key_value<int,
                                                                                                        int>,
                                                                                                        int,
                                                                                                        int>> __873;
                                                                                                        __873 = _Set<R_r1_r2_r3<R_key_value<int,
                                                                                                        int>,
                                                                                                        int,
                                                                                                        int>> {};
                                                                                                        auto& temp = __873;
                                                                                                        {
                                                                                                          auto& __SQL_SUM_AGGREGATE_1_mR1_unwrap = *__SQL_SUM_AGGREGATE_1_mR1;
                                                                                                          {
                                                                                                            _Set<R_r1_r2_r3<R_key_value<int,
                                                                                                            int>,
                                                                                                            int,
                                                                                                            int>> __874;
                                                                                                            __874 = frontier_int_int(R_key_value<R_key_value<int,
                                                                                                            int>,
                                                                                                            _Set<R_r1_r2_r3<R_key_value<int,
                                                                                                            int>,
                                                                                                            int,
                                                                                                            int>>> {min_gc_vid,
                                                                                                            __SQL_SUM_AGGREGATE_1_mR1_unwrap});
                                                                                                            auto& frontier = __874;
                                                                                                            __SQL_SUM_AGGREGATE_1_mR1_unwrap.iterate([this,
                                                                                                            &min_gc_vid,
                                                                                                            temp = std::move(temp)] (const R_r1_r2_r3<R_key_value<int,
                                                                                                            int>,
                                                                                                            int,
                                                                                                            int>& b1) mutable {
                                                                                                              {
                                                                                                                auto& vid = b1.r1;
                                                                                                                auto& map_0 = b1.r2;
                                                                                                                auto& map_val = b1.r3;
                                                                                                                if (vid < min_gc_vid) {
                                                                                                                  return temp.insert(R_r1_r2_r3<R_key_value<int,
                                                                                                                  int>,
                                                                                                                  int,
                                                                                                                  int> {vid,
                                                                                                                  map_0,
                                                                                                                  map_val});
                                                                                                                } else {
                                                                                                                  return unit_t {};
                                                                                                                }
                                                                                                              }
                                                                                                            });
                                                                                                            temp.iterate([this,
                                                                                                            __SQL_SUM_AGGREGATE_1_mR1_unwrap = std::move(__SQL_SUM_AGGREGATE_1_mR1_unwrap)] (const R_r1_r2_r3<R_key_value<int,
                                                                                                            int>,
                                                                                                            int,
                                                                                                            int>& val) mutable {
                                                                                                              return __SQL_SUM_AGGREGATE_1_mR1_unwrap.erase(val);
                                                                                                            });
                                                                                                            frontier.iterate([this,
                                                                                                            __SQL_SUM_AGGREGATE_1_mR1_unwrap = std::move(__SQL_SUM_AGGREGATE_1_mR1_unwrap)] (const R_r1_r2_r3<R_key_value<int,
                                                                                                            int>,
                                                                                                            int,
                                                                                                            int>& val) mutable {
                                                                                                              return __SQL_SUM_AGGREGATE_1_mR1_unwrap.insert(val);
                                                                                                            });
                                                                                                            {
                                                                                                              _Set<R_key_value<R_key_value<int,
                                                                                                              int>,
                                                                                                              int>> __875;
                                                                                                              __875 = _Set<R_key_value<R_key_value<int,
                                                                                                              int>,
                                                                                                              int>> {};
                                                                                                              auto& temp = __875;
                                                                                                              {
                                                                                                                auto& __SQL_SUM_AGGREGATE_2_unwrap = *__SQL_SUM_AGGREGATE_2;
                                                                                                                {
                                                                                                                  _Set<R_key_value<R_key_value<int,
                                                                                                                  int>,
                                                                                                                  int>> __876;
                                                                                                                  __876 = frontier_int(R_key_value<R_key_value<int,
                                                                                                                  int>,
                                                                                                                  _Set<R_key_value<R_key_value<int,
                                                                                                                  int>,
                                                                                                                  int>>> {min_gc_vid,
                                                                                                                  __SQL_SUM_AGGREGATE_2_unwrap});
                                                                                                                  auto& frontier = __876;
                                                                                                                  __SQL_SUM_AGGREGATE_2_unwrap.iterate([this,
                                                                                                                  &min_gc_vid,
                                                                                                                  temp = std::move(temp)] (const R_key_value<R_key_value<int,
                                                                                                                  int>,
                                                                                                                  int>& b1) mutable {
                                                                                                                    {
                                                                                                                      auto& vid = b1.key;
                                                                                                                      auto& map_val = b1.value;
                                                                                                                      if (vid < min_gc_vid) {
                                                                                                                        return temp.insert(R_key_value<R_key_value<int,
                                                                                                                        int>,
                                                                                                                        int> {vid,
                                                                                                                        map_val});
                                                                                                                      } else {
                                                                                                                        return unit_t {};
                                                                                                                      }
                                                                                                                    }
                                                                                                                  });
                                                                                                                  temp.iterate([this,
                                                                                                                  __SQL_SUM_AGGREGATE_2_unwrap = std::move(__SQL_SUM_AGGREGATE_2_unwrap)] (const R_key_value<R_key_value<int,
                                                                                                                  int>,
                                                                                                                  int>& val) mutable {
                                                                                                                    return __SQL_SUM_AGGREGATE_2_unwrap.erase(val);
                                                                                                                  });
                                                                                                                  frontier.iterate([this,
                                                                                                                  __SQL_SUM_AGGREGATE_2_unwrap = std::move(__SQL_SUM_AGGREGATE_2_unwrap)] (const R_key_value<R_key_value<int,
                                                                                                                  int>,
                                                                                                                  int>& val) mutable {
                                                                                                                    return __SQL_SUM_AGGREGATE_2_unwrap.insert(val);
                                                                                                                  });
                                                                                                                  {
                                                                                                                    _Set<R_r1_r2_r3<R_key_value<int,
                                                                                                                    int>,
                                                                                                                    int,
                                                                                                                    int>> __877;
                                                                                                                    __877 = _Set<R_r1_r2_r3<R_key_value<int,
                                                                                                                    int>,
                                                                                                                    int,
                                                                                                                    int>> {};
                                                                                                                    auto& temp = __877;
                                                                                                                    {
                                                                                                                      auto& __SQL_SUM_AGGREGATE_2_mS3_unwrap = *__SQL_SUM_AGGREGATE_2_mS3;
                                                                                                                      {
                                                                                                                        _Set<R_r1_r2_r3<R_key_value<int,
                                                                                                                        int>,
                                                                                                                        int,
                                                                                                                        int>> __878;
                                                                                                                        __878 = frontier_int_int(R_key_value<R_key_value<int,
                                                                                                                        int>,
                                                                                                                        _Set<R_r1_r2_r3<R_key_value<int,
                                                                                                                        int>,
                                                                                                                        int,
                                                                                                                        int>>> {min_gc_vid,
                                                                                                                        __SQL_SUM_AGGREGATE_2_mS3_unwrap});
                                                                                                                        auto& frontier = __878;
                                                                                                                        __SQL_SUM_AGGREGATE_2_mS3_unwrap.iterate([this,
                                                                                                                        &min_gc_vid,
                                                                                                                        temp = std::move(temp)] (const R_r1_r2_r3<R_key_value<int,
                                                                                                                        int>,
                                                                                                                        int,
                                                                                                                        int>& b1) mutable {
                                                                                                                          {
                                                                                                                            auto& vid = b1.r1;
                                                                                                                            auto& map_0 = b1.r2;
                                                                                                                            auto& map_val = b1.r3;
                                                                                                                            if (vid < min_gc_vid) {
                                                                                                                              return temp.insert(R_r1_r2_r3<R_key_value<int,
                                                                                                                              int>,
                                                                                                                              int,
                                                                                                                              int> {vid,
                                                                                                                              map_0,
                                                                                                                              map_val});
                                                                                                                            } else {
                                                                                                                              return unit_t {};
                                                                                                                            }
                                                                                                                          }
                                                                                                                        });
                                                                                                                        temp.iterate([this,
                                                                                                                        __SQL_SUM_AGGREGATE_2_mS3_unwrap = std::move(__SQL_SUM_AGGREGATE_2_mS3_unwrap)] (const R_r1_r2_r3<R_key_value<int,
                                                                                                                        int>,
                                                                                                                        int,
                                                                                                                        int>& val) mutable {
                                                                                                                          return __SQL_SUM_AGGREGATE_2_mS3_unwrap.erase(val);
                                                                                                                        });
                                                                                                                        frontier.iterate([this,
                                                                                                                        __SQL_SUM_AGGREGATE_2_mS3_unwrap = std::move(__SQL_SUM_AGGREGATE_2_mS3_unwrap)] (const R_r1_r2_r3<R_key_value<int,
                                                                                                                        int>,
                                                                                                                        int,
                                                                                                                        int>& val) mutable {
                                                                                                                          return __SQL_SUM_AGGREGATE_2_mS3_unwrap.insert(val);
                                                                                                                        });
                                                                                                                        {
                                                                                                                          _Set<R_r1_r2_r3<R_key_value<int,
                                                                                                                          int>,
                                                                                                                          int,
                                                                                                                          int>> __879;
                                                                                                                          __879 = _Set<R_r1_r2_r3<R_key_value<int,
                                                                                                                          int>,
                                                                                                                          int,
                                                                                                                          int>> {};
                                                                                                                          auto& temp = __879;
                                                                                                                          {
                                                                                                                            auto& __SQL_SUM_AGGREGATE_2_mR1_unwrap = *__SQL_SUM_AGGREGATE_2_mR1;
                                                                                                                            {
                                                                                                                              _Set<R_r1_r2_r3<R_key_value<int,
                                                                                                                              int>,
                                                                                                                              int,
                                                                                                                              int>> __880;
                                                                                                                              __880 = frontier_int_int(R_key_value<R_key_value<int,
                                                                                                                              int>,
                                                                                                                              _Set<R_r1_r2_r3<R_key_value<int,
                                                                                                                              int>,
                                                                                                                              int,
                                                                                                                              int>>> {min_gc_vid,
                                                                                                                              __SQL_SUM_AGGREGATE_2_mR1_unwrap});
                                                                                                                              auto& frontier = __880;
                                                                                                                              __SQL_SUM_AGGREGATE_2_mR1_unwrap.iterate([this,
                                                                                                                              &min_gc_vid,
                                                                                                                              temp = std::move(temp)] (const R_r1_r2_r3<R_key_value<int,
                                                                                                                              int>,
                                                                                                                              int,
                                                                                                                              int>& b1) mutable {
                                                                                                                                {
                                                                                                                                  auto& vid = b1.r1;
                                                                                                                                  auto& map_0 = b1.r2;
                                                                                                                                  auto& map_val = b1.r3;
                                                                                                                                  if (vid < min_gc_vid) {
                                                                                                                                    return temp.insert(R_r1_r2_r3<R_key_value<int,
                                                                                                                                    int>,
                                                                                                                                    int,
                                                                                                                                    int> {vid,
                                                                                                                                    map_0,
                                                                                                                                    map_val});
                                                                                                                                  } else {
                                                                                                                                    return unit_t {};
                                                                                                                                  }
                                                                                                                                }
                                                                                                                              });
                                                                                                                              temp.iterate([this,
                                                                                                                              __SQL_SUM_AGGREGATE_2_mR1_unwrap = std::move(__SQL_SUM_AGGREGATE_2_mR1_unwrap)] (const R_r1_r2_r3<R_key_value<int,
                                                                                                                              int>,
                                                                                                                              int,
                                                                                                                              int>& val) mutable {
                                                                                                                                return __SQL_SUM_AGGREGATE_2_mR1_unwrap.erase(val);
                                                                                                                              });
                                                                                                                              return frontier.iterate([this,
                                                                                                                              __SQL_SUM_AGGREGATE_2_mR1_unwrap = std::move(__SQL_SUM_AGGREGATE_2_mR1_unwrap)] (const R_r1_r2_r3<R_key_value<int,
                                                                                                                              int>,
                                                                                                                              int,
                                                                                                                              int>& val) mutable {
                                                                                                                                return __SQL_SUM_AGGREGATE_2_mR1_unwrap.insert(val);
                                                                                                                              });
                                                                                                                            }
                                                                                                                          }
                                                                                                                        }
                                                                                                                      }
                                                                                                                    }
                                                                                                                  }
                                                                                                                }
                                                                                                              }
                                                                                                            }
                                                                                                          }
                                                                                                        }
                                                                                                      }
                                                                                                    }
                                                                                                  }
                                                                                                }
                                                                                              }
                                                                                            }
                                                                                          }
                                                                                        }
                                                                                      }
                                                                                    }
                                                                                  }
                                                                                }
                                                                              }
                                                                            }
                                                                          }
                                                                        }
                                                                      }
                                                                    }
                                                                  }
                                                                }
                                                              }
                                                            }
                                                          }
                                                        }
                                                      }
                                                    }
                                                  }
                                                }
                                              }
                                            }
                                          }
                                        }
                                      }
                                    }
                                  }
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
      unit_t sw_rcv_token(const R_key_value<int, int>& vid)  {
        if (sw_need_vid_cntr > 0) {
          {
            R_key_value<int, int> __881;
            __881 = R_key_value<int, int> {vid.key, vid.value + sw_need_vid_cntr};
            auto& next_vid = __881;
            auto __882 = std::make_shared<K3::ValDispatcher<R_key_value<int, int>>>(next_vid);
            __engine.send(sw_next_switch_addr, __sw_rcv_token_tid, __882, me);
            sw_token_vid_list.insert(R_key_value<R_key_value<int, int>, int> {vid,
            sw_need_vid_cntr});
            sw_need_vid_cntr = 0;
            sw_highest_vid = next_vid;
            if ((!sw_sent_done) && sw_trig_buf_idx.size(unit_t {}) == 0 && sw_num_ack == sw_num_sent && sw_seen_sentry == true) {
              auto __883 = std::make_shared<K3::ValDispatcher<unit_t>>(unit_t {});
              __engine.send(master_addr, __ms_rcv_switch_done_tid, __883, me);
              sw_sent_done = true;
            }
            auto __884 = std::make_shared<K3::ValDispatcher<unit_t>>(unit_t {});
            __engine.send(me, __sw_driver_trig_tid, __884, me);
            return unit_t {};
          }
        } else {
          auto __885 = std::make_shared<K3::ValDispatcher<R_key_value<int, int>>>(vid);
          __engine.send(sw_next_switch_addr, __sw_rcv_token_tid, __885, me);
          return unit_t {};
        }
      }
      unit_t tm_insert_timer(const R_r1_r2_r3<int, int, Address>& b1)  {
        {
          auto& time = b1.r1;
          auto& trig_id = b1.r2;
          auto& addr = b1.r3;
          tm_timer_list.insert(R_r1_r2_r3<int, int, Address> {time + now_int(unit_t {}), trig_id,
          addr});
          tm_timer_list = tm_timer_list.sort([this] (const R_r1_r2_r3<int, int,
          Address>& b2) mutable {
            return [this, &b2] (const R_r1_r2_r3<int, int, Address>& b6) mutable {
              {
                auto& time1 = b2.r1;
                auto& trig_id1 = b2.r2;
                auto& addr1 = b2.r3;
                {
                  auto& time2 = b6.r1;
                  auto& trig_id2 = b6.r2;
                  auto& addr2 = b6.r3;
                  if (time1 < time2) {
                    return -(1);
                  } else {
                    return 1;
                  }
                }
              }
            };
          });
          auto __886 = std::make_shared<K3::ValDispatcher<unit_t>>(unit_t {});
          __engine.send(me, __tm_check_time_tid, __886, me);
          return unit_t {};
        }
      }
      unit_t tm_check_time(const unit_t& _u)  {
        shared_ptr<R_r1_r2_r3<int, int, Address>> __887;
        __887 = tm_timer_list.peek(unit_t {});
        if (__887) {
          R_r1_r2_r3<int, int, Address>& timer = *__887;
          if (now_int(unit_t {}) >= timer.r1) {
            tm_timer_list.erase(timer);
            {
              auto& time = timer.r1;
              auto& trig_id = timer.r2;
              auto& addr = timer.r3;
              if (trig_id == 0) {
                auto __888 = std::make_shared<K3::ValDispatcher<unit_t>>(unit_t {});
                __engine.send(addr, __ms_send_gc_req_tid, __888, me);
                return unit_t {};
              } else {
                auto __889 = std::make_shared<K3::ValDispatcher<unit_t>>(unit_t {});
                __engine.send(me, __tm_check_time_tid, __889, me);
                return unit_t {};
              }
            }
          } else {
            auto __890 = std::make_shared<K3::ValDispatcher<unit_t>>(unit_t {});
            __engine.send(me, __tm_check_time_tid, __890, me);
            return sleep(1000);
          }
        } else {
          return unit_t {};
        }
      }
      unit_t sw_demux(const R_r1_r2_r3_r4_r5<int, int, int, int, int>& args)  {
        if (args.r1 == 0) {
          return sw_insert_R(R_key_value<int, int> {args.r2, args.r3});
        } else {
          if (args.r1 == 1) {
            return sw_insert_S(R_key_value<int, int> {args.r4, args.r5});
          } else {
            if (args.r1 == (-(1))) {
              sw_trig_buf_idx.insert(R_i<int> {-(1)});
              sw_need_vid_cntr = sw_need_vid_cntr + 1;
              return unit_t {};
            } else {
              return error<unit_t>(print("unidentified trig id"));
            }
          }
        }
      }
      unit_t sw_driver_trig(const unit_t& _u)  {
        if (sw_init && sw_trig_buf_idx.size(unit_t {}) > 0) {
          shared_ptr<R_key_value<int, int>> __891;
          __891 = sw_gen_vid(unit_t {});
          if (__891) {
            R_key_value<int, int>& vid = *__891;
            shared_ptr<R_i<int>> __892;
            __892 = sw_trig_buf_idx.peek(unit_t {});
            if (__892) {
              R_i<int>& trig_id = *__892;
              sw_trig_buf_idx.erase(R_i<int> {trig_id.i});
              if (trig_id.i == (-(1))) {
                sw_seen_sentry = true;
                if ((!sw_sent_done) && sw_trig_buf_idx.size(unit_t {}) == 0 && sw_num_ack == sw_num_sent && sw_seen_sentry == true) {
                  auto __893 = std::make_shared<K3::ValDispatcher<unit_t>>(unit_t {});
                  __engine.send(master_addr, __ms_rcv_switch_done_tid, __893, me);
                  sw_sent_done = true;
                  return unit_t {};
                } else {
                  return unit_t {};
                }
              } else {
                if (trig_id.i == 3) {
                  sw_delete_R_send_fetch(vid);
                } else {
                  if (trig_id.i == 2) {
                    sw_insert_R_send_fetch(vid);
                  } else {
                    if (trig_id.i == 1) {
                      sw_delete_S_send_fetch(vid);
                    } else {
                      if (trig_id.i == 0) {
                        sw_insert_S_send_fetch(vid);
                      } else {
                        error<unit_t>(print("mismatch on trigger id"));
                      }
                    }
                  }
                }
                auto __894 = std::make_shared<K3::ValDispatcher<unit_t>>(unit_t {});
                __engine.send(me, __sw_driver_trig_tid, __894, me);
                return unit_t {};
              }
            } else {
              return unit_t {};
            }
          } else {
            return unit_t {};
          }
        } else {
          return unit_t {};
        }
      }
      unit_t nd_rcv_corr_done(const R_r1_r2_r3_r4<R_key_value<int, int>, int, int, int>& b1)  {
        {
          auto vid = b1.r1;
          auto stmt_id = b1.r2;
          auto hop = b1.r3;
          auto count = b1.r4;
          unit_t __895;
          nd_update_stmt_cntr_corr_map(R_r1_r2_r3_r4_r5_r6<R_key_value<int, int>, int, int, int,
          bool, bool> {vid, stmt_id, hop, count, false, false});
          shared_ptr<R_key_value<R_key_value<R_key_value<int, int>, int>, R_key_value<int,
          _Map<R_key_value<int, int>>>>> __896;
          __896 = nd_stmt_cntrs.filter([this, &stmt_id,
          &vid] (const R_key_value<R_key_value<R_key_value<int, int>, int>, R_key_value<int,
          _Map<R_key_value<int, int>>>>& b1) mutable {
            {
              auto& key = b1.key;
              auto& value = b1.value;
              return key == (R_key_value<R_key_value<int, int>, int> {vid, stmt_id});
            }
          }).peek(unit_t {});
          if (__896) {
            R_key_value<R_key_value<R_key_value<int, int>, int>, R_key_value<int,
            _Map<R_key_value<int, int>>>>& lkup = *__896;
            if (0 == lkup.value.value.size(unit_t {})) {
              nd_stmt_cntrs.erase(lkup);
              if (nd_rcvd_sys_done) {
                if ((!nd_sent_done) && nd_stmt_cntrs.size(unit_t {}) == 0) {
                  auto __897 = std::make_shared<K3::ValDispatcher<bool>>(true);
                  __engine.send(master_addr, __ms_rcv_node_done_tid, __897, me);
                  nd_sent_done = true;
                  __895 = unit_t {};
                } else {
                  __895 = unit_t {};
                }
              } else {
                __895 = unit_t {};
              }
            } else {
              __895 = unit_t {};
            }
          } else {
            __895 = error<unit_t>(print("nd_rcv_corr_done: expected stmt_cntr value"));
          }
          vid = b1.r1;
          stmt_id = b1.r2;
          hop = b1.r3;
          count = b1.r4;
          return __895;
        }
      }
      unit_t nd_insert_S_rcv_put(const R_r1_r2_r3_r4_r5<Address, _Collection<R_key_value<int, int>>,
      R_key_value<int, int>, int, int>& b1)  {
        {
          auto sender_ip = b1.r1;
          auto stmt_cnt_list = b1.r2;
          auto vid = b1.r3;
          auto S_B = b1.r4;
          auto S_C = b1.r5;
          unit_t __898;
          stmt_cnt_list.iterate([this, &S_B, &S_C, &vid] (const R_key_value<int, int>& b1) mutable {
            {
              auto& stmt_id = b1.key;
              auto& count = b1.value;
              if (nd_check_stmt_cntr_index(R_r1_r2_r3<R_key_value<int, int>, int, int> {vid,
              stmt_id, count})) {
                if (stmt_id == 3) {
                  return nd_insert_S_do_complete_s3(R_r1_r2_r3<R_key_value<int, int>, int,
                  int> {vid, S_B, S_C});
                } else {
                  if (stmt_id == 2) {
                    return nd_insert_S_do_complete_s2(R_r1_r2_r3<R_key_value<int, int>, int,
                    int> {vid, S_B, S_C});
                  } else {
                    if (stmt_id == 1) {
                      return nd_insert_S_do_complete_s1(R_r1_r2_r3<R_key_value<int, int>, int,
                      int> {vid, S_B, S_C});
                    } else {
                      if (stmt_id == 0) {
                        return nd_insert_S_do_complete_s0(R_r1_r2_r3<R_key_value<int, int>, int,
                        int> {vid, S_B, S_C});
                      } else {
                        return unit_t {};
                      }
                    }
                  }
                }
              } else {
                return unit_t {};
              }
            }
          });
          auto __899 = std::make_shared<K3::ValDispatcher<R_key_value<Address, R_key_value<int,
          int>>>>(R_key_value<Address, R_key_value<int, int>> {me, vid});
          __engine.send(sender_ip, __sw_ack_rcv_tid, __899, me);
          __898 = unit_t {};
          sender_ip = b1.r1;
          stmt_cnt_list = b1.r2;
          vid = b1.r3;
          S_B = b1.r4;
          S_C = b1.r5;
          return __898;
        }
      }
      unit_t nd_insert_S_rcv_fetch(const R_r1_r2_r3_r4<_Collection<R_key_value<int, int>>,
      R_key_value<int, int>, int, int>& b1)  {
        {
          auto stmt_map_ids = b1.r1;
          auto vid = b1.r2;
          auto S_B = b1.r3;
          auto S_C = b1.r4;
          unit_t __900;
          nd_log_write_insert_S(R_r1_r2_r3<R_key_value<int, int>, int, int> {vid, S_B, S_C});
          __900 = stmt_map_ids.iterate([this, &S_B, &S_C, &vid] (const R_key_value<int,
          int>& b1) mutable {
            {
              auto& stmt_id = b1.key;
              auto& map_id = b1.value;
              if (stmt_id == 0) {
                if (map_id == 2) {
                  auto __901 = std::make_shared<K3::ValDispatcher<R_r1_r2_r3<R_key_value<int, int>,
                  int, int>>>(R_r1_r2_r3<R_key_value<int, int>, int, int> {vid, S_B, S_C});
                  __engine.send(me, __nd_insert_S_send_push_s0_m___SQL_SUM_AGGREGATE_1_mS1_tid,
                  __901, me);
                  return unit_t {};
                } else {
                  return error<unit_t>(print("nd_rcv_fetch: invalid map id"));
                }
              } else {
                if (stmt_id == 2) {
                  if (map_id == 2) {
                    auto __902 = std::make_shared<K3::ValDispatcher<R_r1_r2_r3<R_key_value<int,
                    int>, int, int>>>(R_r1_r2_r3<R_key_value<int, int>, int, int> {vid, S_B, S_C});
                    __engine.send(me, __nd_insert_S_send_push_s2_m___SQL_SUM_AGGREGATE_1_mS1_tid,
                    __902, me);
                    return unit_t {};
                  } else {
                    if (map_id == 5) {
                      auto __903 = std::make_shared<K3::ValDispatcher<R_r1_r2_r3<R_key_value<int,
                      int>, int, int>>>(R_r1_r2_r3<R_key_value<int, int>, int, int> {vid, S_B,
                      S_C});
                      __engine.send(me, __nd_insert_S_send_push_s2_m___SQL_SUM_AGGREGATE_2_mS3_tid,
                      __903, me);
                      return unit_t {};
                    } else {
                      return error<unit_t>(print("nd_rcv_fetch: invalid map id"));
                    }
                  }
                } else {
                  return error<unit_t>(print("nd_rcv_fetch: invalid stmt id"));
                }
              }
            }
          });
          stmt_map_ids = b1.r1;
          vid = b1.r2;
          S_B = b1.r3;
          S_C = b1.r4;
          return __900;
        }
      }
      unit_t nd_insert_S_send_push_s0_m___SQL_SUM_AGGREGATE_1_mS1(const R_r1_r2_r3<R_key_value<int,
      int>, int, int>& b1)  {
        {
          auto vid = b1.r1;
          auto S_B = b1.r2;
          auto S_C = b1.r3;
          unit_t __904;
          {
            auto& __SQL_SUM_AGGREGATE_1_mS1_deref = *__SQL_SUM_AGGREGATE_1_mS1;
            nd_log_master_write(R_key_value<R_key_value<int, int>, int> {vid, 0});
            __904 = shuffle___SQL_SUM_AGGREGATE_1_mS1_to___SQL_SUM_AGGREGATE_1(R_r1_r2_r3<unit_t,
            _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>, bool> {unit_t {},
            frontier_int_int(R_key_value<R_key_value<int, int>, _Set<R_r1_r2_r3<R_key_value<int,
            int>, int, int>>> {vid, __SQL_SUM_AGGREGATE_1_mS1_deref.filter([this,
            &S_B] (const R_r1_r2_r3<R_key_value<int, int>, int, int>& b1) mutable {
              {
                auto& r1 = b1.r1;
                auto& r2 = b1.r2;
                auto& r3 = b1.r3;
                return r2 == S_B;
              }
            })}), true}).iterate([this, &S_B, &S_C, &vid] (const R_key_value<Address,
            _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>>& b1) mutable {
              {
                auto& ip = b1.key;
                auto& tuples = b1.value;
                auto __905 = std::make_shared<K3::ValDispatcher<R_r1_r2_r3_r4<_Set<R_r1_r2_r3<R_key_value<int,
                int>, int, int>>, R_key_value<int, int>, int,
                int>>>(R_r1_r2_r3_r4<_Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>,
                R_key_value<int, int>, int, int> {tuples, vid, S_B, S_C});
                __engine.send(ip, __nd_insert_S_rcv_push_s0_m___SQL_SUM_AGGREGATE_1_mS1_tid, __905,
                me);
                return unit_t {};
              }
            });
          }
          vid = b1.r1;
          S_B = b1.r2;
          S_C = b1.r3;
          return __904;
        }
      }
      unit_t nd_insert_S_send_push_s2_m___SQL_SUM_AGGREGATE_1_mS1(const R_r1_r2_r3<R_key_value<int,
      int>, int, int>& b1)  {
        {
          auto vid = b1.r1;
          auto S_B = b1.r2;
          auto S_C = b1.r3;
          unit_t __906;
          {
            auto& __SQL_SUM_AGGREGATE_1_mS1_deref = *__SQL_SUM_AGGREGATE_1_mS1;
            nd_log_master_write(R_key_value<R_key_value<int, int>, int> {vid, 2});
            __906 = shuffle___SQL_SUM_AGGREGATE_1_mS1_to___SQL_SUM_AGGREGATE_2(R_r1_r2_r3<unit_t,
            _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>, bool> {unit_t {},
            frontier_int_int(R_key_value<R_key_value<int, int>, _Set<R_r1_r2_r3<R_key_value<int,
            int>, int, int>>> {vid, __SQL_SUM_AGGREGATE_1_mS1_deref.filter([this,
            &S_B] (const R_r1_r2_r3<R_key_value<int, int>, int, int>& b1) mutable {
              {
                auto& r1 = b1.r1;
                auto& r2 = b1.r2;
                auto& r3 = b1.r3;
                return r2 == S_B;
              }
            })}), true}).iterate([this, &S_B, &S_C, &vid] (const R_key_value<Address,
            _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>>& b1) mutable {
              {
                auto& ip = b1.key;
                auto& tuples = b1.value;
                auto __907 = std::make_shared<K3::ValDispatcher<R_r1_r2_r3_r4<_Set<R_r1_r2_r3<R_key_value<int,
                int>, int, int>>, R_key_value<int, int>, int,
                int>>>(R_r1_r2_r3_r4<_Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>,
                R_key_value<int, int>, int, int> {tuples, vid, S_B, S_C});
                __engine.send(ip, __nd_insert_S_rcv_push_s2_m___SQL_SUM_AGGREGATE_1_mS1_tid, __907,
                me);
                return unit_t {};
              }
            });
          }
          vid = b1.r1;
          S_B = b1.r2;
          S_C = b1.r3;
          return __906;
        }
      }
      unit_t nd_insert_S_send_push_s2_m___SQL_SUM_AGGREGATE_2_mS3(const R_r1_r2_r3<R_key_value<int,
      int>, int, int>& b1)  {
        {
          auto vid = b1.r1;
          auto S_B = b1.r2;
          auto S_C = b1.r3;
          unit_t __908;
          {
            auto& __SQL_SUM_AGGREGATE_2_mS3_deref = *__SQL_SUM_AGGREGATE_2_mS3;
            nd_log_master_write(R_key_value<R_key_value<int, int>, int> {vid, 2});
            __908 = shuffle___SQL_SUM_AGGREGATE_2_mS3_to___SQL_SUM_AGGREGATE_2(R_r1_r2_r3<unit_t,
            _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>, bool> {unit_t {},
            frontier_int_int(R_key_value<R_key_value<int, int>, _Set<R_r1_r2_r3<R_key_value<int,
            int>, int, int>>> {vid, __SQL_SUM_AGGREGATE_2_mS3_deref.filter([this,
            &S_B] (const R_r1_r2_r3<R_key_value<int, int>, int, int>& b1) mutable {
              {
                auto& r1 = b1.r1;
                auto& r2 = b1.r2;
                auto& r3 = b1.r3;
                return r2 == S_B;
              }
            })}), true}).iterate([this, &S_B, &S_C, &vid] (const R_key_value<Address,
            _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>>& b1) mutable {
              {
                auto& ip = b1.key;
                auto& tuples = b1.value;
                auto __909 = std::make_shared<K3::ValDispatcher<R_r1_r2_r3_r4<_Set<R_r1_r2_r3<R_key_value<int,
                int>, int, int>>, R_key_value<int, int>, int,
                int>>>(R_r1_r2_r3_r4<_Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>,
                R_key_value<int, int>, int, int> {tuples, vid, S_B, S_C});
                __engine.send(ip, __nd_insert_S_rcv_push_s2_m___SQL_SUM_AGGREGATE_2_mS3_tid, __909,
                me);
                return unit_t {};
              }
            });
          }
          vid = b1.r1;
          S_B = b1.r2;
          S_C = b1.r3;
          return __908;
        }
      }
      unit_t nd_insert_S_rcv_push_s0_m___SQL_SUM_AGGREGATE_1_mS1(const R_r1_r2_r3_r4<_Set<R_r1_r2_r3<R_key_value<int,
      int>, int, int>>, R_key_value<int, int>, int, int>& b1)  {
        {
          auto tuples = b1.r1;
          auto vid = b1.r2;
          auto S_B = b1.r3;
          auto S_C = b1.r4;
          unit_t __910;
          nd_log_write_insert_S(R_r1_r2_r3<R_key_value<int, int>, int, int> {vid, S_B, S_C});
          tuples.iterate([this] (const R_r1_r2_r3<R_key_value<int, int>, int, int>& tuple) mutable {
            {
              auto& map___SQL_SUM_AGGREGATE_1_mS1_s0_buf_d = *map___SQL_SUM_AGGREGATE_1_mS1_s0_buf;
              {
                auto _tup0 = tuple.r1;
                auto _tup1 = tuple.r2;
                auto _tup2 = tuple.r3;
                unit_t __911;
                shared_ptr<R_r1_r2_r3<R_key_value<int, int>, int, int>> __912;
                __912 = map___SQL_SUM_AGGREGATE_1_mS1_s0_buf_d.filter([this, &_tup0,
                &_tup1] (const R_r1_r2_r3<R_key_value<int, int>, int, int>& b1) mutable {
                  {
                    auto& r1 = b1.r1;
                    auto& r2 = b1.r2;
                    auto& r3 = b1.r3;
                    return r1 == _tup0 && r2 == _tup1;
                  }
                }).peek(unit_t {});
                if (__912) {
                  R_r1_r2_r3<R_key_value<int, int>, int, int>& vals = *__912;
                  __911 = map___SQL_SUM_AGGREGATE_1_mS1_s0_buf_d.update(vals, tuple);
                } else {
                  __911 = map___SQL_SUM_AGGREGATE_1_mS1_s0_buf_d.insert(tuple);
                }
                _tup0 = tuple.r1;
                _tup1 = tuple.r2;
                _tup2 = tuple.r3;
                return __911;
              }
            }
          });
          if (nd_check_stmt_cntr_index(R_r1_r2_r3<R_key_value<int, int>, int, int> {vid, 0,
          -(1)})) {
            __910 = nd_insert_S_do_complete_s0(R_r1_r2_r3<R_key_value<int, int>, int, int> {vid,
            S_B, S_C});
          } else {
            __910 = unit_t {};
          }
          tuples = b1.r1;
          vid = b1.r2;
          S_B = b1.r3;
          S_C = b1.r4;
          return __910;
        }
      }
      unit_t nd_insert_S_rcv_push_s2_m___SQL_SUM_AGGREGATE_1_mS1(const R_r1_r2_r3_r4<_Set<R_r1_r2_r3<R_key_value<int,
      int>, int, int>>, R_key_value<int, int>, int, int>& b1)  {
        {
          auto tuples = b1.r1;
          auto vid = b1.r2;
          auto S_B = b1.r3;
          auto S_C = b1.r4;
          unit_t __913;
          nd_log_write_insert_S(R_r1_r2_r3<R_key_value<int, int>, int, int> {vid, S_B, S_C});
          tuples.iterate([this] (const R_r1_r2_r3<R_key_value<int, int>, int, int>& tuple) mutable {
            {
              auto& map___SQL_SUM_AGGREGATE_1_mS1_s2_buf_d = *map___SQL_SUM_AGGREGATE_1_mS1_s2_buf;
              {
                auto _tup0 = tuple.r1;
                auto _tup1 = tuple.r2;
                auto _tup2 = tuple.r3;
                unit_t __914;
                shared_ptr<R_r1_r2_r3<R_key_value<int, int>, int, int>> __915;
                __915 = map___SQL_SUM_AGGREGATE_1_mS1_s2_buf_d.filter([this, &_tup0,
                &_tup1] (const R_r1_r2_r3<R_key_value<int, int>, int, int>& b1) mutable {
                  {
                    auto& r1 = b1.r1;
                    auto& r2 = b1.r2;
                    auto& r3 = b1.r3;
                    return r1 == _tup0 && r2 == _tup1;
                  }
                }).peek(unit_t {});
                if (__915) {
                  R_r1_r2_r3<R_key_value<int, int>, int, int>& vals = *__915;
                  __914 = map___SQL_SUM_AGGREGATE_1_mS1_s2_buf_d.update(vals, tuple);
                } else {
                  __914 = map___SQL_SUM_AGGREGATE_1_mS1_s2_buf_d.insert(tuple);
                }
                _tup0 = tuple.r1;
                _tup1 = tuple.r2;
                _tup2 = tuple.r3;
                return __914;
              }
            }
          });
          if (nd_check_stmt_cntr_index(R_r1_r2_r3<R_key_value<int, int>, int, int> {vid, 2,
          -(1)})) {
            __913 = nd_insert_S_do_complete_s2(R_r1_r2_r3<R_key_value<int, int>, int, int> {vid,
            S_B, S_C});
          } else {
            __913 = unit_t {};
          }
          tuples = b1.r1;
          vid = b1.r2;
          S_B = b1.r3;
          S_C = b1.r4;
          return __913;
        }
      }
      unit_t nd_insert_S_rcv_push_s2_m___SQL_SUM_AGGREGATE_2_mS3(const R_r1_r2_r3_r4<_Set<R_r1_r2_r3<R_key_value<int,
      int>, int, int>>, R_key_value<int, int>, int, int>& b1)  {
        {
          auto tuples = b1.r1;
          auto vid = b1.r2;
          auto S_B = b1.r3;
          auto S_C = b1.r4;
          unit_t __916;
          nd_log_write_insert_S(R_r1_r2_r3<R_key_value<int, int>, int, int> {vid, S_B, S_C});
          tuples.iterate([this] (const R_r1_r2_r3<R_key_value<int, int>, int, int>& tuple) mutable {
            {
              auto& map___SQL_SUM_AGGREGATE_2_mS3_s2_buf_d = *map___SQL_SUM_AGGREGATE_2_mS3_s2_buf;
              {
                auto _tup0 = tuple.r1;
                auto _tup1 = tuple.r2;
                auto _tup2 = tuple.r3;
                unit_t __917;
                shared_ptr<R_r1_r2_r3<R_key_value<int, int>, int, int>> __918;
                __918 = map___SQL_SUM_AGGREGATE_2_mS3_s2_buf_d.filter([this, &_tup0,
                &_tup1] (const R_r1_r2_r3<R_key_value<int, int>, int, int>& b1) mutable {
                  {
                    auto& r1 = b1.r1;
                    auto& r2 = b1.r2;
                    auto& r3 = b1.r3;
                    return r1 == _tup0 && r2 == _tup1;
                  }
                }).peek(unit_t {});
                if (__918) {
                  R_r1_r2_r3<R_key_value<int, int>, int, int>& vals = *__918;
                  __917 = map___SQL_SUM_AGGREGATE_2_mS3_s2_buf_d.update(vals, tuple);
                } else {
                  __917 = map___SQL_SUM_AGGREGATE_2_mS3_s2_buf_d.insert(tuple);
                }
                _tup0 = tuple.r1;
                _tup1 = tuple.r2;
                _tup2 = tuple.r3;
                return __917;
              }
            }
          });
          if (nd_check_stmt_cntr_index(R_r1_r2_r3<R_key_value<int, int>, int, int> {vid, 2,
          -(1)})) {
            __916 = nd_insert_S_do_complete_s2(R_r1_r2_r3<R_key_value<int, int>, int, int> {vid,
            S_B, S_C});
          } else {
            __916 = unit_t {};
          }
          tuples = b1.r1;
          vid = b1.r2;
          S_B = b1.r3;
          S_C = b1.r4;
          return __916;
        }
      }
      unit_t nd_insert_S_do_complete_s1_trig(const R_r1_r2_r3<R_key_value<int, int>, int,
      int>& b1)  {
        {
          auto& vid = b1.r1;
          auto& S_B = b1.r2;
          auto& S_C = b1.r3;
          return nd_insert_S_do_complete_s1(R_r1_r2_r3<R_key_value<int, int>, int, int> {vid, S_B,
          S_C});
        }
      }
      unit_t nd_insert_S_do_complete_s3_trig(const R_r1_r2_r3<R_key_value<int, int>, int,
      int>& b1)  {
        {
          auto& vid = b1.r1;
          auto& S_B = b1.r2;
          auto& S_C = b1.r3;
          return nd_insert_S_do_complete_s3(R_r1_r2_r3<R_key_value<int, int>, int, int> {vid, S_B,
          S_C});
        }
      }
      unit_t insert_S_rcv_corrective_s0_m___SQL_SUM_AGGREGATE_1_mS1(const R_r1_r2_r3_r4_r5_r6_r7<Address,
      int, R_key_value<int, int>, int, R_key_value<int, int>, _Seq<R_key_value<int, int>>,
      _Set<R_key_value<int, int>>>& b1)  {
        {
          auto orig_addr = b1.r1;
          auto orig_stmt_id = b1.r2;
          auto orig_vid = b1.r3;
          auto hop = b1.r4;
          auto vid = b1.r5;
          auto compute_vids = b1.r6;
          auto delta_tuples = b1.r7;
          unit_t __919;
          nd_add_delta_to_int_int(R_r1_r2_r3_r4<shared_ptr<_Set<R_r1_r2_r3<R_key_value<int, int>,
          int, int>>>, bool, R_key_value<int, int>, _Set<R_key_value<int,
          int>>> {map___SQL_SUM_AGGREGATE_1_mS1_s0_buf, false, vid, delta_tuples});
          {
            int __920;
            auto __925 = 0;
            for (const auto& __924: compute_vids) {
              __925 = [this, &delta_tuples, &hop, &orig_addr, &orig_stmt_id,
              &orig_vid] (const int& acc_count) mutable {
                return [this, &acc_count, &delta_tuples, &hop, &orig_addr, &orig_stmt_id,
                &orig_vid] (const R_key_value<int, int>& compute_vid) mutable {
                  {
                    int __921;
                    shared_ptr<R_key_value<R_key_value<R_key_value<int, int>, int>, R_key_value<int,
                    _Map<R_key_value<int, int>>>>> __922;
                    __922 = nd_stmt_cntrs.filter([this,
                    &compute_vid] (const R_key_value<R_key_value<R_key_value<int, int>, int>,
                    R_key_value<int, _Map<R_key_value<int, int>>>>& b1) mutable {
                      {
                        auto& key = b1.key;
                        auto& value = b1.value;
                        return key == (R_key_value<R_key_value<int, int>, int> {compute_vid, 0});
                      }
                    }).peek(unit_t {});
                    if (__922) {
                      R_key_value<R_key_value<R_key_value<int, int>, int>, R_key_value<int,
                      _Map<R_key_value<int, int>>>>& lkup = *__922;
                      __921 = lkup.value.key;
                    } else {
                      __921 = 0;
                    }
                    auto& cntr = __921;
                    if (cntr == 0) {
                      R_key_value<int, int> __923;
                      __923 = nd_log_get_bound_insert_S(compute_vid);
                      {
                        auto& S_B = __923.key;
                        auto& S_C = __923.value;
                        return std::move(acc_count + insert_S_do_corrective_s0_m___SQL_SUM_AGGREGATE_1_mS1(R_r1_r2_r3_r4_r5_r6_r7_r8<Address,
                        int, R_key_value<int, int>, int, R_key_value<int, int>, int, int,
                        _Set<R_key_value<int, int>>> {orig_addr, orig_stmt_id, orig_vid, hop,
                        compute_vid, S_B, S_C, delta_tuples}));
                      }
                    } else {
                      return std::move(acc_count);
                    }
                  }
                };
              }(std::move(__925))(__924);
            }
            __920 = std::move(__925);
            auto& sent_msgs = __920;
            auto __926 = std::make_shared<K3::ValDispatcher<R_r1_r2_r3_r4<R_key_value<int, int>,
            int, int, int>>>(R_r1_r2_r3_r4<R_key_value<int, int>, int, int, int> {orig_vid,
            orig_stmt_id, hop, sent_msgs});
            __engine.send(orig_addr, __nd_rcv_corr_done_tid, __926, me);
            __919 = unit_t {};
          }
          orig_addr = b1.r1;
          orig_stmt_id = b1.r2;
          orig_vid = b1.r3;
          hop = b1.r4;
          vid = b1.r5;
          compute_vids = b1.r6;
          delta_tuples = b1.r7;
          return __919;
        }
      }
      unit_t insert_S_rcv_corrective_s2_m___SQL_SUM_AGGREGATE_1_mS1(const R_r1_r2_r3_r4_r5_r6_r7<Address,
      int, R_key_value<int, int>, int, R_key_value<int, int>, _Seq<R_key_value<int, int>>,
      _Set<R_key_value<int, int>>>& b1)  {
        {
          auto orig_addr = b1.r1;
          auto orig_stmt_id = b1.r2;
          auto orig_vid = b1.r3;
          auto hop = b1.r4;
          auto vid = b1.r5;
          auto compute_vids = b1.r6;
          auto delta_tuples = b1.r7;
          unit_t __927;
          nd_add_delta_to_int_int(R_r1_r2_r3_r4<shared_ptr<_Set<R_r1_r2_r3<R_key_value<int, int>,
          int, int>>>, bool, R_key_value<int, int>, _Set<R_key_value<int,
          int>>> {map___SQL_SUM_AGGREGATE_1_mS1_s2_buf, false, vid, delta_tuples});
          {
            int __928;
            auto __933 = 0;
            for (const auto& __932: compute_vids) {
              __933 = [this, &delta_tuples, &hop, &orig_addr, &orig_stmt_id,
              &orig_vid] (const int& acc_count) mutable {
                return [this, &acc_count, &delta_tuples, &hop, &orig_addr, &orig_stmt_id,
                &orig_vid] (const R_key_value<int, int>& compute_vid) mutable {
                  {
                    int __929;
                    shared_ptr<R_key_value<R_key_value<R_key_value<int, int>, int>, R_key_value<int,
                    _Map<R_key_value<int, int>>>>> __930;
                    __930 = nd_stmt_cntrs.filter([this,
                    &compute_vid] (const R_key_value<R_key_value<R_key_value<int, int>, int>,
                    R_key_value<int, _Map<R_key_value<int, int>>>>& b1) mutable {
                      {
                        auto& key = b1.key;
                        auto& value = b1.value;
                        return key == (R_key_value<R_key_value<int, int>, int> {compute_vid, 2});
                      }
                    }).peek(unit_t {});
                    if (__930) {
                      R_key_value<R_key_value<R_key_value<int, int>, int>, R_key_value<int,
                      _Map<R_key_value<int, int>>>>& lkup = *__930;
                      __929 = lkup.value.key;
                    } else {
                      __929 = 0;
                    }
                    auto& cntr = __929;
                    if (cntr == 0) {
                      R_key_value<int, int> __931;
                      __931 = nd_log_get_bound_insert_S(compute_vid);
                      {
                        auto& S_B = __931.key;
                        auto& S_C = __931.value;
                        return std::move(acc_count + insert_S_do_corrective_s2_m___SQL_SUM_AGGREGATE_1_mS1(R_r1_r2_r3_r4_r5_r6_r7_r8<Address,
                        int, R_key_value<int, int>, int, R_key_value<int, int>, int, int,
                        _Set<R_key_value<int, int>>> {orig_addr, orig_stmt_id, orig_vid, hop,
                        compute_vid, S_B, S_C, delta_tuples}));
                      }
                    } else {
                      return std::move(acc_count);
                    }
                  }
                };
              }(std::move(__933))(__932);
            }
            __928 = std::move(__933);
            auto& sent_msgs = __928;
            auto __934 = std::make_shared<K3::ValDispatcher<R_r1_r2_r3_r4<R_key_value<int, int>,
            int, int, int>>>(R_r1_r2_r3_r4<R_key_value<int, int>, int, int, int> {orig_vid,
            orig_stmt_id, hop, sent_msgs});
            __engine.send(orig_addr, __nd_rcv_corr_done_tid, __934, me);
            __927 = unit_t {};
          }
          orig_addr = b1.r1;
          orig_stmt_id = b1.r2;
          orig_vid = b1.r3;
          hop = b1.r4;
          vid = b1.r5;
          compute_vids = b1.r6;
          delta_tuples = b1.r7;
          return __927;
        }
      }
      unit_t insert_S_rcv_corrective_s2_m___SQL_SUM_AGGREGATE_2_mS3(const R_r1_r2_r3_r4_r5_r6_r7<Address,
      int, R_key_value<int, int>, int, R_key_value<int, int>, _Seq<R_key_value<int, int>>,
      _Set<R_key_value<int, int>>>& b1)  {
        {
          auto orig_addr = b1.r1;
          auto orig_stmt_id = b1.r2;
          auto orig_vid = b1.r3;
          auto hop = b1.r4;
          auto vid = b1.r5;
          auto compute_vids = b1.r6;
          auto delta_tuples = b1.r7;
          unit_t __935;
          nd_add_delta_to_int_int(R_r1_r2_r3_r4<shared_ptr<_Set<R_r1_r2_r3<R_key_value<int, int>,
          int, int>>>, bool, R_key_value<int, int>, _Set<R_key_value<int,
          int>>> {map___SQL_SUM_AGGREGATE_2_mS3_s2_buf, false, vid, delta_tuples});
          {
            int __936;
            auto __941 = 0;
            for (const auto& __940: compute_vids) {
              __941 = [this, &delta_tuples, &hop, &orig_addr, &orig_stmt_id,
              &orig_vid] (const int& acc_count) mutable {
                return [this, &acc_count, &delta_tuples, &hop, &orig_addr, &orig_stmt_id,
                &orig_vid] (const R_key_value<int, int>& compute_vid) mutable {
                  {
                    int __937;
                    shared_ptr<R_key_value<R_key_value<R_key_value<int, int>, int>, R_key_value<int,
                    _Map<R_key_value<int, int>>>>> __938;
                    __938 = nd_stmt_cntrs.filter([this,
                    &compute_vid] (const R_key_value<R_key_value<R_key_value<int, int>, int>,
                    R_key_value<int, _Map<R_key_value<int, int>>>>& b1) mutable {
                      {
                        auto& key = b1.key;
                        auto& value = b1.value;
                        return key == (R_key_value<R_key_value<int, int>, int> {compute_vid, 2});
                      }
                    }).peek(unit_t {});
                    if (__938) {
                      R_key_value<R_key_value<R_key_value<int, int>, int>, R_key_value<int,
                      _Map<R_key_value<int, int>>>>& lkup = *__938;
                      __937 = lkup.value.key;
                    } else {
                      __937 = 0;
                    }
                    auto& cntr = __937;
                    if (cntr == 0) {
                      R_key_value<int, int> __939;
                      __939 = nd_log_get_bound_insert_S(compute_vid);
                      {
                        auto& S_B = __939.key;
                        auto& S_C = __939.value;
                        return std::move(acc_count + insert_S_do_corrective_s2_m___SQL_SUM_AGGREGATE_2_mS3(R_r1_r2_r3_r4_r5_r6_r7_r8<Address,
                        int, R_key_value<int, int>, int, R_key_value<int, int>, int, int,
                        _Set<R_key_value<int, int>>> {orig_addr, orig_stmt_id, orig_vid, hop,
                        compute_vid, S_B, S_C, delta_tuples}));
                      }
                    } else {
                      return std::move(acc_count);
                    }
                  }
                };
              }(std::move(__941))(__940);
            }
            __936 = std::move(__941);
            auto& sent_msgs = __936;
            auto __942 = std::make_shared<K3::ValDispatcher<R_r1_r2_r3_r4<R_key_value<int, int>,
            int, int, int>>>(R_r1_r2_r3_r4<R_key_value<int, int>, int, int, int> {orig_vid,
            orig_stmt_id, hop, sent_msgs});
            __engine.send(orig_addr, __nd_rcv_corr_done_tid, __942, me);
            __935 = unit_t {};
          }
          orig_addr = b1.r1;
          orig_stmt_id = b1.r2;
          orig_vid = b1.r3;
          hop = b1.r4;
          vid = b1.r5;
          compute_vids = b1.r6;
          delta_tuples = b1.r7;
          return __935;
        }
      }
      unit_t nd_delete_S_rcv_put(const R_r1_r2_r3_r4_r5<Address, _Collection<R_key_value<int, int>>,
      R_key_value<int, int>, int, int>& b1)  {
        {
          auto sender_ip = b1.r1;
          auto stmt_cnt_list = b1.r2;
          auto vid = b1.r3;
          auto S_B = b1.r4;
          auto S_C = b1.r5;
          unit_t __943;
          stmt_cnt_list.iterate([this, &S_B, &S_C, &vid] (const R_key_value<int, int>& b1) mutable {
            {
              auto& stmt_id = b1.key;
              auto& count = b1.value;
              if (nd_check_stmt_cntr_index(R_r1_r2_r3<R_key_value<int, int>, int, int> {vid,
              stmt_id, count})) {
                if (stmt_id == 7) {
                  return nd_delete_S_do_complete_s7(R_r1_r2_r3<R_key_value<int, int>, int,
                  int> {vid, S_B, S_C});
                } else {
                  if (stmt_id == 6) {
                    return nd_delete_S_do_complete_s6(R_r1_r2_r3<R_key_value<int, int>, int,
                    int> {vid, S_B, S_C});
                  } else {
                    if (stmt_id == 5) {
                      return nd_delete_S_do_complete_s5(R_r1_r2_r3<R_key_value<int, int>, int,
                      int> {vid, S_B, S_C});
                    } else {
                      if (stmt_id == 4) {
                        return nd_delete_S_do_complete_s4(R_r1_r2_r3<R_key_value<int, int>, int,
                        int> {vid, S_B, S_C});
                      } else {
                        return unit_t {};
                      }
                    }
                  }
                }
              } else {
                return unit_t {};
              }
            }
          });
          auto __944 = std::make_shared<K3::ValDispatcher<R_key_value<Address, R_key_value<int,
          int>>>>(R_key_value<Address, R_key_value<int, int>> {me, vid});
          __engine.send(sender_ip, __sw_ack_rcv_tid, __944, me);
          __943 = unit_t {};
          sender_ip = b1.r1;
          stmt_cnt_list = b1.r2;
          vid = b1.r3;
          S_B = b1.r4;
          S_C = b1.r5;
          return __943;
        }
      }
      unit_t nd_delete_S_rcv_fetch(const R_r1_r2_r3_r4<_Collection<R_key_value<int, int>>,
      R_key_value<int, int>, int, int>& b1)  {
        {
          auto stmt_map_ids = b1.r1;
          auto vid = b1.r2;
          auto S_B = b1.r3;
          auto S_C = b1.r4;
          unit_t __945;
          nd_log_write_delete_S(R_r1_r2_r3<R_key_value<int, int>, int, int> {vid, S_B, S_C});
          __945 = stmt_map_ids.iterate([this, &S_B, &S_C, &vid] (const R_key_value<int,
          int>& b1) mutable {
            {
              auto& stmt_id = b1.key;
              auto& map_id = b1.value;
              if (stmt_id == 4) {
                if (map_id == 2) {
                  auto __946 = std::make_shared<K3::ValDispatcher<R_r1_r2_r3<R_key_value<int, int>,
                  int, int>>>(R_r1_r2_r3<R_key_value<int, int>, int, int> {vid, S_B, S_C});
                  __engine.send(me, __nd_delete_S_send_push_s4_m___SQL_SUM_AGGREGATE_1_mS1_tid,
                  __946, me);
                  return unit_t {};
                } else {
                  return error<unit_t>(print("nd_rcv_fetch: invalid map id"));
                }
              } else {
                if (stmt_id == 6) {
                  if (map_id == 2) {
                    auto __947 = std::make_shared<K3::ValDispatcher<R_r1_r2_r3<R_key_value<int,
                    int>, int, int>>>(R_r1_r2_r3<R_key_value<int, int>, int, int> {vid, S_B, S_C});
                    __engine.send(me, __nd_delete_S_send_push_s6_m___SQL_SUM_AGGREGATE_1_mS1_tid,
                    __947, me);
                    return unit_t {};
                  } else {
                    if (map_id == 5) {
                      auto __948 = std::make_shared<K3::ValDispatcher<R_r1_r2_r3<R_key_value<int,
                      int>, int, int>>>(R_r1_r2_r3<R_key_value<int, int>, int, int> {vid, S_B,
                      S_C});
                      __engine.send(me, __nd_delete_S_send_push_s6_m___SQL_SUM_AGGREGATE_2_mS3_tid,
                      __948, me);
                      return unit_t {};
                    } else {
                      return error<unit_t>(print("nd_rcv_fetch: invalid map id"));
                    }
                  }
                } else {
                  return error<unit_t>(print("nd_rcv_fetch: invalid stmt id"));
                }
              }
            }
          });
          stmt_map_ids = b1.r1;
          vid = b1.r2;
          S_B = b1.r3;
          S_C = b1.r4;
          return __945;
        }
      }
      unit_t nd_delete_S_send_push_s4_m___SQL_SUM_AGGREGATE_1_mS1(const R_r1_r2_r3<R_key_value<int,
      int>, int, int>& b1)  {
        {
          auto vid = b1.r1;
          auto S_B = b1.r2;
          auto S_C = b1.r3;
          unit_t __949;
          {
            auto& __SQL_SUM_AGGREGATE_1_mS1_deref = *__SQL_SUM_AGGREGATE_1_mS1;
            nd_log_master_write(R_key_value<R_key_value<int, int>, int> {vid, 4});
            __949 = shuffle___SQL_SUM_AGGREGATE_1_mS1_to___SQL_SUM_AGGREGATE_1(R_r1_r2_r3<unit_t,
            _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>, bool> {unit_t {},
            frontier_int_int(R_key_value<R_key_value<int, int>, _Set<R_r1_r2_r3<R_key_value<int,
            int>, int, int>>> {vid, __SQL_SUM_AGGREGATE_1_mS1_deref.filter([this,
            &S_B] (const R_r1_r2_r3<R_key_value<int, int>, int, int>& b1) mutable {
              {
                auto& r1 = b1.r1;
                auto& r2 = b1.r2;
                auto& r3 = b1.r3;
                return r2 == S_B;
              }
            })}), true}).iterate([this, &S_B, &S_C, &vid] (const R_key_value<Address,
            _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>>& b1) mutable {
              {
                auto& ip = b1.key;
                auto& tuples = b1.value;
                auto __950 = std::make_shared<K3::ValDispatcher<R_r1_r2_r3_r4<_Set<R_r1_r2_r3<R_key_value<int,
                int>, int, int>>, R_key_value<int, int>, int,
                int>>>(R_r1_r2_r3_r4<_Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>,
                R_key_value<int, int>, int, int> {tuples, vid, S_B, S_C});
                __engine.send(ip, __nd_delete_S_rcv_push_s4_m___SQL_SUM_AGGREGATE_1_mS1_tid, __950,
                me);
                return unit_t {};
              }
            });
          }
          vid = b1.r1;
          S_B = b1.r2;
          S_C = b1.r3;
          return __949;
        }
      }
      unit_t nd_delete_S_send_push_s6_m___SQL_SUM_AGGREGATE_1_mS1(const R_r1_r2_r3<R_key_value<int,
      int>, int, int>& b1)  {
        {
          auto vid = b1.r1;
          auto S_B = b1.r2;
          auto S_C = b1.r3;
          unit_t __951;
          {
            auto& __SQL_SUM_AGGREGATE_1_mS1_deref = *__SQL_SUM_AGGREGATE_1_mS1;
            nd_log_master_write(R_key_value<R_key_value<int, int>, int> {vid, 6});
            __951 = shuffle___SQL_SUM_AGGREGATE_1_mS1_to___SQL_SUM_AGGREGATE_2(R_r1_r2_r3<unit_t,
            _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>, bool> {unit_t {},
            frontier_int_int(R_key_value<R_key_value<int, int>, _Set<R_r1_r2_r3<R_key_value<int,
            int>, int, int>>> {vid, __SQL_SUM_AGGREGATE_1_mS1_deref.filter([this,
            &S_B] (const R_r1_r2_r3<R_key_value<int, int>, int, int>& b1) mutable {
              {
                auto& r1 = b1.r1;
                auto& r2 = b1.r2;
                auto& r3 = b1.r3;
                return r2 == S_B;
              }
            })}), true}).iterate([this, &S_B, &S_C, &vid] (const R_key_value<Address,
            _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>>& b1) mutable {
              {
                auto& ip = b1.key;
                auto& tuples = b1.value;
                auto __952 = std::make_shared<K3::ValDispatcher<R_r1_r2_r3_r4<_Set<R_r1_r2_r3<R_key_value<int,
                int>, int, int>>, R_key_value<int, int>, int,
                int>>>(R_r1_r2_r3_r4<_Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>,
                R_key_value<int, int>, int, int> {tuples, vid, S_B, S_C});
                __engine.send(ip, __nd_delete_S_rcv_push_s6_m___SQL_SUM_AGGREGATE_1_mS1_tid, __952,
                me);
                return unit_t {};
              }
            });
          }
          vid = b1.r1;
          S_B = b1.r2;
          S_C = b1.r3;
          return __951;
        }
      }
      unit_t nd_delete_S_send_push_s6_m___SQL_SUM_AGGREGATE_2_mS3(const R_r1_r2_r3<R_key_value<int,
      int>, int, int>& b1)  {
        {
          auto vid = b1.r1;
          auto S_B = b1.r2;
          auto S_C = b1.r3;
          unit_t __953;
          {
            auto& __SQL_SUM_AGGREGATE_2_mS3_deref = *__SQL_SUM_AGGREGATE_2_mS3;
            nd_log_master_write(R_key_value<R_key_value<int, int>, int> {vid, 6});
            __953 = shuffle___SQL_SUM_AGGREGATE_2_mS3_to___SQL_SUM_AGGREGATE_2(R_r1_r2_r3<unit_t,
            _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>, bool> {unit_t {},
            frontier_int_int(R_key_value<R_key_value<int, int>, _Set<R_r1_r2_r3<R_key_value<int,
            int>, int, int>>> {vid, __SQL_SUM_AGGREGATE_2_mS3_deref.filter([this,
            &S_B] (const R_r1_r2_r3<R_key_value<int, int>, int, int>& b1) mutable {
              {
                auto& r1 = b1.r1;
                auto& r2 = b1.r2;
                auto& r3 = b1.r3;
                return r2 == S_B;
              }
            })}), true}).iterate([this, &S_B, &S_C, &vid] (const R_key_value<Address,
            _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>>& b1) mutable {
              {
                auto& ip = b1.key;
                auto& tuples = b1.value;
                auto __954 = std::make_shared<K3::ValDispatcher<R_r1_r2_r3_r4<_Set<R_r1_r2_r3<R_key_value<int,
                int>, int, int>>, R_key_value<int, int>, int,
                int>>>(R_r1_r2_r3_r4<_Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>,
                R_key_value<int, int>, int, int> {tuples, vid, S_B, S_C});
                __engine.send(ip, __nd_delete_S_rcv_push_s6_m___SQL_SUM_AGGREGATE_2_mS3_tid, __954,
                me);
                return unit_t {};
              }
            });
          }
          vid = b1.r1;
          S_B = b1.r2;
          S_C = b1.r3;
          return __953;
        }
      }
      unit_t nd_delete_S_rcv_push_s4_m___SQL_SUM_AGGREGATE_1_mS1(const R_r1_r2_r3_r4<_Set<R_r1_r2_r3<R_key_value<int,
      int>, int, int>>, R_key_value<int, int>, int, int>& b1)  {
        {
          auto tuples = b1.r1;
          auto vid = b1.r2;
          auto S_B = b1.r3;
          auto S_C = b1.r4;
          unit_t __955;
          nd_log_write_delete_S(R_r1_r2_r3<R_key_value<int, int>, int, int> {vid, S_B, S_C});
          tuples.iterate([this] (const R_r1_r2_r3<R_key_value<int, int>, int, int>& tuple) mutable {
            {
              auto& map___SQL_SUM_AGGREGATE_1_mS1_s4_buf_d = *map___SQL_SUM_AGGREGATE_1_mS1_s4_buf;
              {
                auto _tup0 = tuple.r1;
                auto _tup1 = tuple.r2;
                auto _tup2 = tuple.r3;
                unit_t __956;
                shared_ptr<R_r1_r2_r3<R_key_value<int, int>, int, int>> __957;
                __957 = map___SQL_SUM_AGGREGATE_1_mS1_s4_buf_d.filter([this, &_tup0,
                &_tup1] (const R_r1_r2_r3<R_key_value<int, int>, int, int>& b1) mutable {
                  {
                    auto& r1 = b1.r1;
                    auto& r2 = b1.r2;
                    auto& r3 = b1.r3;
                    return r1 == _tup0 && r2 == _tup1;
                  }
                }).peek(unit_t {});
                if (__957) {
                  R_r1_r2_r3<R_key_value<int, int>, int, int>& vals = *__957;
                  __956 = map___SQL_SUM_AGGREGATE_1_mS1_s4_buf_d.update(vals, tuple);
                } else {
                  __956 = map___SQL_SUM_AGGREGATE_1_mS1_s4_buf_d.insert(tuple);
                }
                _tup0 = tuple.r1;
                _tup1 = tuple.r2;
                _tup2 = tuple.r3;
                return __956;
              }
            }
          });
          if (nd_check_stmt_cntr_index(R_r1_r2_r3<R_key_value<int, int>, int, int> {vid, 4,
          -(1)})) {
            __955 = nd_delete_S_do_complete_s4(R_r1_r2_r3<R_key_value<int, int>, int, int> {vid,
            S_B, S_C});
          } else {
            __955 = unit_t {};
          }
          tuples = b1.r1;
          vid = b1.r2;
          S_B = b1.r3;
          S_C = b1.r4;
          return __955;
        }
      }
      unit_t nd_delete_S_rcv_push_s6_m___SQL_SUM_AGGREGATE_1_mS1(const R_r1_r2_r3_r4<_Set<R_r1_r2_r3<R_key_value<int,
      int>, int, int>>, R_key_value<int, int>, int, int>& b1)  {
        {
          auto tuples = b1.r1;
          auto vid = b1.r2;
          auto S_B = b1.r3;
          auto S_C = b1.r4;
          unit_t __958;
          nd_log_write_delete_S(R_r1_r2_r3<R_key_value<int, int>, int, int> {vid, S_B, S_C});
          tuples.iterate([this] (const R_r1_r2_r3<R_key_value<int, int>, int, int>& tuple) mutable {
            {
              auto& map___SQL_SUM_AGGREGATE_1_mS1_s6_buf_d = *map___SQL_SUM_AGGREGATE_1_mS1_s6_buf;
              {
                auto _tup0 = tuple.r1;
                auto _tup1 = tuple.r2;
                auto _tup2 = tuple.r3;
                unit_t __959;
                shared_ptr<R_r1_r2_r3<R_key_value<int, int>, int, int>> __960;
                __960 = map___SQL_SUM_AGGREGATE_1_mS1_s6_buf_d.filter([this, &_tup0,
                &_tup1] (const R_r1_r2_r3<R_key_value<int, int>, int, int>& b1) mutable {
                  {
                    auto& r1 = b1.r1;
                    auto& r2 = b1.r2;
                    auto& r3 = b1.r3;
                    return r1 == _tup0 && r2 == _tup1;
                  }
                }).peek(unit_t {});
                if (__960) {
                  R_r1_r2_r3<R_key_value<int, int>, int, int>& vals = *__960;
                  __959 = map___SQL_SUM_AGGREGATE_1_mS1_s6_buf_d.update(vals, tuple);
                } else {
                  __959 = map___SQL_SUM_AGGREGATE_1_mS1_s6_buf_d.insert(tuple);
                }
                _tup0 = tuple.r1;
                _tup1 = tuple.r2;
                _tup2 = tuple.r3;
                return __959;
              }
            }
          });
          if (nd_check_stmt_cntr_index(R_r1_r2_r3<R_key_value<int, int>, int, int> {vid, 6,
          -(1)})) {
            __958 = nd_delete_S_do_complete_s6(R_r1_r2_r3<R_key_value<int, int>, int, int> {vid,
            S_B, S_C});
          } else {
            __958 = unit_t {};
          }
          tuples = b1.r1;
          vid = b1.r2;
          S_B = b1.r3;
          S_C = b1.r4;
          return __958;
        }
      }
      unit_t nd_delete_S_rcv_push_s6_m___SQL_SUM_AGGREGATE_2_mS3(const R_r1_r2_r3_r4<_Set<R_r1_r2_r3<R_key_value<int,
      int>, int, int>>, R_key_value<int, int>, int, int>& b1)  {
        {
          auto tuples = b1.r1;
          auto vid = b1.r2;
          auto S_B = b1.r3;
          auto S_C = b1.r4;
          unit_t __961;
          nd_log_write_delete_S(R_r1_r2_r3<R_key_value<int, int>, int, int> {vid, S_B, S_C});
          tuples.iterate([this] (const R_r1_r2_r3<R_key_value<int, int>, int, int>& tuple) mutable {
            {
              auto& map___SQL_SUM_AGGREGATE_2_mS3_s6_buf_d = *map___SQL_SUM_AGGREGATE_2_mS3_s6_buf;
              {
                auto _tup0 = tuple.r1;
                auto _tup1 = tuple.r2;
                auto _tup2 = tuple.r3;
                unit_t __962;
                shared_ptr<R_r1_r2_r3<R_key_value<int, int>, int, int>> __963;
                __963 = map___SQL_SUM_AGGREGATE_2_mS3_s6_buf_d.filter([this, &_tup0,
                &_tup1] (const R_r1_r2_r3<R_key_value<int, int>, int, int>& b1) mutable {
                  {
                    auto& r1 = b1.r1;
                    auto& r2 = b1.r2;
                    auto& r3 = b1.r3;
                    return r1 == _tup0 && r2 == _tup1;
                  }
                }).peek(unit_t {});
                if (__963) {
                  R_r1_r2_r3<R_key_value<int, int>, int, int>& vals = *__963;
                  __962 = map___SQL_SUM_AGGREGATE_2_mS3_s6_buf_d.update(vals, tuple);
                } else {
                  __962 = map___SQL_SUM_AGGREGATE_2_mS3_s6_buf_d.insert(tuple);
                }
                _tup0 = tuple.r1;
                _tup1 = tuple.r2;
                _tup2 = tuple.r3;
                return __962;
              }
            }
          });
          if (nd_check_stmt_cntr_index(R_r1_r2_r3<R_key_value<int, int>, int, int> {vid, 6,
          -(1)})) {
            __961 = nd_delete_S_do_complete_s6(R_r1_r2_r3<R_key_value<int, int>, int, int> {vid,
            S_B, S_C});
          } else {
            __961 = unit_t {};
          }
          tuples = b1.r1;
          vid = b1.r2;
          S_B = b1.r3;
          S_C = b1.r4;
          return __961;
        }
      }
      unit_t nd_delete_S_do_complete_s5_trig(const R_r1_r2_r3<R_key_value<int, int>, int,
      int>& b1)  {
        {
          auto& vid = b1.r1;
          auto& S_B = b1.r2;
          auto& S_C = b1.r3;
          return nd_delete_S_do_complete_s5(R_r1_r2_r3<R_key_value<int, int>, int, int> {vid, S_B,
          S_C});
        }
      }
      unit_t nd_delete_S_do_complete_s7_trig(const R_r1_r2_r3<R_key_value<int, int>, int,
      int>& b1)  {
        {
          auto& vid = b1.r1;
          auto& S_B = b1.r2;
          auto& S_C = b1.r3;
          return nd_delete_S_do_complete_s7(R_r1_r2_r3<R_key_value<int, int>, int, int> {vid, S_B,
          S_C});
        }
      }
      unit_t delete_S_rcv_corrective_s4_m___SQL_SUM_AGGREGATE_1_mS1(const R_r1_r2_r3_r4_r5_r6_r7<Address,
      int, R_key_value<int, int>, int, R_key_value<int, int>, _Seq<R_key_value<int, int>>,
      _Set<R_key_value<int, int>>>& b1)  {
        {
          auto orig_addr = b1.r1;
          auto orig_stmt_id = b1.r2;
          auto orig_vid = b1.r3;
          auto hop = b1.r4;
          auto vid = b1.r5;
          auto compute_vids = b1.r6;
          auto delta_tuples = b1.r7;
          unit_t __964;
          nd_add_delta_to_int_int(R_r1_r2_r3_r4<shared_ptr<_Set<R_r1_r2_r3<R_key_value<int, int>,
          int, int>>>, bool, R_key_value<int, int>, _Set<R_key_value<int,
          int>>> {map___SQL_SUM_AGGREGATE_1_mS1_s4_buf, false, vid, delta_tuples});
          {
            int __965;
            auto __970 = 0;
            for (const auto& __969: compute_vids) {
              __970 = [this, &delta_tuples, &hop, &orig_addr, &orig_stmt_id,
              &orig_vid] (const int& acc_count) mutable {
                return [this, &acc_count, &delta_tuples, &hop, &orig_addr, &orig_stmt_id,
                &orig_vid] (const R_key_value<int, int>& compute_vid) mutable {
                  {
                    int __966;
                    shared_ptr<R_key_value<R_key_value<R_key_value<int, int>, int>, R_key_value<int,
                    _Map<R_key_value<int, int>>>>> __967;
                    __967 = nd_stmt_cntrs.filter([this,
                    &compute_vid] (const R_key_value<R_key_value<R_key_value<int, int>, int>,
                    R_key_value<int, _Map<R_key_value<int, int>>>>& b1) mutable {
                      {
                        auto& key = b1.key;
                        auto& value = b1.value;
                        return key == (R_key_value<R_key_value<int, int>, int> {compute_vid, 4});
                      }
                    }).peek(unit_t {});
                    if (__967) {
                      R_key_value<R_key_value<R_key_value<int, int>, int>, R_key_value<int,
                      _Map<R_key_value<int, int>>>>& lkup = *__967;
                      __966 = lkup.value.key;
                    } else {
                      __966 = 0;
                    }
                    auto& cntr = __966;
                    if (cntr == 0) {
                      R_key_value<int, int> __968;
                      __968 = nd_log_get_bound_delete_S(compute_vid);
                      {
                        auto& S_B = __968.key;
                        auto& S_C = __968.value;
                        return std::move(acc_count + delete_S_do_corrective_s4_m___SQL_SUM_AGGREGATE_1_mS1(R_r1_r2_r3_r4_r5_r6_r7_r8<Address,
                        int, R_key_value<int, int>, int, R_key_value<int, int>, int, int,
                        _Set<R_key_value<int, int>>> {orig_addr, orig_stmt_id, orig_vid, hop,
                        compute_vid, S_B, S_C, delta_tuples}));
                      }
                    } else {
                      return std::move(acc_count);
                    }
                  }
                };
              }(std::move(__970))(__969);
            }
            __965 = std::move(__970);
            auto& sent_msgs = __965;
            auto __971 = std::make_shared<K3::ValDispatcher<R_r1_r2_r3_r4<R_key_value<int, int>,
            int, int, int>>>(R_r1_r2_r3_r4<R_key_value<int, int>, int, int, int> {orig_vid,
            orig_stmt_id, hop, sent_msgs});
            __engine.send(orig_addr, __nd_rcv_corr_done_tid, __971, me);
            __964 = unit_t {};
          }
          orig_addr = b1.r1;
          orig_stmt_id = b1.r2;
          orig_vid = b1.r3;
          hop = b1.r4;
          vid = b1.r5;
          compute_vids = b1.r6;
          delta_tuples = b1.r7;
          return __964;
        }
      }
      unit_t delete_S_rcv_corrective_s6_m___SQL_SUM_AGGREGATE_1_mS1(const R_r1_r2_r3_r4_r5_r6_r7<Address,
      int, R_key_value<int, int>, int, R_key_value<int, int>, _Seq<R_key_value<int, int>>,
      _Set<R_key_value<int, int>>>& b1)  {
        {
          auto orig_addr = b1.r1;
          auto orig_stmt_id = b1.r2;
          auto orig_vid = b1.r3;
          auto hop = b1.r4;
          auto vid = b1.r5;
          auto compute_vids = b1.r6;
          auto delta_tuples = b1.r7;
          unit_t __972;
          nd_add_delta_to_int_int(R_r1_r2_r3_r4<shared_ptr<_Set<R_r1_r2_r3<R_key_value<int, int>,
          int, int>>>, bool, R_key_value<int, int>, _Set<R_key_value<int,
          int>>> {map___SQL_SUM_AGGREGATE_1_mS1_s6_buf, false, vid, delta_tuples});
          {
            int __973;
            auto __978 = 0;
            for (const auto& __977: compute_vids) {
              __978 = [this, &delta_tuples, &hop, &orig_addr, &orig_stmt_id,
              &orig_vid] (const int& acc_count) mutable {
                return [this, &acc_count, &delta_tuples, &hop, &orig_addr, &orig_stmt_id,
                &orig_vid] (const R_key_value<int, int>& compute_vid) mutable {
                  {
                    int __974;
                    shared_ptr<R_key_value<R_key_value<R_key_value<int, int>, int>, R_key_value<int,
                    _Map<R_key_value<int, int>>>>> __975;
                    __975 = nd_stmt_cntrs.filter([this,
                    &compute_vid] (const R_key_value<R_key_value<R_key_value<int, int>, int>,
                    R_key_value<int, _Map<R_key_value<int, int>>>>& b1) mutable {
                      {
                        auto& key = b1.key;
                        auto& value = b1.value;
                        return key == (R_key_value<R_key_value<int, int>, int> {compute_vid, 6});
                      }
                    }).peek(unit_t {});
                    if (__975) {
                      R_key_value<R_key_value<R_key_value<int, int>, int>, R_key_value<int,
                      _Map<R_key_value<int, int>>>>& lkup = *__975;
                      __974 = lkup.value.key;
                    } else {
                      __974 = 0;
                    }
                    auto& cntr = __974;
                    if (cntr == 0) {
                      R_key_value<int, int> __976;
                      __976 = nd_log_get_bound_delete_S(compute_vid);
                      {
                        auto& S_B = __976.key;
                        auto& S_C = __976.value;
                        return std::move(acc_count + delete_S_do_corrective_s6_m___SQL_SUM_AGGREGATE_1_mS1(R_r1_r2_r3_r4_r5_r6_r7_r8<Address,
                        int, R_key_value<int, int>, int, R_key_value<int, int>, int, int,
                        _Set<R_key_value<int, int>>> {orig_addr, orig_stmt_id, orig_vid, hop,
                        compute_vid, S_B, S_C, delta_tuples}));
                      }
                    } else {
                      return std::move(acc_count);
                    }
                  }
                };
              }(std::move(__978))(__977);
            }
            __973 = std::move(__978);
            auto& sent_msgs = __973;
            auto __979 = std::make_shared<K3::ValDispatcher<R_r1_r2_r3_r4<R_key_value<int, int>,
            int, int, int>>>(R_r1_r2_r3_r4<R_key_value<int, int>, int, int, int> {orig_vid,
            orig_stmt_id, hop, sent_msgs});
            __engine.send(orig_addr, __nd_rcv_corr_done_tid, __979, me);
            __972 = unit_t {};
          }
          orig_addr = b1.r1;
          orig_stmt_id = b1.r2;
          orig_vid = b1.r3;
          hop = b1.r4;
          vid = b1.r5;
          compute_vids = b1.r6;
          delta_tuples = b1.r7;
          return __972;
        }
      }
      unit_t delete_S_rcv_corrective_s6_m___SQL_SUM_AGGREGATE_2_mS3(const R_r1_r2_r3_r4_r5_r6_r7<Address,
      int, R_key_value<int, int>, int, R_key_value<int, int>, _Seq<R_key_value<int, int>>,
      _Set<R_key_value<int, int>>>& b1)  {
        {
          auto orig_addr = b1.r1;
          auto orig_stmt_id = b1.r2;
          auto orig_vid = b1.r3;
          auto hop = b1.r4;
          auto vid = b1.r5;
          auto compute_vids = b1.r6;
          auto delta_tuples = b1.r7;
          unit_t __980;
          nd_add_delta_to_int_int(R_r1_r2_r3_r4<shared_ptr<_Set<R_r1_r2_r3<R_key_value<int, int>,
          int, int>>>, bool, R_key_value<int, int>, _Set<R_key_value<int,
          int>>> {map___SQL_SUM_AGGREGATE_2_mS3_s6_buf, false, vid, delta_tuples});
          {
            int __981;
            auto __986 = 0;
            for (const auto& __985: compute_vids) {
              __986 = [this, &delta_tuples, &hop, &orig_addr, &orig_stmt_id,
              &orig_vid] (const int& acc_count) mutable {
                return [this, &acc_count, &delta_tuples, &hop, &orig_addr, &orig_stmt_id,
                &orig_vid] (const R_key_value<int, int>& compute_vid) mutable {
                  {
                    int __982;
                    shared_ptr<R_key_value<R_key_value<R_key_value<int, int>, int>, R_key_value<int,
                    _Map<R_key_value<int, int>>>>> __983;
                    __983 = nd_stmt_cntrs.filter([this,
                    &compute_vid] (const R_key_value<R_key_value<R_key_value<int, int>, int>,
                    R_key_value<int, _Map<R_key_value<int, int>>>>& b1) mutable {
                      {
                        auto& key = b1.key;
                        auto& value = b1.value;
                        return key == (R_key_value<R_key_value<int, int>, int> {compute_vid, 6});
                      }
                    }).peek(unit_t {});
                    if (__983) {
                      R_key_value<R_key_value<R_key_value<int, int>, int>, R_key_value<int,
                      _Map<R_key_value<int, int>>>>& lkup = *__983;
                      __982 = lkup.value.key;
                    } else {
                      __982 = 0;
                    }
                    auto& cntr = __982;
                    if (cntr == 0) {
                      R_key_value<int, int> __984;
                      __984 = nd_log_get_bound_delete_S(compute_vid);
                      {
                        auto& S_B = __984.key;
                        auto& S_C = __984.value;
                        return std::move(acc_count + delete_S_do_corrective_s6_m___SQL_SUM_AGGREGATE_2_mS3(R_r1_r2_r3_r4_r5_r6_r7_r8<Address,
                        int, R_key_value<int, int>, int, R_key_value<int, int>, int, int,
                        _Set<R_key_value<int, int>>> {orig_addr, orig_stmt_id, orig_vid, hop,
                        compute_vid, S_B, S_C, delta_tuples}));
                      }
                    } else {
                      return std::move(acc_count);
                    }
                  }
                };
              }(std::move(__986))(__985);
            }
            __981 = std::move(__986);
            auto& sent_msgs = __981;
            auto __987 = std::make_shared<K3::ValDispatcher<R_r1_r2_r3_r4<R_key_value<int, int>,
            int, int, int>>>(R_r1_r2_r3_r4<R_key_value<int, int>, int, int, int> {orig_vid,
            orig_stmt_id, hop, sent_msgs});
            __engine.send(orig_addr, __nd_rcv_corr_done_tid, __987, me);
            __980 = unit_t {};
          }
          orig_addr = b1.r1;
          orig_stmt_id = b1.r2;
          orig_vid = b1.r3;
          hop = b1.r4;
          vid = b1.r5;
          compute_vids = b1.r6;
          delta_tuples = b1.r7;
          return __980;
        }
      }
      unit_t nd_insert_R_rcv_put(const R_r1_r2_r3_r4_r5<Address, _Collection<R_key_value<int, int>>,
      R_key_value<int, int>, int, int>& b1)  {
        {
          auto sender_ip = b1.r1;
          auto stmt_cnt_list = b1.r2;
          auto vid = b1.r3;
          auto R_A = b1.r4;
          auto R_B = b1.r5;
          unit_t __988;
          stmt_cnt_list.iterate([this, &R_A, &R_B, &vid] (const R_key_value<int, int>& b1) mutable {
            {
              auto& stmt_id = b1.key;
              auto& count = b1.value;
              if (nd_check_stmt_cntr_index(R_r1_r2_r3<R_key_value<int, int>, int, int> {vid,
              stmt_id, count})) {
                if (stmt_id == 11) {
                  return nd_insert_R_do_complete_s11(R_r1_r2_r3<R_key_value<int, int>, int,
                  int> {vid, R_A, R_B});
                } else {
                  if (stmt_id == 10) {
                    return nd_insert_R_do_complete_s10(R_r1_r2_r3<R_key_value<int, int>, int,
                    int> {vid, R_A, R_B});
                  } else {
                    if (stmt_id == 9) {
                      return nd_insert_R_do_complete_s9(R_r1_r2_r3<R_key_value<int, int>, int,
                      int> {vid, R_A, R_B});
                    } else {
                      if (stmt_id == 8) {
                        return nd_insert_R_do_complete_s8(R_r1_r2_r3<R_key_value<int, int>, int,
                        int> {vid, R_A, R_B});
                      } else {
                        return unit_t {};
                      }
                    }
                  }
                }
              } else {
                return unit_t {};
              }
            }
          });
          auto __989 = std::make_shared<K3::ValDispatcher<R_key_value<Address, R_key_value<int,
          int>>>>(R_key_value<Address, R_key_value<int, int>> {me, vid});
          __engine.send(sender_ip, __sw_ack_rcv_tid, __989, me);
          __988 = unit_t {};
          sender_ip = b1.r1;
          stmt_cnt_list = b1.r2;
          vid = b1.r3;
          R_A = b1.r4;
          R_B = b1.r5;
          return __988;
        }
      }
      unit_t nd_insert_R_rcv_fetch(const R_r1_r2_r3_r4<_Collection<R_key_value<int, int>>,
      R_key_value<int, int>, int, int>& b1)  {
        {
          auto stmt_map_ids = b1.r1;
          auto vid = b1.r2;
          auto R_A = b1.r3;
          auto R_B = b1.r4;
          unit_t __990;
          nd_log_write_insert_R(R_r1_r2_r3<R_key_value<int, int>, int, int> {vid, R_A, R_B});
          __990 = stmt_map_ids.iterate([this, &R_A, &R_B, &vid] (const R_key_value<int,
          int>& b1) mutable {
            {
              auto& stmt_id = b1.key;
              auto& map_id = b1.value;
              if (stmt_id == 8) {
                if (map_id == 3) {
                  auto __991 = std::make_shared<K3::ValDispatcher<R_r1_r2_r3<R_key_value<int, int>,
                  int, int>>>(R_r1_r2_r3<R_key_value<int, int>, int, int> {vid, R_A, R_B});
                  __engine.send(me, __nd_insert_R_send_push_s8_m___SQL_SUM_AGGREGATE_1_mR1_tid,
                  __991, me);
                  return unit_t {};
                } else {
                  return error<unit_t>(print("nd_rcv_fetch: invalid map id"));
                }
              } else {
                if (stmt_id == 10) {
                  if (map_id == 6) {
                    auto __992 = std::make_shared<K3::ValDispatcher<R_r1_r2_r3<R_key_value<int,
                    int>, int, int>>>(R_r1_r2_r3<R_key_value<int, int>, int, int> {vid, R_A, R_B});
                    __engine.send(me, __nd_insert_R_send_push_s10_m___SQL_SUM_AGGREGATE_2_mR1_tid,
                    __992, me);
                    return unit_t {};
                  } else {
                    if (map_id == 3) {
                      auto __993 = std::make_shared<K3::ValDispatcher<R_r1_r2_r3<R_key_value<int,
                      int>, int, int>>>(R_r1_r2_r3<R_key_value<int, int>, int, int> {vid, R_A,
                      R_B});
                      __engine.send(me, __nd_insert_R_send_push_s10_m___SQL_SUM_AGGREGATE_1_mR1_tid,
                      __993, me);
                      return unit_t {};
                    } else {
                      return error<unit_t>(print("nd_rcv_fetch: invalid map id"));
                    }
                  }
                } else {
                  return error<unit_t>(print("nd_rcv_fetch: invalid stmt id"));
                }
              }
            }
          });
          stmt_map_ids = b1.r1;
          vid = b1.r2;
          R_A = b1.r3;
          R_B = b1.r4;
          return __990;
        }
      }
      unit_t nd_insert_R_send_push_s8_m___SQL_SUM_AGGREGATE_1_mR1(const R_r1_r2_r3<R_key_value<int,
      int>, int, int>& b1)  {
        {
          auto vid = b1.r1;
          auto R_A = b1.r2;
          auto R_B = b1.r3;
          unit_t __994;
          {
            auto& __SQL_SUM_AGGREGATE_1_mR1_deref = *__SQL_SUM_AGGREGATE_1_mR1;
            nd_log_master_write(R_key_value<R_key_value<int, int>, int> {vid, 8});
            __994 = shuffle___SQL_SUM_AGGREGATE_1_mR1_to___SQL_SUM_AGGREGATE_1(R_r1_r2_r3<unit_t,
            _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>, bool> {unit_t {},
            frontier_int_int(R_key_value<R_key_value<int, int>, _Set<R_r1_r2_r3<R_key_value<int,
            int>, int, int>>> {vid, __SQL_SUM_AGGREGATE_1_mR1_deref.filter([this,
            &R_B] (const R_r1_r2_r3<R_key_value<int, int>, int, int>& b1) mutable {
              {
                auto& r1 = b1.r1;
                auto& r2 = b1.r2;
                auto& r3 = b1.r3;
                return r2 == R_B;
              }
            })}), true}).iterate([this, &R_A, &R_B, &vid] (const R_key_value<Address,
            _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>>& b1) mutable {
              {
                auto& ip = b1.key;
                auto& tuples = b1.value;
                auto __995 = std::make_shared<K3::ValDispatcher<R_r1_r2_r3_r4<_Set<R_r1_r2_r3<R_key_value<int,
                int>, int, int>>, R_key_value<int, int>, int,
                int>>>(R_r1_r2_r3_r4<_Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>,
                R_key_value<int, int>, int, int> {tuples, vid, R_A, R_B});
                __engine.send(ip, __nd_insert_R_rcv_push_s8_m___SQL_SUM_AGGREGATE_1_mR1_tid, __995,
                me);
                return unit_t {};
              }
            });
          }
          vid = b1.r1;
          R_A = b1.r2;
          R_B = b1.r3;
          return __994;
        }
      }
      unit_t nd_insert_R_send_push_s10_m___SQL_SUM_AGGREGATE_2_mR1(const R_r1_r2_r3<R_key_value<int,
      int>, int, int>& b1)  {
        {
          auto vid = b1.r1;
          auto R_A = b1.r2;
          auto R_B = b1.r3;
          unit_t __996;
          {
            auto& __SQL_SUM_AGGREGATE_2_mR1_deref = *__SQL_SUM_AGGREGATE_2_mR1;
            nd_log_master_write(R_key_value<R_key_value<int, int>, int> {vid, 10});
            __996 = shuffle___SQL_SUM_AGGREGATE_2_mR1_to___SQL_SUM_AGGREGATE_2(R_r1_r2_r3<unit_t,
            _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>, bool> {unit_t {},
            frontier_int_int(R_key_value<R_key_value<int, int>, _Set<R_r1_r2_r3<R_key_value<int,
            int>, int, int>>> {vid, __SQL_SUM_AGGREGATE_2_mR1_deref.filter([this,
            &R_B] (const R_r1_r2_r3<R_key_value<int, int>, int, int>& b1) mutable {
              {
                auto& r1 = b1.r1;
                auto& r2 = b1.r2;
                auto& r3 = b1.r3;
                return r2 == R_B;
              }
            })}), true}).iterate([this, &R_A, &R_B, &vid] (const R_key_value<Address,
            _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>>& b1) mutable {
              {
                auto& ip = b1.key;
                auto& tuples = b1.value;
                auto __997 = std::make_shared<K3::ValDispatcher<R_r1_r2_r3_r4<_Set<R_r1_r2_r3<R_key_value<int,
                int>, int, int>>, R_key_value<int, int>, int,
                int>>>(R_r1_r2_r3_r4<_Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>,
                R_key_value<int, int>, int, int> {tuples, vid, R_A, R_B});
                __engine.send(ip, __nd_insert_R_rcv_push_s10_m___SQL_SUM_AGGREGATE_2_mR1_tid, __997,
                me);
                return unit_t {};
              }
            });
          }
          vid = b1.r1;
          R_A = b1.r2;
          R_B = b1.r3;
          return __996;
        }
      }
      unit_t nd_insert_R_send_push_s10_m___SQL_SUM_AGGREGATE_1_mR1(const R_r1_r2_r3<R_key_value<int,
      int>, int, int>& b1)  {
        {
          auto vid = b1.r1;
          auto R_A = b1.r2;
          auto R_B = b1.r3;
          unit_t __998;
          {
            auto& __SQL_SUM_AGGREGATE_1_mR1_deref = *__SQL_SUM_AGGREGATE_1_mR1;
            nd_log_master_write(R_key_value<R_key_value<int, int>, int> {vid, 10});
            __998 = shuffle___SQL_SUM_AGGREGATE_1_mR1_to___SQL_SUM_AGGREGATE_2(R_r1_r2_r3<unit_t,
            _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>, bool> {unit_t {},
            frontier_int_int(R_key_value<R_key_value<int, int>, _Set<R_r1_r2_r3<R_key_value<int,
            int>, int, int>>> {vid, __SQL_SUM_AGGREGATE_1_mR1_deref.filter([this,
            &R_B] (const R_r1_r2_r3<R_key_value<int, int>, int, int>& b1) mutable {
              {
                auto& r1 = b1.r1;
                auto& r2 = b1.r2;
                auto& r3 = b1.r3;
                return r2 == R_B;
              }
            })}), true}).iterate([this, &R_A, &R_B, &vid] (const R_key_value<Address,
            _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>>& b1) mutable {
              {
                auto& ip = b1.key;
                auto& tuples = b1.value;
                auto __999 = std::make_shared<K3::ValDispatcher<R_r1_r2_r3_r4<_Set<R_r1_r2_r3<R_key_value<int,
                int>, int, int>>, R_key_value<int, int>, int,
                int>>>(R_r1_r2_r3_r4<_Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>,
                R_key_value<int, int>, int, int> {tuples, vid, R_A, R_B});
                __engine.send(ip, __nd_insert_R_rcv_push_s10_m___SQL_SUM_AGGREGATE_1_mR1_tid, __999,
                me);
                return unit_t {};
              }
            });
          }
          vid = b1.r1;
          R_A = b1.r2;
          R_B = b1.r3;
          return __998;
        }
      }
      unit_t nd_insert_R_rcv_push_s8_m___SQL_SUM_AGGREGATE_1_mR1(const R_r1_r2_r3_r4<_Set<R_r1_r2_r3<R_key_value<int,
      int>, int, int>>, R_key_value<int, int>, int, int>& b1)  {
        {
          auto tuples = b1.r1;
          auto vid = b1.r2;
          auto R_A = b1.r3;
          auto R_B = b1.r4;
          unit_t __1000;
          nd_log_write_insert_R(R_r1_r2_r3<R_key_value<int, int>, int, int> {vid, R_A, R_B});
          tuples.iterate([this] (const R_r1_r2_r3<R_key_value<int, int>, int, int>& tuple) mutable {
            {
              auto& map___SQL_SUM_AGGREGATE_1_mR1_s8_buf_d = *map___SQL_SUM_AGGREGATE_1_mR1_s8_buf;
              {
                auto _tup0 = tuple.r1;
                auto _tup1 = tuple.r2;
                auto _tup2 = tuple.r3;
                unit_t __1001;
                shared_ptr<R_r1_r2_r3<R_key_value<int, int>, int, int>> __1002;
                __1002 = map___SQL_SUM_AGGREGATE_1_mR1_s8_buf_d.filter([this, &_tup0,
                &_tup1] (const R_r1_r2_r3<R_key_value<int, int>, int, int>& b1) mutable {
                  {
                    auto& r1 = b1.r1;
                    auto& r2 = b1.r2;
                    auto& r3 = b1.r3;
                    return r1 == _tup0 && r2 == _tup1;
                  }
                }).peek(unit_t {});
                if (__1002) {
                  R_r1_r2_r3<R_key_value<int, int>, int, int>& vals = *__1002;
                  __1001 = map___SQL_SUM_AGGREGATE_1_mR1_s8_buf_d.update(vals, tuple);
                } else {
                  __1001 = map___SQL_SUM_AGGREGATE_1_mR1_s8_buf_d.insert(tuple);
                }
                _tup0 = tuple.r1;
                _tup1 = tuple.r2;
                _tup2 = tuple.r3;
                return __1001;
              }
            }
          });
          if (nd_check_stmt_cntr_index(R_r1_r2_r3<R_key_value<int, int>, int, int> {vid, 8,
          -(1)})) {
            __1000 = nd_insert_R_do_complete_s8(R_r1_r2_r3<R_key_value<int, int>, int, int> {vid,
            R_A, R_B});
          } else {
            __1000 = unit_t {};
          }
          tuples = b1.r1;
          vid = b1.r2;
          R_A = b1.r3;
          R_B = b1.r4;
          return __1000;
        }
      }
      unit_t nd_insert_R_rcv_push_s10_m___SQL_SUM_AGGREGATE_2_mR1(const R_r1_r2_r3_r4<_Set<R_r1_r2_r3<R_key_value<int,
      int>, int, int>>, R_key_value<int, int>, int, int>& b1)  {
        {
          auto tuples = b1.r1;
          auto vid = b1.r2;
          auto R_A = b1.r3;
          auto R_B = b1.r4;
          unit_t __1003;
          nd_log_write_insert_R(R_r1_r2_r3<R_key_value<int, int>, int, int> {vid, R_A, R_B});
          tuples.iterate([this] (const R_r1_r2_r3<R_key_value<int, int>, int, int>& tuple) mutable {
            {
              auto& map___SQL_SUM_AGGREGATE_2_mR1_s10_buf_d = *map___SQL_SUM_AGGREGATE_2_mR1_s10_buf;
              {
                auto _tup0 = tuple.r1;
                auto _tup1 = tuple.r2;
                auto _tup2 = tuple.r3;
                unit_t __1004;
                shared_ptr<R_r1_r2_r3<R_key_value<int, int>, int, int>> __1005;
                __1005 = map___SQL_SUM_AGGREGATE_2_mR1_s10_buf_d.filter([this, &_tup0,
                &_tup1] (const R_r1_r2_r3<R_key_value<int, int>, int, int>& b1) mutable {
                  {
                    auto& r1 = b1.r1;
                    auto& r2 = b1.r2;
                    auto& r3 = b1.r3;
                    return r1 == _tup0 && r2 == _tup1;
                  }
                }).peek(unit_t {});
                if (__1005) {
                  R_r1_r2_r3<R_key_value<int, int>, int, int>& vals = *__1005;
                  __1004 = map___SQL_SUM_AGGREGATE_2_mR1_s10_buf_d.update(vals, tuple);
                } else {
                  __1004 = map___SQL_SUM_AGGREGATE_2_mR1_s10_buf_d.insert(tuple);
                }
                _tup0 = tuple.r1;
                _tup1 = tuple.r2;
                _tup2 = tuple.r3;
                return __1004;
              }
            }
          });
          if (nd_check_stmt_cntr_index(R_r1_r2_r3<R_key_value<int, int>, int, int> {vid, 10,
          -(1)})) {
            __1003 = nd_insert_R_do_complete_s10(R_r1_r2_r3<R_key_value<int, int>, int, int> {vid,
            R_A, R_B});
          } else {
            __1003 = unit_t {};
          }
          tuples = b1.r1;
          vid = b1.r2;
          R_A = b1.r3;
          R_B = b1.r4;
          return __1003;
        }
      }
      unit_t nd_insert_R_rcv_push_s10_m___SQL_SUM_AGGREGATE_1_mR1(const R_r1_r2_r3_r4<_Set<R_r1_r2_r3<R_key_value<int,
      int>, int, int>>, R_key_value<int, int>, int, int>& b1)  {
        {
          auto tuples = b1.r1;
          auto vid = b1.r2;
          auto R_A = b1.r3;
          auto R_B = b1.r4;
          unit_t __1006;
          nd_log_write_insert_R(R_r1_r2_r3<R_key_value<int, int>, int, int> {vid, R_A, R_B});
          tuples.iterate([this] (const R_r1_r2_r3<R_key_value<int, int>, int, int>& tuple) mutable {
            {
              auto& map___SQL_SUM_AGGREGATE_1_mR1_s10_buf_d = *map___SQL_SUM_AGGREGATE_1_mR1_s10_buf;
              {
                auto _tup0 = tuple.r1;
                auto _tup1 = tuple.r2;
                auto _tup2 = tuple.r3;
                unit_t __1007;
                shared_ptr<R_r1_r2_r3<R_key_value<int, int>, int, int>> __1008;
                __1008 = map___SQL_SUM_AGGREGATE_1_mR1_s10_buf_d.filter([this, &_tup0,
                &_tup1] (const R_r1_r2_r3<R_key_value<int, int>, int, int>& b1) mutable {
                  {
                    auto& r1 = b1.r1;
                    auto& r2 = b1.r2;
                    auto& r3 = b1.r3;
                    return r1 == _tup0 && r2 == _tup1;
                  }
                }).peek(unit_t {});
                if (__1008) {
                  R_r1_r2_r3<R_key_value<int, int>, int, int>& vals = *__1008;
                  __1007 = map___SQL_SUM_AGGREGATE_1_mR1_s10_buf_d.update(vals, tuple);
                } else {
                  __1007 = map___SQL_SUM_AGGREGATE_1_mR1_s10_buf_d.insert(tuple);
                }
                _tup0 = tuple.r1;
                _tup1 = tuple.r2;
                _tup2 = tuple.r3;
                return __1007;
              }
            }
          });
          if (nd_check_stmt_cntr_index(R_r1_r2_r3<R_key_value<int, int>, int, int> {vid, 10,
          -(1)})) {
            __1006 = nd_insert_R_do_complete_s10(R_r1_r2_r3<R_key_value<int, int>, int, int> {vid,
            R_A, R_B});
          } else {
            __1006 = unit_t {};
          }
          tuples = b1.r1;
          vid = b1.r2;
          R_A = b1.r3;
          R_B = b1.r4;
          return __1006;
        }
      }
      unit_t nd_insert_R_do_complete_s9_trig(const R_r1_r2_r3<R_key_value<int, int>, int,
      int>& b1)  {
        {
          auto& vid = b1.r1;
          auto& R_A = b1.r2;
          auto& R_B = b1.r3;
          return nd_insert_R_do_complete_s9(R_r1_r2_r3<R_key_value<int, int>, int, int> {vid, R_A,
          R_B});
        }
      }
      unit_t nd_insert_R_do_complete_s11_trig(const R_r1_r2_r3<R_key_value<int, int>, int,
      int>& b1)  {
        {
          auto& vid = b1.r1;
          auto& R_A = b1.r2;
          auto& R_B = b1.r3;
          return nd_insert_R_do_complete_s11(R_r1_r2_r3<R_key_value<int, int>, int, int> {vid, R_A,
          R_B});
        }
      }
      unit_t insert_R_rcv_corrective_s8_m___SQL_SUM_AGGREGATE_1_mR1(const R_r1_r2_r3_r4_r5_r6_r7<Address,
      int, R_key_value<int, int>, int, R_key_value<int, int>, _Seq<R_key_value<int, int>>,
      _Set<R_key_value<int, int>>>& b1)  {
        {
          auto orig_addr = b1.r1;
          auto orig_stmt_id = b1.r2;
          auto orig_vid = b1.r3;
          auto hop = b1.r4;
          auto vid = b1.r5;
          auto compute_vids = b1.r6;
          auto delta_tuples = b1.r7;
          unit_t __1009;
          nd_add_delta_to_int_int(R_r1_r2_r3_r4<shared_ptr<_Set<R_r1_r2_r3<R_key_value<int, int>,
          int, int>>>, bool, R_key_value<int, int>, _Set<R_key_value<int,
          int>>> {map___SQL_SUM_AGGREGATE_1_mR1_s8_buf, false, vid, delta_tuples});
          {
            int __1010;
            auto __1015 = 0;
            for (const auto& __1014: compute_vids) {
              __1015 = [this, &delta_tuples, &hop, &orig_addr, &orig_stmt_id,
              &orig_vid] (const int& acc_count) mutable {
                return [this, &acc_count, &delta_tuples, &hop, &orig_addr, &orig_stmt_id,
                &orig_vid] (const R_key_value<int, int>& compute_vid) mutable {
                  {
                    int __1011;
                    shared_ptr<R_key_value<R_key_value<R_key_value<int, int>, int>, R_key_value<int,
                    _Map<R_key_value<int, int>>>>> __1012;
                    __1012 = nd_stmt_cntrs.filter([this,
                    &compute_vid] (const R_key_value<R_key_value<R_key_value<int, int>, int>,
                    R_key_value<int, _Map<R_key_value<int, int>>>>& b1) mutable {
                      {
                        auto& key = b1.key;
                        auto& value = b1.value;
                        return key == (R_key_value<R_key_value<int, int>, int> {compute_vid, 8});
                      }
                    }).peek(unit_t {});
                    if (__1012) {
                      R_key_value<R_key_value<R_key_value<int, int>, int>, R_key_value<int,
                      _Map<R_key_value<int, int>>>>& lkup = *__1012;
                      __1011 = lkup.value.key;
                    } else {
                      __1011 = 0;
                    }
                    auto& cntr = __1011;
                    if (cntr == 0) {
                      R_key_value<int, int> __1013;
                      __1013 = nd_log_get_bound_insert_R(compute_vid);
                      {
                        auto& R_A = __1013.key;
                        auto& R_B = __1013.value;
                        return std::move(acc_count + insert_R_do_corrective_s8_m___SQL_SUM_AGGREGATE_1_mR1(R_r1_r2_r3_r4_r5_r6_r7_r8<Address,
                        int, R_key_value<int, int>, int, R_key_value<int, int>, int, int,
                        _Set<R_key_value<int, int>>> {orig_addr, orig_stmt_id, orig_vid, hop,
                        compute_vid, R_A, R_B, delta_tuples}));
                      }
                    } else {
                      return std::move(acc_count);
                    }
                  }
                };
              }(std::move(__1015))(__1014);
            }
            __1010 = std::move(__1015);
            auto& sent_msgs = __1010;
            auto __1016 = std::make_shared<K3::ValDispatcher<R_r1_r2_r3_r4<R_key_value<int, int>,
            int, int, int>>>(R_r1_r2_r3_r4<R_key_value<int, int>, int, int, int> {orig_vid,
            orig_stmt_id, hop, sent_msgs});
            __engine.send(orig_addr, __nd_rcv_corr_done_tid, __1016, me);
            __1009 = unit_t {};
          }
          orig_addr = b1.r1;
          orig_stmt_id = b1.r2;
          orig_vid = b1.r3;
          hop = b1.r4;
          vid = b1.r5;
          compute_vids = b1.r6;
          delta_tuples = b1.r7;
          return __1009;
        }
      }
      unit_t insert_R_rcv_corrective_s10_m___SQL_SUM_AGGREGATE_2_mR1(const R_r1_r2_r3_r4_r5_r6_r7<Address,
      int, R_key_value<int, int>, int, R_key_value<int, int>, _Seq<R_key_value<int, int>>,
      _Set<R_key_value<int, int>>>& b1)  {
        {
          auto orig_addr = b1.r1;
          auto orig_stmt_id = b1.r2;
          auto orig_vid = b1.r3;
          auto hop = b1.r4;
          auto vid = b1.r5;
          auto compute_vids = b1.r6;
          auto delta_tuples = b1.r7;
          unit_t __1017;
          nd_add_delta_to_int_int(R_r1_r2_r3_r4<shared_ptr<_Set<R_r1_r2_r3<R_key_value<int, int>,
          int, int>>>, bool, R_key_value<int, int>, _Set<R_key_value<int,
          int>>> {map___SQL_SUM_AGGREGATE_2_mR1_s10_buf, false, vid, delta_tuples});
          {
            int __1018;
            auto __1023 = 0;
            for (const auto& __1022: compute_vids) {
              __1023 = [this, &delta_tuples, &hop, &orig_addr, &orig_stmt_id,
              &orig_vid] (const int& acc_count) mutable {
                return [this, &acc_count, &delta_tuples, &hop, &orig_addr, &orig_stmt_id,
                &orig_vid] (const R_key_value<int, int>& compute_vid) mutable {
                  {
                    int __1019;
                    shared_ptr<R_key_value<R_key_value<R_key_value<int, int>, int>, R_key_value<int,
                    _Map<R_key_value<int, int>>>>> __1020;
                    __1020 = nd_stmt_cntrs.filter([this,
                    &compute_vid] (const R_key_value<R_key_value<R_key_value<int, int>, int>,
                    R_key_value<int, _Map<R_key_value<int, int>>>>& b1) mutable {
                      {
                        auto& key = b1.key;
                        auto& value = b1.value;
                        return key == (R_key_value<R_key_value<int, int>, int> {compute_vid, 10});
                      }
                    }).peek(unit_t {});
                    if (__1020) {
                      R_key_value<R_key_value<R_key_value<int, int>, int>, R_key_value<int,
                      _Map<R_key_value<int, int>>>>& lkup = *__1020;
                      __1019 = lkup.value.key;
                    } else {
                      __1019 = 0;
                    }
                    auto& cntr = __1019;
                    if (cntr == 0) {
                      R_key_value<int, int> __1021;
                      __1021 = nd_log_get_bound_insert_R(compute_vid);
                      {
                        auto& R_A = __1021.key;
                        auto& R_B = __1021.value;
                        return std::move(acc_count + insert_R_do_corrective_s10_m___SQL_SUM_AGGREGATE_2_mR1(R_r1_r2_r3_r4_r5_r6_r7_r8<Address,
                        int, R_key_value<int, int>, int, R_key_value<int, int>, int, int,
                        _Set<R_key_value<int, int>>> {orig_addr, orig_stmt_id, orig_vid, hop,
                        compute_vid, R_A, R_B, delta_tuples}));
                      }
                    } else {
                      return std::move(acc_count);
                    }
                  }
                };
              }(std::move(__1023))(__1022);
            }
            __1018 = std::move(__1023);
            auto& sent_msgs = __1018;
            auto __1024 = std::make_shared<K3::ValDispatcher<R_r1_r2_r3_r4<R_key_value<int, int>,
            int, int, int>>>(R_r1_r2_r3_r4<R_key_value<int, int>, int, int, int> {orig_vid,
            orig_stmt_id, hop, sent_msgs});
            __engine.send(orig_addr, __nd_rcv_corr_done_tid, __1024, me);
            __1017 = unit_t {};
          }
          orig_addr = b1.r1;
          orig_stmt_id = b1.r2;
          orig_vid = b1.r3;
          hop = b1.r4;
          vid = b1.r5;
          compute_vids = b1.r6;
          delta_tuples = b1.r7;
          return __1017;
        }
      }
      unit_t insert_R_rcv_corrective_s10_m___SQL_SUM_AGGREGATE_1_mR1(const R_r1_r2_r3_r4_r5_r6_r7<Address,
      int, R_key_value<int, int>, int, R_key_value<int, int>, _Seq<R_key_value<int, int>>,
      _Set<R_key_value<int, int>>>& b1)  {
        {
          auto orig_addr = b1.r1;
          auto orig_stmt_id = b1.r2;
          auto orig_vid = b1.r3;
          auto hop = b1.r4;
          auto vid = b1.r5;
          auto compute_vids = b1.r6;
          auto delta_tuples = b1.r7;
          unit_t __1025;
          nd_add_delta_to_int_int(R_r1_r2_r3_r4<shared_ptr<_Set<R_r1_r2_r3<R_key_value<int, int>,
          int, int>>>, bool, R_key_value<int, int>, _Set<R_key_value<int,
          int>>> {map___SQL_SUM_AGGREGATE_1_mR1_s10_buf, false, vid, delta_tuples});
          {
            int __1026;
            auto __1031 = 0;
            for (const auto& __1030: compute_vids) {
              __1031 = [this, &delta_tuples, &hop, &orig_addr, &orig_stmt_id,
              &orig_vid] (const int& acc_count) mutable {
                return [this, &acc_count, &delta_tuples, &hop, &orig_addr, &orig_stmt_id,
                &orig_vid] (const R_key_value<int, int>& compute_vid) mutable {
                  {
                    int __1027;
                    shared_ptr<R_key_value<R_key_value<R_key_value<int, int>, int>, R_key_value<int,
                    _Map<R_key_value<int, int>>>>> __1028;
                    __1028 = nd_stmt_cntrs.filter([this,
                    &compute_vid] (const R_key_value<R_key_value<R_key_value<int, int>, int>,
                    R_key_value<int, _Map<R_key_value<int, int>>>>& b1) mutable {
                      {
                        auto& key = b1.key;
                        auto& value = b1.value;
                        return key == (R_key_value<R_key_value<int, int>, int> {compute_vid, 10});
                      }
                    }).peek(unit_t {});
                    if (__1028) {
                      R_key_value<R_key_value<R_key_value<int, int>, int>, R_key_value<int,
                      _Map<R_key_value<int, int>>>>& lkup = *__1028;
                      __1027 = lkup.value.key;
                    } else {
                      __1027 = 0;
                    }
                    auto& cntr = __1027;
                    if (cntr == 0) {
                      R_key_value<int, int> __1029;
                      __1029 = nd_log_get_bound_insert_R(compute_vid);
                      {
                        auto& R_A = __1029.key;
                        auto& R_B = __1029.value;
                        return std::move(acc_count + insert_R_do_corrective_s10_m___SQL_SUM_AGGREGATE_1_mR1(R_r1_r2_r3_r4_r5_r6_r7_r8<Address,
                        int, R_key_value<int, int>, int, R_key_value<int, int>, int, int,
                        _Set<R_key_value<int, int>>> {orig_addr, orig_stmt_id, orig_vid, hop,
                        compute_vid, R_A, R_B, delta_tuples}));
                      }
                    } else {
                      return std::move(acc_count);
                    }
                  }
                };
              }(std::move(__1031))(__1030);
            }
            __1026 = std::move(__1031);
            auto& sent_msgs = __1026;
            auto __1032 = std::make_shared<K3::ValDispatcher<R_r1_r2_r3_r4<R_key_value<int, int>,
            int, int, int>>>(R_r1_r2_r3_r4<R_key_value<int, int>, int, int, int> {orig_vid,
            orig_stmt_id, hop, sent_msgs});
            __engine.send(orig_addr, __nd_rcv_corr_done_tid, __1032, me);
            __1025 = unit_t {};
          }
          orig_addr = b1.r1;
          orig_stmt_id = b1.r2;
          orig_vid = b1.r3;
          hop = b1.r4;
          vid = b1.r5;
          compute_vids = b1.r6;
          delta_tuples = b1.r7;
          return __1025;
        }
      }
      unit_t nd_delete_R_rcv_put(const R_r1_r2_r3_r4_r5<Address, _Collection<R_key_value<int, int>>,
      R_key_value<int, int>, int, int>& b1)  {
        {
          auto sender_ip = b1.r1;
          auto stmt_cnt_list = b1.r2;
          auto vid = b1.r3;
          auto R_A = b1.r4;
          auto R_B = b1.r5;
          unit_t __1033;
          stmt_cnt_list.iterate([this, &R_A, &R_B, &vid] (const R_key_value<int, int>& b1) mutable {
            {
              auto& stmt_id = b1.key;
              auto& count = b1.value;
              if (nd_check_stmt_cntr_index(R_r1_r2_r3<R_key_value<int, int>, int, int> {vid,
              stmt_id, count})) {
                if (stmt_id == 15) {
                  return nd_delete_R_do_complete_s15(R_r1_r2_r3<R_key_value<int, int>, int,
                  int> {vid, R_A, R_B});
                } else {
                  if (stmt_id == 14) {
                    return nd_delete_R_do_complete_s14(R_r1_r2_r3<R_key_value<int, int>, int,
                    int> {vid, R_A, R_B});
                  } else {
                    if (stmt_id == 13) {
                      return nd_delete_R_do_complete_s13(R_r1_r2_r3<R_key_value<int, int>, int,
                      int> {vid, R_A, R_B});
                    } else {
                      if (stmt_id == 12) {
                        return nd_delete_R_do_complete_s12(R_r1_r2_r3<R_key_value<int, int>, int,
                        int> {vid, R_A, R_B});
                      } else {
                        return unit_t {};
                      }
                    }
                  }
                }
              } else {
                return unit_t {};
              }
            }
          });
          auto __1034 = std::make_shared<K3::ValDispatcher<R_key_value<Address, R_key_value<int,
          int>>>>(R_key_value<Address, R_key_value<int, int>> {me, vid});
          __engine.send(sender_ip, __sw_ack_rcv_tid, __1034, me);
          __1033 = unit_t {};
          sender_ip = b1.r1;
          stmt_cnt_list = b1.r2;
          vid = b1.r3;
          R_A = b1.r4;
          R_B = b1.r5;
          return __1033;
        }
      }
      unit_t nd_delete_R_rcv_fetch(const R_r1_r2_r3_r4<_Collection<R_key_value<int, int>>,
      R_key_value<int, int>, int, int>& b1)  {
        {
          auto stmt_map_ids = b1.r1;
          auto vid = b1.r2;
          auto R_A = b1.r3;
          auto R_B = b1.r4;
          unit_t __1035;
          nd_log_write_delete_R(R_r1_r2_r3<R_key_value<int, int>, int, int> {vid, R_A, R_B});
          __1035 = stmt_map_ids.iterate([this, &R_A, &R_B, &vid] (const R_key_value<int,
          int>& b1) mutable {
            {
              auto& stmt_id = b1.key;
              auto& map_id = b1.value;
              if (stmt_id == 12) {
                if (map_id == 3) {
                  auto __1036 = std::make_shared<K3::ValDispatcher<R_r1_r2_r3<R_key_value<int, int>,
                  int, int>>>(R_r1_r2_r3<R_key_value<int, int>, int, int> {vid, R_A, R_B});
                  __engine.send(me, __nd_delete_R_send_push_s12_m___SQL_SUM_AGGREGATE_1_mR1_tid,
                  __1036, me);
                  return unit_t {};
                } else {
                  return error<unit_t>(print("nd_rcv_fetch: invalid map id"));
                }
              } else {
                if (stmt_id == 14) {
                  if (map_id == 6) {
                    auto __1037 = std::make_shared<K3::ValDispatcher<R_r1_r2_r3<R_key_value<int,
                    int>, int, int>>>(R_r1_r2_r3<R_key_value<int, int>, int, int> {vid, R_A, R_B});
                    __engine.send(me, __nd_delete_R_send_push_s14_m___SQL_SUM_AGGREGATE_2_mR1_tid,
                    __1037, me);
                    return unit_t {};
                  } else {
                    if (map_id == 3) {
                      auto __1038 = std::make_shared<K3::ValDispatcher<R_r1_r2_r3<R_key_value<int,
                      int>, int, int>>>(R_r1_r2_r3<R_key_value<int, int>, int, int> {vid, R_A,
                      R_B});
                      __engine.send(me, __nd_delete_R_send_push_s14_m___SQL_SUM_AGGREGATE_1_mR1_tid,
                      __1038, me);
                      return unit_t {};
                    } else {
                      return error<unit_t>(print("nd_rcv_fetch: invalid map id"));
                    }
                  }
                } else {
                  return error<unit_t>(print("nd_rcv_fetch: invalid stmt id"));
                }
              }
            }
          });
          stmt_map_ids = b1.r1;
          vid = b1.r2;
          R_A = b1.r3;
          R_B = b1.r4;
          return __1035;
        }
      }
      unit_t nd_delete_R_send_push_s12_m___SQL_SUM_AGGREGATE_1_mR1(const R_r1_r2_r3<R_key_value<int,
      int>, int, int>& b1)  {
        {
          auto vid = b1.r1;
          auto R_A = b1.r2;
          auto R_B = b1.r3;
          unit_t __1039;
          {
            auto& __SQL_SUM_AGGREGATE_1_mR1_deref = *__SQL_SUM_AGGREGATE_1_mR1;
            nd_log_master_write(R_key_value<R_key_value<int, int>, int> {vid, 12});
            __1039 = shuffle___SQL_SUM_AGGREGATE_1_mR1_to___SQL_SUM_AGGREGATE_1(R_r1_r2_r3<unit_t,
            _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>, bool> {unit_t {},
            frontier_int_int(R_key_value<R_key_value<int, int>, _Set<R_r1_r2_r3<R_key_value<int,
            int>, int, int>>> {vid, __SQL_SUM_AGGREGATE_1_mR1_deref.filter([this,
            &R_B] (const R_r1_r2_r3<R_key_value<int, int>, int, int>& b1) mutable {
              {
                auto& r1 = b1.r1;
                auto& r2 = b1.r2;
                auto& r3 = b1.r3;
                return r2 == R_B;
              }
            })}), true}).iterate([this, &R_A, &R_B, &vid] (const R_key_value<Address,
            _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>>& b1) mutable {
              {
                auto& ip = b1.key;
                auto& tuples = b1.value;
                auto __1040 = std::make_shared<K3::ValDispatcher<R_r1_r2_r3_r4<_Set<R_r1_r2_r3<R_key_value<int,
                int>, int, int>>, R_key_value<int, int>, int,
                int>>>(R_r1_r2_r3_r4<_Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>,
                R_key_value<int, int>, int, int> {tuples, vid, R_A, R_B});
                __engine.send(ip, __nd_delete_R_rcv_push_s12_m___SQL_SUM_AGGREGATE_1_mR1_tid,
                __1040, me);
                return unit_t {};
              }
            });
          }
          vid = b1.r1;
          R_A = b1.r2;
          R_B = b1.r3;
          return __1039;
        }
      }
      unit_t nd_delete_R_send_push_s14_m___SQL_SUM_AGGREGATE_2_mR1(const R_r1_r2_r3<R_key_value<int,
      int>, int, int>& b1)  {
        {
          auto vid = b1.r1;
          auto R_A = b1.r2;
          auto R_B = b1.r3;
          unit_t __1041;
          {
            auto& __SQL_SUM_AGGREGATE_2_mR1_deref = *__SQL_SUM_AGGREGATE_2_mR1;
            nd_log_master_write(R_key_value<R_key_value<int, int>, int> {vid, 14});
            __1041 = shuffle___SQL_SUM_AGGREGATE_2_mR1_to___SQL_SUM_AGGREGATE_2(R_r1_r2_r3<unit_t,
            _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>, bool> {unit_t {},
            frontier_int_int(R_key_value<R_key_value<int, int>, _Set<R_r1_r2_r3<R_key_value<int,
            int>, int, int>>> {vid, __SQL_SUM_AGGREGATE_2_mR1_deref.filter([this,
            &R_B] (const R_r1_r2_r3<R_key_value<int, int>, int, int>& b1) mutable {
              {
                auto& r1 = b1.r1;
                auto& r2 = b1.r2;
                auto& r3 = b1.r3;
                return r2 == R_B;
              }
            })}), true}).iterate([this, &R_A, &R_B, &vid] (const R_key_value<Address,
            _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>>& b1) mutable {
              {
                auto& ip = b1.key;
                auto& tuples = b1.value;
                auto __1042 = std::make_shared<K3::ValDispatcher<R_r1_r2_r3_r4<_Set<R_r1_r2_r3<R_key_value<int,
                int>, int, int>>, R_key_value<int, int>, int,
                int>>>(R_r1_r2_r3_r4<_Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>,
                R_key_value<int, int>, int, int> {tuples, vid, R_A, R_B});
                __engine.send(ip, __nd_delete_R_rcv_push_s14_m___SQL_SUM_AGGREGATE_2_mR1_tid,
                __1042, me);
                return unit_t {};
              }
            });
          }
          vid = b1.r1;
          R_A = b1.r2;
          R_B = b1.r3;
          return __1041;
        }
      }
      unit_t nd_delete_R_send_push_s14_m___SQL_SUM_AGGREGATE_1_mR1(const R_r1_r2_r3<R_key_value<int,
      int>, int, int>& b1)  {
        {
          auto vid = b1.r1;
          auto R_A = b1.r2;
          auto R_B = b1.r3;
          unit_t __1043;
          {
            auto& __SQL_SUM_AGGREGATE_1_mR1_deref = *__SQL_SUM_AGGREGATE_1_mR1;
            nd_log_master_write(R_key_value<R_key_value<int, int>, int> {vid, 14});
            __1043 = shuffle___SQL_SUM_AGGREGATE_1_mR1_to___SQL_SUM_AGGREGATE_2(R_r1_r2_r3<unit_t,
            _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>, bool> {unit_t {},
            frontier_int_int(R_key_value<R_key_value<int, int>, _Set<R_r1_r2_r3<R_key_value<int,
            int>, int, int>>> {vid, __SQL_SUM_AGGREGATE_1_mR1_deref.filter([this,
            &R_B] (const R_r1_r2_r3<R_key_value<int, int>, int, int>& b1) mutable {
              {
                auto& r1 = b1.r1;
                auto& r2 = b1.r2;
                auto& r3 = b1.r3;
                return r2 == R_B;
              }
            })}), true}).iterate([this, &R_A, &R_B, &vid] (const R_key_value<Address,
            _Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>>& b1) mutable {
              {
                auto& ip = b1.key;
                auto& tuples = b1.value;
                auto __1044 = std::make_shared<K3::ValDispatcher<R_r1_r2_r3_r4<_Set<R_r1_r2_r3<R_key_value<int,
                int>, int, int>>, R_key_value<int, int>, int,
                int>>>(R_r1_r2_r3_r4<_Set<R_r1_r2_r3<R_key_value<int, int>, int, int>>,
                R_key_value<int, int>, int, int> {tuples, vid, R_A, R_B});
                __engine.send(ip, __nd_delete_R_rcv_push_s14_m___SQL_SUM_AGGREGATE_1_mR1_tid,
                __1044, me);
                return unit_t {};
              }
            });
          }
          vid = b1.r1;
          R_A = b1.r2;
          R_B = b1.r3;
          return __1043;
        }
      }
      unit_t nd_delete_R_rcv_push_s12_m___SQL_SUM_AGGREGATE_1_mR1(const R_r1_r2_r3_r4<_Set<R_r1_r2_r3<R_key_value<int,
      int>, int, int>>, R_key_value<int, int>, int, int>& b1)  {
        {
          auto tuples = b1.r1;
          auto vid = b1.r2;
          auto R_A = b1.r3;
          auto R_B = b1.r4;
          unit_t __1045;
          nd_log_write_delete_R(R_r1_r2_r3<R_key_value<int, int>, int, int> {vid, R_A, R_B});
          tuples.iterate([this] (const R_r1_r2_r3<R_key_value<int, int>, int, int>& tuple) mutable {
            {
              auto& map___SQL_SUM_AGGREGATE_1_mR1_s12_buf_d = *map___SQL_SUM_AGGREGATE_1_mR1_s12_buf;
              {
                auto _tup0 = tuple.r1;
                auto _tup1 = tuple.r2;
                auto _tup2 = tuple.r3;
                unit_t __1046;
                shared_ptr<R_r1_r2_r3<R_key_value<int, int>, int, int>> __1047;
                __1047 = map___SQL_SUM_AGGREGATE_1_mR1_s12_buf_d.filter([this, &_tup0,
                &_tup1] (const R_r1_r2_r3<R_key_value<int, int>, int, int>& b1) mutable {
                  {
                    auto& r1 = b1.r1;
                    auto& r2 = b1.r2;
                    auto& r3 = b1.r3;
                    return r1 == _tup0 && r2 == _tup1;
                  }
                }).peek(unit_t {});
                if (__1047) {
                  R_r1_r2_r3<R_key_value<int, int>, int, int>& vals = *__1047;
                  __1046 = map___SQL_SUM_AGGREGATE_1_mR1_s12_buf_d.update(vals, tuple);
                } else {
                  __1046 = map___SQL_SUM_AGGREGATE_1_mR1_s12_buf_d.insert(tuple);
                }
                _tup0 = tuple.r1;
                _tup1 = tuple.r2;
                _tup2 = tuple.r3;
                return __1046;
              }
            }
          });
          if (nd_check_stmt_cntr_index(R_r1_r2_r3<R_key_value<int, int>, int, int> {vid, 12,
          -(1)})) {
            __1045 = nd_delete_R_do_complete_s12(R_r1_r2_r3<R_key_value<int, int>, int, int> {vid,
            R_A, R_B});
          } else {
            __1045 = unit_t {};
          }
          tuples = b1.r1;
          vid = b1.r2;
          R_A = b1.r3;
          R_B = b1.r4;
          return __1045;
        }
      }
      unit_t nd_delete_R_rcv_push_s14_m___SQL_SUM_AGGREGATE_2_mR1(const R_r1_r2_r3_r4<_Set<R_r1_r2_r3<R_key_value<int,
      int>, int, int>>, R_key_value<int, int>, int, int>& b1)  {
        {
          auto tuples = b1.r1;
          auto vid = b1.r2;
          auto R_A = b1.r3;
          auto R_B = b1.r4;
          unit_t __1048;
          nd_log_write_delete_R(R_r1_r2_r3<R_key_value<int, int>, int, int> {vid, R_A, R_B});
          tuples.iterate([this] (const R_r1_r2_r3<R_key_value<int, int>, int, int>& tuple) mutable {
            {
              auto& map___SQL_SUM_AGGREGATE_2_mR1_s14_buf_d = *map___SQL_SUM_AGGREGATE_2_mR1_s14_buf;
              {
                auto _tup0 = tuple.r1;
                auto _tup1 = tuple.r2;
                auto _tup2 = tuple.r3;
                unit_t __1049;
                shared_ptr<R_r1_r2_r3<R_key_value<int, int>, int, int>> __1050;
                __1050 = map___SQL_SUM_AGGREGATE_2_mR1_s14_buf_d.filter([this, &_tup0,
                &_tup1] (const R_r1_r2_r3<R_key_value<int, int>, int, int>& b1) mutable {
                  {
                    auto& r1 = b1.r1;
                    auto& r2 = b1.r2;
                    auto& r3 = b1.r3;
                    return r1 == _tup0 && r2 == _tup1;
                  }
                }).peek(unit_t {});
                if (__1050) {
                  R_r1_r2_r3<R_key_value<int, int>, int, int>& vals = *__1050;
                  __1049 = map___SQL_SUM_AGGREGATE_2_mR1_s14_buf_d.update(vals, tuple);
                } else {
                  __1049 = map___SQL_SUM_AGGREGATE_2_mR1_s14_buf_d.insert(tuple);
                }
                _tup0 = tuple.r1;
                _tup1 = tuple.r2;
                _tup2 = tuple.r3;
                return __1049;
              }
            }
          });
          if (nd_check_stmt_cntr_index(R_r1_r2_r3<R_key_value<int, int>, int, int> {vid, 14,
          -(1)})) {
            __1048 = nd_delete_R_do_complete_s14(R_r1_r2_r3<R_key_value<int, int>, int, int> {vid,
            R_A, R_B});
          } else {
            __1048 = unit_t {};
          }
          tuples = b1.r1;
          vid = b1.r2;
          R_A = b1.r3;
          R_B = b1.r4;
          return __1048;
        }
      }
      unit_t nd_delete_R_rcv_push_s14_m___SQL_SUM_AGGREGATE_1_mR1(const R_r1_r2_r3_r4<_Set<R_r1_r2_r3<R_key_value<int,
      int>, int, int>>, R_key_value<int, int>, int, int>& b1)  {
        {
          auto tuples = b1.r1;
          auto vid = b1.r2;
          auto R_A = b1.r3;
          auto R_B = b1.r4;
          unit_t __1051;
          nd_log_write_delete_R(R_r1_r2_r3<R_key_value<int, int>, int, int> {vid, R_A, R_B});
          tuples.iterate([this] (const R_r1_r2_r3<R_key_value<int, int>, int, int>& tuple) mutable {
            {
              auto& map___SQL_SUM_AGGREGATE_1_mR1_s14_buf_d = *map___SQL_SUM_AGGREGATE_1_mR1_s14_buf;
              {
                auto _tup0 = tuple.r1;
                auto _tup1 = tuple.r2;
                auto _tup2 = tuple.r3;
                unit_t __1052;
                shared_ptr<R_r1_r2_r3<R_key_value<int, int>, int, int>> __1053;
                __1053 = map___SQL_SUM_AGGREGATE_1_mR1_s14_buf_d.filter([this, &_tup0,
                &_tup1] (const R_r1_r2_r3<R_key_value<int, int>, int, int>& b1) mutable {
                  {
                    auto& r1 = b1.r1;
                    auto& r2 = b1.r2;
                    auto& r3 = b1.r3;
                    return r1 == _tup0 && r2 == _tup1;
                  }
                }).peek(unit_t {});
                if (__1053) {
                  R_r1_r2_r3<R_key_value<int, int>, int, int>& vals = *__1053;
                  __1052 = map___SQL_SUM_AGGREGATE_1_mR1_s14_buf_d.update(vals, tuple);
                } else {
                  __1052 = map___SQL_SUM_AGGREGATE_1_mR1_s14_buf_d.insert(tuple);
                }
                _tup0 = tuple.r1;
                _tup1 = tuple.r2;
                _tup2 = tuple.r3;
                return __1052;
              }
            }
          });
          if (nd_check_stmt_cntr_index(R_r1_r2_r3<R_key_value<int, int>, int, int> {vid, 14,
          -(1)})) {
            __1051 = nd_delete_R_do_complete_s14(R_r1_r2_r3<R_key_value<int, int>, int, int> {vid,
            R_A, R_B});
          } else {
            __1051 = unit_t {};
          }
          tuples = b1.r1;
          vid = b1.r2;
          R_A = b1.r3;
          R_B = b1.r4;
          return __1051;
        }
      }
      unit_t nd_delete_R_do_complete_s13_trig(const R_r1_r2_r3<R_key_value<int, int>, int,
      int>& b1)  {
        {
          auto& vid = b1.r1;
          auto& R_A = b1.r2;
          auto& R_B = b1.r3;
          return nd_delete_R_do_complete_s13(R_r1_r2_r3<R_key_value<int, int>, int, int> {vid, R_A,
          R_B});
        }
      }
      unit_t nd_delete_R_do_complete_s15_trig(const R_r1_r2_r3<R_key_value<int, int>, int,
      int>& b1)  {
        {
          auto& vid = b1.r1;
          auto& R_A = b1.r2;
          auto& R_B = b1.r3;
          return nd_delete_R_do_complete_s15(R_r1_r2_r3<R_key_value<int, int>, int, int> {vid, R_A,
          R_B});
        }
      }
      unit_t delete_R_rcv_corrective_s12_m___SQL_SUM_AGGREGATE_1_mR1(const R_r1_r2_r3_r4_r5_r6_r7<Address,
      int, R_key_value<int, int>, int, R_key_value<int, int>, _Seq<R_key_value<int, int>>,
      _Set<R_key_value<int, int>>>& b1)  {
        {
          auto orig_addr = b1.r1;
          auto orig_stmt_id = b1.r2;
          auto orig_vid = b1.r3;
          auto hop = b1.r4;
          auto vid = b1.r5;
          auto compute_vids = b1.r6;
          auto delta_tuples = b1.r7;
          unit_t __1054;
          nd_add_delta_to_int_int(R_r1_r2_r3_r4<shared_ptr<_Set<R_r1_r2_r3<R_key_value<int, int>,
          int, int>>>, bool, R_key_value<int, int>, _Set<R_key_value<int,
          int>>> {map___SQL_SUM_AGGREGATE_1_mR1_s12_buf, false, vid, delta_tuples});
          {
            int __1055;
            auto __1060 = 0;
            for (const auto& __1059: compute_vids) {
              __1060 = [this, &delta_tuples, &hop, &orig_addr, &orig_stmt_id,
              &orig_vid] (const int& acc_count) mutable {
                return [this, &acc_count, &delta_tuples, &hop, &orig_addr, &orig_stmt_id,
                &orig_vid] (const R_key_value<int, int>& compute_vid) mutable {
                  {
                    int __1056;
                    shared_ptr<R_key_value<R_key_value<R_key_value<int, int>, int>, R_key_value<int,
                    _Map<R_key_value<int, int>>>>> __1057;
                    __1057 = nd_stmt_cntrs.filter([this,
                    &compute_vid] (const R_key_value<R_key_value<R_key_value<int, int>, int>,
                    R_key_value<int, _Map<R_key_value<int, int>>>>& b1) mutable {
                      {
                        auto& key = b1.key;
                        auto& value = b1.value;
                        return key == (R_key_value<R_key_value<int, int>, int> {compute_vid, 12});
                      }
                    }).peek(unit_t {});
                    if (__1057) {
                      R_key_value<R_key_value<R_key_value<int, int>, int>, R_key_value<int,
                      _Map<R_key_value<int, int>>>>& lkup = *__1057;
                      __1056 = lkup.value.key;
                    } else {
                      __1056 = 0;
                    }
                    auto& cntr = __1056;
                    if (cntr == 0) {
                      R_key_value<int, int> __1058;
                      __1058 = nd_log_get_bound_delete_R(compute_vid);
                      {
                        auto& R_A = __1058.key;
                        auto& R_B = __1058.value;
                        return std::move(acc_count + delete_R_do_corrective_s12_m___SQL_SUM_AGGREGATE_1_mR1(R_r1_r2_r3_r4_r5_r6_r7_r8<Address,
                        int, R_key_value<int, int>, int, R_key_value<int, int>, int, int,
                        _Set<R_key_value<int, int>>> {orig_addr, orig_stmt_id, orig_vid, hop,
                        compute_vid, R_A, R_B, delta_tuples}));
                      }
                    } else {
                      return std::move(acc_count);
                    }
                  }
                };
              }(std::move(__1060))(__1059);
            }
            __1055 = std::move(__1060);
            auto& sent_msgs = __1055;
            auto __1061 = std::make_shared<K3::ValDispatcher<R_r1_r2_r3_r4<R_key_value<int, int>,
            int, int, int>>>(R_r1_r2_r3_r4<R_key_value<int, int>, int, int, int> {orig_vid,
            orig_stmt_id, hop, sent_msgs});
            __engine.send(orig_addr, __nd_rcv_corr_done_tid, __1061, me);
            __1054 = unit_t {};
          }
          orig_addr = b1.r1;
          orig_stmt_id = b1.r2;
          orig_vid = b1.r3;
          hop = b1.r4;
          vid = b1.r5;
          compute_vids = b1.r6;
          delta_tuples = b1.r7;
          return __1054;
        }
      }
      unit_t delete_R_rcv_corrective_s14_m___SQL_SUM_AGGREGATE_2_mR1(const R_r1_r2_r3_r4_r5_r6_r7<Address,
      int, R_key_value<int, int>, int, R_key_value<int, int>, _Seq<R_key_value<int, int>>,
      _Set<R_key_value<int, int>>>& b1)  {
        {
          auto orig_addr = b1.r1;
          auto orig_stmt_id = b1.r2;
          auto orig_vid = b1.r3;
          auto hop = b1.r4;
          auto vid = b1.r5;
          auto compute_vids = b1.r6;
          auto delta_tuples = b1.r7;
          unit_t __1062;
          nd_add_delta_to_int_int(R_r1_r2_r3_r4<shared_ptr<_Set<R_r1_r2_r3<R_key_value<int, int>,
          int, int>>>, bool, R_key_value<int, int>, _Set<R_key_value<int,
          int>>> {map___SQL_SUM_AGGREGATE_2_mR1_s14_buf, false, vid, delta_tuples});
          {
            int __1063;
            auto __1068 = 0;
            for (const auto& __1067: compute_vids) {
              __1068 = [this, &delta_tuples, &hop, &orig_addr, &orig_stmt_id,
              &orig_vid] (const int& acc_count) mutable {
                return [this, &acc_count, &delta_tuples, &hop, &orig_addr, &orig_stmt_id,
                &orig_vid] (const R_key_value<int, int>& compute_vid) mutable {
                  {
                    int __1064;
                    shared_ptr<R_key_value<R_key_value<R_key_value<int, int>, int>, R_key_value<int,
                    _Map<R_key_value<int, int>>>>> __1065;
                    __1065 = nd_stmt_cntrs.filter([this,
                    &compute_vid] (const R_key_value<R_key_value<R_key_value<int, int>, int>,
                    R_key_value<int, _Map<R_key_value<int, int>>>>& b1) mutable {
                      {
                        auto& key = b1.key;
                        auto& value = b1.value;
                        return key == (R_key_value<R_key_value<int, int>, int> {compute_vid, 14});
                      }
                    }).peek(unit_t {});
                    if (__1065) {
                      R_key_value<R_key_value<R_key_value<int, int>, int>, R_key_value<int,
                      _Map<R_key_value<int, int>>>>& lkup = *__1065;
                      __1064 = lkup.value.key;
                    } else {
                      __1064 = 0;
                    }
                    auto& cntr = __1064;
                    if (cntr == 0) {
                      R_key_value<int, int> __1066;
                      __1066 = nd_log_get_bound_delete_R(compute_vid);
                      {
                        auto& R_A = __1066.key;
                        auto& R_B = __1066.value;
                        return std::move(acc_count + delete_R_do_corrective_s14_m___SQL_SUM_AGGREGATE_2_mR1(R_r1_r2_r3_r4_r5_r6_r7_r8<Address,
                        int, R_key_value<int, int>, int, R_key_value<int, int>, int, int,
                        _Set<R_key_value<int, int>>> {orig_addr, orig_stmt_id, orig_vid, hop,
                        compute_vid, R_A, R_B, delta_tuples}));
                      }
                    } else {
                      return std::move(acc_count);
                    }
                  }
                };
              }(std::move(__1068))(__1067);
            }
            __1063 = std::move(__1068);
            auto& sent_msgs = __1063;
            auto __1069 = std::make_shared<K3::ValDispatcher<R_r1_r2_r3_r4<R_key_value<int, int>,
            int, int, int>>>(R_r1_r2_r3_r4<R_key_value<int, int>, int, int, int> {orig_vid,
            orig_stmt_id, hop, sent_msgs});
            __engine.send(orig_addr, __nd_rcv_corr_done_tid, __1069, me);
            __1062 = unit_t {};
          }
          orig_addr = b1.r1;
          orig_stmt_id = b1.r2;
          orig_vid = b1.r3;
          hop = b1.r4;
          vid = b1.r5;
          compute_vids = b1.r6;
          delta_tuples = b1.r7;
          return __1062;
        }
      }
      unit_t delete_R_rcv_corrective_s14_m___SQL_SUM_AGGREGATE_1_mR1(const R_r1_r2_r3_r4_r5_r6_r7<Address,
      int, R_key_value<int, int>, int, R_key_value<int, int>, _Seq<R_key_value<int, int>>,
      _Set<R_key_value<int, int>>>& b1)  {
        {
          auto orig_addr = b1.r1;
          auto orig_stmt_id = b1.r2;
          auto orig_vid = b1.r3;
          auto hop = b1.r4;
          auto vid = b1.r5;
          auto compute_vids = b1.r6;
          auto delta_tuples = b1.r7;
          unit_t __1070;
          nd_add_delta_to_int_int(R_r1_r2_r3_r4<shared_ptr<_Set<R_r1_r2_r3<R_key_value<int, int>,
          int, int>>>, bool, R_key_value<int, int>, _Set<R_key_value<int,
          int>>> {map___SQL_SUM_AGGREGATE_1_mR1_s14_buf, false, vid, delta_tuples});
          {
            int __1071;
            auto __1076 = 0;
            for (const auto& __1075: compute_vids) {
              __1076 = [this, &delta_tuples, &hop, &orig_addr, &orig_stmt_id,
              &orig_vid] (const int& acc_count) mutable {
                return [this, &acc_count, &delta_tuples, &hop, &orig_addr, &orig_stmt_id,
                &orig_vid] (const R_key_value<int, int>& compute_vid) mutable {
                  {
                    int __1072;
                    shared_ptr<R_key_value<R_key_value<R_key_value<int, int>, int>, R_key_value<int,
                    _Map<R_key_value<int, int>>>>> __1073;
                    __1073 = nd_stmt_cntrs.filter([this,
                    &compute_vid] (const R_key_value<R_key_value<R_key_value<int, int>, int>,
                    R_key_value<int, _Map<R_key_value<int, int>>>>& b1) mutable {
                      {
                        auto& key = b1.key;
                        auto& value = b1.value;
                        return key == (R_key_value<R_key_value<int, int>, int> {compute_vid, 14});
                      }
                    }).peek(unit_t {});
                    if (__1073) {
                      R_key_value<R_key_value<R_key_value<int, int>, int>, R_key_value<int,
                      _Map<R_key_value<int, int>>>>& lkup = *__1073;
                      __1072 = lkup.value.key;
                    } else {
                      __1072 = 0;
                    }
                    auto& cntr = __1072;
                    if (cntr == 0) {
                      R_key_value<int, int> __1074;
                      __1074 = nd_log_get_bound_delete_R(compute_vid);
                      {
                        auto& R_A = __1074.key;
                        auto& R_B = __1074.value;
                        return std::move(acc_count + delete_R_do_corrective_s14_m___SQL_SUM_AGGREGATE_1_mR1(R_r1_r2_r3_r4_r5_r6_r7_r8<Address,
                        int, R_key_value<int, int>, int, R_key_value<int, int>, int, int,
                        _Set<R_key_value<int, int>>> {orig_addr, orig_stmt_id, orig_vid, hop,
                        compute_vid, R_A, R_B, delta_tuples}));
                      }
                    } else {
                      return std::move(acc_count);
                    }
                  }
                };
              }(std::move(__1076))(__1075);
            }
            __1071 = std::move(__1076);
            auto& sent_msgs = __1071;
            auto __1077 = std::make_shared<K3::ValDispatcher<R_r1_r2_r3_r4<R_key_value<int, int>,
            int, int, int>>>(R_r1_r2_r3_r4<R_key_value<int, int>, int, int, int> {orig_vid,
            orig_stmt_id, hop, sent_msgs});
            __engine.send(orig_addr, __nd_rcv_corr_done_tid, __1077, me);
            __1070 = unit_t {};
          }
          orig_addr = b1.r1;
          orig_stmt_id = b1.r2;
          orig_vid = b1.r3;
          hop = b1.r4;
          vid = b1.r5;
          compute_vids = b1.r6;
          delta_tuples = b1.r7;
          return __1070;
        }
      }
      unit_t switchInit(const unit_t& _)  {
        return openFile("switch", "/k3/K3/temp/rs.csv", "psv", "r");
      }
      unit_t switchStart(const unit_t& _)  {
        auto __1078 = std::make_shared<K3::ValDispatcher<unit_t>>(unit_t {});
        __engine.send(me, __switchController_tid, __1078, me);
        return unit_t {};
      }
      unit_t switchFinal(const unit_t& _)  {
        return close("switch");
      }
      bool switchHasRead(unit_t _)  {
        return __engine.hasRead("switch");
      }
      R_r1_r2_r3_r4_r5<int, int, int, int, int> switchRead(unit_t _)  {
        shared_ptr<R_r1_r2_r3_r4_r5<int, int, int, int,
        int>> result = __engine.doReadExternal<R_r1_r2_r3_r4_r5<int, int, int, int, int>>("switch");
        if (result) {
          return *result;
        } else {
          throw(std::runtime_error("Invalid doRead for switch"));
        }
      }
      unit_t switchController(const unit_t& _)  {
        if (switchHasRead(unit_t {})) {
          switchProcess(unit_t {});
          auto __1079 = std::make_shared<K3::ValDispatcher<unit_t>>(unit_t {});
          __engine.send(me, __switchController_tid, __1079, me);
          return unit_t {};
        } else {
          return unit_t {};
        }
      }
      unit_t masterProcess(const unit_t& _)  {
        return [this] (const unit_t& next) mutable {
          auto __1080 = std::make_shared<K3::ValDispatcher<unit_t>>(next);
          __engine.send(me, __ms_send_addr_self_tid, __1080, me);
          return unit_t {};
        }(unit_t {});
      }
      unit_t switchProcess(const unit_t& _)  {
        return [this] (const R_r1_r2_r3_r4_r5<int, int, int, int, int>& next) mutable {
          auto __1081 = std::make_shared<K3::ValDispatcher<R_r1_r2_r3_r4_r5<int, int, int, int,
          int>>>(next);
          __engine.send(me, __sw_demux_tid, __1081, me);
          return unit_t {};
        }(switchRead(unit_t {}));
      }
      unit_t processRole(const unit_t& _)  {
        if (role == "master") {
          return masterProcess(unit_t {});
        } else {
          if (role == "switch") {
            switchInit(unit_t {});
            return switchStart(unit_t {});
          } else {
            return unit_t {};
          }
        }
      }
      unit_t initDecls(unit_t _)  {
        if (!__my_peers_set__) {
          auto __1 = _Collection<R_i<Address>> {};
          for (const auto& __0: peers) {
            __1 = [this] (_Collection<R_i<Address>> acc) mutable {
              return [this, acc = std::move(acc)] (const R_addr<Address>& x) mutable {
                acc.insert(R_i<Address> {x.addr});
                return std::move(acc);
              };
            }(std::move(__1))(__0);
          }
          my_peers = std::move(__1);
          __my_peers_set__ = true;
        }
        if (!__nd_sent_done_set__) {
          nd_sent_done = false;
          __nd_sent_done_set__ = true;
        }
        if (!__sw_sent_done_set__) {
          sw_sent_done = false;
          __sw_sent_done_set__ = true;
        }
        if (!__ms_rcv_sw_init_ack_cnt_set__) {
          ms_rcv_sw_init_ack_cnt = 0;
          __ms_rcv_sw_init_ack_cnt_set__ = true;
        }
        if (!__ms_rcv_jobs_ack_cnt_set__) {
          ms_rcv_jobs_ack_cnt = 0;
          __ms_rcv_jobs_ack_cnt_set__ = true;
        }
        if (!__ms_rcv_job_cnt_set__) {
          ms_rcv_job_cnt = 0;
          __ms_rcv_job_cnt_set__ = true;
        }
        if (!__ms_rcv_node_done_cnt_set__) {
          ms_rcv_node_done_cnt = 0;
          __ms_rcv_node_done_cnt_set__ = true;
        }
        if (!__ms_rcv_switch_done_cnt_set__) {
          ms_rcv_switch_done_cnt = 0;
          __ms_rcv_switch_done_cnt_set__ = true;
        }
        if (!__g_init_vid_set__) {
          g_init_vid = R_key_value<int, int> {0, 0};
          __g_init_vid_set__ = true;
        }
        if (!__g_min_vid_set__) {
          g_min_vid = R_key_value<int, int> {0, 0};
          __g_min_vid_set__ = true;
        }
        if (!__g_max_vid_set__) {
          g_max_vid = R_key_value<int, int> {get_max_int(unit_t {}), get_max_int(unit_t {})};
          __g_max_vid_set__ = true;
        }
        if (!__g_start_vid_set__) {
          g_start_vid = R_key_value<int, int> {0, 1};
          __g_start_vid_set__ = true;
        }
        if (!__job_master_set__) {
          job_master = 0;
          __job_master_set__ = true;
        }
        if (!__job_switch_set__) {
          job_switch = 1;
          __job_switch_set__ = true;
        }
        if (!__job_node_set__) {
          job_node = 2;
          __job_node_set__ = true;
        }
        if (!__job_timer_set__) {
          job_timer = 3;
          __job_timer_set__ = true;
        }
        if (!__job_set__) {
          if (role == "master") {
            job = job_master;
          } else {
            if (role == "switch") {
              job = job_switch;
            } else {
              if (role == "node") {
                job = job_node;
              } else {
                if (role == "timer") {
                  job = job_timer;
                } else {
                  job = error<int>(print("failed to find proper role"));
                }
              }
            }
          }
          __job_set__ = true;
        }
        if (!__num_peers_set__) {
          num_peers = my_peers.size(unit_t {});
          __num_peers_set__ = true;
        }
        if (!__map_ids_set__) {
          {
            _Collection<R_r1_r2_r3<int, K3::base_string, int>> __2;
            __2 = _Collection<R_r1_r2_r3<int, K3::base_string, int>> {};
            auto& __collection = __2;
            __collection.insert(R_r1_r2_r3<int, K3::base_string, int> {1, "__SQL_SUM_AGGREGATE_1",
            1});
            __collection.insert(R_r1_r2_r3<int, K3::base_string, int> {2,
            "__SQL_SUM_AGGREGATE_1_mS1", 2});
            __collection.insert(R_r1_r2_r3<int, K3::base_string, int> {3,
            "__SQL_SUM_AGGREGATE_1_mR1", 2});
            __collection.insert(R_r1_r2_r3<int, K3::base_string, int> {4, "__SQL_SUM_AGGREGATE_2",
            1});
            __collection.insert(R_r1_r2_r3<int, K3::base_string, int> {5,
            "__SQL_SUM_AGGREGATE_2_mS3", 2});
            __collection.insert(R_r1_r2_r3<int, K3::base_string, int> {6,
            "__SQL_SUM_AGGREGATE_2_mR1", 2});
            map_ids = __collection;
          }
          __map_ids_set__ = true;
        }
        if (!__trig_ids_set__) {
          {
            _Collection<R_r1_r2_r3<int, K3::base_string, _Seq<R_i<int>>>> __3;
            __3 = _Collection<R_r1_r2_r3<int, K3::base_string, _Seq<R_i<int>>>> {};
            auto& __collection = __3;
            _Seq<R_i<int>> __4;
            {
              _Seq<R_i<int>> __5;
              __5 = _Seq<R_i<int>> {};
              auto& __collection = __5;
              __collection.insert(R_i<int> {1});
              __collection.insert(R_i<int> {2});
              __4 = __collection;
            }
            __collection.insert(R_r1_r2_r3<int, K3::base_string, _Seq<R_i<int>>> {0, "sw_insert_R",
            __4});
            _Seq<R_i<int>> __6;
            {
              _Seq<R_i<int>> __7;
              __7 = _Seq<R_i<int>> {};
              auto& __collection = __7;
              __collection.insert(R_i<int> {3});
              __collection.insert(R_i<int> {4});
              __6 = __collection;
            }
            __collection.insert(R_r1_r2_r3<int, K3::base_string, _Seq<R_i<int>>> {1, "sw_insert_S",
            __6});
            trig_ids = __collection;
          }
          __trig_ids_set__ = true;
        }
        if (!__nd_rcvd_sys_done_set__) {
          nd_rcvd_sys_done = false;
          __nd_rcvd_sys_done_set__ = true;
        }
        if (!__sw_init_set__) {
          sw_init = false;
          __sw_init_set__ = true;
        }
        if (!__sw_seen_sentry_set__) {
          sw_seen_sentry = false;
          __sw_seen_sentry_set__ = true;
        }
        if (!____SQL_SUM_AGGREGATE_1_set__) {
          _Set<R_key_value<R_key_value<int, int>, int>> __8;
          {
            _Set<R_key_value<R_key_value<int, int>, int>> __9;
            __9 = _Set<R_key_value<R_key_value<int, int>, int>> {};
            auto& __collection = __9;
            __collection.insert(R_key_value<R_key_value<int, int>, int> {g_init_vid, 0});
            __8 = __collection;
          }
          __SQL_SUM_AGGREGATE_1 = make_shared<_Set<R_key_value<R_key_value<int, int>, int>>>(__8);
          ____SQL_SUM_AGGREGATE_1_set__ = true;
        }
        if (!____SQL_SUM_AGGREGATE_1_mS1_set__) {
          __SQL_SUM_AGGREGATE_1_mS1 = make_shared<_Set<R_r1_r2_r3<R_key_value<int, int>, int,
          int>>>(_Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> {});
          ____SQL_SUM_AGGREGATE_1_mS1_set__ = true;
        }
        if (!____SQL_SUM_AGGREGATE_1_mR1_set__) {
          __SQL_SUM_AGGREGATE_1_mR1 = make_shared<_Set<R_r1_r2_r3<R_key_value<int, int>, int,
          int>>>(_Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> {});
          ____SQL_SUM_AGGREGATE_1_mR1_set__ = true;
        }
        if (!____SQL_SUM_AGGREGATE_2_set__) {
          _Set<R_key_value<R_key_value<int, int>, int>> __10;
          {
            _Set<R_key_value<R_key_value<int, int>, int>> __11;
            __11 = _Set<R_key_value<R_key_value<int, int>, int>> {};
            auto& __collection = __11;
            __collection.insert(R_key_value<R_key_value<int, int>, int> {g_init_vid, 0});
            __10 = __collection;
          }
          __SQL_SUM_AGGREGATE_2 = make_shared<_Set<R_key_value<R_key_value<int, int>, int>>>(__10);
          ____SQL_SUM_AGGREGATE_2_set__ = true;
        }
        if (!____SQL_SUM_AGGREGATE_2_mS3_set__) {
          __SQL_SUM_AGGREGATE_2_mS3 = make_shared<_Set<R_r1_r2_r3<R_key_value<int, int>, int,
          int>>>(_Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> {});
          ____SQL_SUM_AGGREGATE_2_mS3_set__ = true;
        }
        if (!____SQL_SUM_AGGREGATE_2_mR1_set__) {
          __SQL_SUM_AGGREGATE_2_mR1 = make_shared<_Set<R_r1_r2_r3<R_key_value<int, int>, int,
          int>>>(_Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> {});
          ____SQL_SUM_AGGREGATE_2_mR1_set__ = true;
        }
        if (!__map___SQL_SUM_AGGREGATE_1_mS1_s0_buf_set__) {
          map___SQL_SUM_AGGREGATE_1_mS1_s0_buf = make_shared<_Set<R_r1_r2_r3<R_key_value<int, int>,
          int, int>>>(_Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> {});
          __map___SQL_SUM_AGGREGATE_1_mS1_s0_buf_set__ = true;
        }
        if (!__map___SQL_SUM_AGGREGATE_1_mS1_s2_buf_set__) {
          map___SQL_SUM_AGGREGATE_1_mS1_s2_buf = make_shared<_Set<R_r1_r2_r3<R_key_value<int, int>,
          int, int>>>(_Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> {});
          __map___SQL_SUM_AGGREGATE_1_mS1_s2_buf_set__ = true;
        }
        if (!__map___SQL_SUM_AGGREGATE_2_mS3_s2_buf_set__) {
          map___SQL_SUM_AGGREGATE_2_mS3_s2_buf = make_shared<_Set<R_r1_r2_r3<R_key_value<int, int>,
          int, int>>>(_Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> {});
          __map___SQL_SUM_AGGREGATE_2_mS3_s2_buf_set__ = true;
        }
        if (!__map___SQL_SUM_AGGREGATE_1_mS1_s4_buf_set__) {
          map___SQL_SUM_AGGREGATE_1_mS1_s4_buf = make_shared<_Set<R_r1_r2_r3<R_key_value<int, int>,
          int, int>>>(_Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> {});
          __map___SQL_SUM_AGGREGATE_1_mS1_s4_buf_set__ = true;
        }
        if (!__map___SQL_SUM_AGGREGATE_1_mS1_s6_buf_set__) {
          map___SQL_SUM_AGGREGATE_1_mS1_s6_buf = make_shared<_Set<R_r1_r2_r3<R_key_value<int, int>,
          int, int>>>(_Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> {});
          __map___SQL_SUM_AGGREGATE_1_mS1_s6_buf_set__ = true;
        }
        if (!__map___SQL_SUM_AGGREGATE_2_mS3_s6_buf_set__) {
          map___SQL_SUM_AGGREGATE_2_mS3_s6_buf = make_shared<_Set<R_r1_r2_r3<R_key_value<int, int>,
          int, int>>>(_Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> {});
          __map___SQL_SUM_AGGREGATE_2_mS3_s6_buf_set__ = true;
        }
        if (!__map___SQL_SUM_AGGREGATE_1_mR1_s8_buf_set__) {
          map___SQL_SUM_AGGREGATE_1_mR1_s8_buf = make_shared<_Set<R_r1_r2_r3<R_key_value<int, int>,
          int, int>>>(_Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> {});
          __map___SQL_SUM_AGGREGATE_1_mR1_s8_buf_set__ = true;
        }
        if (!__map___SQL_SUM_AGGREGATE_2_mR1_s10_buf_set__) {
          map___SQL_SUM_AGGREGATE_2_mR1_s10_buf = make_shared<_Set<R_r1_r2_r3<R_key_value<int, int>,
          int, int>>>(_Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> {});
          __map___SQL_SUM_AGGREGATE_2_mR1_s10_buf_set__ = true;
        }
        if (!__map___SQL_SUM_AGGREGATE_1_mR1_s10_buf_set__) {
          map___SQL_SUM_AGGREGATE_1_mR1_s10_buf = make_shared<_Set<R_r1_r2_r3<R_key_value<int, int>,
          int, int>>>(_Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> {});
          __map___SQL_SUM_AGGREGATE_1_mR1_s10_buf_set__ = true;
        }
        if (!__map___SQL_SUM_AGGREGATE_1_mR1_s12_buf_set__) {
          map___SQL_SUM_AGGREGATE_1_mR1_s12_buf = make_shared<_Set<R_r1_r2_r3<R_key_value<int, int>,
          int, int>>>(_Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> {});
          __map___SQL_SUM_AGGREGATE_1_mR1_s12_buf_set__ = true;
        }
        if (!__map___SQL_SUM_AGGREGATE_2_mR1_s14_buf_set__) {
          map___SQL_SUM_AGGREGATE_2_mR1_s14_buf = make_shared<_Set<R_r1_r2_r3<R_key_value<int, int>,
          int, int>>>(_Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> {});
          __map___SQL_SUM_AGGREGATE_2_mR1_s14_buf_set__ = true;
        }
        if (!__map___SQL_SUM_AGGREGATE_1_mR1_s14_buf_set__) {
          map___SQL_SUM_AGGREGATE_1_mR1_s14_buf = make_shared<_Set<R_r1_r2_r3<R_key_value<int, int>,
          int, int>>>(_Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> {});
          __map___SQL_SUM_AGGREGATE_1_mR1_s14_buf_set__ = true;
        }
        if (!__sw_need_vid_cntr_set__) {
          sw_need_vid_cntr = 0;
          __sw_need_vid_cntr_set__ = true;
        }
        if (!__replicas_set__) {
          replicas = 8;
          __replicas_set__ = true;
        }
        if (!__pmap_input_set__) {
          {
            _Seq<R_key_value<K3::base_string, _Seq<R_key_value<int, int>>>> __12;
            __12 = _Seq<R_key_value<K3::base_string, _Seq<R_key_value<int, int>>>> {};
            auto& __collection = __12;
            _Seq<R_key_value<int, int>> __13;
            {
              _Seq<R_key_value<int, int>> __14;
              __14 = _Seq<R_key_value<int, int>> {};
              auto& __collection = __14;
              __collection.insert(R_key_value<int, int> {0, 16});
              __13 = __collection;
            }
            __collection.insert(R_key_value<K3::base_string, _Seq<R_key_value<int,
            int>>> {"__SQL_SUM_AGGREGATE_1_mS1", __13});
            _Seq<R_key_value<int, int>> __15;
            {
              _Seq<R_key_value<int, int>> __16;
              __16 = _Seq<R_key_value<int, int>> {};
              auto& __collection = __16;
              __collection.insert(R_key_value<int, int> {0, 16});
              __15 = __collection;
            }
            __collection.insert(R_key_value<K3::base_string, _Seq<R_key_value<int,
            int>>> {"__SQL_SUM_AGGREGATE_1_mR1", __15});
            _Seq<R_key_value<int, int>> __17;
            {
              _Seq<R_key_value<int, int>> __18;
              __18 = _Seq<R_key_value<int, int>> {};
              auto& __collection = __18;
              __collection.insert(R_key_value<int, int> {0, 16});
              __17 = __collection;
            }
            __collection.insert(R_key_value<K3::base_string, _Seq<R_key_value<int,
            int>>> {"__SQL_SUM_AGGREGATE_2_mS3", __17});
            _Seq<R_key_value<int, int>> __19;
            {
              _Seq<R_key_value<int, int>> __20;
              __20 = _Seq<R_key_value<int, int>> {};
              auto& __collection = __20;
              __collection.insert(R_key_value<int, int> {0, 16});
              __19 = __collection;
            }
            __collection.insert(R_key_value<K3::base_string, _Seq<R_key_value<int,
            int>>> {"__SQL_SUM_AGGREGATE_2_mR1", __19});
            pmap_input = __collection;
          }
          __pmap_input_set__ = true;
        }
        if (!__pmap_data_set__) {
          auto __24 = _Seq<R_key_value<int, _Seq<R_key_value<int, int>>>> {};
          for (const auto& __23: pmap_input) {
            __24 = [this] (_Seq<R_key_value<int, _Seq<R_key_value<int, int>>>> _accmap) mutable {
              return [this, _accmap = std::move(_accmap)] (const R_key_value<K3::base_string,
              _Seq<R_key_value<int, int>>>& b3) mutable {
                {
                  auto& map_name = b3.key;
                  auto& map_types = b3.value;
                  R_r1_r2_r3<int, K3::base_string, int> __21;
                  shared_ptr<R_r1_r2_r3<int, K3::base_string, int>> __22;
                  __22 = map_ids.filter([this, &map_name] (const R_r1_r2_r3<int, K3::base_string,
                  int>& b1) mutable {
                    {
                      auto& r1 = b1.r1;
                      auto& r2 = b1.r2;
                      auto& r3 = b1.r3;
                      return r2 == map_name;
                    }
                  }).peek(unit_t {});
                  if (__22) {
                    R_r1_r2_r3<int, K3::base_string, int>& x = *__22;
                    __21 = x;
                  } else {
                    __21 = error<R_r1_r2_r3<int, K3::base_string,
                    int>>(print("can't find map in map_ids"));
                  }
                  _accmap.insert(R_key_value<int, _Seq<R_key_value<int, int>>> {__21.r1,
                  map_types});
                  return std::move(_accmap);
                }
              };
            }(std::move(__24))(__23);
          }
          pmap_data = std::move(__24);
          __pmap_data_set__ = true;
        }
        if (!__ms_gc_interval_set__) {
          ms_gc_interval = 20000;
          __ms_gc_interval_set__ = true;
        }
        if (!__ms_gc_vid_ctr_set__) {
          ms_gc_vid_ctr = 0;
          __ms_gc_vid_ctr_set__ = true;
        }
        if (!__ms_num_gc_expected_set__) {
          ms_num_gc_expected = my_peers.size(unit_t {});
          __ms_num_gc_expected_set__ = true;
        }
        return unit_t {};
      }
      void __patch(std::string);
      std::map<std::string, std::string> __prettify()  {
        std::map<std::string, std::string> __result;
        __result["ms_num_gc_expected"] = K3::prettify_int(ms_num_gc_expected);
        __result["ms_gc_vid_ctr"] = K3::prettify_int(ms_gc_vid_ctr);
        __result["ms_gc_vid_map"] = K3::prettify_collection(ms_gc_vid_map, [] (R_key_value<Address,
        R_key_value<int, int>> x)  {
          return K3::prettify_record(x, [] (R_key_value<Address, R_key_value<int, int>> x)  {
            ostringstream oss;
            oss << "{";
            oss << std::string("key:") << K3::prettify_address(x.key);
            oss << ",";
            oss << std::string("value:") << K3::prettify_record(x.value, [] (R_key_value<int,
            int> x)  {
              ostringstream oss;
              oss << "{";
              oss << std::string("key:") << K3::prettify_int(x.key);
              oss << ",";
              oss << std::string("value:") << K3::prettify_int(x.value);
              oss << "}";
              return string_impl(oss.str());
            });
            oss << "}";
            return string_impl(oss.str());
          });
        });
        __result["ms_gc_interval"] = K3::prettify_int(ms_gc_interval);
        __result["sw_ack_log"] = K3::prettify_collection(sw_ack_log,
        [] (R_key_value<R_key_value<int, int>, int> x)  {
          return K3::prettify_record(x, [] (R_key_value<R_key_value<int, int>, int> x)  {
            ostringstream oss;
            oss << "{";
            oss << std::string("key:") << K3::prettify_record(x.key, [] (R_key_value<int, int> x)  {
              ostringstream oss;
              oss << "{";
              oss << std::string("key:") << K3::prettify_int(x.key);
              oss << ",";
              oss << std::string("value:") << K3::prettify_int(x.value);
              oss << "}";
              return string_impl(oss.str());
            });
            oss << ",";
            oss << std::string("value:") << K3::prettify_int(x.value);
            oss << "}";
            return string_impl(oss.str());
          });
        });
        __result["sw_num_sent"] = K3::prettify_int(sw_num_sent);
        __result["sw_num_ack"] = K3::prettify_int(sw_num_ack);
        __result["pmap_data"] = K3::prettify_collection(pmap_data, [] (R_key_value<int,
        _Seq<R_key_value<int, int>>> x)  {
          return K3::prettify_record(x, [] (R_key_value<int, _Seq<R_key_value<int, int>>> x)  {
            ostringstream oss;
            oss << "{";
            oss << std::string("key:") << K3::prettify_int(x.key);
            oss << ",";
            oss << std::string("value:") << K3::prettify_collection(x.value, [] (R_key_value<int,
            int> x)  {
              return K3::prettify_record(x, [] (R_key_value<int, int> x)  {
                ostringstream oss;
                oss << "{";
                oss << std::string("key:") << K3::prettify_int(x.key);
                oss << ",";
                oss << std::string("value:") << K3::prettify_int(x.value);
                oss << "}";
                return string_impl(oss.str());
              });
            });
            oss << "}";
            return string_impl(oss.str());
          });
        });
        __result["pmap_input"] = K3::prettify_collection(pmap_input,
        [] (R_key_value<K3::base_string, _Seq<R_key_value<int, int>>> x)  {
          return K3::prettify_record(x, [] (R_key_value<K3::base_string, _Seq<R_key_value<int,
          int>>> x)  {
            ostringstream oss;
            oss << "{";
            oss << std::string("key:") << K3::prettify_string(x.key);
            oss << ",";
            oss << std::string("value:") << K3::prettify_collection(x.value, [] (R_key_value<int,
            int> x)  {
              return K3::prettify_record(x, [] (R_key_value<int, int> x)  {
                ostringstream oss;
                oss << "{";
                oss << std::string("key:") << K3::prettify_int(x.key);
                oss << ",";
                oss << std::string("value:") << K3::prettify_int(x.value);
                oss << "}";
                return string_impl(oss.str());
              });
            });
            oss << "}";
            return string_impl(oss.str());
          });
        });
        __result["replicas"] = K3::prettify_int(replicas);
        __result["node_ring"] = K3::prettify_collection(node_ring, [] (R_key_value<Address,
        int> x)  {
          return K3::prettify_record(x, [] (R_key_value<Address, int> x)  {
            ostringstream oss;
            oss << "{";
            oss << std::string("key:") << K3::prettify_address(x.key);
            oss << ",";
            oss << std::string("value:") << K3::prettify_int(x.value);
            oss << "}";
            return string_impl(oss.str());
          });
        });
        __result["sw_highest_vid"] = K3::prettify_record(sw_highest_vid, [] (R_key_value<int,
        int> x)  {
          ostringstream oss;
          oss << "{";
          oss << std::string("key:") << K3::prettify_int(x.key);
          oss << ",";
          oss << std::string("value:") << K3::prettify_int(x.value);
          oss << "}";
          return string_impl(oss.str());
        });
        __result["sw_token_vid_list"] = K3::prettify_collection(sw_token_vid_list,
        [] (R_key_value<R_key_value<int, int>, int> x)  {
          return K3::prettify_record(x, [] (R_key_value<R_key_value<int, int>, int> x)  {
            ostringstream oss;
            oss << "{";
            oss << std::string("key:") << K3::prettify_record(x.key, [] (R_key_value<int, int> x)  {
              ostringstream oss;
              oss << "{";
              oss << std::string("key:") << K3::prettify_int(x.key);
              oss << ",";
              oss << std::string("value:") << K3::prettify_int(x.value);
              oss << "}";
              return string_impl(oss.str());
            });
            oss << ",";
            oss << std::string("value:") << K3::prettify_int(x.value);
            oss << "}";
            return string_impl(oss.str());
          });
        });
        __result["sw_need_vid_cntr"] = K3::prettify_int(sw_need_vid_cntr);
        __result["sw_next_switch_addr"] = K3::prettify_address(sw_next_switch_addr);
        __result["tm_timer_list"] = K3::prettify_collection(tm_timer_list, [] (R_r1_r2_r3<int, int,
        Address> x)  {
          return K3::prettify_record(x, [] (R_r1_r2_r3<int, int, Address> x)  {
            ostringstream oss;
            oss << "{";
            oss << std::string("r1:") << K3::prettify_int(x.r1);
            oss << ",";
            oss << std::string("r2:") << K3::prettify_int(x.r2);
            oss << ",";
            oss << std::string("r3:") << K3::prettify_address(x.r3);
            oss << "}";
            return string_impl(oss.str());
          });
        });
        __result["map___SQL_SUM_AGGREGATE_1_mR1_s14_buf"] = K3::prettify_indirection(map___SQL_SUM_AGGREGATE_1_mR1_s14_buf,
        [] (_Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> x)  {
          return K3::prettify_collection(x, [] (R_r1_r2_r3<R_key_value<int, int>, int, int> x)  {
            return K3::prettify_record(x, [] (R_r1_r2_r3<R_key_value<int, int>, int, int> x)  {
              ostringstream oss;
              oss << "{";
              oss << std::string("r1:") << K3::prettify_record(x.r1, [] (R_key_value<int, int> x)  {
                ostringstream oss;
                oss << "{";
                oss << std::string("key:") << K3::prettify_int(x.key);
                oss << ",";
                oss << std::string("value:") << K3::prettify_int(x.value);
                oss << "}";
                return string_impl(oss.str());
              });
              oss << ",";
              oss << std::string("r2:") << K3::prettify_int(x.r2);
              oss << ",";
              oss << std::string("r3:") << K3::prettify_int(x.r3);
              oss << "}";
              return string_impl(oss.str());
            });
          });
        });
        __result["map___SQL_SUM_AGGREGATE_2_mR1_s14_buf"] = K3::prettify_indirection(map___SQL_SUM_AGGREGATE_2_mR1_s14_buf,
        [] (_Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> x)  {
          return K3::prettify_collection(x, [] (R_r1_r2_r3<R_key_value<int, int>, int, int> x)  {
            return K3::prettify_record(x, [] (R_r1_r2_r3<R_key_value<int, int>, int, int> x)  {
              ostringstream oss;
              oss << "{";
              oss << std::string("r1:") << K3::prettify_record(x.r1, [] (R_key_value<int, int> x)  {
                ostringstream oss;
                oss << "{";
                oss << std::string("key:") << K3::prettify_int(x.key);
                oss << ",";
                oss << std::string("value:") << K3::prettify_int(x.value);
                oss << "}";
                return string_impl(oss.str());
              });
              oss << ",";
              oss << std::string("r2:") << K3::prettify_int(x.r2);
              oss << ",";
              oss << std::string("r3:") << K3::prettify_int(x.r3);
              oss << "}";
              return string_impl(oss.str());
            });
          });
        });
        __result["map___SQL_SUM_AGGREGATE_1_mR1_s12_buf"] = K3::prettify_indirection(map___SQL_SUM_AGGREGATE_1_mR1_s12_buf,
        [] (_Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> x)  {
          return K3::prettify_collection(x, [] (R_r1_r2_r3<R_key_value<int, int>, int, int> x)  {
            return K3::prettify_record(x, [] (R_r1_r2_r3<R_key_value<int, int>, int, int> x)  {
              ostringstream oss;
              oss << "{";
              oss << std::string("r1:") << K3::prettify_record(x.r1, [] (R_key_value<int, int> x)  {
                ostringstream oss;
                oss << "{";
                oss << std::string("key:") << K3::prettify_int(x.key);
                oss << ",";
                oss << std::string("value:") << K3::prettify_int(x.value);
                oss << "}";
                return string_impl(oss.str());
              });
              oss << ",";
              oss << std::string("r2:") << K3::prettify_int(x.r2);
              oss << ",";
              oss << std::string("r3:") << K3::prettify_int(x.r3);
              oss << "}";
              return string_impl(oss.str());
            });
          });
        });
        __result["map___SQL_SUM_AGGREGATE_1_mR1_s10_buf"] = K3::prettify_indirection(map___SQL_SUM_AGGREGATE_1_mR1_s10_buf,
        [] (_Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> x)  {
          return K3::prettify_collection(x, [] (R_r1_r2_r3<R_key_value<int, int>, int, int> x)  {
            return K3::prettify_record(x, [] (R_r1_r2_r3<R_key_value<int, int>, int, int> x)  {
              ostringstream oss;
              oss << "{";
              oss << std::string("r1:") << K3::prettify_record(x.r1, [] (R_key_value<int, int> x)  {
                ostringstream oss;
                oss << "{";
                oss << std::string("key:") << K3::prettify_int(x.key);
                oss << ",";
                oss << std::string("value:") << K3::prettify_int(x.value);
                oss << "}";
                return string_impl(oss.str());
              });
              oss << ",";
              oss << std::string("r2:") << K3::prettify_int(x.r2);
              oss << ",";
              oss << std::string("r3:") << K3::prettify_int(x.r3);
              oss << "}";
              return string_impl(oss.str());
            });
          });
        });
        __result["map___SQL_SUM_AGGREGATE_2_mR1_s10_buf"] = K3::prettify_indirection(map___SQL_SUM_AGGREGATE_2_mR1_s10_buf,
        [] (_Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> x)  {
          return K3::prettify_collection(x, [] (R_r1_r2_r3<R_key_value<int, int>, int, int> x)  {
            return K3::prettify_record(x, [] (R_r1_r2_r3<R_key_value<int, int>, int, int> x)  {
              ostringstream oss;
              oss << "{";
              oss << std::string("r1:") << K3::prettify_record(x.r1, [] (R_key_value<int, int> x)  {
                ostringstream oss;
                oss << "{";
                oss << std::string("key:") << K3::prettify_int(x.key);
                oss << ",";
                oss << std::string("value:") << K3::prettify_int(x.value);
                oss << "}";
                return string_impl(oss.str());
              });
              oss << ",";
              oss << std::string("r2:") << K3::prettify_int(x.r2);
              oss << ",";
              oss << std::string("r3:") << K3::prettify_int(x.r3);
              oss << "}";
              return string_impl(oss.str());
            });
          });
        });
        __result["map___SQL_SUM_AGGREGATE_1_mR1_s8_buf"] = K3::prettify_indirection(map___SQL_SUM_AGGREGATE_1_mR1_s8_buf,
        [] (_Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> x)  {
          return K3::prettify_collection(x, [] (R_r1_r2_r3<R_key_value<int, int>, int, int> x)  {
            return K3::prettify_record(x, [] (R_r1_r2_r3<R_key_value<int, int>, int, int> x)  {
              ostringstream oss;
              oss << "{";
              oss << std::string("r1:") << K3::prettify_record(x.r1, [] (R_key_value<int, int> x)  {
                ostringstream oss;
                oss << "{";
                oss << std::string("key:") << K3::prettify_int(x.key);
                oss << ",";
                oss << std::string("value:") << K3::prettify_int(x.value);
                oss << "}";
                return string_impl(oss.str());
              });
              oss << ",";
              oss << std::string("r2:") << K3::prettify_int(x.r2);
              oss << ",";
              oss << std::string("r3:") << K3::prettify_int(x.r3);
              oss << "}";
              return string_impl(oss.str());
            });
          });
        });
        __result["map___SQL_SUM_AGGREGATE_2_mS3_s6_buf"] = K3::prettify_indirection(map___SQL_SUM_AGGREGATE_2_mS3_s6_buf,
        [] (_Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> x)  {
          return K3::prettify_collection(x, [] (R_r1_r2_r3<R_key_value<int, int>, int, int> x)  {
            return K3::prettify_record(x, [] (R_r1_r2_r3<R_key_value<int, int>, int, int> x)  {
              ostringstream oss;
              oss << "{";
              oss << std::string("r1:") << K3::prettify_record(x.r1, [] (R_key_value<int, int> x)  {
                ostringstream oss;
                oss << "{";
                oss << std::string("key:") << K3::prettify_int(x.key);
                oss << ",";
                oss << std::string("value:") << K3::prettify_int(x.value);
                oss << "}";
                return string_impl(oss.str());
              });
              oss << ",";
              oss << std::string("r2:") << K3::prettify_int(x.r2);
              oss << ",";
              oss << std::string("r3:") << K3::prettify_int(x.r3);
              oss << "}";
              return string_impl(oss.str());
            });
          });
        });
        __result["map___SQL_SUM_AGGREGATE_1_mS1_s6_buf"] = K3::prettify_indirection(map___SQL_SUM_AGGREGATE_1_mS1_s6_buf,
        [] (_Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> x)  {
          return K3::prettify_collection(x, [] (R_r1_r2_r3<R_key_value<int, int>, int, int> x)  {
            return K3::prettify_record(x, [] (R_r1_r2_r3<R_key_value<int, int>, int, int> x)  {
              ostringstream oss;
              oss << "{";
              oss << std::string("r1:") << K3::prettify_record(x.r1, [] (R_key_value<int, int> x)  {
                ostringstream oss;
                oss << "{";
                oss << std::string("key:") << K3::prettify_int(x.key);
                oss << ",";
                oss << std::string("value:") << K3::prettify_int(x.value);
                oss << "}";
                return string_impl(oss.str());
              });
              oss << ",";
              oss << std::string("r2:") << K3::prettify_int(x.r2);
              oss << ",";
              oss << std::string("r3:") << K3::prettify_int(x.r3);
              oss << "}";
              return string_impl(oss.str());
            });
          });
        });
        __result["map___SQL_SUM_AGGREGATE_1_mS1_s4_buf"] = K3::prettify_indirection(map___SQL_SUM_AGGREGATE_1_mS1_s4_buf,
        [] (_Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> x)  {
          return K3::prettify_collection(x, [] (R_r1_r2_r3<R_key_value<int, int>, int, int> x)  {
            return K3::prettify_record(x, [] (R_r1_r2_r3<R_key_value<int, int>, int, int> x)  {
              ostringstream oss;
              oss << "{";
              oss << std::string("r1:") << K3::prettify_record(x.r1, [] (R_key_value<int, int> x)  {
                ostringstream oss;
                oss << "{";
                oss << std::string("key:") << K3::prettify_int(x.key);
                oss << ",";
                oss << std::string("value:") << K3::prettify_int(x.value);
                oss << "}";
                return string_impl(oss.str());
              });
              oss << ",";
              oss << std::string("r2:") << K3::prettify_int(x.r2);
              oss << ",";
              oss << std::string("r3:") << K3::prettify_int(x.r3);
              oss << "}";
              return string_impl(oss.str());
            });
          });
        });
        __result["map___SQL_SUM_AGGREGATE_2_mS3_s2_buf"] = K3::prettify_indirection(map___SQL_SUM_AGGREGATE_2_mS3_s2_buf,
        [] (_Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> x)  {
          return K3::prettify_collection(x, [] (R_r1_r2_r3<R_key_value<int, int>, int, int> x)  {
            return K3::prettify_record(x, [] (R_r1_r2_r3<R_key_value<int, int>, int, int> x)  {
              ostringstream oss;
              oss << "{";
              oss << std::string("r1:") << K3::prettify_record(x.r1, [] (R_key_value<int, int> x)  {
                ostringstream oss;
                oss << "{";
                oss << std::string("key:") << K3::prettify_int(x.key);
                oss << ",";
                oss << std::string("value:") << K3::prettify_int(x.value);
                oss << "}";
                return string_impl(oss.str());
              });
              oss << ",";
              oss << std::string("r2:") << K3::prettify_int(x.r2);
              oss << ",";
              oss << std::string("r3:") << K3::prettify_int(x.r3);
              oss << "}";
              return string_impl(oss.str());
            });
          });
        });
        __result["map___SQL_SUM_AGGREGATE_1_mS1_s2_buf"] = K3::prettify_indirection(map___SQL_SUM_AGGREGATE_1_mS1_s2_buf,
        [] (_Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> x)  {
          return K3::prettify_collection(x, [] (R_r1_r2_r3<R_key_value<int, int>, int, int> x)  {
            return K3::prettify_record(x, [] (R_r1_r2_r3<R_key_value<int, int>, int, int> x)  {
              ostringstream oss;
              oss << "{";
              oss << std::string("r1:") << K3::prettify_record(x.r1, [] (R_key_value<int, int> x)  {
                ostringstream oss;
                oss << "{";
                oss << std::string("key:") << K3::prettify_int(x.key);
                oss << ",";
                oss << std::string("value:") << K3::prettify_int(x.value);
                oss << "}";
                return string_impl(oss.str());
              });
              oss << ",";
              oss << std::string("r2:") << K3::prettify_int(x.r2);
              oss << ",";
              oss << std::string("r3:") << K3::prettify_int(x.r3);
              oss << "}";
              return string_impl(oss.str());
            });
          });
        });
        __result["map___SQL_SUM_AGGREGATE_1_mS1_s0_buf"] = K3::prettify_indirection(map___SQL_SUM_AGGREGATE_1_mS1_s0_buf,
        [] (_Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> x)  {
          return K3::prettify_collection(x, [] (R_r1_r2_r3<R_key_value<int, int>, int, int> x)  {
            return K3::prettify_record(x, [] (R_r1_r2_r3<R_key_value<int, int>, int, int> x)  {
              ostringstream oss;
              oss << "{";
              oss << std::string("r1:") << K3::prettify_record(x.r1, [] (R_key_value<int, int> x)  {
                ostringstream oss;
                oss << "{";
                oss << std::string("key:") << K3::prettify_int(x.key);
                oss << ",";
                oss << std::string("value:") << K3::prettify_int(x.value);
                oss << "}";
                return string_impl(oss.str());
              });
              oss << ",";
              oss << std::string("r2:") << K3::prettify_int(x.r2);
              oss << ",";
              oss << std::string("r3:") << K3::prettify_int(x.r3);
              oss << "}";
              return string_impl(oss.str());
            });
          });
        });
        __result["__SQL_SUM_AGGREGATE_2_mR1"] = K3::prettify_indirection(__SQL_SUM_AGGREGATE_2_mR1,
        [] (_Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> x)  {
          return K3::prettify_collection(x, [] (R_r1_r2_r3<R_key_value<int, int>, int, int> x)  {
            return K3::prettify_record(x, [] (R_r1_r2_r3<R_key_value<int, int>, int, int> x)  {
              ostringstream oss;
              oss << "{";
              oss << std::string("r1:") << K3::prettify_record(x.r1, [] (R_key_value<int, int> x)  {
                ostringstream oss;
                oss << "{";
                oss << std::string("key:") << K3::prettify_int(x.key);
                oss << ",";
                oss << std::string("value:") << K3::prettify_int(x.value);
                oss << "}";
                return string_impl(oss.str());
              });
              oss << ",";
              oss << std::string("r2:") << K3::prettify_int(x.r2);
              oss << ",";
              oss << std::string("r3:") << K3::prettify_int(x.r3);
              oss << "}";
              return string_impl(oss.str());
            });
          });
        });
        __result["__SQL_SUM_AGGREGATE_2_mS3"] = K3::prettify_indirection(__SQL_SUM_AGGREGATE_2_mS3,
        [] (_Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> x)  {
          return K3::prettify_collection(x, [] (R_r1_r2_r3<R_key_value<int, int>, int, int> x)  {
            return K3::prettify_record(x, [] (R_r1_r2_r3<R_key_value<int, int>, int, int> x)  {
              ostringstream oss;
              oss << "{";
              oss << std::string("r1:") << K3::prettify_record(x.r1, [] (R_key_value<int, int> x)  {
                ostringstream oss;
                oss << "{";
                oss << std::string("key:") << K3::prettify_int(x.key);
                oss << ",";
                oss << std::string("value:") << K3::prettify_int(x.value);
                oss << "}";
                return string_impl(oss.str());
              });
              oss << ",";
              oss << std::string("r2:") << K3::prettify_int(x.r2);
              oss << ",";
              oss << std::string("r3:") << K3::prettify_int(x.r3);
              oss << "}";
              return string_impl(oss.str());
            });
          });
        });
        __result["__SQL_SUM_AGGREGATE_2"] = K3::prettify_indirection(__SQL_SUM_AGGREGATE_2,
        [] (_Set<R_key_value<R_key_value<int, int>, int>> x)  {
          return K3::prettify_collection(x, [] (R_key_value<R_key_value<int, int>, int> x)  {
            return K3::prettify_record(x, [] (R_key_value<R_key_value<int, int>, int> x)  {
              ostringstream oss;
              oss << "{";
              oss << std::string("key:") << K3::prettify_record(x.key, [] (R_key_value<int,
              int> x)  {
                ostringstream oss;
                oss << "{";
                oss << std::string("key:") << K3::prettify_int(x.key);
                oss << ",";
                oss << std::string("value:") << K3::prettify_int(x.value);
                oss << "}";
                return string_impl(oss.str());
              });
              oss << ",";
              oss << std::string("value:") << K3::prettify_int(x.value);
              oss << "}";
              return string_impl(oss.str());
            });
          });
        });
        __result["__SQL_SUM_AGGREGATE_1_mR1"] = K3::prettify_indirection(__SQL_SUM_AGGREGATE_1_mR1,
        [] (_Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> x)  {
          return K3::prettify_collection(x, [] (R_r1_r2_r3<R_key_value<int, int>, int, int> x)  {
            return K3::prettify_record(x, [] (R_r1_r2_r3<R_key_value<int, int>, int, int> x)  {
              ostringstream oss;
              oss << "{";
              oss << std::string("r1:") << K3::prettify_record(x.r1, [] (R_key_value<int, int> x)  {
                ostringstream oss;
                oss << "{";
                oss << std::string("key:") << K3::prettify_int(x.key);
                oss << ",";
                oss << std::string("value:") << K3::prettify_int(x.value);
                oss << "}";
                return string_impl(oss.str());
              });
              oss << ",";
              oss << std::string("r2:") << K3::prettify_int(x.r2);
              oss << ",";
              oss << std::string("r3:") << K3::prettify_int(x.r3);
              oss << "}";
              return string_impl(oss.str());
            });
          });
        });
        __result["__SQL_SUM_AGGREGATE_1_mS1"] = K3::prettify_indirection(__SQL_SUM_AGGREGATE_1_mS1,
        [] (_Set<R_r1_r2_r3<R_key_value<int, int>, int, int>> x)  {
          return K3::prettify_collection(x, [] (R_r1_r2_r3<R_key_value<int, int>, int, int> x)  {
            return K3::prettify_record(x, [] (R_r1_r2_r3<R_key_value<int, int>, int, int> x)  {
              ostringstream oss;
              oss << "{";
              oss << std::string("r1:") << K3::prettify_record(x.r1, [] (R_key_value<int, int> x)  {
                ostringstream oss;
                oss << "{";
                oss << std::string("key:") << K3::prettify_int(x.key);
                oss << ",";
                oss << std::string("value:") << K3::prettify_int(x.value);
                oss << "}";
                return string_impl(oss.str());
              });
              oss << ",";
              oss << std::string("r2:") << K3::prettify_int(x.r2);
              oss << ",";
              oss << std::string("r3:") << K3::prettify_int(x.r3);
              oss << "}";
              return string_impl(oss.str());
            });
          });
        });
        __result["__SQL_SUM_AGGREGATE_1"] = K3::prettify_indirection(__SQL_SUM_AGGREGATE_1,
        [] (_Set<R_key_value<R_key_value<int, int>, int>> x)  {
          return K3::prettify_collection(x, [] (R_key_value<R_key_value<int, int>, int> x)  {
            return K3::prettify_record(x, [] (R_key_value<R_key_value<int, int>, int> x)  {
              ostringstream oss;
              oss << "{";
              oss << std::string("key:") << K3::prettify_record(x.key, [] (R_key_value<int,
              int> x)  {
                ostringstream oss;
                oss << "{";
                oss << std::string("key:") << K3::prettify_int(x.key);
                oss << ",";
                oss << std::string("value:") << K3::prettify_int(x.value);
                oss << "}";
                return string_impl(oss.str());
              });
              oss << ",";
              oss << std::string("value:") << K3::prettify_int(x.value);
              oss << "}";
              return string_impl(oss.str());
            });
          });
        });
        __result["nd_log_delete_R"] = K3::prettify_collection(nd_log_delete_R,
        [] (R_key_value<R_key_value<int, int>, R_key_value<int, int>> x)  {
          return K3::prettify_record(x, [] (R_key_value<R_key_value<int, int>, R_key_value<int,
          int>> x)  {
            ostringstream oss;
            oss << "{";
            oss << std::string("key:") << K3::prettify_record(x.key, [] (R_key_value<int, int> x)  {
              ostringstream oss;
              oss << "{";
              oss << std::string("key:") << K3::prettify_int(x.key);
              oss << ",";
              oss << std::string("value:") << K3::prettify_int(x.value);
              oss << "}";
              return string_impl(oss.str());
            });
            oss << ",";
            oss << std::string("value:") << K3::prettify_record(x.value, [] (R_key_value<int,
            int> x)  {
              ostringstream oss;
              oss << "{";
              oss << std::string("key:") << K3::prettify_int(x.key);
              oss << ",";
              oss << std::string("value:") << K3::prettify_int(x.value);
              oss << "}";
              return string_impl(oss.str());
            });
            oss << "}";
            return string_impl(oss.str());
          });
        });
        __result["nd_log_insert_R"] = K3::prettify_collection(nd_log_insert_R,
        [] (R_key_value<R_key_value<int, int>, R_key_value<int, int>> x)  {
          return K3::prettify_record(x, [] (R_key_value<R_key_value<int, int>, R_key_value<int,
          int>> x)  {
            ostringstream oss;
            oss << "{";
            oss << std::string("key:") << K3::prettify_record(x.key, [] (R_key_value<int, int> x)  {
              ostringstream oss;
              oss << "{";
              oss << std::string("key:") << K3::prettify_int(x.key);
              oss << ",";
              oss << std::string("value:") << K3::prettify_int(x.value);
              oss << "}";
              return string_impl(oss.str());
            });
            oss << ",";
            oss << std::string("value:") << K3::prettify_record(x.value, [] (R_key_value<int,
            int> x)  {
              ostringstream oss;
              oss << "{";
              oss << std::string("key:") << K3::prettify_int(x.key);
              oss << ",";
              oss << std::string("value:") << K3::prettify_int(x.value);
              oss << "}";
              return string_impl(oss.str());
            });
            oss << "}";
            return string_impl(oss.str());
          });
        });
        __result["nd_log_delete_S"] = K3::prettify_collection(nd_log_delete_S,
        [] (R_key_value<R_key_value<int, int>, R_key_value<int, int>> x)  {
          return K3::prettify_record(x, [] (R_key_value<R_key_value<int, int>, R_key_value<int,
          int>> x)  {
            ostringstream oss;
            oss << "{";
            oss << std::string("key:") << K3::prettify_record(x.key, [] (R_key_value<int, int> x)  {
              ostringstream oss;
              oss << "{";
              oss << std::string("key:") << K3::prettify_int(x.key);
              oss << ",";
              oss << std::string("value:") << K3::prettify_int(x.value);
              oss << "}";
              return string_impl(oss.str());
            });
            oss << ",";
            oss << std::string("value:") << K3::prettify_record(x.value, [] (R_key_value<int,
            int> x)  {
              ostringstream oss;
              oss << "{";
              oss << std::string("key:") << K3::prettify_int(x.key);
              oss << ",";
              oss << std::string("value:") << K3::prettify_int(x.value);
              oss << "}";
              return string_impl(oss.str());
            });
            oss << "}";
            return string_impl(oss.str());
          });
        });
        __result["nd_log_insert_S"] = K3::prettify_collection(nd_log_insert_S,
        [] (R_key_value<R_key_value<int, int>, R_key_value<int, int>> x)  {
          return K3::prettify_record(x, [] (R_key_value<R_key_value<int, int>, R_key_value<int,
          int>> x)  {
            ostringstream oss;
            oss << "{";
            oss << std::string("key:") << K3::prettify_record(x.key, [] (R_key_value<int, int> x)  {
              ostringstream oss;
              oss << "{";
              oss << std::string("key:") << K3::prettify_int(x.key);
              oss << ",";
              oss << std::string("value:") << K3::prettify_int(x.value);
              oss << "}";
              return string_impl(oss.str());
            });
            oss << ",";
            oss << std::string("value:") << K3::prettify_record(x.value, [] (R_key_value<int,
            int> x)  {
              ostringstream oss;
              oss << "{";
              oss << std::string("key:") << K3::prettify_int(x.key);
              oss << ",";
              oss << std::string("value:") << K3::prettify_int(x.value);
              oss << "}";
              return string_impl(oss.str());
            });
            oss << "}";
            return string_impl(oss.str());
          });
        });
        __result["sw_buf_delete_R"] = K3::prettify_collection(sw_buf_delete_R, [] (R_key_value<int,
        int> x)  {
          return K3::prettify_record(x, [] (R_key_value<int, int> x)  {
            ostringstream oss;
            oss << "{";
            oss << std::string("key:") << K3::prettify_int(x.key);
            oss << ",";
            oss << std::string("value:") << K3::prettify_int(x.value);
            oss << "}";
            return string_impl(oss.str());
          });
        });
        __result["sw_buf_insert_R"] = K3::prettify_collection(sw_buf_insert_R, [] (R_key_value<int,
        int> x)  {
          return K3::prettify_record(x, [] (R_key_value<int, int> x)  {
            ostringstream oss;
            oss << "{";
            oss << std::string("key:") << K3::prettify_int(x.key);
            oss << ",";
            oss << std::string("value:") << K3::prettify_int(x.value);
            oss << "}";
            return string_impl(oss.str());
          });
        });
        __result["sw_buf_delete_S"] = K3::prettify_collection(sw_buf_delete_S, [] (R_key_value<int,
        int> x)  {
          return K3::prettify_record(x, [] (R_key_value<int, int> x)  {
            ostringstream oss;
            oss << "{";
            oss << std::string("key:") << K3::prettify_int(x.key);
            oss << ",";
            oss << std::string("value:") << K3::prettify_int(x.value);
            oss << "}";
            return string_impl(oss.str());
          });
        });
        __result["sw_buf_insert_S"] = K3::prettify_collection(sw_buf_insert_S, [] (R_key_value<int,
        int> x)  {
          return K3::prettify_record(x, [] (R_key_value<int, int> x)  {
            ostringstream oss;
            oss << "{";
            oss << std::string("key:") << K3::prettify_int(x.key);
            oss << ",";
            oss << std::string("value:") << K3::prettify_int(x.value);
            oss << "}";
            return string_impl(oss.str());
          });
        });
        __result["ms_end_time"] = K3::prettify_int(ms_end_time);
        __result["ms_start_time"] = K3::prettify_int(ms_start_time);
        __result["sw_trig_buf_idx"] = K3::prettify_collection(sw_trig_buf_idx, [] (R_i<int> x)  {
          return K3::prettify_record(x, [] (R_i<int> x)  {
            ostringstream oss;
            oss << "{";
            oss << std::string("i:") << K3::prettify_int(x.i);
            oss << "}";
            return string_impl(oss.str());
          });
        });
        __result["sw_seen_sentry"] = K3::prettify_bool(sw_seen_sentry);
        __result["sw_init"] = K3::prettify_bool(sw_init);
        __result["nd_rcvd_sys_done"] = K3::prettify_bool(nd_rcvd_sys_done);
        __result["nd_log_master"] = K3::prettify_collection(nd_log_master,
        [] (R_key_value<R_key_value<int, int>, int> x)  {
          return K3::prettify_record(x, [] (R_key_value<R_key_value<int, int>, int> x)  {
            ostringstream oss;
            oss << "{";
            oss << std::string("key:") << K3::prettify_record(x.key, [] (R_key_value<int, int> x)  {
              ostringstream oss;
              oss << "{";
              oss << std::string("key:") << K3::prettify_int(x.key);
              oss << ",";
              oss << std::string("value:") << K3::prettify_int(x.value);
              oss << "}";
              return string_impl(oss.str());
            });
            oss << ",";
            oss << std::string("value:") << K3::prettify_int(x.value);
            oss << "}";
            return string_impl(oss.str());
          });
        });
        __result["nd_stmt_cntrs"] = K3::prettify_collection(nd_stmt_cntrs,
        [] (R_key_value<R_key_value<R_key_value<int, int>, int>, R_key_value<int,
        _Map<R_key_value<int, int>>>> x)  {
          return K3::prettify_record(x, [] (R_key_value<R_key_value<R_key_value<int, int>, int>,
          R_key_value<int, _Map<R_key_value<int, int>>>> x)  {
            ostringstream oss;
            oss << "{";
            oss << std::string("key:") << K3::prettify_record(x.key,
            [] (R_key_value<R_key_value<int, int>, int> x)  {
              ostringstream oss;
              oss << "{";
              oss << std::string("key:") << K3::prettify_record(x.key, [] (R_key_value<int,
              int> x)  {
                ostringstream oss;
                oss << "{";
                oss << std::string("key:") << K3::prettify_int(x.key);
                oss << ",";
                oss << std::string("value:") << K3::prettify_int(x.value);
                oss << "}";
                return string_impl(oss.str());
              });
              oss << ",";
              oss << std::string("value:") << K3::prettify_int(x.value);
              oss << "}";
              return string_impl(oss.str());
            });
            oss << ",";
            oss << std::string("value:") << K3::prettify_record(x.value, [] (R_key_value<int,
            _Map<R_key_value<int, int>>> x)  {
              ostringstream oss;
              oss << "{";
              oss << std::string("key:") << K3::prettify_int(x.key);
              oss << ",";
              oss << std::string("value:") << K3::prettify_collection(x.value, [] (R_key_value<int,
              int> x)  {
                return K3::prettify_record(x, [] (R_key_value<int, int> x)  {
                  ostringstream oss;
                  oss << "{";
                  oss << std::string("key:") << K3::prettify_int(x.key);
                  oss << ",";
                  oss << std::string("value:") << K3::prettify_int(x.value);
                  oss << "}";
                  return string_impl(oss.str());
                });
              });
              oss << "}";
              return string_impl(oss.str());
            });
            oss << "}";
            return string_impl(oss.str());
          });
        });
        __result["trig_ids"] = K3::prettify_collection(trig_ids, [] (R_r1_r2_r3<int,
        K3::base_string, _Seq<R_i<int>>> x)  {
          return K3::prettify_record(x, [] (R_r1_r2_r3<int, K3::base_string, _Seq<R_i<int>>> x)  {
            ostringstream oss;
            oss << "{";
            oss << std::string("r1:") << K3::prettify_int(x.r1);
            oss << ",";
            oss << std::string("r2:") << K3::prettify_string(x.r2);
            oss << ",";
            oss << std::string("r3:") << K3::prettify_collection(x.r3, [] (R_i<int> x)  {
              return K3::prettify_record(x, [] (R_i<int> x)  {
                ostringstream oss;
                oss << "{";
                oss << std::string("i:") << K3::prettify_int(x.i);
                oss << "}";
                return string_impl(oss.str());
              });
            });
            oss << "}";
            return string_impl(oss.str());
          });
        });
        __result["map_ids"] = K3::prettify_collection(map_ids, [] (R_r1_r2_r3<int, K3::base_string,
        int> x)  {
          return K3::prettify_record(x, [] (R_r1_r2_r3<int, K3::base_string, int> x)  {
            ostringstream oss;
            oss << "{";
            oss << std::string("r1:") << K3::prettify_int(x.r1);
            oss << ",";
            oss << std::string("r2:") << K3::prettify_string(x.r2);
            oss << ",";
            oss << std::string("r3:") << K3::prettify_int(x.r3);
            oss << "}";
            return string_impl(oss.str());
          });
        });
        __result["num_nodes"] = K3::prettify_int(num_nodes);
        __result["num_switches"] = K3::prettify_int(num_switches);
        __result["num_peers"] = K3::prettify_int(num_peers);
        __result["switches"] = K3::prettify_collection(switches, [] (R_i<Address> x)  {
          return K3::prettify_record(x, [] (R_i<Address> x)  {
            ostringstream oss;
            oss << "{";
            oss << std::string("i:") << K3::prettify_address(x.i);
            oss << "}";
            return string_impl(oss.str());
          });
        });
        __result["nodes"] = K3::prettify_collection(nodes, [] (R_i<Address> x)  {
          return K3::prettify_record(x, [] (R_i<Address> x)  {
            ostringstream oss;
            oss << "{";
            oss << std::string("i:") << K3::prettify_address(x.i);
            oss << "}";
            return string_impl(oss.str());
          });
        });
        __result["timer_addr"] = K3::prettify_address(timer_addr);
        __result["master_addr"] = K3::prettify_address(master_addr);
        __result["jobs"] = K3::prettify_collection(jobs, [] (R_key_value<Address, int> x)  {
          return K3::prettify_record(x, [] (R_key_value<Address, int> x)  {
            ostringstream oss;
            oss << "{";
            oss << std::string("key:") << K3::prettify_address(x.key);
            oss << ",";
            oss << std::string("value:") << K3::prettify_int(x.value);
            oss << "}";
            return string_impl(oss.str());
          });
        });
        __result["job"] = K3::prettify_int(job);
        __result["job_timer"] = K3::prettify_int(job_timer);
        __result["job_node"] = K3::prettify_int(job_node);
        __result["job_switch"] = K3::prettify_int(job_switch);
        __result["job_master"] = K3::prettify_int(job_master);
        __result["g_start_vid"] = K3::prettify_record(g_start_vid, [] (R_key_value<int, int> x)  {
          ostringstream oss;
          oss << "{";
          oss << std::string("key:") << K3::prettify_int(x.key);
          oss << ",";
          oss << std::string("value:") << K3::prettify_int(x.value);
          oss << "}";
          return string_impl(oss.str());
        });
        __result["g_max_vid"] = K3::prettify_record(g_max_vid, [] (R_key_value<int, int> x)  {
          ostringstream oss;
          oss << "{";
          oss << std::string("key:") << K3::prettify_int(x.key);
          oss << ",";
          oss << std::string("value:") << K3::prettify_int(x.value);
          oss << "}";
          return string_impl(oss.str());
        });
        __result["g_min_vid"] = K3::prettify_record(g_min_vid, [] (R_key_value<int, int> x)  {
          ostringstream oss;
          oss << "{";
          oss << std::string("key:") << K3::prettify_int(x.key);
          oss << ",";
          oss << std::string("value:") << K3::prettify_int(x.value);
          oss << "}";
          return string_impl(oss.str());
        });
        __result["g_init_vid"] = K3::prettify_record(g_init_vid, [] (R_key_value<int, int> x)  {
          ostringstream oss;
          oss << "{";
          oss << std::string("key:") << K3::prettify_int(x.key);
          oss << ",";
          oss << std::string("value:") << K3::prettify_int(x.value);
          oss << "}";
          return string_impl(oss.str());
        });
        __result["ms_rcv_switch_done_cnt"] = K3::prettify_int(ms_rcv_switch_done_cnt);
        __result["ms_rcv_node_done_cnt"] = K3::prettify_int(ms_rcv_node_done_cnt);
        __result["ms_rcv_job_cnt"] = K3::prettify_int(ms_rcv_job_cnt);
        __result["ms_rcv_jobs_ack_cnt"] = K3::prettify_int(ms_rcv_jobs_ack_cnt);
        __result["ms_rcv_sw_init_ack_cnt"] = K3::prettify_int(ms_rcv_sw_init_ack_cnt);
        __result["sw_sent_done"] = K3::prettify_bool(sw_sent_done);
        __result["nd_sent_done"] = K3::prettify_bool(nd_sent_done);
        __result["my_peers"] = K3::prettify_collection(my_peers, [] (R_i<Address> x)  {
          return K3::prettify_record(x, [] (R_i<Address> x)  {
            ostringstream oss;
            oss << "{";
            oss << std::string("i:") << K3::prettify_address(x.i);
            oss << "}";
            return string_impl(oss.str());
          });
        });
        __result["role"] = K3::prettify_string(role);
        __result["args"] = K3::prettify_tuple(args);
        __result["peers"] = K3::prettify_collection(peers, [] (R_addr<Address> x)  {
          return K3::prettify_record(x, [] (R_addr<Address> x)  {
            ostringstream oss;
            oss << "{";
            oss << std::string("addr:") << K3::prettify_address(x.addr);
            oss << "}";
            return string_impl(oss.str());
          });
        });
        __result["me"] = K3::prettify_address(me);
        return __result;
      }
      std::map<std::string, std::string> __jsonify()  {
        std::map<std::string, std::string> __result;
        __result["ms_num_gc_expected"] = K3::serialization::json::encode<int>(ms_num_gc_expected);
        __result["ms_gc_vid_ctr"] = K3::serialization::json::encode<int>(ms_gc_vid_ctr);
        __result["ms_gc_vid_map"] = K3::serialization::json::encode<_Map<R_key_value<Address,
        R_key_value<int, int>>>>(ms_gc_vid_map);
        __result["ms_gc_interval"] = K3::serialization::json::encode<int>(ms_gc_interval);
        __result["sw_ack_log"] = K3::serialization::json::encode<_Map<R_key_value<R_key_value<int,
        int>, int>>>(sw_ack_log);
        __result["sw_num_sent"] = K3::serialization::json::encode<int>(sw_num_sent);
        __result["sw_num_ack"] = K3::serialization::json::encode<int>(sw_num_ack);
        __result["pmap_data"] = K3::serialization::json::encode<_Seq<R_key_value<int,
        _Seq<R_key_value<int, int>>>>>(pmap_data);
        __result["pmap_input"] = K3::serialization::json::encode<_Seq<R_key_value<K3::base_string,
        _Seq<R_key_value<int, int>>>>>(pmap_input);
        __result["replicas"] = K3::serialization::json::encode<int>(replicas);
        __result["node_ring"] = K3::serialization::json::encode<_Seq<R_key_value<Address,
        int>>>(node_ring);
        __result["sw_highest_vid"] = K3::serialization::json::encode<R_key_value<int,
        int>>(sw_highest_vid);
        __result["sw_token_vid_list"] = K3::serialization::json::encode<_Seq<R_key_value<R_key_value<int,
        int>, int>>>(sw_token_vid_list);
        __result["sw_need_vid_cntr"] = K3::serialization::json::encode<int>(sw_need_vid_cntr);
        __result["sw_next_switch_addr"] = K3::serialization::json::encode<Address>(sw_next_switch_addr);
        __result["tm_timer_list"] = K3::serialization::json::encode<_Seq<R_r1_r2_r3<int, int,
        Address>>>(tm_timer_list);
        __result["map___SQL_SUM_AGGREGATE_1_mR1_s14_buf"] = K3::serialization::json::encode<shared_ptr<_Set<R_r1_r2_r3<R_key_value<int,
        int>, int, int>>>>(map___SQL_SUM_AGGREGATE_1_mR1_s14_buf);
        __result["map___SQL_SUM_AGGREGATE_2_mR1_s14_buf"] = K3::serialization::json::encode<shared_ptr<_Set<R_r1_r2_r3<R_key_value<int,
        int>, int, int>>>>(map___SQL_SUM_AGGREGATE_2_mR1_s14_buf);
        __result["map___SQL_SUM_AGGREGATE_1_mR1_s12_buf"] = K3::serialization::json::encode<shared_ptr<_Set<R_r1_r2_r3<R_key_value<int,
        int>, int, int>>>>(map___SQL_SUM_AGGREGATE_1_mR1_s12_buf);
        __result["map___SQL_SUM_AGGREGATE_1_mR1_s10_buf"] = K3::serialization::json::encode<shared_ptr<_Set<R_r1_r2_r3<R_key_value<int,
        int>, int, int>>>>(map___SQL_SUM_AGGREGATE_1_mR1_s10_buf);
        __result["map___SQL_SUM_AGGREGATE_2_mR1_s10_buf"] = K3::serialization::json::encode<shared_ptr<_Set<R_r1_r2_r3<R_key_value<int,
        int>, int, int>>>>(map___SQL_SUM_AGGREGATE_2_mR1_s10_buf);
        __result["map___SQL_SUM_AGGREGATE_1_mR1_s8_buf"] = K3::serialization::json::encode<shared_ptr<_Set<R_r1_r2_r3<R_key_value<int,
        int>, int, int>>>>(map___SQL_SUM_AGGREGATE_1_mR1_s8_buf);
        __result["map___SQL_SUM_AGGREGATE_2_mS3_s6_buf"] = K3::serialization::json::encode<shared_ptr<_Set<R_r1_r2_r3<R_key_value<int,
        int>, int, int>>>>(map___SQL_SUM_AGGREGATE_2_mS3_s6_buf);
        __result["map___SQL_SUM_AGGREGATE_1_mS1_s6_buf"] = K3::serialization::json::encode<shared_ptr<_Set<R_r1_r2_r3<R_key_value<int,
        int>, int, int>>>>(map___SQL_SUM_AGGREGATE_1_mS1_s6_buf);
        __result["map___SQL_SUM_AGGREGATE_1_mS1_s4_buf"] = K3::serialization::json::encode<shared_ptr<_Set<R_r1_r2_r3<R_key_value<int,
        int>, int, int>>>>(map___SQL_SUM_AGGREGATE_1_mS1_s4_buf);
        __result["map___SQL_SUM_AGGREGATE_2_mS3_s2_buf"] = K3::serialization::json::encode<shared_ptr<_Set<R_r1_r2_r3<R_key_value<int,
        int>, int, int>>>>(map___SQL_SUM_AGGREGATE_2_mS3_s2_buf);
        __result["map___SQL_SUM_AGGREGATE_1_mS1_s2_buf"] = K3::serialization::json::encode<shared_ptr<_Set<R_r1_r2_r3<R_key_value<int,
        int>, int, int>>>>(map___SQL_SUM_AGGREGATE_1_mS1_s2_buf);
        __result["map___SQL_SUM_AGGREGATE_1_mS1_s0_buf"] = K3::serialization::json::encode<shared_ptr<_Set<R_r1_r2_r3<R_key_value<int,
        int>, int, int>>>>(map___SQL_SUM_AGGREGATE_1_mS1_s0_buf);
        __result["__SQL_SUM_AGGREGATE_2_mR1"] = K3::serialization::json::encode<shared_ptr<_Set<R_r1_r2_r3<R_key_value<int,
        int>, int, int>>>>(__SQL_SUM_AGGREGATE_2_mR1);
        __result["__SQL_SUM_AGGREGATE_2_mS3"] = K3::serialization::json::encode<shared_ptr<_Set<R_r1_r2_r3<R_key_value<int,
        int>, int, int>>>>(__SQL_SUM_AGGREGATE_2_mS3);
        __result["__SQL_SUM_AGGREGATE_2"] = K3::serialization::json::encode<shared_ptr<_Set<R_key_value<R_key_value<int,
        int>, int>>>>(__SQL_SUM_AGGREGATE_2);
        __result["__SQL_SUM_AGGREGATE_1_mR1"] = K3::serialization::json::encode<shared_ptr<_Set<R_r1_r2_r3<R_key_value<int,
        int>, int, int>>>>(__SQL_SUM_AGGREGATE_1_mR1);
        __result["__SQL_SUM_AGGREGATE_1_mS1"] = K3::serialization::json::encode<shared_ptr<_Set<R_r1_r2_r3<R_key_value<int,
        int>, int, int>>>>(__SQL_SUM_AGGREGATE_1_mS1);
        __result["__SQL_SUM_AGGREGATE_1"] = K3::serialization::json::encode<shared_ptr<_Set<R_key_value<R_key_value<int,
        int>, int>>>>(__SQL_SUM_AGGREGATE_1);
        __result["nd_log_delete_R"] = K3::serialization::json::encode<_Map<R_key_value<R_key_value<int,
        int>, R_key_value<int, int>>>>(nd_log_delete_R);
        __result["nd_log_insert_R"] = K3::serialization::json::encode<_Map<R_key_value<R_key_value<int,
        int>, R_key_value<int, int>>>>(nd_log_insert_R);
        __result["nd_log_delete_S"] = K3::serialization::json::encode<_Map<R_key_value<R_key_value<int,
        int>, R_key_value<int, int>>>>(nd_log_delete_S);
        __result["nd_log_insert_S"] = K3::serialization::json::encode<_Map<R_key_value<R_key_value<int,
        int>, R_key_value<int, int>>>>(nd_log_insert_S);
        __result["sw_buf_delete_R"] = K3::serialization::json::encode<_Seq<R_key_value<int,
        int>>>(sw_buf_delete_R);
        __result["sw_buf_insert_R"] = K3::serialization::json::encode<_Seq<R_key_value<int,
        int>>>(sw_buf_insert_R);
        __result["sw_buf_delete_S"] = K3::serialization::json::encode<_Seq<R_key_value<int,
        int>>>(sw_buf_delete_S);
        __result["sw_buf_insert_S"] = K3::serialization::json::encode<_Seq<R_key_value<int,
        int>>>(sw_buf_insert_S);
        __result["ms_end_time"] = K3::serialization::json::encode<int>(ms_end_time);
        __result["ms_start_time"] = K3::serialization::json::encode<int>(ms_start_time);
        __result["sw_trig_buf_idx"] = K3::serialization::json::encode<_Seq<R_i<int>>>(sw_trig_buf_idx);
        __result["sw_seen_sentry"] = K3::serialization::json::encode<bool>(sw_seen_sentry);
        __result["sw_init"] = K3::serialization::json::encode<bool>(sw_init);
        __result["nd_rcvd_sys_done"] = K3::serialization::json::encode<bool>(nd_rcvd_sys_done);
        __result["nd_log_master"] = K3::serialization::json::encode<_Set<R_key_value<R_key_value<int,
        int>, int>>>(nd_log_master);
        __result["nd_stmt_cntrs"] = K3::serialization::json::encode<_Map<R_key_value<R_key_value<R_key_value<int,
        int>, int>, R_key_value<int, _Map<R_key_value<int, int>>>>>>(nd_stmt_cntrs);
        __result["trig_ids"] = K3::serialization::json::encode<_Collection<R_r1_r2_r3<int,
        K3::base_string, _Seq<R_i<int>>>>>(trig_ids);
        __result["map_ids"] = K3::serialization::json::encode<_Collection<R_r1_r2_r3<int,
        K3::base_string, int>>>(map_ids);
        __result["num_nodes"] = K3::serialization::json::encode<int>(num_nodes);
        __result["num_switches"] = K3::serialization::json::encode<int>(num_switches);
        __result["num_peers"] = K3::serialization::json::encode<int>(num_peers);
        __result["switches"] = K3::serialization::json::encode<_Collection<R_i<Address>>>(switches);
        __result["nodes"] = K3::serialization::json::encode<_Collection<R_i<Address>>>(nodes);
        __result["timer_addr"] = K3::serialization::json::encode<Address>(timer_addr);
        __result["master_addr"] = K3::serialization::json::encode<Address>(master_addr);
        __result["jobs"] = K3::serialization::json::encode<_Map<R_key_value<Address, int>>>(jobs);
        __result["job"] = K3::serialization::json::encode<int>(job);
        __result["job_timer"] = K3::serialization::json::encode<int>(job_timer);
        __result["job_node"] = K3::serialization::json::encode<int>(job_node);
        __result["job_switch"] = K3::serialization::json::encode<int>(job_switch);
        __result["job_master"] = K3::serialization::json::encode<int>(job_master);
        __result["g_start_vid"] = K3::serialization::json::encode<R_key_value<int,
        int>>(g_start_vid);
        __result["g_max_vid"] = K3::serialization::json::encode<R_key_value<int, int>>(g_max_vid);
        __result["g_min_vid"] = K3::serialization::json::encode<R_key_value<int, int>>(g_min_vid);
        __result["g_init_vid"] = K3::serialization::json::encode<R_key_value<int, int>>(g_init_vid);
        __result["ms_rcv_switch_done_cnt"] = K3::serialization::json::encode<int>(ms_rcv_switch_done_cnt);
        __result["ms_rcv_node_done_cnt"] = K3::serialization::json::encode<int>(ms_rcv_node_done_cnt);
        __result["ms_rcv_job_cnt"] = K3::serialization::json::encode<int>(ms_rcv_job_cnt);
        __result["ms_rcv_jobs_ack_cnt"] = K3::serialization::json::encode<int>(ms_rcv_jobs_ack_cnt);
        __result["ms_rcv_sw_init_ack_cnt"] = K3::serialization::json::encode<int>(ms_rcv_sw_init_ack_cnt);
        __result["sw_sent_done"] = K3::serialization::json::encode<bool>(sw_sent_done);
        __result["nd_sent_done"] = K3::serialization::json::encode<bool>(nd_sent_done);
        __result["my_peers"] = K3::serialization::json::encode<_Collection<R_i<Address>>>(my_peers);
        __result["role"] = K3::serialization::json::encode<K3::base_string>(role);
        __result["args"] = K3::serialization::json::encode<std::tuple<_Collection<R_arg<K3::base_string>>,
        _Collection<R_key_value<K3::base_string, K3::base_string>>>>(args);
        __result["peers"] = K3::serialization::json::encode<_Collection<R_addr<Address>>>(peers);
        __result["me"] = K3::serialization::json::encode<Address>(me);
        return __result;
      }
      void __dispatch(int trigger_id, void* payload, const Address& source)  {
        dispatch_table[trigger_id](payload, source);
      }
  protected:
      std::map<int, std::function<void(void*, const Address&)>> dispatch_table;
};
int __global_context::__ms_rcv_sw_init_ack_tid;
int __global_context::__sw_rcv_init_tid;
int __global_context::__ms_rcv_jobs_ack_tid;
int __global_context::__rcv_jobs_tid;
int __global_context::__ms_rcv_job_tid;
int __global_context::__rcv_master_addr_tid;
int __global_context::__ms_send_addr_self_tid;
int __global_context::__shutdown_trig_tid;
int __global_context::__ms_shutdown_tid;
int __global_context::__ms_rcv_node_done_tid;
int __global_context::__nd_rcv_done_tid;
int __global_context::__ms_rcv_switch_done_tid;
int __global_context::__sw_ack_rcv_tid;
int __global_context::__ms_rcv_gc_vid_tid;
int __global_context::__rcv_req_gc_vid_tid;
int __global_context::__ms_send_gc_req_tid;
int __global_context::__do_gc_tid;
int __global_context::__sw_rcv_token_tid;
int __global_context::__tm_insert_timer_tid;
int __global_context::__tm_check_time_tid;
int __global_context::__sw_demux_tid;
int __global_context::__sw_driver_trig_tid;
int __global_context::__nd_rcv_corr_done_tid;
int __global_context::__nd_insert_S_rcv_put_tid;
int __global_context::__nd_insert_S_rcv_fetch_tid;
int __global_context::__nd_insert_S_send_push_s0_m___SQL_SUM_AGGREGATE_1_mS1_tid;
int __global_context::__nd_insert_S_send_push_s2_m___SQL_SUM_AGGREGATE_1_mS1_tid;
int __global_context::__nd_insert_S_send_push_s2_m___SQL_SUM_AGGREGATE_2_mS3_tid;
int __global_context::__nd_insert_S_rcv_push_s0_m___SQL_SUM_AGGREGATE_1_mS1_tid;
int __global_context::__nd_insert_S_rcv_push_s2_m___SQL_SUM_AGGREGATE_1_mS1_tid;
int __global_context::__nd_insert_S_rcv_push_s2_m___SQL_SUM_AGGREGATE_2_mS3_tid;
int __global_context::__nd_insert_S_do_complete_s1_trig_tid;
int __global_context::__nd_insert_S_do_complete_s3_trig_tid;
int __global_context::__insert_S_rcv_corrective_s0_m___SQL_SUM_AGGREGATE_1_mS1_tid;
int __global_context::__insert_S_rcv_corrective_s2_m___SQL_SUM_AGGREGATE_1_mS1_tid;
int __global_context::__insert_S_rcv_corrective_s2_m___SQL_SUM_AGGREGATE_2_mS3_tid;
int __global_context::__nd_delete_S_rcv_put_tid;
int __global_context::__nd_delete_S_rcv_fetch_tid;
int __global_context::__nd_delete_S_send_push_s4_m___SQL_SUM_AGGREGATE_1_mS1_tid;
int __global_context::__nd_delete_S_send_push_s6_m___SQL_SUM_AGGREGATE_1_mS1_tid;
int __global_context::__nd_delete_S_send_push_s6_m___SQL_SUM_AGGREGATE_2_mS3_tid;
int __global_context::__nd_delete_S_rcv_push_s4_m___SQL_SUM_AGGREGATE_1_mS1_tid;
int __global_context::__nd_delete_S_rcv_push_s6_m___SQL_SUM_AGGREGATE_1_mS1_tid;
int __global_context::__nd_delete_S_rcv_push_s6_m___SQL_SUM_AGGREGATE_2_mS3_tid;
int __global_context::__nd_delete_S_do_complete_s5_trig_tid;
int __global_context::__nd_delete_S_do_complete_s7_trig_tid;
int __global_context::__delete_S_rcv_corrective_s4_m___SQL_SUM_AGGREGATE_1_mS1_tid;
int __global_context::__delete_S_rcv_corrective_s6_m___SQL_SUM_AGGREGATE_1_mS1_tid;
int __global_context::__delete_S_rcv_corrective_s6_m___SQL_SUM_AGGREGATE_2_mS3_tid;
int __global_context::__nd_insert_R_rcv_put_tid;
int __global_context::__nd_insert_R_rcv_fetch_tid;
int __global_context::__nd_insert_R_send_push_s8_m___SQL_SUM_AGGREGATE_1_mR1_tid;
int __global_context::__nd_insert_R_send_push_s10_m___SQL_SUM_AGGREGATE_2_mR1_tid;
int __global_context::__nd_insert_R_send_push_s10_m___SQL_SUM_AGGREGATE_1_mR1_tid;
int __global_context::__nd_insert_R_rcv_push_s8_m___SQL_SUM_AGGREGATE_1_mR1_tid;
int __global_context::__nd_insert_R_rcv_push_s10_m___SQL_SUM_AGGREGATE_2_mR1_tid;
int __global_context::__nd_insert_R_rcv_push_s10_m___SQL_SUM_AGGREGATE_1_mR1_tid;
int __global_context::__nd_insert_R_do_complete_s9_trig_tid;
int __global_context::__nd_insert_R_do_complete_s11_trig_tid;
int __global_context::__insert_R_rcv_corrective_s8_m___SQL_SUM_AGGREGATE_1_mR1_tid;
int __global_context::__insert_R_rcv_corrective_s10_m___SQL_SUM_AGGREGATE_2_mR1_tid;
int __global_context::__insert_R_rcv_corrective_s10_m___SQL_SUM_AGGREGATE_1_mR1_tid;
int __global_context::__nd_delete_R_rcv_put_tid;
int __global_context::__nd_delete_R_rcv_fetch_tid;
int __global_context::__nd_delete_R_send_push_s12_m___SQL_SUM_AGGREGATE_1_mR1_tid;
int __global_context::__nd_delete_R_send_push_s14_m___SQL_SUM_AGGREGATE_2_mR1_tid;
int __global_context::__nd_delete_R_send_push_s14_m___SQL_SUM_AGGREGATE_1_mR1_tid;
int __global_context::__nd_delete_R_rcv_push_s12_m___SQL_SUM_AGGREGATE_1_mR1_tid;
int __global_context::__nd_delete_R_rcv_push_s14_m___SQL_SUM_AGGREGATE_2_mR1_tid;
int __global_context::__nd_delete_R_rcv_push_s14_m___SQL_SUM_AGGREGATE_1_mR1_tid;
int __global_context::__nd_delete_R_do_complete_s13_trig_tid;
int __global_context::__nd_delete_R_do_complete_s15_trig_tid;
int __global_context::__delete_R_rcv_corrective_s12_m___SQL_SUM_AGGREGATE_1_mR1_tid;
int __global_context::__delete_R_rcv_corrective_s14_m___SQL_SUM_AGGREGATE_2_mR1_tid;
int __global_context::__delete_R_rcv_corrective_s14_m___SQL_SUM_AGGREGATE_1_mR1_tid;
int __global_context::__switchController_tid;
namespace YAML {
  template <>
  class convert<__global_context> {
    public:
        static Node encode(const __global_context& context)  {
          Node _node;
          _node["ms_num_gc_expected"] = convert<int>::encode(context.ms_num_gc_expected);
          _node["ms_gc_vid_ctr"] = convert<int>::encode(context.ms_gc_vid_ctr);
          _node["ms_gc_vid_map"] = convert<_Map<R_key_value<Address, R_key_value<int,
          int>>>>::encode(context.ms_gc_vid_map);
          _node["ms_gc_interval"] = convert<int>::encode(context.ms_gc_interval);
          _node["sw_ack_log"] = convert<_Map<R_key_value<R_key_value<int, int>,
          int>>>::encode(context.sw_ack_log);
          _node["sw_num_sent"] = convert<int>::encode(context.sw_num_sent);
          _node["sw_num_ack"] = convert<int>::encode(context.sw_num_ack);
          _node["pmap_data"] = convert<_Seq<R_key_value<int, _Seq<R_key_value<int,
          int>>>>>::encode(context.pmap_data);
          _node["pmap_input"] = convert<_Seq<R_key_value<K3::base_string, _Seq<R_key_value<int,
          int>>>>>::encode(context.pmap_input);
          _node["replicas"] = convert<int>::encode(context.replicas);
          _node["node_ring"] = convert<_Seq<R_key_value<Address, int>>>::encode(context.node_ring);
          _node["sw_highest_vid"] = convert<R_key_value<int, int>>::encode(context.sw_highest_vid);
          _node["sw_token_vid_list"] = convert<_Seq<R_key_value<R_key_value<int, int>,
          int>>>::encode(context.sw_token_vid_list);
          _node["sw_need_vid_cntr"] = convert<int>::encode(context.sw_need_vid_cntr);
          _node["sw_next_switch_addr"] = convert<Address>::encode(context.sw_next_switch_addr);
          _node["tm_timer_list"] = convert<_Seq<R_r1_r2_r3<int, int,
          Address>>>::encode(context.tm_timer_list);
          _node["map___SQL_SUM_AGGREGATE_1_mR1_s14_buf"] = convert<shared_ptr<_Set<R_r1_r2_r3<R_key_value<int,
          int>, int, int>>>>::encode(context.map___SQL_SUM_AGGREGATE_1_mR1_s14_buf);
          _node["map___SQL_SUM_AGGREGATE_2_mR1_s14_buf"] = convert<shared_ptr<_Set<R_r1_r2_r3<R_key_value<int,
          int>, int, int>>>>::encode(context.map___SQL_SUM_AGGREGATE_2_mR1_s14_buf);
          _node["map___SQL_SUM_AGGREGATE_1_mR1_s12_buf"] = convert<shared_ptr<_Set<R_r1_r2_r3<R_key_value<int,
          int>, int, int>>>>::encode(context.map___SQL_SUM_AGGREGATE_1_mR1_s12_buf);
          _node["map___SQL_SUM_AGGREGATE_1_mR1_s10_buf"] = convert<shared_ptr<_Set<R_r1_r2_r3<R_key_value<int,
          int>, int, int>>>>::encode(context.map___SQL_SUM_AGGREGATE_1_mR1_s10_buf);
          _node["map___SQL_SUM_AGGREGATE_2_mR1_s10_buf"] = convert<shared_ptr<_Set<R_r1_r2_r3<R_key_value<int,
          int>, int, int>>>>::encode(context.map___SQL_SUM_AGGREGATE_2_mR1_s10_buf);
          _node["map___SQL_SUM_AGGREGATE_1_mR1_s8_buf"] = convert<shared_ptr<_Set<R_r1_r2_r3<R_key_value<int,
          int>, int, int>>>>::encode(context.map___SQL_SUM_AGGREGATE_1_mR1_s8_buf);
          _node["map___SQL_SUM_AGGREGATE_2_mS3_s6_buf"] = convert<shared_ptr<_Set<R_r1_r2_r3<R_key_value<int,
          int>, int, int>>>>::encode(context.map___SQL_SUM_AGGREGATE_2_mS3_s6_buf);
          _node["map___SQL_SUM_AGGREGATE_1_mS1_s6_buf"] = convert<shared_ptr<_Set<R_r1_r2_r3<R_key_value<int,
          int>, int, int>>>>::encode(context.map___SQL_SUM_AGGREGATE_1_mS1_s6_buf);
          _node["map___SQL_SUM_AGGREGATE_1_mS1_s4_buf"] = convert<shared_ptr<_Set<R_r1_r2_r3<R_key_value<int,
          int>, int, int>>>>::encode(context.map___SQL_SUM_AGGREGATE_1_mS1_s4_buf);
          _node["map___SQL_SUM_AGGREGATE_2_mS3_s2_buf"] = convert<shared_ptr<_Set<R_r1_r2_r3<R_key_value<int,
          int>, int, int>>>>::encode(context.map___SQL_SUM_AGGREGATE_2_mS3_s2_buf);
          _node["map___SQL_SUM_AGGREGATE_1_mS1_s2_buf"] = convert<shared_ptr<_Set<R_r1_r2_r3<R_key_value<int,
          int>, int, int>>>>::encode(context.map___SQL_SUM_AGGREGATE_1_mS1_s2_buf);
          _node["map___SQL_SUM_AGGREGATE_1_mS1_s0_buf"] = convert<shared_ptr<_Set<R_r1_r2_r3<R_key_value<int,
          int>, int, int>>>>::encode(context.map___SQL_SUM_AGGREGATE_1_mS1_s0_buf);
          _node["__SQL_SUM_AGGREGATE_2_mR1"] = convert<shared_ptr<_Set<R_r1_r2_r3<R_key_value<int,
          int>, int, int>>>>::encode(context.__SQL_SUM_AGGREGATE_2_mR1);
          _node["__SQL_SUM_AGGREGATE_2_mS3"] = convert<shared_ptr<_Set<R_r1_r2_r3<R_key_value<int,
          int>, int, int>>>>::encode(context.__SQL_SUM_AGGREGATE_2_mS3);
          _node["__SQL_SUM_AGGREGATE_2"] = convert<shared_ptr<_Set<R_key_value<R_key_value<int,
          int>, int>>>>::encode(context.__SQL_SUM_AGGREGATE_2);
          _node["__SQL_SUM_AGGREGATE_1_mR1"] = convert<shared_ptr<_Set<R_r1_r2_r3<R_key_value<int,
          int>, int, int>>>>::encode(context.__SQL_SUM_AGGREGATE_1_mR1);
          _node["__SQL_SUM_AGGREGATE_1_mS1"] = convert<shared_ptr<_Set<R_r1_r2_r3<R_key_value<int,
          int>, int, int>>>>::encode(context.__SQL_SUM_AGGREGATE_1_mS1);
          _node["__SQL_SUM_AGGREGATE_1"] = convert<shared_ptr<_Set<R_key_value<R_key_value<int,
          int>, int>>>>::encode(context.__SQL_SUM_AGGREGATE_1);
          _node["nd_log_delete_R"] = convert<_Map<R_key_value<R_key_value<int, int>,
          R_key_value<int, int>>>>::encode(context.nd_log_delete_R);
          _node["nd_log_insert_R"] = convert<_Map<R_key_value<R_key_value<int, int>,
          R_key_value<int, int>>>>::encode(context.nd_log_insert_R);
          _node["nd_log_delete_S"] = convert<_Map<R_key_value<R_key_value<int, int>,
          R_key_value<int, int>>>>::encode(context.nd_log_delete_S);
          _node["nd_log_insert_S"] = convert<_Map<R_key_value<R_key_value<int, int>,
          R_key_value<int, int>>>>::encode(context.nd_log_insert_S);
          _node["sw_buf_delete_R"] = convert<_Seq<R_key_value<int,
          int>>>::encode(context.sw_buf_delete_R);
          _node["sw_buf_insert_R"] = convert<_Seq<R_key_value<int,
          int>>>::encode(context.sw_buf_insert_R);
          _node["sw_buf_delete_S"] = convert<_Seq<R_key_value<int,
          int>>>::encode(context.sw_buf_delete_S);
          _node["sw_buf_insert_S"] = convert<_Seq<R_key_value<int,
          int>>>::encode(context.sw_buf_insert_S);
          _node["ms_end_time"] = convert<int>::encode(context.ms_end_time);
          _node["ms_start_time"] = convert<int>::encode(context.ms_start_time);
          _node["sw_trig_buf_idx"] = convert<_Seq<R_i<int>>>::encode(context.sw_trig_buf_idx);
          _node["sw_seen_sentry"] = convert<bool>::encode(context.sw_seen_sentry);
          _node["sw_init"] = convert<bool>::encode(context.sw_init);
          _node["nd_rcvd_sys_done"] = convert<bool>::encode(context.nd_rcvd_sys_done);
          _node["nd_log_master"] = convert<_Set<R_key_value<R_key_value<int, int>,
          int>>>::encode(context.nd_log_master);
          _node["nd_stmt_cntrs"] = convert<_Map<R_key_value<R_key_value<R_key_value<int, int>, int>,
          R_key_value<int, _Map<R_key_value<int, int>>>>>>::encode(context.nd_stmt_cntrs);
          _node["trig_ids"] = convert<_Collection<R_r1_r2_r3<int, K3::base_string,
          _Seq<R_i<int>>>>>::encode(context.trig_ids);
          _node["map_ids"] = convert<_Collection<R_r1_r2_r3<int, K3::base_string,
          int>>>::encode(context.map_ids);
          _node["num_nodes"] = convert<int>::encode(context.num_nodes);
          _node["num_switches"] = convert<int>::encode(context.num_switches);
          _node["num_peers"] = convert<int>::encode(context.num_peers);
          _node["switches"] = convert<_Collection<R_i<Address>>>::encode(context.switches);
          _node["nodes"] = convert<_Collection<R_i<Address>>>::encode(context.nodes);
          _node["timer_addr"] = convert<Address>::encode(context.timer_addr);
          _node["master_addr"] = convert<Address>::encode(context.master_addr);
          _node["jobs"] = convert<_Map<R_key_value<Address, int>>>::encode(context.jobs);
          _node["job"] = convert<int>::encode(context.job);
          _node["job_timer"] = convert<int>::encode(context.job_timer);
          _node["job_node"] = convert<int>::encode(context.job_node);
          _node["job_switch"] = convert<int>::encode(context.job_switch);
          _node["job_master"] = convert<int>::encode(context.job_master);
          _node["g_start_vid"] = convert<R_key_value<int, int>>::encode(context.g_start_vid);
          _node["g_max_vid"] = convert<R_key_value<int, int>>::encode(context.g_max_vid);
          _node["g_min_vid"] = convert<R_key_value<int, int>>::encode(context.g_min_vid);
          _node["g_init_vid"] = convert<R_key_value<int, int>>::encode(context.g_init_vid);
          _node["ms_rcv_switch_done_cnt"] = convert<int>::encode(context.ms_rcv_switch_done_cnt);
          _node["ms_rcv_node_done_cnt"] = convert<int>::encode(context.ms_rcv_node_done_cnt);
          _node["ms_rcv_job_cnt"] = convert<int>::encode(context.ms_rcv_job_cnt);
          _node["ms_rcv_jobs_ack_cnt"] = convert<int>::encode(context.ms_rcv_jobs_ack_cnt);
          _node["ms_rcv_sw_init_ack_cnt"] = convert<int>::encode(context.ms_rcv_sw_init_ack_cnt);
          _node["sw_sent_done"] = convert<bool>::encode(context.sw_sent_done);
          _node["nd_sent_done"] = convert<bool>::encode(context.nd_sent_done);
          _node["my_peers"] = convert<_Collection<R_i<Address>>>::encode(context.my_peers);
          _node["role"] = convert<K3::base_string>::encode(context.role);
          _node["args"] = convert<std::tuple<_Collection<R_arg<K3::base_string>>,
          _Collection<R_key_value<K3::base_string, K3::base_string>>>>::encode(context.args);
          _node["peers"] = convert<_Collection<R_addr<Address>>>::encode(context.peers);
          _node["me"] = convert<Address>::encode(context.me);
          return _node;
        }
        static bool decode(const Node& node, __global_context& context)  {
          if (!node.IsMap()) {
            return false;
          }
          if (node["ms_num_gc_expected"]) {
            context.ms_num_gc_expected = node["ms_num_gc_expected"].as<int>();
            context.__ms_num_gc_expected_set__ = true;
          }
          if (node["ms_gc_vid_ctr"]) {
            context.ms_gc_vid_ctr = node["ms_gc_vid_ctr"].as<int>();
            context.__ms_gc_vid_ctr_set__ = true;
          }
          if (node["ms_gc_vid_map"]) {
            context.ms_gc_vid_map = node["ms_gc_vid_map"].as<_Map<R_key_value<Address,
            R_key_value<int, int>>>>();
          }
          if (node["ms_gc_interval"]) {
            context.ms_gc_interval = node["ms_gc_interval"].as<int>();
            context.__ms_gc_interval_set__ = true;
          }
          if (node["sw_ack_log"]) {
            context.sw_ack_log = node["sw_ack_log"].as<_Map<R_key_value<R_key_value<int, int>,
            int>>>();
          }
          if (node["sw_num_sent"]) {
            context.sw_num_sent = node["sw_num_sent"].as<int>();
          }
          if (node["sw_num_ack"]) {
            context.sw_num_ack = node["sw_num_ack"].as<int>();
          }
          if (node["pmap_data"]) {
            context.pmap_data = node["pmap_data"].as<_Seq<R_key_value<int, _Seq<R_key_value<int,
            int>>>>>();
            context.__pmap_data_set__ = true;
          }
          if (node["pmap_input"]) {
            context.pmap_input = node["pmap_input"].as<_Seq<R_key_value<K3::base_string,
            _Seq<R_key_value<int, int>>>>>();
            context.__pmap_input_set__ = true;
          }
          if (node["replicas"]) {
            context.replicas = node["replicas"].as<int>();
            context.__replicas_set__ = true;
          }
          if (node["node_ring"]) {
            context.node_ring = node["node_ring"].as<_Seq<R_key_value<Address, int>>>();
          }
          if (node["sw_highest_vid"]) {
            context.sw_highest_vid = node["sw_highest_vid"].as<R_key_value<int, int>>();
          }
          if (node["sw_token_vid_list"]) {
            context.sw_token_vid_list = node["sw_token_vid_list"].as<_Seq<R_key_value<R_key_value<int,
            int>, int>>>();
          }
          if (node["sw_need_vid_cntr"]) {
            context.sw_need_vid_cntr = node["sw_need_vid_cntr"].as<int>();
            context.__sw_need_vid_cntr_set__ = true;
          }
          if (node["sw_next_switch_addr"]) {
            context.sw_next_switch_addr = node["sw_next_switch_addr"].as<Address>();
          }
          if (node["tm_timer_list"]) {
            context.tm_timer_list = node["tm_timer_list"].as<_Seq<R_r1_r2_r3<int, int, Address>>>();
          }
          if (node["map___SQL_SUM_AGGREGATE_1_mR1_s14_buf"]) {
            context.map___SQL_SUM_AGGREGATE_1_mR1_s14_buf = node["map___SQL_SUM_AGGREGATE_1_mR1_s14_buf"].as<shared_ptr<_Set<R_r1_r2_r3<R_key_value<int,
            int>, int, int>>>>();
            context.__map___SQL_SUM_AGGREGATE_1_mR1_s14_buf_set__ = true;
          }
          if (node["map___SQL_SUM_AGGREGATE_2_mR1_s14_buf"]) {
            context.map___SQL_SUM_AGGREGATE_2_mR1_s14_buf = node["map___SQL_SUM_AGGREGATE_2_mR1_s14_buf"].as<shared_ptr<_Set<R_r1_r2_r3<R_key_value<int,
            int>, int, int>>>>();
            context.__map___SQL_SUM_AGGREGATE_2_mR1_s14_buf_set__ = true;
          }
          if (node["map___SQL_SUM_AGGREGATE_1_mR1_s12_buf"]) {
            context.map___SQL_SUM_AGGREGATE_1_mR1_s12_buf = node["map___SQL_SUM_AGGREGATE_1_mR1_s12_buf"].as<shared_ptr<_Set<R_r1_r2_r3<R_key_value<int,
            int>, int, int>>>>();
            context.__map___SQL_SUM_AGGREGATE_1_mR1_s12_buf_set__ = true;
          }
          if (node["map___SQL_SUM_AGGREGATE_1_mR1_s10_buf"]) {
            context.map___SQL_SUM_AGGREGATE_1_mR1_s10_buf = node["map___SQL_SUM_AGGREGATE_1_mR1_s10_buf"].as<shared_ptr<_Set<R_r1_r2_r3<R_key_value<int,
            int>, int, int>>>>();
            context.__map___SQL_SUM_AGGREGATE_1_mR1_s10_buf_set__ = true;
          }
          if (node["map___SQL_SUM_AGGREGATE_2_mR1_s10_buf"]) {
            context.map___SQL_SUM_AGGREGATE_2_mR1_s10_buf = node["map___SQL_SUM_AGGREGATE_2_mR1_s10_buf"].as<shared_ptr<_Set<R_r1_r2_r3<R_key_value<int,
            int>, int, int>>>>();
            context.__map___SQL_SUM_AGGREGATE_2_mR1_s10_buf_set__ = true;
          }
          if (node["map___SQL_SUM_AGGREGATE_1_mR1_s8_buf"]) {
            context.map___SQL_SUM_AGGREGATE_1_mR1_s8_buf = node["map___SQL_SUM_AGGREGATE_1_mR1_s8_buf"].as<shared_ptr<_Set<R_r1_r2_r3<R_key_value<int,
            int>, int, int>>>>();
            context.__map___SQL_SUM_AGGREGATE_1_mR1_s8_buf_set__ = true;
          }
          if (node["map___SQL_SUM_AGGREGATE_2_mS3_s6_buf"]) {
            context.map___SQL_SUM_AGGREGATE_2_mS3_s6_buf = node["map___SQL_SUM_AGGREGATE_2_mS3_s6_buf"].as<shared_ptr<_Set<R_r1_r2_r3<R_key_value<int,
            int>, int, int>>>>();
            context.__map___SQL_SUM_AGGREGATE_2_mS3_s6_buf_set__ = true;
          }
          if (node["map___SQL_SUM_AGGREGATE_1_mS1_s6_buf"]) {
            context.map___SQL_SUM_AGGREGATE_1_mS1_s6_buf = node["map___SQL_SUM_AGGREGATE_1_mS1_s6_buf"].as<shared_ptr<_Set<R_r1_r2_r3<R_key_value<int,
            int>, int, int>>>>();
            context.__map___SQL_SUM_AGGREGATE_1_mS1_s6_buf_set__ = true;
          }
          if (node["map___SQL_SUM_AGGREGATE_1_mS1_s4_buf"]) {
            context.map___SQL_SUM_AGGREGATE_1_mS1_s4_buf = node["map___SQL_SUM_AGGREGATE_1_mS1_s4_buf"].as<shared_ptr<_Set<R_r1_r2_r3<R_key_value<int,
            int>, int, int>>>>();
            context.__map___SQL_SUM_AGGREGATE_1_mS1_s4_buf_set__ = true;
          }
          if (node["map___SQL_SUM_AGGREGATE_2_mS3_s2_buf"]) {
            context.map___SQL_SUM_AGGREGATE_2_mS3_s2_buf = node["map___SQL_SUM_AGGREGATE_2_mS3_s2_buf"].as<shared_ptr<_Set<R_r1_r2_r3<R_key_value<int,
            int>, int, int>>>>();
            context.__map___SQL_SUM_AGGREGATE_2_mS3_s2_buf_set__ = true;
          }
          if (node["map___SQL_SUM_AGGREGATE_1_mS1_s2_buf"]) {
            context.map___SQL_SUM_AGGREGATE_1_mS1_s2_buf = node["map___SQL_SUM_AGGREGATE_1_mS1_s2_buf"].as<shared_ptr<_Set<R_r1_r2_r3<R_key_value<int,
            int>, int, int>>>>();
            context.__map___SQL_SUM_AGGREGATE_1_mS1_s2_buf_set__ = true;
          }
          if (node["map___SQL_SUM_AGGREGATE_1_mS1_s0_buf"]) {
            context.map___SQL_SUM_AGGREGATE_1_mS1_s0_buf = node["map___SQL_SUM_AGGREGATE_1_mS1_s0_buf"].as<shared_ptr<_Set<R_r1_r2_r3<R_key_value<int,
            int>, int, int>>>>();
            context.__map___SQL_SUM_AGGREGATE_1_mS1_s0_buf_set__ = true;
          }
          if (node["__SQL_SUM_AGGREGATE_2_mR1"]) {
            context.__SQL_SUM_AGGREGATE_2_mR1 = node["__SQL_SUM_AGGREGATE_2_mR1"].as<shared_ptr<_Set<R_r1_r2_r3<R_key_value<int,
            int>, int, int>>>>();
            context.____SQL_SUM_AGGREGATE_2_mR1_set__ = true;
          }
          if (node["__SQL_SUM_AGGREGATE_2_mS3"]) {
            context.__SQL_SUM_AGGREGATE_2_mS3 = node["__SQL_SUM_AGGREGATE_2_mS3"].as<shared_ptr<_Set<R_r1_r2_r3<R_key_value<int,
            int>, int, int>>>>();
            context.____SQL_SUM_AGGREGATE_2_mS3_set__ = true;
          }
          if (node["__SQL_SUM_AGGREGATE_2"]) {
            context.__SQL_SUM_AGGREGATE_2 = node["__SQL_SUM_AGGREGATE_2"].as<shared_ptr<_Set<R_key_value<R_key_value<int,
            int>, int>>>>();
            context.____SQL_SUM_AGGREGATE_2_set__ = true;
          }
          if (node["__SQL_SUM_AGGREGATE_1_mR1"]) {
            context.__SQL_SUM_AGGREGATE_1_mR1 = node["__SQL_SUM_AGGREGATE_1_mR1"].as<shared_ptr<_Set<R_r1_r2_r3<R_key_value<int,
            int>, int, int>>>>();
            context.____SQL_SUM_AGGREGATE_1_mR1_set__ = true;
          }
          if (node["__SQL_SUM_AGGREGATE_1_mS1"]) {
            context.__SQL_SUM_AGGREGATE_1_mS1 = node["__SQL_SUM_AGGREGATE_1_mS1"].as<shared_ptr<_Set<R_r1_r2_r3<R_key_value<int,
            int>, int, int>>>>();
            context.____SQL_SUM_AGGREGATE_1_mS1_set__ = true;
          }
          if (node["__SQL_SUM_AGGREGATE_1"]) {
            context.__SQL_SUM_AGGREGATE_1 = node["__SQL_SUM_AGGREGATE_1"].as<shared_ptr<_Set<R_key_value<R_key_value<int,
            int>, int>>>>();
            context.____SQL_SUM_AGGREGATE_1_set__ = true;
          }
          if (node["nd_log_delete_R"]) {
            context.nd_log_delete_R = node["nd_log_delete_R"].as<_Map<R_key_value<R_key_value<int,
            int>, R_key_value<int, int>>>>();
          }
          if (node["nd_log_insert_R"]) {
            context.nd_log_insert_R = node["nd_log_insert_R"].as<_Map<R_key_value<R_key_value<int,
            int>, R_key_value<int, int>>>>();
          }
          if (node["nd_log_delete_S"]) {
            context.nd_log_delete_S = node["nd_log_delete_S"].as<_Map<R_key_value<R_key_value<int,
            int>, R_key_value<int, int>>>>();
          }
          if (node["nd_log_insert_S"]) {
            context.nd_log_insert_S = node["nd_log_insert_S"].as<_Map<R_key_value<R_key_value<int,
            int>, R_key_value<int, int>>>>();
          }
          if (node["sw_buf_delete_R"]) {
            context.sw_buf_delete_R = node["sw_buf_delete_R"].as<_Seq<R_key_value<int, int>>>();
          }
          if (node["sw_buf_insert_R"]) {
            context.sw_buf_insert_R = node["sw_buf_insert_R"].as<_Seq<R_key_value<int, int>>>();
          }
          if (node["sw_buf_delete_S"]) {
            context.sw_buf_delete_S = node["sw_buf_delete_S"].as<_Seq<R_key_value<int, int>>>();
          }
          if (node["sw_buf_insert_S"]) {
            context.sw_buf_insert_S = node["sw_buf_insert_S"].as<_Seq<R_key_value<int, int>>>();
          }
          if (node["ms_end_time"]) {
            context.ms_end_time = node["ms_end_time"].as<int>();
          }
          if (node["ms_start_time"]) {
            context.ms_start_time = node["ms_start_time"].as<int>();
          }
          if (node["sw_trig_buf_idx"]) {
            context.sw_trig_buf_idx = node["sw_trig_buf_idx"].as<_Seq<R_i<int>>>();
          }
          if (node["sw_seen_sentry"]) {
            context.sw_seen_sentry = node["sw_seen_sentry"].as<bool>();
            context.__sw_seen_sentry_set__ = true;
          }
          if (node["sw_init"]) {
            context.sw_init = node["sw_init"].as<bool>();
            context.__sw_init_set__ = true;
          }
          if (node["nd_rcvd_sys_done"]) {
            context.nd_rcvd_sys_done = node["nd_rcvd_sys_done"].as<bool>();
            context.__nd_rcvd_sys_done_set__ = true;
          }
          if (node["nd_log_master"]) {
            context.nd_log_master = node["nd_log_master"].as<_Set<R_key_value<R_key_value<int, int>,
            int>>>();
          }
          if (node["nd_stmt_cntrs"]) {
            context.nd_stmt_cntrs = node["nd_stmt_cntrs"].as<_Map<R_key_value<R_key_value<R_key_value<int,
            int>, int>, R_key_value<int, _Map<R_key_value<int, int>>>>>>();
          }
          if (node["trig_ids"]) {
            context.trig_ids = node["trig_ids"].as<_Collection<R_r1_r2_r3<int, K3::base_string,
            _Seq<R_i<int>>>>>();
            context.__trig_ids_set__ = true;
          }
          if (node["map_ids"]) {
            context.map_ids = node["map_ids"].as<_Collection<R_r1_r2_r3<int, K3::base_string,
            int>>>();
            context.__map_ids_set__ = true;
          }
          if (node["num_nodes"]) {
            context.num_nodes = node["num_nodes"].as<int>();
          }
          if (node["num_switches"]) {
            context.num_switches = node["num_switches"].as<int>();
          }
          if (node["num_peers"]) {
            context.num_peers = node["num_peers"].as<int>();
            context.__num_peers_set__ = true;
          }
          if (node["switches"]) {
            context.switches = node["switches"].as<_Collection<R_i<Address>>>();
          }
          if (node["nodes"]) {
            context.nodes = node["nodes"].as<_Collection<R_i<Address>>>();
          }
          if (node["timer_addr"]) {
            context.timer_addr = node["timer_addr"].as<Address>();
          }
          if (node["master_addr"]) {
            context.master_addr = node["master_addr"].as<Address>();
          }
          if (node["jobs"]) {
            context.jobs = node["jobs"].as<_Map<R_key_value<Address, int>>>();
          }
          if (node["job"]) {
            context.job = node["job"].as<int>();
            context.__job_set__ = true;
          }
          if (node["job_timer"]) {
            context.job_timer = node["job_timer"].as<int>();
            context.__job_timer_set__ = true;
          }
          if (node["job_node"]) {
            context.job_node = node["job_node"].as<int>();
            context.__job_node_set__ = true;
          }
          if (node["job_switch"]) {
            context.job_switch = node["job_switch"].as<int>();
            context.__job_switch_set__ = true;
          }
          if (node["job_master"]) {
            context.job_master = node["job_master"].as<int>();
            context.__job_master_set__ = true;
          }
          if (node["g_start_vid"]) {
            context.g_start_vid = node["g_start_vid"].as<R_key_value<int, int>>();
            context.__g_start_vid_set__ = true;
          }
          if (node["g_max_vid"]) {
            context.g_max_vid = node["g_max_vid"].as<R_key_value<int, int>>();
            context.__g_max_vid_set__ = true;
          }
          if (node["g_min_vid"]) {
            context.g_min_vid = node["g_min_vid"].as<R_key_value<int, int>>();
            context.__g_min_vid_set__ = true;
          }
          if (node["g_init_vid"]) {
            context.g_init_vid = node["g_init_vid"].as<R_key_value<int, int>>();
            context.__g_init_vid_set__ = true;
          }
          if (node["ms_rcv_switch_done_cnt"]) {
            context.ms_rcv_switch_done_cnt = node["ms_rcv_switch_done_cnt"].as<int>();
            context.__ms_rcv_switch_done_cnt_set__ = true;
          }
          if (node["ms_rcv_node_done_cnt"]) {
            context.ms_rcv_node_done_cnt = node["ms_rcv_node_done_cnt"].as<int>();
            context.__ms_rcv_node_done_cnt_set__ = true;
          }
          if (node["ms_rcv_job_cnt"]) {
            context.ms_rcv_job_cnt = node["ms_rcv_job_cnt"].as<int>();
            context.__ms_rcv_job_cnt_set__ = true;
          }
          if (node["ms_rcv_jobs_ack_cnt"]) {
            context.ms_rcv_jobs_ack_cnt = node["ms_rcv_jobs_ack_cnt"].as<int>();
            context.__ms_rcv_jobs_ack_cnt_set__ = true;
          }
          if (node["ms_rcv_sw_init_ack_cnt"]) {
            context.ms_rcv_sw_init_ack_cnt = node["ms_rcv_sw_init_ack_cnt"].as<int>();
            context.__ms_rcv_sw_init_ack_cnt_set__ = true;
          }
          if (node["sw_sent_done"]) {
            context.sw_sent_done = node["sw_sent_done"].as<bool>();
            context.__sw_sent_done_set__ = true;
          }
          if (node["nd_sent_done"]) {
            context.nd_sent_done = node["nd_sent_done"].as<bool>();
            context.__nd_sent_done_set__ = true;
          }
          if (node["my_peers"]) {
            context.my_peers = node["my_peers"].as<_Collection<R_i<Address>>>();
            context.__my_peers_set__ = true;
          }
          if (node["role"]) {
            context.role = node["role"].as<K3::base_string>();
          }
          if (node["args"]) {
            context.args = node["args"].as<std::tuple<_Collection<R_arg<K3::base_string>>,
            _Collection<R_key_value<K3::base_string, K3::base_string>>>>();
          }
          if (node["peers"]) {
            context.peers = node["peers"].as<_Collection<R_addr<Address>>>();
          }
          if (node["me"]) {
            context.me = node["me"].as<Address>();
          }
          return true;
        }
  };
}
void __global_context::__patch(std::string s)  {
  YAML::convert<__global_context>::decode(YAML::Load(s), *this);
}
int main(int argc, char** argv)  {
  __global_context::__ms_rcv_sw_init_ack_tid = 0;
  __global_context::__sw_rcv_init_tid = 1;
  __global_context::__ms_rcv_jobs_ack_tid = 2;
  __global_context::__rcv_jobs_tid = 3;
  __global_context::__ms_rcv_job_tid = 4;
  __global_context::__rcv_master_addr_tid = 5;
  __global_context::__ms_send_addr_self_tid = 6;
  __global_context::__shutdown_trig_tid = 7;
  __global_context::__ms_shutdown_tid = 8;
  __global_context::__ms_rcv_node_done_tid = 9;
  __global_context::__nd_rcv_done_tid = 10;
  __global_context::__ms_rcv_switch_done_tid = 11;
  __global_context::__sw_ack_rcv_tid = 12;
  __global_context::__ms_rcv_gc_vid_tid = 13;
  __global_context::__rcv_req_gc_vid_tid = 14;
  __global_context::__ms_send_gc_req_tid = 15;
  __global_context::__do_gc_tid = 16;
  __global_context::__sw_rcv_token_tid = 17;
  __global_context::__tm_insert_timer_tid = 18;
  __global_context::__tm_check_time_tid = 19;
  __global_context::__sw_demux_tid = 20;
  __global_context::__sw_driver_trig_tid = 21;
  __global_context::__nd_rcv_corr_done_tid = 22;
  __global_context::__nd_insert_S_rcv_put_tid = 23;
  __global_context::__nd_insert_S_rcv_fetch_tid = 24;
  __global_context::__nd_insert_S_send_push_s0_m___SQL_SUM_AGGREGATE_1_mS1_tid = 25;
  __global_context::__nd_insert_S_send_push_s2_m___SQL_SUM_AGGREGATE_1_mS1_tid = 26;
  __global_context::__nd_insert_S_send_push_s2_m___SQL_SUM_AGGREGATE_2_mS3_tid = 27;
  __global_context::__nd_insert_S_rcv_push_s0_m___SQL_SUM_AGGREGATE_1_mS1_tid = 28;
  __global_context::__nd_insert_S_rcv_push_s2_m___SQL_SUM_AGGREGATE_1_mS1_tid = 29;
  __global_context::__nd_insert_S_rcv_push_s2_m___SQL_SUM_AGGREGATE_2_mS3_tid = 30;
  __global_context::__nd_insert_S_do_complete_s1_trig_tid = 31;
  __global_context::__nd_insert_S_do_complete_s3_trig_tid = 32;
  __global_context::__insert_S_rcv_corrective_s0_m___SQL_SUM_AGGREGATE_1_mS1_tid = 33;
  __global_context::__insert_S_rcv_corrective_s2_m___SQL_SUM_AGGREGATE_1_mS1_tid = 34;
  __global_context::__insert_S_rcv_corrective_s2_m___SQL_SUM_AGGREGATE_2_mS3_tid = 35;
  __global_context::__nd_delete_S_rcv_put_tid = 36;
  __global_context::__nd_delete_S_rcv_fetch_tid = 37;
  __global_context::__nd_delete_S_send_push_s4_m___SQL_SUM_AGGREGATE_1_mS1_tid = 38;
  __global_context::__nd_delete_S_send_push_s6_m___SQL_SUM_AGGREGATE_1_mS1_tid = 39;
  __global_context::__nd_delete_S_send_push_s6_m___SQL_SUM_AGGREGATE_2_mS3_tid = 40;
  __global_context::__nd_delete_S_rcv_push_s4_m___SQL_SUM_AGGREGATE_1_mS1_tid = 41;
  __global_context::__nd_delete_S_rcv_push_s6_m___SQL_SUM_AGGREGATE_1_mS1_tid = 42;
  __global_context::__nd_delete_S_rcv_push_s6_m___SQL_SUM_AGGREGATE_2_mS3_tid = 43;
  __global_context::__nd_delete_S_do_complete_s5_trig_tid = 44;
  __global_context::__nd_delete_S_do_complete_s7_trig_tid = 45;
  __global_context::__delete_S_rcv_corrective_s4_m___SQL_SUM_AGGREGATE_1_mS1_tid = 46;
  __global_context::__delete_S_rcv_corrective_s6_m___SQL_SUM_AGGREGATE_1_mS1_tid = 47;
  __global_context::__delete_S_rcv_corrective_s6_m___SQL_SUM_AGGREGATE_2_mS3_tid = 48;
  __global_context::__nd_insert_R_rcv_put_tid = 49;
  __global_context::__nd_insert_R_rcv_fetch_tid = 50;
  __global_context::__nd_insert_R_send_push_s8_m___SQL_SUM_AGGREGATE_1_mR1_tid = 51;
  __global_context::__nd_insert_R_send_push_s10_m___SQL_SUM_AGGREGATE_2_mR1_tid = 52;
  __global_context::__nd_insert_R_send_push_s10_m___SQL_SUM_AGGREGATE_1_mR1_tid = 53;
  __global_context::__nd_insert_R_rcv_push_s8_m___SQL_SUM_AGGREGATE_1_mR1_tid = 54;
  __global_context::__nd_insert_R_rcv_push_s10_m___SQL_SUM_AGGREGATE_2_mR1_tid = 55;
  __global_context::__nd_insert_R_rcv_push_s10_m___SQL_SUM_AGGREGATE_1_mR1_tid = 56;
  __global_context::__nd_insert_R_do_complete_s9_trig_tid = 57;
  __global_context::__nd_insert_R_do_complete_s11_trig_tid = 58;
  __global_context::__insert_R_rcv_corrective_s8_m___SQL_SUM_AGGREGATE_1_mR1_tid = 59;
  __global_context::__insert_R_rcv_corrective_s10_m___SQL_SUM_AGGREGATE_2_mR1_tid = 60;
  __global_context::__insert_R_rcv_corrective_s10_m___SQL_SUM_AGGREGATE_1_mR1_tid = 61;
  __global_context::__nd_delete_R_rcv_put_tid = 62;
  __global_context::__nd_delete_R_rcv_fetch_tid = 63;
  __global_context::__nd_delete_R_send_push_s12_m___SQL_SUM_AGGREGATE_1_mR1_tid = 64;
  __global_context::__nd_delete_R_send_push_s14_m___SQL_SUM_AGGREGATE_2_mR1_tid = 65;
  __global_context::__nd_delete_R_send_push_s14_m___SQL_SUM_AGGREGATE_1_mR1_tid = 66;
  __global_context::__nd_delete_R_rcv_push_s12_m___SQL_SUM_AGGREGATE_1_mR1_tid = 67;
  __global_context::__nd_delete_R_rcv_push_s14_m___SQL_SUM_AGGREGATE_2_mR1_tid = 68;
  __global_context::__nd_delete_R_rcv_push_s14_m___SQL_SUM_AGGREGATE_1_mR1_tid = 69;
  __global_context::__nd_delete_R_do_complete_s13_trig_tid = 70;
  __global_context::__nd_delete_R_do_complete_s15_trig_tid = 71;
  __global_context::__delete_R_rcv_corrective_s12_m___SQL_SUM_AGGREGATE_1_mR1_tid = 72;
  __global_context::__delete_R_rcv_corrective_s14_m___SQL_SUM_AGGREGATE_2_mR1_tid = 73;
  __global_context::__delete_R_rcv_corrective_s14_m___SQL_SUM_AGGREGATE_1_mR1_tid = 74;
  __global_context::__switchController_tid = 75;
  __k3_context::__trigger_names[__global_context::__switchController_tid] = "switchController";
  __k3_context::__trigger_names[__global_context::__delete_R_rcv_corrective_s14_m___SQL_SUM_AGGREGATE_1_mR1_tid] = "delete_R_rcv_corrective_s14_m___SQL_SUM_AGGREGATE_1_mR1";
  __k3_context::__trigger_names[__global_context::__delete_R_rcv_corrective_s14_m___SQL_SUM_AGGREGATE_2_mR1_tid] = "delete_R_rcv_corrective_s14_m___SQL_SUM_AGGREGATE_2_mR1";
  __k3_context::__trigger_names[__global_context::__delete_R_rcv_corrective_s12_m___SQL_SUM_AGGREGATE_1_mR1_tid] = "delete_R_rcv_corrective_s12_m___SQL_SUM_AGGREGATE_1_mR1";
  __k3_context::__trigger_names[__global_context::__nd_delete_R_do_complete_s15_trig_tid] = "nd_delete_R_do_complete_s15_trig";
  __k3_context::__trigger_names[__global_context::__nd_delete_R_do_complete_s13_trig_tid] = "nd_delete_R_do_complete_s13_trig";
  __k3_context::__trigger_names[__global_context::__nd_delete_R_rcv_push_s14_m___SQL_SUM_AGGREGATE_1_mR1_tid] = "nd_delete_R_rcv_push_s14_m___SQL_SUM_AGGREGATE_1_mR1";
  __k3_context::__trigger_names[__global_context::__nd_delete_R_rcv_push_s14_m___SQL_SUM_AGGREGATE_2_mR1_tid] = "nd_delete_R_rcv_push_s14_m___SQL_SUM_AGGREGATE_2_mR1";
  __k3_context::__trigger_names[__global_context::__nd_delete_R_rcv_push_s12_m___SQL_SUM_AGGREGATE_1_mR1_tid] = "nd_delete_R_rcv_push_s12_m___SQL_SUM_AGGREGATE_1_mR1";
  __k3_context::__trigger_names[__global_context::__nd_delete_R_send_push_s14_m___SQL_SUM_AGGREGATE_1_mR1_tid] = "nd_delete_R_send_push_s14_m___SQL_SUM_AGGREGATE_1_mR1";
  __k3_context::__trigger_names[__global_context::__nd_delete_R_send_push_s14_m___SQL_SUM_AGGREGATE_2_mR1_tid] = "nd_delete_R_send_push_s14_m___SQL_SUM_AGGREGATE_2_mR1";
  __k3_context::__trigger_names[__global_context::__nd_delete_R_send_push_s12_m___SQL_SUM_AGGREGATE_1_mR1_tid] = "nd_delete_R_send_push_s12_m___SQL_SUM_AGGREGATE_1_mR1";
  __k3_context::__trigger_names[__global_context::__nd_delete_R_rcv_fetch_tid] = "nd_delete_R_rcv_fetch";
  __k3_context::__trigger_names[__global_context::__nd_delete_R_rcv_put_tid] = "nd_delete_R_rcv_put";
  __k3_context::__trigger_names[__global_context::__insert_R_rcv_corrective_s10_m___SQL_SUM_AGGREGATE_1_mR1_tid] = "insert_R_rcv_corrective_s10_m___SQL_SUM_AGGREGATE_1_mR1";
  __k3_context::__trigger_names[__global_context::__insert_R_rcv_corrective_s10_m___SQL_SUM_AGGREGATE_2_mR1_tid] = "insert_R_rcv_corrective_s10_m___SQL_SUM_AGGREGATE_2_mR1";
  __k3_context::__trigger_names[__global_context::__insert_R_rcv_corrective_s8_m___SQL_SUM_AGGREGATE_1_mR1_tid] = "insert_R_rcv_corrective_s8_m___SQL_SUM_AGGREGATE_1_mR1";
  __k3_context::__trigger_names[__global_context::__nd_insert_R_do_complete_s11_trig_tid] = "nd_insert_R_do_complete_s11_trig";
  __k3_context::__trigger_names[__global_context::__nd_insert_R_do_complete_s9_trig_tid] = "nd_insert_R_do_complete_s9_trig";
  __k3_context::__trigger_names[__global_context::__nd_insert_R_rcv_push_s10_m___SQL_SUM_AGGREGATE_1_mR1_tid] = "nd_insert_R_rcv_push_s10_m___SQL_SUM_AGGREGATE_1_mR1";
  __k3_context::__trigger_names[__global_context::__nd_insert_R_rcv_push_s10_m___SQL_SUM_AGGREGATE_2_mR1_tid] = "nd_insert_R_rcv_push_s10_m___SQL_SUM_AGGREGATE_2_mR1";
  __k3_context::__trigger_names[__global_context::__nd_insert_R_rcv_push_s8_m___SQL_SUM_AGGREGATE_1_mR1_tid] = "nd_insert_R_rcv_push_s8_m___SQL_SUM_AGGREGATE_1_mR1";
  __k3_context::__trigger_names[__global_context::__nd_insert_R_send_push_s10_m___SQL_SUM_AGGREGATE_1_mR1_tid] = "nd_insert_R_send_push_s10_m___SQL_SUM_AGGREGATE_1_mR1";
  __k3_context::__trigger_names[__global_context::__nd_insert_R_send_push_s10_m___SQL_SUM_AGGREGATE_2_mR1_tid] = "nd_insert_R_send_push_s10_m___SQL_SUM_AGGREGATE_2_mR1";
  __k3_context::__trigger_names[__global_context::__nd_insert_R_send_push_s8_m___SQL_SUM_AGGREGATE_1_mR1_tid] = "nd_insert_R_send_push_s8_m___SQL_SUM_AGGREGATE_1_mR1";
  __k3_context::__trigger_names[__global_context::__nd_insert_R_rcv_fetch_tid] = "nd_insert_R_rcv_fetch";
  __k3_context::__trigger_names[__global_context::__nd_insert_R_rcv_put_tid] = "nd_insert_R_rcv_put";
  __k3_context::__trigger_names[__global_context::__delete_S_rcv_corrective_s6_m___SQL_SUM_AGGREGATE_2_mS3_tid] = "delete_S_rcv_corrective_s6_m___SQL_SUM_AGGREGATE_2_mS3";
  __k3_context::__trigger_names[__global_context::__delete_S_rcv_corrective_s6_m___SQL_SUM_AGGREGATE_1_mS1_tid] = "delete_S_rcv_corrective_s6_m___SQL_SUM_AGGREGATE_1_mS1";
  __k3_context::__trigger_names[__global_context::__delete_S_rcv_corrective_s4_m___SQL_SUM_AGGREGATE_1_mS1_tid] = "delete_S_rcv_corrective_s4_m___SQL_SUM_AGGREGATE_1_mS1";
  __k3_context::__trigger_names[__global_context::__nd_delete_S_do_complete_s7_trig_tid] = "nd_delete_S_do_complete_s7_trig";
  __k3_context::__trigger_names[__global_context::__nd_delete_S_do_complete_s5_trig_tid] = "nd_delete_S_do_complete_s5_trig";
  __k3_context::__trigger_names[__global_context::__nd_delete_S_rcv_push_s6_m___SQL_SUM_AGGREGATE_2_mS3_tid] = "nd_delete_S_rcv_push_s6_m___SQL_SUM_AGGREGATE_2_mS3";
  __k3_context::__trigger_names[__global_context::__nd_delete_S_rcv_push_s6_m___SQL_SUM_AGGREGATE_1_mS1_tid] = "nd_delete_S_rcv_push_s6_m___SQL_SUM_AGGREGATE_1_mS1";
  __k3_context::__trigger_names[__global_context::__nd_delete_S_rcv_push_s4_m___SQL_SUM_AGGREGATE_1_mS1_tid] = "nd_delete_S_rcv_push_s4_m___SQL_SUM_AGGREGATE_1_mS1";
  __k3_context::__trigger_names[__global_context::__nd_delete_S_send_push_s6_m___SQL_SUM_AGGREGATE_2_mS3_tid] = "nd_delete_S_send_push_s6_m___SQL_SUM_AGGREGATE_2_mS3";
  __k3_context::__trigger_names[__global_context::__nd_delete_S_send_push_s6_m___SQL_SUM_AGGREGATE_1_mS1_tid] = "nd_delete_S_send_push_s6_m___SQL_SUM_AGGREGATE_1_mS1";
  __k3_context::__trigger_names[__global_context::__nd_delete_S_send_push_s4_m___SQL_SUM_AGGREGATE_1_mS1_tid] = "nd_delete_S_send_push_s4_m___SQL_SUM_AGGREGATE_1_mS1";
  __k3_context::__trigger_names[__global_context::__nd_delete_S_rcv_fetch_tid] = "nd_delete_S_rcv_fetch";
  __k3_context::__trigger_names[__global_context::__nd_delete_S_rcv_put_tid] = "nd_delete_S_rcv_put";
  __k3_context::__trigger_names[__global_context::__insert_S_rcv_corrective_s2_m___SQL_SUM_AGGREGATE_2_mS3_tid] = "insert_S_rcv_corrective_s2_m___SQL_SUM_AGGREGATE_2_mS3";
  __k3_context::__trigger_names[__global_context::__insert_S_rcv_corrective_s2_m___SQL_SUM_AGGREGATE_1_mS1_tid] = "insert_S_rcv_corrective_s2_m___SQL_SUM_AGGREGATE_1_mS1";
  __k3_context::__trigger_names[__global_context::__insert_S_rcv_corrective_s0_m___SQL_SUM_AGGREGATE_1_mS1_tid] = "insert_S_rcv_corrective_s0_m___SQL_SUM_AGGREGATE_1_mS1";
  __k3_context::__trigger_names[__global_context::__nd_insert_S_do_complete_s3_trig_tid] = "nd_insert_S_do_complete_s3_trig";
  __k3_context::__trigger_names[__global_context::__nd_insert_S_do_complete_s1_trig_tid] = "nd_insert_S_do_complete_s1_trig";
  __k3_context::__trigger_names[__global_context::__nd_insert_S_rcv_push_s2_m___SQL_SUM_AGGREGATE_2_mS3_tid] = "nd_insert_S_rcv_push_s2_m___SQL_SUM_AGGREGATE_2_mS3";
  __k3_context::__trigger_names[__global_context::__nd_insert_S_rcv_push_s2_m___SQL_SUM_AGGREGATE_1_mS1_tid] = "nd_insert_S_rcv_push_s2_m___SQL_SUM_AGGREGATE_1_mS1";
  __k3_context::__trigger_names[__global_context::__nd_insert_S_rcv_push_s0_m___SQL_SUM_AGGREGATE_1_mS1_tid] = "nd_insert_S_rcv_push_s0_m___SQL_SUM_AGGREGATE_1_mS1";
  __k3_context::__trigger_names[__global_context::__nd_insert_S_send_push_s2_m___SQL_SUM_AGGREGATE_2_mS3_tid] = "nd_insert_S_send_push_s2_m___SQL_SUM_AGGREGATE_2_mS3";
  __k3_context::__trigger_names[__global_context::__nd_insert_S_send_push_s2_m___SQL_SUM_AGGREGATE_1_mS1_tid] = "nd_insert_S_send_push_s2_m___SQL_SUM_AGGREGATE_1_mS1";
  __k3_context::__trigger_names[__global_context::__nd_insert_S_send_push_s0_m___SQL_SUM_AGGREGATE_1_mS1_tid] = "nd_insert_S_send_push_s0_m___SQL_SUM_AGGREGATE_1_mS1";
  __k3_context::__trigger_names[__global_context::__nd_insert_S_rcv_fetch_tid] = "nd_insert_S_rcv_fetch";
  __k3_context::__trigger_names[__global_context::__nd_insert_S_rcv_put_tid] = "nd_insert_S_rcv_put";
  __k3_context::__trigger_names[__global_context::__nd_rcv_corr_done_tid] = "nd_rcv_corr_done";
  __k3_context::__trigger_names[__global_context::__sw_driver_trig_tid] = "sw_driver_trig";
  __k3_context::__trigger_names[__global_context::__sw_demux_tid] = "sw_demux";
  __k3_context::__trigger_names[__global_context::__tm_check_time_tid] = "tm_check_time";
  __k3_context::__trigger_names[__global_context::__tm_insert_timer_tid] = "tm_insert_timer";
  __k3_context::__trigger_names[__global_context::__sw_rcv_token_tid] = "sw_rcv_token";
  __k3_context::__trigger_names[__global_context::__do_gc_tid] = "do_gc";
  __k3_context::__trigger_names[__global_context::__ms_send_gc_req_tid] = "ms_send_gc_req";
  __k3_context::__trigger_names[__global_context::__rcv_req_gc_vid_tid] = "rcv_req_gc_vid";
  __k3_context::__trigger_names[__global_context::__ms_rcv_gc_vid_tid] = "ms_rcv_gc_vid";
  __k3_context::__trigger_names[__global_context::__sw_ack_rcv_tid] = "sw_ack_rcv";
  __k3_context::__trigger_names[__global_context::__ms_rcv_switch_done_tid] = "ms_rcv_switch_done";
  __k3_context::__trigger_names[__global_context::__nd_rcv_done_tid] = "nd_rcv_done";
  __k3_context::__trigger_names[__global_context::__ms_rcv_node_done_tid] = "ms_rcv_node_done";
  __k3_context::__trigger_names[__global_context::__ms_shutdown_tid] = "ms_shutdown";
  __k3_context::__trigger_names[__global_context::__shutdown_trig_tid] = "shutdown_trig";
  __k3_context::__trigger_names[__global_context::__ms_send_addr_self_tid] = "ms_send_addr_self";
  __k3_context::__trigger_names[__global_context::__rcv_master_addr_tid] = "rcv_master_addr";
  __k3_context::__trigger_names[__global_context::__ms_rcv_job_tid] = "ms_rcv_job";
  __k3_context::__trigger_names[__global_context::__rcv_jobs_tid] = "rcv_jobs";
  __k3_context::__trigger_names[__global_context::__ms_rcv_jobs_ack_tid] = "ms_rcv_jobs_ack";
  __k3_context::__trigger_names[__global_context::__sw_rcv_init_tid] = "sw_rcv_init";
  __k3_context::__trigger_names[__global_context::__ms_rcv_sw_init_ack_tid] = "ms_rcv_sw_init_ack";
  __k3_context::__clonable_dispatchers[__global_context::__switchController_tid] = make_shared<ValDispatcher<unit_t>>();
  __k3_context::__clonable_dispatchers[__global_context::__delete_R_rcv_corrective_s14_m___SQL_SUM_AGGREGATE_1_mR1_tid] = make_shared<ValDispatcher<R_r1_r2_r3_r4_r5_r6_r7<Address,
  int, R_key_value<int, int>, int, R_key_value<int, int>, _Seq<R_key_value<int, int>>,
  _Set<R_key_value<int, int>>>>>();
  __k3_context::__clonable_dispatchers[__global_context::__delete_R_rcv_corrective_s14_m___SQL_SUM_AGGREGATE_2_mR1_tid] = make_shared<ValDispatcher<R_r1_r2_r3_r4_r5_r6_r7<Address,
  int, R_key_value<int, int>, int, R_key_value<int, int>, _Seq<R_key_value<int, int>>,
  _Set<R_key_value<int, int>>>>>();
  __k3_context::__clonable_dispatchers[__global_context::__delete_R_rcv_corrective_s12_m___SQL_SUM_AGGREGATE_1_mR1_tid] = make_shared<ValDispatcher<R_r1_r2_r3_r4_r5_r6_r7<Address,
  int, R_key_value<int, int>, int, R_key_value<int, int>, _Seq<R_key_value<int, int>>,
  _Set<R_key_value<int, int>>>>>();
  __k3_context::__clonable_dispatchers[__global_context::__nd_delete_R_do_complete_s15_trig_tid] = make_shared<ValDispatcher<R_r1_r2_r3<R_key_value<int,
  int>, int, int>>>();
  __k3_context::__clonable_dispatchers[__global_context::__nd_delete_R_do_complete_s13_trig_tid] = make_shared<ValDispatcher<R_r1_r2_r3<R_key_value<int,
  int>, int, int>>>();
  __k3_context::__clonable_dispatchers[__global_context::__nd_delete_R_rcv_push_s14_m___SQL_SUM_AGGREGATE_1_mR1_tid] = make_shared<ValDispatcher<R_r1_r2_r3_r4<_Set<R_r1_r2_r3<R_key_value<int,
  int>, int, int>>, R_key_value<int, int>, int, int>>>();
  __k3_context::__clonable_dispatchers[__global_context::__nd_delete_R_rcv_push_s14_m___SQL_SUM_AGGREGATE_2_mR1_tid] = make_shared<ValDispatcher<R_r1_r2_r3_r4<_Set<R_r1_r2_r3<R_key_value<int,
  int>, int, int>>, R_key_value<int, int>, int, int>>>();
  __k3_context::__clonable_dispatchers[__global_context::__nd_delete_R_rcv_push_s12_m___SQL_SUM_AGGREGATE_1_mR1_tid] = make_shared<ValDispatcher<R_r1_r2_r3_r4<_Set<R_r1_r2_r3<R_key_value<int,
  int>, int, int>>, R_key_value<int, int>, int, int>>>();
  __k3_context::__clonable_dispatchers[__global_context::__nd_delete_R_send_push_s14_m___SQL_SUM_AGGREGATE_1_mR1_tid] = make_shared<ValDispatcher<R_r1_r2_r3<R_key_value<int,
  int>, int, int>>>();
  __k3_context::__clonable_dispatchers[__global_context::__nd_delete_R_send_push_s14_m___SQL_SUM_AGGREGATE_2_mR1_tid] = make_shared<ValDispatcher<R_r1_r2_r3<R_key_value<int,
  int>, int, int>>>();
  __k3_context::__clonable_dispatchers[__global_context::__nd_delete_R_send_push_s12_m___SQL_SUM_AGGREGATE_1_mR1_tid] = make_shared<ValDispatcher<R_r1_r2_r3<R_key_value<int,
  int>, int, int>>>();
  __k3_context::__clonable_dispatchers[__global_context::__nd_delete_R_rcv_fetch_tid] = make_shared<ValDispatcher<R_r1_r2_r3_r4<_Collection<R_key_value<int,
  int>>, R_key_value<int, int>, int, int>>>();
  __k3_context::__clonable_dispatchers[__global_context::__nd_delete_R_rcv_put_tid] = make_shared<ValDispatcher<R_r1_r2_r3_r4_r5<Address,
  _Collection<R_key_value<int, int>>, R_key_value<int, int>, int, int>>>();
  __k3_context::__clonable_dispatchers[__global_context::__insert_R_rcv_corrective_s10_m___SQL_SUM_AGGREGATE_1_mR1_tid] = make_shared<ValDispatcher<R_r1_r2_r3_r4_r5_r6_r7<Address,
  int, R_key_value<int, int>, int, R_key_value<int, int>, _Seq<R_key_value<int, int>>,
  _Set<R_key_value<int, int>>>>>();
  __k3_context::__clonable_dispatchers[__global_context::__insert_R_rcv_corrective_s10_m___SQL_SUM_AGGREGATE_2_mR1_tid] = make_shared<ValDispatcher<R_r1_r2_r3_r4_r5_r6_r7<Address,
  int, R_key_value<int, int>, int, R_key_value<int, int>, _Seq<R_key_value<int, int>>,
  _Set<R_key_value<int, int>>>>>();
  __k3_context::__clonable_dispatchers[__global_context::__insert_R_rcv_corrective_s8_m___SQL_SUM_AGGREGATE_1_mR1_tid] = make_shared<ValDispatcher<R_r1_r2_r3_r4_r5_r6_r7<Address,
  int, R_key_value<int, int>, int, R_key_value<int, int>, _Seq<R_key_value<int, int>>,
  _Set<R_key_value<int, int>>>>>();
  __k3_context::__clonable_dispatchers[__global_context::__nd_insert_R_do_complete_s11_trig_tid] = make_shared<ValDispatcher<R_r1_r2_r3<R_key_value<int,
  int>, int, int>>>();
  __k3_context::__clonable_dispatchers[__global_context::__nd_insert_R_do_complete_s9_trig_tid] = make_shared<ValDispatcher<R_r1_r2_r3<R_key_value<int,
  int>, int, int>>>();
  __k3_context::__clonable_dispatchers[__global_context::__nd_insert_R_rcv_push_s10_m___SQL_SUM_AGGREGATE_1_mR1_tid] = make_shared<ValDispatcher<R_r1_r2_r3_r4<_Set<R_r1_r2_r3<R_key_value<int,
  int>, int, int>>, R_key_value<int, int>, int, int>>>();
  __k3_context::__clonable_dispatchers[__global_context::__nd_insert_R_rcv_push_s10_m___SQL_SUM_AGGREGATE_2_mR1_tid] = make_shared<ValDispatcher<R_r1_r2_r3_r4<_Set<R_r1_r2_r3<R_key_value<int,
  int>, int, int>>, R_key_value<int, int>, int, int>>>();
  __k3_context::__clonable_dispatchers[__global_context::__nd_insert_R_rcv_push_s8_m___SQL_SUM_AGGREGATE_1_mR1_tid] = make_shared<ValDispatcher<R_r1_r2_r3_r4<_Set<R_r1_r2_r3<R_key_value<int,
  int>, int, int>>, R_key_value<int, int>, int, int>>>();
  __k3_context::__clonable_dispatchers[__global_context::__nd_insert_R_send_push_s10_m___SQL_SUM_AGGREGATE_1_mR1_tid] = make_shared<ValDispatcher<R_r1_r2_r3<R_key_value<int,
  int>, int, int>>>();
  __k3_context::__clonable_dispatchers[__global_context::__nd_insert_R_send_push_s10_m___SQL_SUM_AGGREGATE_2_mR1_tid] = make_shared<ValDispatcher<R_r1_r2_r3<R_key_value<int,
  int>, int, int>>>();
  __k3_context::__clonable_dispatchers[__global_context::__nd_insert_R_send_push_s8_m___SQL_SUM_AGGREGATE_1_mR1_tid] = make_shared<ValDispatcher<R_r1_r2_r3<R_key_value<int,
  int>, int, int>>>();
  __k3_context::__clonable_dispatchers[__global_context::__nd_insert_R_rcv_fetch_tid] = make_shared<ValDispatcher<R_r1_r2_r3_r4<_Collection<R_key_value<int,
  int>>, R_key_value<int, int>, int, int>>>();
  __k3_context::__clonable_dispatchers[__global_context::__nd_insert_R_rcv_put_tid] = make_shared<ValDispatcher<R_r1_r2_r3_r4_r5<Address,
  _Collection<R_key_value<int, int>>, R_key_value<int, int>, int, int>>>();
  __k3_context::__clonable_dispatchers[__global_context::__delete_S_rcv_corrective_s6_m___SQL_SUM_AGGREGATE_2_mS3_tid] = make_shared<ValDispatcher<R_r1_r2_r3_r4_r5_r6_r7<Address,
  int, R_key_value<int, int>, int, R_key_value<int, int>, _Seq<R_key_value<int, int>>,
  _Set<R_key_value<int, int>>>>>();
  __k3_context::__clonable_dispatchers[__global_context::__delete_S_rcv_corrective_s6_m___SQL_SUM_AGGREGATE_1_mS1_tid] = make_shared<ValDispatcher<R_r1_r2_r3_r4_r5_r6_r7<Address,
  int, R_key_value<int, int>, int, R_key_value<int, int>, _Seq<R_key_value<int, int>>,
  _Set<R_key_value<int, int>>>>>();
  __k3_context::__clonable_dispatchers[__global_context::__delete_S_rcv_corrective_s4_m___SQL_SUM_AGGREGATE_1_mS1_tid] = make_shared<ValDispatcher<R_r1_r2_r3_r4_r5_r6_r7<Address,
  int, R_key_value<int, int>, int, R_key_value<int, int>, _Seq<R_key_value<int, int>>,
  _Set<R_key_value<int, int>>>>>();
  __k3_context::__clonable_dispatchers[__global_context::__nd_delete_S_do_complete_s7_trig_tid] = make_shared<ValDispatcher<R_r1_r2_r3<R_key_value<int,
  int>, int, int>>>();
  __k3_context::__clonable_dispatchers[__global_context::__nd_delete_S_do_complete_s5_trig_tid] = make_shared<ValDispatcher<R_r1_r2_r3<R_key_value<int,
  int>, int, int>>>();
  __k3_context::__clonable_dispatchers[__global_context::__nd_delete_S_rcv_push_s6_m___SQL_SUM_AGGREGATE_2_mS3_tid] = make_shared<ValDispatcher<R_r1_r2_r3_r4<_Set<R_r1_r2_r3<R_key_value<int,
  int>, int, int>>, R_key_value<int, int>, int, int>>>();
  __k3_context::__clonable_dispatchers[__global_context::__nd_delete_S_rcv_push_s6_m___SQL_SUM_AGGREGATE_1_mS1_tid] = make_shared<ValDispatcher<R_r1_r2_r3_r4<_Set<R_r1_r2_r3<R_key_value<int,
  int>, int, int>>, R_key_value<int, int>, int, int>>>();
  __k3_context::__clonable_dispatchers[__global_context::__nd_delete_S_rcv_push_s4_m___SQL_SUM_AGGREGATE_1_mS1_tid] = make_shared<ValDispatcher<R_r1_r2_r3_r4<_Set<R_r1_r2_r3<R_key_value<int,
  int>, int, int>>, R_key_value<int, int>, int, int>>>();
  __k3_context::__clonable_dispatchers[__global_context::__nd_delete_S_send_push_s6_m___SQL_SUM_AGGREGATE_2_mS3_tid] = make_shared<ValDispatcher<R_r1_r2_r3<R_key_value<int,
  int>, int, int>>>();
  __k3_context::__clonable_dispatchers[__global_context::__nd_delete_S_send_push_s6_m___SQL_SUM_AGGREGATE_1_mS1_tid] = make_shared<ValDispatcher<R_r1_r2_r3<R_key_value<int,
  int>, int, int>>>();
  __k3_context::__clonable_dispatchers[__global_context::__nd_delete_S_send_push_s4_m___SQL_SUM_AGGREGATE_1_mS1_tid] = make_shared<ValDispatcher<R_r1_r2_r3<R_key_value<int,
  int>, int, int>>>();
  __k3_context::__clonable_dispatchers[__global_context::__nd_delete_S_rcv_fetch_tid] = make_shared<ValDispatcher<R_r1_r2_r3_r4<_Collection<R_key_value<int,
  int>>, R_key_value<int, int>, int, int>>>();
  __k3_context::__clonable_dispatchers[__global_context::__nd_delete_S_rcv_put_tid] = make_shared<ValDispatcher<R_r1_r2_r3_r4_r5<Address,
  _Collection<R_key_value<int, int>>, R_key_value<int, int>, int, int>>>();
  __k3_context::__clonable_dispatchers[__global_context::__insert_S_rcv_corrective_s2_m___SQL_SUM_AGGREGATE_2_mS3_tid] = make_shared<ValDispatcher<R_r1_r2_r3_r4_r5_r6_r7<Address,
  int, R_key_value<int, int>, int, R_key_value<int, int>, _Seq<R_key_value<int, int>>,
  _Set<R_key_value<int, int>>>>>();
  __k3_context::__clonable_dispatchers[__global_context::__insert_S_rcv_corrective_s2_m___SQL_SUM_AGGREGATE_1_mS1_tid] = make_shared<ValDispatcher<R_r1_r2_r3_r4_r5_r6_r7<Address,
  int, R_key_value<int, int>, int, R_key_value<int, int>, _Seq<R_key_value<int, int>>,
  _Set<R_key_value<int, int>>>>>();
  __k3_context::__clonable_dispatchers[__global_context::__insert_S_rcv_corrective_s0_m___SQL_SUM_AGGREGATE_1_mS1_tid] = make_shared<ValDispatcher<R_r1_r2_r3_r4_r5_r6_r7<Address,
  int, R_key_value<int, int>, int, R_key_value<int, int>, _Seq<R_key_value<int, int>>,
  _Set<R_key_value<int, int>>>>>();
  __k3_context::__clonable_dispatchers[__global_context::__nd_insert_S_do_complete_s3_trig_tid] = make_shared<ValDispatcher<R_r1_r2_r3<R_key_value<int,
  int>, int, int>>>();
  __k3_context::__clonable_dispatchers[__global_context::__nd_insert_S_do_complete_s1_trig_tid] = make_shared<ValDispatcher<R_r1_r2_r3<R_key_value<int,
  int>, int, int>>>();
  __k3_context::__clonable_dispatchers[__global_context::__nd_insert_S_rcv_push_s2_m___SQL_SUM_AGGREGATE_2_mS3_tid] = make_shared<ValDispatcher<R_r1_r2_r3_r4<_Set<R_r1_r2_r3<R_key_value<int,
  int>, int, int>>, R_key_value<int, int>, int, int>>>();
  __k3_context::__clonable_dispatchers[__global_context::__nd_insert_S_rcv_push_s2_m___SQL_SUM_AGGREGATE_1_mS1_tid] = make_shared<ValDispatcher<R_r1_r2_r3_r4<_Set<R_r1_r2_r3<R_key_value<int,
  int>, int, int>>, R_key_value<int, int>, int, int>>>();
  __k3_context::__clonable_dispatchers[__global_context::__nd_insert_S_rcv_push_s0_m___SQL_SUM_AGGREGATE_1_mS1_tid] = make_shared<ValDispatcher<R_r1_r2_r3_r4<_Set<R_r1_r2_r3<R_key_value<int,
  int>, int, int>>, R_key_value<int, int>, int, int>>>();
  __k3_context::__clonable_dispatchers[__global_context::__nd_insert_S_send_push_s2_m___SQL_SUM_AGGREGATE_2_mS3_tid] = make_shared<ValDispatcher<R_r1_r2_r3<R_key_value<int,
  int>, int, int>>>();
  __k3_context::__clonable_dispatchers[__global_context::__nd_insert_S_send_push_s2_m___SQL_SUM_AGGREGATE_1_mS1_tid] = make_shared<ValDispatcher<R_r1_r2_r3<R_key_value<int,
  int>, int, int>>>();
  __k3_context::__clonable_dispatchers[__global_context::__nd_insert_S_send_push_s0_m___SQL_SUM_AGGREGATE_1_mS1_tid] = make_shared<ValDispatcher<R_r1_r2_r3<R_key_value<int,
  int>, int, int>>>();
  __k3_context::__clonable_dispatchers[__global_context::__nd_insert_S_rcv_fetch_tid] = make_shared<ValDispatcher<R_r1_r2_r3_r4<_Collection<R_key_value<int,
  int>>, R_key_value<int, int>, int, int>>>();
  __k3_context::__clonable_dispatchers[__global_context::__nd_insert_S_rcv_put_tid] = make_shared<ValDispatcher<R_r1_r2_r3_r4_r5<Address,
  _Collection<R_key_value<int, int>>, R_key_value<int, int>, int, int>>>();
  __k3_context::__clonable_dispatchers[__global_context::__nd_rcv_corr_done_tid] = make_shared<ValDispatcher<R_r1_r2_r3_r4<R_key_value<int,
  int>, int, int, int>>>();
  __k3_context::__clonable_dispatchers[__global_context::__sw_driver_trig_tid] = make_shared<ValDispatcher<unit_t>>();
  __k3_context::__clonable_dispatchers[__global_context::__sw_demux_tid] = make_shared<ValDispatcher<R_r1_r2_r3_r4_r5<int,
  int, int, int, int>>>();
  __k3_context::__clonable_dispatchers[__global_context::__tm_check_time_tid] = make_shared<ValDispatcher<unit_t>>();
  __k3_context::__clonable_dispatchers[__global_context::__tm_insert_timer_tid] = make_shared<ValDispatcher<R_r1_r2_r3<int,
  int, Address>>>();
  __k3_context::__clonable_dispatchers[__global_context::__sw_rcv_token_tid] = make_shared<ValDispatcher<R_key_value<int,
  int>>>();
  __k3_context::__clonable_dispatchers[__global_context::__do_gc_tid] = make_shared<ValDispatcher<R_key_value<int,
  int>>>();
  __k3_context::__clonable_dispatchers[__global_context::__ms_send_gc_req_tid] = make_shared<ValDispatcher<unit_t>>();
  __k3_context::__clonable_dispatchers[__global_context::__rcv_req_gc_vid_tid] = make_shared<ValDispatcher<unit_t>>();
  __k3_context::__clonable_dispatchers[__global_context::__ms_rcv_gc_vid_tid] = make_shared<ValDispatcher<R_key_value<Address,
  R_key_value<int, int>>>>();
  __k3_context::__clonable_dispatchers[__global_context::__sw_ack_rcv_tid] = make_shared<ValDispatcher<R_key_value<Address,
  R_key_value<int, int>>>>();
  __k3_context::__clonable_dispatchers[__global_context::__ms_rcv_switch_done_tid] = make_shared<ValDispatcher<unit_t>>();
  __k3_context::__clonable_dispatchers[__global_context::__nd_rcv_done_tid] = make_shared<ValDispatcher<unit_t>>();
  __k3_context::__clonable_dispatchers[__global_context::__ms_rcv_node_done_tid] = make_shared<ValDispatcher<bool>>();
  __k3_context::__clonable_dispatchers[__global_context::__ms_shutdown_tid] = make_shared<ValDispatcher<unit_t>>();
  __k3_context::__clonable_dispatchers[__global_context::__shutdown_trig_tid] = make_shared<ValDispatcher<unit_t>>();
  __k3_context::__clonable_dispatchers[__global_context::__ms_send_addr_self_tid] = make_shared<ValDispatcher<unit_t>>();
  __k3_context::__clonable_dispatchers[__global_context::__rcv_master_addr_tid] = make_shared<ValDispatcher<Address>>();
  __k3_context::__clonable_dispatchers[__global_context::__ms_rcv_job_tid] = make_shared<ValDispatcher<R_key_value<Address,
  int>>>();
  __k3_context::__clonable_dispatchers[__global_context::__rcv_jobs_tid] = make_shared<ValDispatcher<_Map<R_key_value<Address,
  int>>>>();
  __k3_context::__clonable_dispatchers[__global_context::__ms_rcv_jobs_ack_tid] = make_shared<ValDispatcher<unit_t>>();
  __k3_context::__clonable_dispatchers[__global_context::__sw_rcv_init_tid] = make_shared<ValDispatcher<unit_t>>();
  __k3_context::__clonable_dispatchers[__global_context::__ms_rcv_sw_init_ack_tid] = make_shared<ValDispatcher<unit_t>>();
  Options opt;
  if (opt.parse(argc, argv)) {
    return 0;
  }
  runProgram<__global_context>(opt);
}