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
#include "Builtins.hpp"
#include "Run.hpp"
#include "Prettify.hpp"
#include "dataspace/Dataspace.hpp"
#include "strtk.hpp"
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
using K3::DefaultInternalCodec;
using std::make_tuple;
using std::make_shared;
using std::shared_ptr;
using std::get;
using std::map;
using std::list;
using std::ostringstream;
#ifndef K3_R_adRevenue_destURL_sourceIP_visitDate
#define K3_R_adRevenue_destURL_sourceIP_visitDate
template <class _T0, class _T1, class _T2, class _T3>
class R_adRevenue_destURL_sourceIP_visitDate {
  public:
      R_adRevenue_destURL_sourceIP_visitDate(): adRevenue(), destURL(), sourceIP(), visitDate()  {}
      template <class __T0, class __T1, class __T2, class __T3>
      R_adRevenue_destURL_sourceIP_visitDate(__T0&& _adRevenue, __T1&& _destURL, __T2&& _sourceIP,
      __T3&& _visitDate): adRevenue(std::forward<__T0>(_adRevenue)),
      destURL(std::forward<__T1>(_destURL)), sourceIP(std::forward<__T2>(_sourceIP)),
      visitDate(std::forward<__T3>(_visitDate))  {}
      bool operator==(const R_adRevenue_destURL_sourceIP_visitDate<_T0, _T1, _T2,
      _T3>& __other) const {
        return adRevenue == (__other.adRevenue) && destURL == (__other.destURL) && sourceIP == (__other.sourceIP) && visitDate == (__other.visitDate);
      }
      bool operator!=(const R_adRevenue_destURL_sourceIP_visitDate<_T0, _T1, _T2,
      _T3>& __other) const {
        return std::tie(adRevenue, destURL, sourceIP, visitDate) != std::tie(__other.adRevenue,
        __other.destURL, __other.sourceIP, __other.visitDate);
      }
      bool operator<(const R_adRevenue_destURL_sourceIP_visitDate<_T0, _T1, _T2,
      _T3>& __other) const {
        return std::tie(adRevenue, destURL, sourceIP, visitDate) < std::tie(__other.adRevenue,
        __other.destURL, __other.sourceIP, __other.visitDate);
      }
      bool operator>(const R_adRevenue_destURL_sourceIP_visitDate<_T0, _T1, _T2,
      _T3>& __other) const {
        return std::tie(adRevenue, destURL, sourceIP, visitDate) > std::tie(__other.adRevenue,
        __other.destURL, __other.sourceIP, __other.visitDate);
      }
      template <class archive>
      void serialize(archive& _archive, const unsigned int _version)  {
        _archive & adRevenue;
        _archive & destURL;
        _archive & sourceIP;
        _archive & visitDate;
      }
      _T0 adRevenue;
      _T1 destURL;
      _T2 sourceIP;
      _T3 visitDate;
};
#endif
namespace YAML {
  template <class _T0, class _T1, class _T2, class _T3>
  class convert<R_adRevenue_destURL_sourceIP_visitDate<_T0, _T1, _T2, _T3>> {
    public:
        static Node encode(const R_adRevenue_destURL_sourceIP_visitDate<_T0, _T1, _T2, _T3>& r)  {
          Node node;
          node["adRevenue"] = convert<_T0>::encode(r.adRevenue);
          node["destURL"] = convert<_T1>::encode(r.destURL);
          node["sourceIP"] = convert<_T2>::encode(r.sourceIP);
          node["visitDate"] = convert<_T3>::encode(r.visitDate);
          return node;
        }
        static bool decode(const Node& node, R_adRevenue_destURL_sourceIP_visitDate<_T0, _T1, _T2,
        _T3>& r)  {
          if (!(node.IsMap())) {
            return false;
          }
          if (node["adRevenue"]) {
            r.adRevenue = (node["adRevenue"]).as<_T0>();
          }
          if (node["destURL"]) {
            r.destURL = (node["destURL"]).as<_T1>();
          }
          if (node["sourceIP"]) {
            r.sourceIP = (node["sourceIP"]).as<_T2>();
          }
          if (node["visitDate"]) {
            r.visitDate = (node["visitDate"]).as<_T3>();
          }
          return true;
        }
  };
}
#ifndef K3_R_adRevenue_destURL_sourceIP_visitDate_hash_value
#define K3_R_adRevenue_destURL_sourceIP_visitDate_hash_value
template <class _T0, class _T1, class _T2, class _T3>
std::size_t hash_value(const R_adRevenue_destURL_sourceIP_visitDate<_T0, _T1, _T2, _T3>& r)  {
  boost::hash<std::tuple<_T0, _T1, _T2, _T3>> hasher;
  return hasher(std::tie(r.adRevenue, r.destURL, r.sourceIP, r.visitDate));
}
#endif
#ifndef K3_R_addr
#define K3_R_addr
template <class _T0>
class R_addr {
  public:
      R_addr(): addr()  {}
      R_addr(const _T0& _addr): addr(_addr)  {}
      R_addr(_T0&& _addr): addr(std::move(_addr))  {}
      bool operator==(const R_addr<_T0>& __other) const {
        return addr == (__other.addr);
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
      template <class archive>
      void serialize(archive& _archive, const unsigned int _version)  {
        _archive & addr;
      }
      _T0 addr;
};
#endif
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
          if (!(node.IsMap())) {
            return false;
          }
          if (node["addr"]) {
            r.addr = (node["addr"]).as<_T0>();
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
#ifndef K3_R_arSum_prCount_prSum
#define K3_R_arSum_prCount_prSum
template <class _T0, class _T1, class _T2>
class R_arSum_prCount_prSum {
  public:
      R_arSum_prCount_prSum(): arSum(), prCount(), prSum()  {}
      template <class __T0, class __T1, class __T2>
      R_arSum_prCount_prSum(__T0&& _arSum, __T1&& _prCount,
      __T2&& _prSum): arSum(std::forward<__T0>(_arSum)), prCount(std::forward<__T1>(_prCount)),
      prSum(std::forward<__T2>(_prSum))  {}
      bool operator==(const R_arSum_prCount_prSum<_T0, _T1, _T2>& __other) const {
        return arSum == (__other.arSum) && prCount == (__other.prCount) && prSum == (__other.prSum);
      }
      bool operator!=(const R_arSum_prCount_prSum<_T0, _T1, _T2>& __other) const {
        return std::tie(arSum, prCount, prSum) != std::tie(__other.arSum, __other.prCount,
        __other.prSum);
      }
      bool operator<(const R_arSum_prCount_prSum<_T0, _T1, _T2>& __other) const {
        return std::tie(arSum, prCount, prSum) < std::tie(__other.arSum, __other.prCount,
        __other.prSum);
      }
      bool operator>(const R_arSum_prCount_prSum<_T0, _T1, _T2>& __other) const {
        return std::tie(arSum, prCount, prSum) > std::tie(__other.arSum, __other.prCount,
        __other.prSum);
      }
      template <class archive>
      void serialize(archive& _archive, const unsigned int _version)  {
        _archive & arSum;
        _archive & prCount;
        _archive & prSum;
      }
      _T0 arSum;
      _T1 prCount;
      _T2 prSum;
};
#endif
namespace YAML {
  template <class _T0, class _T1, class _T2>
  class convert<R_arSum_prCount_prSum<_T0, _T1, _T2>> {
    public:
        static Node encode(const R_arSum_prCount_prSum<_T0, _T1, _T2>& r)  {
          Node node;
          node["arSum"] = convert<_T0>::encode(r.arSum);
          node["prCount"] = convert<_T1>::encode(r.prCount);
          node["prSum"] = convert<_T2>::encode(r.prSum);
          return node;
        }
        static bool decode(const Node& node, R_arSum_prCount_prSum<_T0, _T1, _T2>& r)  {
          if (!(node.IsMap())) {
            return false;
          }
          if (node["arSum"]) {
            r.arSum = (node["arSum"]).as<_T0>();
          }
          if (node["prCount"]) {
            r.prCount = (node["prCount"]).as<_T1>();
          }
          if (node["prSum"]) {
            r.prSum = (node["prSum"]).as<_T2>();
          }
          return true;
        }
  };
}
#ifndef K3_R_arSum_prCount_prSum_hash_value
#define K3_R_arSum_prCount_prSum_hash_value
template <class _T0, class _T1, class _T2>
std::size_t hash_value(const R_arSum_prCount_prSum<_T0, _T1, _T2>& r)  {
  boost::hash<std::tuple<_T0, _T1, _T2>> hasher;
  return hasher(std::tie(r.arSum, r.prCount, r.prSum));
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
        return arg == (__other.arg);
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
      template <class archive>
      void serialize(archive& _archive, const unsigned int _version)  {
        _archive & arg;
      }
      _T0 arg;
};
#endif
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
          if (!(node.IsMap())) {
            return false;
          }
          if (node["arg"]) {
            r.arg = (node["arg"]).as<_T0>();
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
#ifndef K3_R_avgPageRank_sourceIP_totalRevenue
#define K3_R_avgPageRank_sourceIP_totalRevenue
template <class _T0, class _T1, class _T2>
class R_avgPageRank_sourceIP_totalRevenue {
  public:
      R_avgPageRank_sourceIP_totalRevenue(): avgPageRank(), sourceIP(), totalRevenue()  {}
      template <class __T0, class __T1, class __T2>
      R_avgPageRank_sourceIP_totalRevenue(__T0&& _avgPageRank, __T1&& _sourceIP,
      __T2&& _totalRevenue): avgPageRank(std::forward<__T0>(_avgPageRank)),
      sourceIP(std::forward<__T1>(_sourceIP)), totalRevenue(std::forward<__T2>(_totalRevenue))  {}
      bool operator==(const R_avgPageRank_sourceIP_totalRevenue<_T0, _T1, _T2>& __other) const {
        return avgPageRank == (__other.avgPageRank) && sourceIP == (__other.sourceIP) && totalRevenue == (__other.totalRevenue);
      }
      bool operator!=(const R_avgPageRank_sourceIP_totalRevenue<_T0, _T1, _T2>& __other) const {
        return std::tie(avgPageRank, sourceIP, totalRevenue) != std::tie(__other.avgPageRank,
        __other.sourceIP, __other.totalRevenue);
      }
      bool operator<(const R_avgPageRank_sourceIP_totalRevenue<_T0, _T1, _T2>& __other) const {
        return std::tie(avgPageRank, sourceIP, totalRevenue) < std::tie(__other.avgPageRank,
        __other.sourceIP, __other.totalRevenue);
      }
      bool operator>(const R_avgPageRank_sourceIP_totalRevenue<_T0, _T1, _T2>& __other) const {
        return std::tie(avgPageRank, sourceIP, totalRevenue) > std::tie(__other.avgPageRank,
        __other.sourceIP, __other.totalRevenue);
      }
      template <class archive>
      void serialize(archive& _archive, const unsigned int _version)  {
        _archive & avgPageRank;
        _archive & sourceIP;
        _archive & totalRevenue;
      }
      _T0 avgPageRank;
      _T1 sourceIP;
      _T2 totalRevenue;
};
#endif
namespace YAML {
  template <class _T0, class _T1, class _T2>
  class convert<R_avgPageRank_sourceIP_totalRevenue<_T0, _T1, _T2>> {
    public:
        static Node encode(const R_avgPageRank_sourceIP_totalRevenue<_T0, _T1, _T2>& r)  {
          Node node;
          node["avgPageRank"] = convert<_T0>::encode(r.avgPageRank);
          node["sourceIP"] = convert<_T1>::encode(r.sourceIP);
          node["totalRevenue"] = convert<_T2>::encode(r.totalRevenue);
          return node;
        }
        static bool decode(const Node& node, R_avgPageRank_sourceIP_totalRevenue<_T0, _T1,
        _T2>& r)  {
          if (!(node.IsMap())) {
            return false;
          }
          if (node["avgPageRank"]) {
            r.avgPageRank = (node["avgPageRank"]).as<_T0>();
          }
          if (node["sourceIP"]) {
            r.sourceIP = (node["sourceIP"]).as<_T1>();
          }
          if (node["totalRevenue"]) {
            r.totalRevenue = (node["totalRevenue"]).as<_T2>();
          }
          return true;
        }
  };
}
#ifndef K3_R_avgPageRank_sourceIP_totalRevenue_hash_value
#define K3_R_avgPageRank_sourceIP_totalRevenue_hash_value
template <class _T0, class _T1, class _T2>
std::size_t hash_value(const R_avgPageRank_sourceIP_totalRevenue<_T0, _T1, _T2>& r)  {
  boost::hash<std::tuple<_T0, _T1, _T2>> hasher;
  return hasher(std::tie(r.avgPageRank, r.sourceIP, r.totalRevenue));
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
        return key == (__other.key) && value == (__other.value);
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
      template <class archive>
      void serialize(archive& _archive, const unsigned int _version)  {
        _archive & key;
        _archive & value;
      }
      _T0 key;
      _T1 value;
};
#endif
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
          if (!(node.IsMap())) {
            return false;
          }
          if (node["key"]) {
            r.key = (node["key"]).as<_T0>();
          }
          if (node["value"]) {
            r.value = (node["value"]).as<_T1>();
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
#ifndef K3_R_path
#define K3_R_path
template <class _T0>
class R_path {
  public:
      R_path(): path()  {}
      R_path(const _T0& _path): path(_path)  {}
      R_path(_T0&& _path): path(std::move(_path))  {}
      bool operator==(const R_path<_T0>& __other) const {
        return path == (__other.path);
      }
      bool operator!=(const R_path<_T0>& __other) const {
        return std::tie(path) != std::tie(__other.path);
      }
      bool operator<(const R_path<_T0>& __other) const {
        return std::tie(path) < std::tie(__other.path);
      }
      bool operator>(const R_path<_T0>& __other) const {
        return std::tie(path) > std::tie(__other.path);
      }
      template <class archive>
      void serialize(archive& _archive, const unsigned int _version)  {
        _archive & path;
      }
      _T0 path;
};
#endif
namespace YAML {
  template <class _T0>
  class convert<R_path<_T0>> {
    public:
        static Node encode(const R_path<_T0>& r)  {
          Node node;
          node["path"] = convert<_T0>::encode(r.path);
          return node;
        }
        static bool decode(const Node& node, R_path<_T0>& r)  {
          if (!(node.IsMap())) {
            return false;
          }
          if (node["path"]) {
            r.path = (node["path"]).as<_T0>();
          }
          return true;
        }
  };
}
#ifndef K3_R_path_hash_value
#define K3_R_path_hash_value
template <class _T0>
std::size_t hash_value(const R_path<_T0>& r)  {
  boost::hash<std::tuple<_T0>> hasher;
  return hasher(std::tie(r.path));
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
        _archive & boost::serialization::base_object<K3::Collection<__CONTENT>>(*this);
      }
};
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
template <class __CONTENT>
class _Map: public K3::Map<__CONTENT> {
  public:
      _Map(): K3::Map<__CONTENT>()  {}
      _Map(const K3::Map<__CONTENT>& __other1): K3::Map<__CONTENT>(__other1)  {}
      _Map(K3::Map<__CONTENT>&& __other1): K3::Map<__CONTENT>(std::move(__other1))  {}
      template <class archive>
      void serialize(archive& _archive, const unsigned int _version)  {
        _archive & boost::serialization::base_object<K3::Map<__CONTENT>>(*this);
      }
};
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
template <class __CONTENT>
class _Seq: public K3::Seq<__CONTENT> {
  public:
      _Seq(): K3::Seq<__CONTENT>()  {}
      _Seq(const K3::Seq<__CONTENT>& __other1): K3::Seq<__CONTENT>(__other1)  {}
      _Seq(K3::Seq<__CONTENT>&& __other1): K3::Seq<__CONTENT>(std::move(__other1))  {}
      template <class archive>
      void serialize(archive& _archive, const unsigned int _version)  {
        _archive & boost::serialization::base_object<K3::Seq<__CONTENT>>(*this);
      }
};
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
class __global_context: public K3::__standard_context, public K3::__string_context,
public K3::__time_context {
  public:
      __global_context(Engine& __engine): K3::__standard_context(__engine), K3::__string_context(),
      K3::__time_context()  {
        dispatch_table[__shutdown_tid] = [this] (void* payload)   {
          shutdown_(*(static_cast<unit_t*>(payload)));
        };
        dispatch_table[__loadAll_tid] = [this] (void* payload)   {
          loadAll(*(static_cast<unit_t*>(payload)));
        };
        dispatch_table[__ready_tid] = [this] (void* payload)   {
          ready(*(static_cast<unit_t*>(payload)));
        };
        dispatch_table[__finished_tid] = [this] (void* payload)   {
          finished(*(static_cast<unit_t*>(payload)));
        };
        dispatch_table[__find_global_max_tid] = [this] (void* payload)   {
          find_global_max(*(static_cast<R_key_value<K3::base_string, R_arSum_prCount_prSum<double,
          int, int>>*>(payload)));
        };
        dispatch_table[__find_local_max_tid] = [this] (void* payload)   {
          find_local_max(*(static_cast<unit_t*>(payload)));
        };
        dispatch_table[__count_merges_tid] = [this] (void* payload)   {
          count_merges(*(static_cast<unit_t*>(payload)));
        };
        dispatch_table[__merge_tid] = [this] (void* payload)   {
          merge(*(static_cast<_Map<R_key_value<K3::base_string, R_arSum_prCount_prSum<double, int,
          int>>>*>(payload)));
        };
        dispatch_table[__q3_local_tid] = [this] (void* payload)   {
          q3_local(*(static_cast<unit_t*>(payload)));
        };
      }
      static int __q3_local_tid;
      static int __merge_tid;
      static int __count_merges_tid;
      static int __find_local_max_tid;
      static int __find_global_max_tid;
      static int __finished_tid;
      static int __ready_tid;
      static int __loadAll_tid;
      static int __shutdown_tid;
      Address me;
      _Collection<R_addr<Address>> peers;
      std::tuple<_Collection<R_arg<K3::base_string>>, _Collection<R_key_value<K3::base_string,
      K3::base_string>>> args;
      K3::base_string role;
      Address master;
      bool __master_set__ = false;
      _Seq<R_addr<Address>> peer_seq;
      int index_by_hash(const K3::base_string& s)  {
        {
          int n;
          n = peer_seq.size(unit_t {});
          {
            int h;
            h = hash(s);
            return (h % n + n) % n;
          }
        }
      }
      int start_ms;
      int end_ms;
      int elapsed_ms;
      _Collection<R_adRevenue_destURL_sourceIP_visitDate<double, K3::base_string, K3::base_string,
      K3::base_string>> user_visits;
      static _Map<R_key_value<K3::base_string, int>> rankingsMap;
      _Map<R_key_value<K3::base_string, R_arSum_prCount_prSum<double, int,
      int>>> empty_source_ip_to_aggs_C;
      R_arSum_prCount_prSum<double, int, int> empty_aggs;
      _Map<R_key_value<int, _Map<R_key_value<K3::base_string, R_arSum_prCount_prSum<double, int,
      int>>>>> partial_aggs;
      _Map<R_key_value<K3::base_string, R_arSum_prCount_prSum<double, int, int>>> merged_partials;
      K3::base_string date_lb;
      bool __date_lb_set__ = false;
      K3::base_string date_ub;
      bool __date_ub_set__ = false;
      bool valid_date(const K3::base_string& i)  {
        return i <= date_ub && i >= date_lb;
      }
      bool flag;
      bool __flag_set__ = false;
      bool flag2;
      bool __flag2_set__ = false;
      unit_t q3_local(unit_t _)  {
        user_visits.iterate(std::move([this] (const R_adRevenue_destURL_sourceIP_visitDate<double,
        K3::base_string, K3::base_string, K3::base_string>& uv) mutable  {
          if (valid_date(std::move(uv.visitDate))) {
            {
              K3::base_string ip;
              ip = uv.sourceIP;
              {
                K3::base_string url;
                url = uv.destURL;
                {
                  int a;
                  a = index_by_hash(ip);
                  shared_ptr<R_key_value<int, _Map<R_key_value<K3::base_string,
                  R_arSum_prCount_prSum<double, int, int>>>>> __0;
                  __0 = partial_aggs.lookup(R_key_value<int, _Map<R_key_value<K3::base_string,
                  R_arSum_prCount_prSum<double, int, int>>>> {a, empty_source_ip_to_aggs_C});
                  if (__0) {
                    auto& aggs = *(__0);
                    shared_ptr<R_key_value<K3::base_string, int>> __1;
                    __1 = rankingsMap.lookup(R_key_value<K3::base_string, int> {url, 0});
                    if (__1) {
                      auto& r = *(__1);
                      shared_ptr<R_key_value<K3::base_string, R_arSum_prCount_prSum<double, int,
                      int>>> __2;
                      __2 = (aggs.value).lookup(R_key_value<K3::base_string,
                      R_arSum_prCount_prSum<double, int, int>> {ip, empty_aggs});
                      if (__2) {
                        auto& pa_v = *(__2);
                        (aggs.value).insert(R_key_value<K3::base_string,
                        R_arSum_prCount_prSum<double, int, int>> {ip, R_arSum_prCount_prSum<double,
                        int, int> {((pa_v.value).arSum) + (uv.adRevenue),
                        ((pa_v.value).prCount) + (1), ((pa_v.value).prSum) + (r.value)}});
                      } else {
                        (aggs.value).insert(R_key_value<K3::base_string,
                        R_arSum_prCount_prSum<double, int, int>> {ip, R_arSum_prCount_prSum<double,
                        int, int> {uv.adRevenue, 1, r.value}});
                      }
                      return partial_aggs.insert(R_key_value<int, _Map<R_key_value<K3::base_string,
                      R_arSum_prCount_prSum<double, int, int>>>> {a, aggs.value});
                    } else {
                      return unit_t {};
                    }
                  } else {
                    shared_ptr<R_key_value<K3::base_string, int>> __3;
                    __3 = rankingsMap.lookup(R_key_value<K3::base_string, int> {url, 0});
                    if (__3) {
                      auto& r = *(__3);
                      _Map<R_key_value<K3::base_string, R_arSum_prCount_prSum<double, int,
                      int>>> __4;
                      {
                        _Map<R_key_value<K3::base_string, R_arSum_prCount_prSum<double, int,
                        int>>> __collection;
                        __collection = _Map<R_key_value<K3::base_string,
                        R_arSum_prCount_prSum<double, int, int>>> {};
                        __collection.insert(R_key_value<K3::base_string,
                        R_arSum_prCount_prSum<double, int, int>> {ip, R_arSum_prCount_prSum<double,
                        int, int> {uv.adRevenue, 1, r.value}});
                        __4 = __collection;
                      }
                      return partial_aggs.insert(R_key_value<int, _Map<R_key_value<K3::base_string,
                      R_arSum_prCount_prSum<double, int, int>>>> {a, __4});
                    } else {
                      return unit_t {};
                    }
                  }
                }
              }
            }
          } else {
            return unit_t {};
          }
        }));
        partial_aggs.iterate(std::move([this] (const R_key_value<int,
        _Map<R_key_value<K3::base_string, R_arSum_prCount_prSum<double, int, int>>>>& kv) mutable  {
          auto __5 = std::make_shared<K3::ValDispatcher<_Map<R_key_value<K3::base_string,
          R_arSum_prCount_prSum<double, int, int>>>>>(kv.value);
          __engine.send((peer_seq.at(std::move(kv.key))).addr, __merge_tid, __5);
          return unit_t {};
        }));
        return peers.iterate(std::move([this] (const R_addr<Address>& p) mutable  {
          auto __6 = std::make_shared<K3::ValDispatcher<unit_t>>(unit_t {});
          __engine.send(p.addr, __count_merges_tid, __6);
          return unit_t {};
        }));
      }
      unit_t merge(const _Map<R_key_value<K3::base_string, R_arSum_prCount_prSum<double, int,
      int>>>& aggs_map)  {
        return aggs_map.iterate(std::move([this] (const R_key_value<K3::base_string,
        R_arSum_prCount_prSum<double, int, int>>& kv) mutable  {
          shared_ptr<R_key_value<K3::base_string, R_arSum_prCount_prSum<double, int, int>>> __7;
          __7 = merged_partials.lookup(R_key_value<K3::base_string, R_arSum_prCount_prSum<double,
          int, int>> {kv.key, kv.value});
          if (__7) {
            auto& agg = *(__7);
            return merged_partials.insert(R_key_value<K3::base_string, R_arSum_prCount_prSum<double,
            int, int>> {kv.key, R_arSum_prCount_prSum<double, int,
            int> {((agg.value).arSum) + ((kv.value).arSum),
            ((agg.value).prCount) + ((kv.value).prCount),
            ((agg.value).prSum) + ((kv.value).prSum)}});
          } else {
            return merged_partials.insert(std::move(kv));
          }
        }));
      }
      int merged_peers;
      bool __merged_peers_set__ = false;
      unit_t count_merges(unit_t _)  {
        merged_peers = merged_peers + (1);
        if (merged_peers == peers.size(unit_t {})) {
          auto __8 = std::make_shared<K3::ValDispatcher<unit_t>>(unit_t {});
          __engine.send(me, __find_local_max_tid, __8);
          return unit_t {};
        } else {
          return unit_t {};
        }
      }
      R_key_value<K3::base_string, R_arSum_prCount_prSum<double, int, int>> local_max;
      unit_t find_local_max(unit_t _)  {
        merged_partials.iterate(std::move([this] (const R_key_value<K3::base_string,
        R_arSum_prCount_prSum<double, int, int>>& kv) mutable  {
          if (((kv.value).arSum) > ((local_max.value).arSum)) {
            local_max = kv;
            return unit_t {};
          } else {
            return unit_t {};
          }
        }));
        auto __9 = std::make_shared<K3::ValDispatcher<R_key_value<K3::base_string,
        R_arSum_prCount_prSum<double, int, int>>>>(local_max);
        __engine.send(master, __find_global_max_tid, __9);
        return unit_t {};
      }
      int peers_done;
      bool __peers_done_set__ = false;
      R_key_value<K3::base_string, R_arSum_prCount_prSum<double, int, int>> global_max;
      unit_t find_global_max(const R_key_value<K3::base_string, R_arSum_prCount_prSum<double, int,
      int>>& kv)  {
        if (((kv.value).arSum) > ((global_max.value).arSum)) {
          global_max = kv;
        }
        peers_done = peers_done + (1);
        if (peers_done == peers.size(unit_t {})) {
          auto __10 = std::make_shared<K3::ValDispatcher<unit_t>>(unit_t {});
          __engine.send(master, __finished_tid, __10);
          return unit_t {};
        } else {
          return unit_t {};
        }
      }
      K3::base_string outFile;
      _Collection<R_avgPageRank_sourceIP_totalRevenue<double, K3::base_string, double>> q3_result;
      unit_t q3Logger(string file, _Collection<R_avgPageRank_sourceIP_totalRevenue<double,
      K3::base_string, double>>& c, const string& sep)  {
        return logHelper(file, c, [] (std::ofstream& file,
        const R_avgPageRank_sourceIP_totalRevenue<double, K3::base_string, double>& elem,
        const string& sep)   {
          file << (elem.sourceIP);
          file << sep;
          file << (elem.totalRevenue);
          file << sep;
          file << (elem.avgPageRank);
        }, sep);
      }
      unit_t logOutput(unit_t _)  {
        q3_result.insert(R_avgPageRank_sourceIP_totalRevenue<double, K3::base_string,
        double> {(1.0) * ((global_max.value).prSum) / ((global_max.value).prCount), global_max.key,
        (global_max.value).arSum});
        return q3Logger(outFile, q3_result, "|");
      }
      unit_t finished(unit_t _)  {
        end_ms = now_int(unit_t {});
        elapsed_ms = end_ms - start_ms;
        print(std::move(concat("Elapsed: ", std::move(itos(elapsed_ms)))));
        print(std::move(concat("Global max.key: ", global_max.key)));
        logOutput(unit_t {});
        return peers.iterate(std::move([this] (const R_addr<Address>& p) mutable  {
          auto __11 = std::make_shared<K3::ValDispatcher<unit_t>>(unit_t {});
          __engine.send(p.addr, __shutdown_tid, __11);
          return unit_t {};
        }));
      }
      int peers_ready;
      bool __peers_ready_set__ = false;
      unit_t ready(unit_t _)  {
        peers_ready = peers_ready + (1);
        if (peers_ready == peers.size(unit_t {})) {
          start_ms = now_int(unit_t {});
          return peers.iterate(std::move([this] (const R_addr<Address>& p) mutable  {
            auto __12 = std::make_shared<K3::ValDispatcher<unit_t>>(unit_t {});
            __engine.send(p.addr, __q3_local_tid, __12);
            return unit_t {};
          }));
        } else {
          return unit_t {};
        }
      }
      _Collection<R_path<K3::base_string>> uvFiles;
      _Collection<R_path<K3::base_string>> rkFiles;
      unit_t loadAll(unit_t _)  {
        peers.iterate(std::move([this] (const R_addr<Address>& p) mutable  {
          return peer_seq.insert(std::move(p));
        }));
        uvFiles.iterate(std::move([this] (R_path<K3::base_string> e) mutable  {
          return loadUVQ3(std::move(e.path), user_visits);
        }));
        rkFiles.iterate(std::move([this] (R_path<K3::base_string> e) mutable  {
          return loadRKQ3(std::move(e.path), rankingsMap);
        }));
        auto __13 = std::make_shared<K3::ValDispatcher<unit_t>>(unit_t {});
        __engine.send(master, __ready_tid, __13);
        return unit_t {};
      }
      unit_t shutdown_(unit_t _)  {
        return haltEngine(unit_t {});
      }
      unit_t rowsProcess(unit_t _)  {
        return [this] (const unit_t& next) mutable  {
          auto __14 = std::make_shared<K3::ValDispatcher<unit_t>>(next);
          __engine.send(me, __loadAll_tid, __14);
          return unit_t {};
        }(unit_t {});
      }
      unit_t processRole(const unit_t& _)  {
        if (role == ("rows")) {
          return rowsProcess(unit_t {});
        } else {
          return unit_t {};
        }
      }
      unit_t graphLoader(string file, _Collection<R_key_value<int, int>>& c)  {
        std::ifstream _in;
        _in.open(file);
        K3::read_records(_in, c, [] (std::istream& in, std::string& tmp_buffer)   {
          R_key_value<int, int> record;
          std::getline(in, tmp_buffer, ',');
          record.key = std::atoi(tmp_buffer.c_str());
          std::getline(in, tmp_buffer);
          record.value = std::atoi(tmp_buffer.c_str());
          return record;
        });
        return unit_t {};
      }
      unit_t initDecls(unit_t _)  {
        if (!(__master_set__)) {
          master = make_address("127.0.0.1", 30001);
          __master_set__ = true;
        }
        if (!(__date_lb_set__)) {
          date_lb = "1980-01-01";
          __date_lb_set__ = true;
        }
        if (!(__date_ub_set__)) {
          date_ub = "1990-01-01";
          __date_ub_set__ = true;
        }
        if (!(__flag_set__)) {
          flag = false;
          __flag_set__ = true;
        }
        if (!(__flag2_set__)) {
          flag2 = false;
          __flag2_set__ = true;
        }
        if (!(__merged_peers_set__)) {
          merged_peers = 0;
          __merged_peers_set__ = true;
        }
        if (!(__peers_done_set__)) {
          peers_done = 0;
          __peers_done_set__ = true;
        }
        if (!(__peers_ready_set__)) {
          peers_ready = 0;
          __peers_ready_set__ = true;
        }
        return unit_t {};
      }
      void __patch(std::string);
      std::map<std::string, std::string> __prettify()  {
        std::map<std::string, std::string> __result;
        __result["rkFiles"] = K3::prettify_collection(rkFiles, [] (R_path<K3::base_string> x)   {
          return K3::prettify_record(x, [] (R_path<K3::base_string> x)   {
            ostringstream oss;
            oss << ("{");
            oss << std::string("path:") << K3::prettify_string(x.path);
            oss << ("}");
            return string_impl(oss.str());
          });
        });
        __result["uvFiles"] = K3::prettify_collection(uvFiles, [] (R_path<K3::base_string> x)   {
          return K3::prettify_record(x, [] (R_path<K3::base_string> x)   {
            ostringstream oss;
            oss << ("{");
            oss << std::string("path:") << K3::prettify_string(x.path);
            oss << ("}");
            return string_impl(oss.str());
          });
        });
        __result["peers_ready"] = K3::prettify_int(peers_ready);
        __result["q3_result"] = K3::prettify_collection(q3_result,
        [] (R_avgPageRank_sourceIP_totalRevenue<double, K3::base_string, double> x)   {
          return K3::prettify_record(x, [] (R_avgPageRank_sourceIP_totalRevenue<double,
          K3::base_string, double> x)   {
            ostringstream oss;
            oss << ("{");
            oss << std::string("sourceIP:") << K3::prettify_string(x.sourceIP);
            oss << (",");
            oss << std::string("totalRevenue:") << K3::prettify_real(x.totalRevenue);
            oss << (",");
            oss << std::string("avgPageRank:") << K3::prettify_real(x.avgPageRank);
            oss << ("}");
            return string_impl(oss.str());
          });
        });
        __result["outFile"] = K3::prettify_string(outFile);
        __result["global_max"] = K3::prettify_record(global_max, [] (R_key_value<K3::base_string,
        R_arSum_prCount_prSum<double, int, int>> x)   {
          ostringstream oss;
          oss << ("{");
          oss << std::string("key:") << K3::prettify_string(x.key);
          oss << (",");
          oss << std::string("value:") << K3::prettify_record(x.value,
          [] (R_arSum_prCount_prSum<double, int, int> x)   {
            ostringstream oss;
            oss << ("{");
            oss << std::string("prSum:") << K3::prettify_int(x.prSum);
            oss << (",");
            oss << std::string("prCount:") << K3::prettify_int(x.prCount);
            oss << (",");
            oss << std::string("arSum:") << K3::prettify_real(x.arSum);
            oss << ("}");
            return string_impl(oss.str());
          });
          oss << ("}");
          return string_impl(oss.str());
        });
        __result["peers_done"] = K3::prettify_int(peers_done);
        __result["local_max"] = K3::prettify_record(local_max, [] (R_key_value<K3::base_string,
        R_arSum_prCount_prSum<double, int, int>> x)   {
          ostringstream oss;
          oss << ("{");
          oss << std::string("key:") << K3::prettify_string(x.key);
          oss << (",");
          oss << std::string("value:") << K3::prettify_record(x.value,
          [] (R_arSum_prCount_prSum<double, int, int> x)   {
            ostringstream oss;
            oss << ("{");
            oss << std::string("prSum:") << K3::prettify_int(x.prSum);
            oss << (",");
            oss << std::string("prCount:") << K3::prettify_int(x.prCount);
            oss << (",");
            oss << std::string("arSum:") << K3::prettify_real(x.arSum);
            oss << ("}");
            return string_impl(oss.str());
          });
          oss << ("}");
          return string_impl(oss.str());
        });
        __result["merged_peers"] = K3::prettify_int(merged_peers);
        __result["flag2"] = K3::prettify_bool(flag2);
        __result["flag"] = K3::prettify_bool(flag);
        __result["date_ub"] = K3::prettify_string(date_ub);
        __result["date_lb"] = K3::prettify_string(date_lb);
        __result["merged_partials"] = K3::prettify_collection(merged_partials,
        [] (R_key_value<K3::base_string, R_arSum_prCount_prSum<double, int, int>> x)   {
          return K3::prettify_record(x, [] (R_key_value<K3::base_string,
          R_arSum_prCount_prSum<double, int, int>> x)   {
            ostringstream oss;
            oss << ("{");
            oss << std::string("key:") << K3::prettify_string(x.key);
            oss << (",");
            oss << std::string("value:") << K3::prettify_record(x.value,
            [] (R_arSum_prCount_prSum<double, int, int> x)   {
              ostringstream oss;
              oss << ("{");
              oss << std::string("prSum:") << K3::prettify_int(x.prSum);
              oss << (",");
              oss << std::string("prCount:") << K3::prettify_int(x.prCount);
              oss << (",");
              oss << std::string("arSum:") << K3::prettify_real(x.arSum);
              oss << ("}");
              return string_impl(oss.str());
            });
            oss << ("}");
            return string_impl(oss.str());
          });
        });
        __result["partial_aggs"] = K3::prettify_collection(partial_aggs, [] (R_key_value<int,
        _Map<R_key_value<K3::base_string, R_arSum_prCount_prSum<double, int, int>>>> x)   {
          return K3::prettify_record(x, [] (R_key_value<int, _Map<R_key_value<K3::base_string,
          R_arSum_prCount_prSum<double, int, int>>>> x)   {
            ostringstream oss;
            oss << ("{");
            oss << std::string("key:") << K3::prettify_int(x.key);
            oss << (",");
            oss << std::string("value:") << K3::prettify_collection(x.value,
            [] (R_key_value<K3::base_string, R_arSum_prCount_prSum<double, int, int>> x)   {
              return K3::prettify_record(x, [] (R_key_value<K3::base_string,
              R_arSum_prCount_prSum<double, int, int>> x)   {
                ostringstream oss;
                oss << ("{");
                oss << std::string("key:") << K3::prettify_string(x.key);
                oss << (",");
                oss << std::string("value:") << K3::prettify_record(x.value,
                [] (R_arSum_prCount_prSum<double, int, int> x)   {
                  ostringstream oss;
                  oss << ("{");
                  oss << std::string("prSum:") << K3::prettify_int(x.prSum);
                  oss << (",");
                  oss << std::string("prCount:") << K3::prettify_int(x.prCount);
                  oss << (",");
                  oss << std::string("arSum:") << K3::prettify_real(x.arSum);
                  oss << ("}");
                  return string_impl(oss.str());
                });
                oss << ("}");
                return string_impl(oss.str());
              });
            });
            oss << ("}");
            return string_impl(oss.str());
          });
        });
        __result["empty_aggs"] = K3::prettify_record(empty_aggs, [] (R_arSum_prCount_prSum<double,
        int, int> x)   {
          ostringstream oss;
          oss << ("{");
          oss << std::string("prSum:") << K3::prettify_int(x.prSum);
          oss << (",");
          oss << std::string("prCount:") << K3::prettify_int(x.prCount);
          oss << (",");
          oss << std::string("arSum:") << K3::prettify_real(x.arSum);
          oss << ("}");
          return string_impl(oss.str());
        });
        __result["empty_source_ip_to_aggs_C"] = K3::prettify_collection(empty_source_ip_to_aggs_C,
        [] (R_key_value<K3::base_string, R_arSum_prCount_prSum<double, int, int>> x)   {
          return K3::prettify_record(x, [] (R_key_value<K3::base_string,
          R_arSum_prCount_prSum<double, int, int>> x)   {
            ostringstream oss;
            oss << ("{");
            oss << std::string("key:") << K3::prettify_string(x.key);
            oss << (",");
            oss << std::string("value:") << K3::prettify_record(x.value,
            [] (R_arSum_prCount_prSum<double, int, int> x)   {
              ostringstream oss;
              oss << ("{");
              oss << std::string("prSum:") << K3::prettify_int(x.prSum);
              oss << (",");
              oss << std::string("prCount:") << K3::prettify_int(x.prCount);
              oss << (",");
              oss << std::string("arSum:") << K3::prettify_real(x.arSum);
              oss << ("}");
              return string_impl(oss.str());
            });
            oss << ("}");
            return string_impl(oss.str());
          });
        });
        __result["rankingsMap"] = K3::prettify_collection(rankingsMap,
        [] (R_key_value<K3::base_string, int> x)   {
          return K3::prettify_record(x, [] (R_key_value<K3::base_string, int> x)   {
            ostringstream oss;
            oss << ("{");
            oss << std::string("key:") << K3::prettify_string(x.key);
            oss << (",");
            oss << std::string("value:") << K3::prettify_int(x.value);
            oss << ("}");
            return string_impl(oss.str());
          });
        });
        __result["user_visits"] = K3::prettify_collection(user_visits,
        [] (R_adRevenue_destURL_sourceIP_visitDate<double, K3::base_string, K3::base_string,
        K3::base_string> x)   {
          return K3::prettify_record(x, [] (R_adRevenue_destURL_sourceIP_visitDate<double,
          K3::base_string, K3::base_string, K3::base_string> x)   {
            ostringstream oss;
            oss << ("{");
            oss << std::string("sourceIP:") << K3::prettify_string(x.sourceIP);
            oss << (",");
            oss << std::string("destURL:") << K3::prettify_string(x.destURL);
            oss << (",");
            oss << std::string("visitDate:") << K3::prettify_string(x.visitDate);
            oss << (",");
            oss << std::string("adRevenue:") << K3::prettify_real(x.adRevenue);
            oss << ("}");
            return string_impl(oss.str());
          });
        });
        __result["elapsed_ms"] = K3::prettify_int(elapsed_ms);
        __result["end_ms"] = K3::prettify_int(end_ms);
        __result["start_ms"] = K3::prettify_int(start_ms);
        __result["peer_seq"] = K3::prettify_collection(peer_seq, [] (R_addr<Address> x)   {
          return K3::prettify_record(x, [] (R_addr<Address> x)   {
            ostringstream oss;
            oss << ("{");
            oss << std::string("addr:") << K3::prettify_address(x.addr);
            oss << ("}");
            return string_impl(oss.str());
          });
        });
        __result["master"] = K3::prettify_address(master);
        __result["role"] = K3::prettify_string(role);
        __result["args"] = K3::prettify_tuple(args);
        __result["peers"] = K3::prettify_collection(peers, [] (R_addr<Address> x)   {
          return K3::prettify_record(x, [] (R_addr<Address> x)   {
            ostringstream oss;
            oss << ("{");
            oss << std::string("addr:") << K3::prettify_address(x.addr);
            oss << ("}");
            return string_impl(oss.str());
          });
        });
        __result["me"] = K3::prettify_address(me);
        return __result;
      }
      void __dispatch(int trigger_id, void* payload)  {
        dispatch_table[trigger_id](payload);
      }
  protected:
      std::map<int, std::function<void(void*)>> dispatch_table;
};
int __global_context::__q3_local_tid;
int __global_context::__merge_tid;
int __global_context::__count_merges_tid;
int __global_context::__find_local_max_tid;
int __global_context::__find_global_max_tid;
int __global_context::__finished_tid;
int __global_context::__ready_tid;
int __global_context::__loadAll_tid;
int __global_context::__shutdown_tid;
_Map<R_key_value<K3::base_string, int>> __global_context::rankingsMap;
namespace YAML {
  template <>
  class convert<__global_context> {
    public:
        static Node encode(const __global_context& context)  {
          Node _node;
          _node["rkFiles"] = convert<_Collection<R_path<K3::base_string>>>::encode(context.rkFiles);
          _node["uvFiles"] = convert<_Collection<R_path<K3::base_string>>>::encode(context.uvFiles);
          _node["peers_ready"] = convert<int>::encode(context.peers_ready);
          _node["q3_result"] = convert<_Collection<R_avgPageRank_sourceIP_totalRevenue<double,
          K3::base_string, double>>>::encode(context.q3_result);
          _node["outFile"] = convert<K3::base_string>::encode(context.outFile);
          _node["global_max"] = convert<R_key_value<K3::base_string, R_arSum_prCount_prSum<double,
          int, int>>>::encode(context.global_max);
          _node["peers_done"] = convert<int>::encode(context.peers_done);
          _node["local_max"] = convert<R_key_value<K3::base_string, R_arSum_prCount_prSum<double,
          int, int>>>::encode(context.local_max);
          _node["merged_peers"] = convert<int>::encode(context.merged_peers);
          _node["flag2"] = convert<bool>::encode(context.flag2);
          _node["flag"] = convert<bool>::encode(context.flag);
          _node["date_ub"] = convert<K3::base_string>::encode(context.date_ub);
          _node["date_lb"] = convert<K3::base_string>::encode(context.date_lb);
          _node["merged_partials"] = convert<_Map<R_key_value<K3::base_string,
          R_arSum_prCount_prSum<double, int, int>>>>::encode(context.merged_partials);
          _node["partial_aggs"] = convert<_Map<R_key_value<int, _Map<R_key_value<K3::base_string,
          R_arSum_prCount_prSum<double, int, int>>>>>>::encode(context.partial_aggs);
          _node["empty_aggs"] = convert<R_arSum_prCount_prSum<double, int,
          int>>::encode(context.empty_aggs);
          _node["empty_source_ip_to_aggs_C"] = convert<_Map<R_key_value<K3::base_string,
          R_arSum_prCount_prSum<double, int, int>>>>::encode(context.empty_source_ip_to_aggs_C);
          _node["rankingsMap"] = convert<_Map<R_key_value<K3::base_string,
          int>>>::encode(context.rankingsMap);
          _node["user_visits"] = convert<_Collection<R_adRevenue_destURL_sourceIP_visitDate<double,
          K3::base_string, K3::base_string, K3::base_string>>>::encode(context.user_visits);
          _node["elapsed_ms"] = convert<int>::encode(context.elapsed_ms);
          _node["end_ms"] = convert<int>::encode(context.end_ms);
          _node["start_ms"] = convert<int>::encode(context.start_ms);
          _node["peer_seq"] = convert<_Seq<R_addr<Address>>>::encode(context.peer_seq);
          _node["master"] = convert<Address>::encode(context.master);
          _node["role"] = convert<K3::base_string>::encode(context.role);
          _node["args"] = convert<std::tuple<_Collection<R_arg<K3::base_string>>,
          _Collection<R_key_value<K3::base_string, K3::base_string>>>>::encode(context.args);
          _node["peers"] = convert<_Collection<R_addr<Address>>>::encode(context.peers);
          _node["me"] = convert<Address>::encode(context.me);
          return _node;
        }
        static bool decode(const Node& node, __global_context& context)  {
          if (!(node.IsMap())) {
            return false;
          }
          if (node["rkFiles"]) {
            context.rkFiles = (node["rkFiles"]).as<_Collection<R_path<K3::base_string>>>();
          }
          if (node["uvFiles"]) {
            context.uvFiles = (node["uvFiles"]).as<_Collection<R_path<K3::base_string>>>();
          }
          if (node["peers_ready"]) {
            context.peers_ready = (node["peers_ready"]).as<int>();
            context.__peers_ready_set__ = true;
          }
          if (node["q3_result"]) {
            context.q3_result = (node["q3_result"]).as<_Collection<R_avgPageRank_sourceIP_totalRevenue<double,
            K3::base_string, double>>>();
          }
          if (node["outFile"]) {
            context.outFile = (node["outFile"]).as<K3::base_string>();
          }
          if (node["global_max"]) {
            context.global_max = (node["global_max"]).as<R_key_value<K3::base_string,
            R_arSum_prCount_prSum<double, int, int>>>();
          }
          if (node["peers_done"]) {
            context.peers_done = (node["peers_done"]).as<int>();
            context.__peers_done_set__ = true;
          }
          if (node["local_max"]) {
            context.local_max = (node["local_max"]).as<R_key_value<K3::base_string,
            R_arSum_prCount_prSum<double, int, int>>>();
          }
          if (node["merged_peers"]) {
            context.merged_peers = (node["merged_peers"]).as<int>();
            context.__merged_peers_set__ = true;
          }
          if (node["flag2"]) {
            context.flag2 = (node["flag2"]).as<bool>();
            context.__flag2_set__ = true;
          }
          if (node["flag"]) {
            context.flag = (node["flag"]).as<bool>();
            context.__flag_set__ = true;
          }
          if (node["date_ub"]) {
            context.date_ub = (node["date_ub"]).as<K3::base_string>();
            context.__date_ub_set__ = true;
          }
          if (node["date_lb"]) {
            context.date_lb = (node["date_lb"]).as<K3::base_string>();
            context.__date_lb_set__ = true;
          }
          if (node["merged_partials"]) {
            context.merged_partials = (node["merged_partials"]).as<_Map<R_key_value<K3::base_string,
            R_arSum_prCount_prSum<double, int, int>>>>();
          }
          if (node["partial_aggs"]) {
            context.partial_aggs = (node["partial_aggs"]).as<_Map<R_key_value<int,
            _Map<R_key_value<K3::base_string, R_arSum_prCount_prSum<double, int, int>>>>>>();
          }
          if (node["empty_aggs"]) {
            context.empty_aggs = (node["empty_aggs"]).as<R_arSum_prCount_prSum<double, int, int>>();
          }
          if (node["empty_source_ip_to_aggs_C"]) {
            context.empty_source_ip_to_aggs_C = (node["empty_source_ip_to_aggs_C"]).as<_Map<R_key_value<K3::base_string,
            R_arSum_prCount_prSum<double, int, int>>>>();
          }
          if (node["rankingsMap"]) {
            context.rankingsMap = (node["rankingsMap"]).as<_Map<R_key_value<K3::base_string,
            int>>>();
          }
          if (node["user_visits"]) {
            context.user_visits = (node["user_visits"]).as<_Collection<R_adRevenue_destURL_sourceIP_visitDate<double,
            K3::base_string, K3::base_string, K3::base_string>>>();
          }
          if (node["elapsed_ms"]) {
            context.elapsed_ms = (node["elapsed_ms"]).as<int>();
          }
          if (node["end_ms"]) {
            context.end_ms = (node["end_ms"]).as<int>();
          }
          if (node["start_ms"]) {
            context.start_ms = (node["start_ms"]).as<int>();
          }
          if (node["peer_seq"]) {
            context.peer_seq = (node["peer_seq"]).as<_Seq<R_addr<Address>>>();
          }
          if (node["master"]) {
            context.master = (node["master"]).as<Address>();
            context.__master_set__ = true;
          }
          if (node["role"]) {
            context.role = (node["role"]).as<K3::base_string>();
          }
          if (node["args"]) {
            context.args = (node["args"]).as<std::tuple<_Collection<R_arg<K3::base_string>>,
            _Collection<R_key_value<K3::base_string, K3::base_string>>>>();
          }
          if (node["peers"]) {
            context.peers = (node["peers"]).as<_Collection<R_addr<Address>>>();
          }
          if (node["me"]) {
            context.me = (node["me"]).as<Address>();
          }
          return true;
        }
  };
}
void __global_context::__patch(std::string s)  {
  YAML::convert<__global_context>::decode(YAML::Load(s), *(this));
}
int main(int argc, char** argv)  {
  __global_context::__q3_local_tid = 0;
  __global_context::__merge_tid = 1;
  __global_context::__count_merges_tid = 2;
  __global_context::__find_local_max_tid = 3;
  __global_context::__find_global_max_tid = 4;
  __global_context::__finished_tid = 5;
  __global_context::__ready_tid = 6;
  __global_context::__loadAll_tid = 7;
  __global_context::__shutdown_tid = 8;
  __k3_context::__trigger_names[__global_context::__shutdown_tid] = "shutdown_";
  __k3_context::__trigger_names[__global_context::__loadAll_tid] = "loadAll";
  __k3_context::__trigger_names[__global_context::__ready_tid] = "ready";
  __k3_context::__trigger_names[__global_context::__finished_tid] = "finished";
  __k3_context::__trigger_names[__global_context::__find_global_max_tid] = "find_global_max";
  __k3_context::__trigger_names[__global_context::__find_local_max_tid] = "find_local_max";
  __k3_context::__trigger_names[__global_context::__count_merges_tid] = "count_merges";
  __k3_context::__trigger_names[__global_context::__merge_tid] = "merge";
  __k3_context::__trigger_names[__global_context::__q3_local_tid] = "q3_local";
  __k3_context::__clonable_dispatchers[__global_context::__shutdown_tid] = make_shared<ValDispatcher<unit_t>>();
  __k3_context::__clonable_dispatchers[__global_context::__loadAll_tid] = make_shared<ValDispatcher<unit_t>>();
  __k3_context::__clonable_dispatchers[__global_context::__ready_tid] = make_shared<ValDispatcher<unit_t>>();
  __k3_context::__clonable_dispatchers[__global_context::__finished_tid] = make_shared<ValDispatcher<unit_t>>();
  __k3_context::__clonable_dispatchers[__global_context::__find_global_max_tid] = make_shared<ValDispatcher<R_key_value<K3::base_string,
  R_arSum_prCount_prSum<double, int, int>>>>();
  __k3_context::__clonable_dispatchers[__global_context::__find_local_max_tid] = make_shared<ValDispatcher<unit_t>>();
  __k3_context::__clonable_dispatchers[__global_context::__count_merges_tid] = make_shared<ValDispatcher<unit_t>>();
  __k3_context::__clonable_dispatchers[__global_context::__merge_tid] = make_shared<ValDispatcher<_Map<R_key_value<K3::base_string,
  R_arSum_prCount_prSum<double, int, int>>>>>();
  __k3_context::__clonable_dispatchers[__global_context::__q3_local_tid] = make_shared<ValDispatcher<unit_t>>();
  Options opt;
  if (opt.parse(argc, argv)) {
    return 0;
  }
  runProgram<__global_context>(opt.peer_strings, opt.simulation, opt.log_level);
}
