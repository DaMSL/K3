#include "functional"
#include "map"
#include "memory"
#include "sstream"
#include "string"
#include "tuple"
#include "BaseTypes.hpp"
#include "Common.hpp"
#include "Context.hpp"
#include "Dispatch.hpp"
#include "Engine.hpp"
#include "MessageProcessor.hpp"
#include "Literals.hpp"
#include "Serialization.hpp"
#include "Builtins.hpp"
#include "dataspace/Dataspace.hpp"
#include "strtk.hpp"
using K3::unit_t;
using K3::Address;
using K3::Engine;
using K3::Options;
using K3::ValDispatcher;
using K3::Dispatcher;
using K3::virtualizing_message_processor;
using K3::make_address;
using K3::__k3_context;
using K3::SystemEnvironment;
using K3::processRoles;
using K3::do_patch;
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
#ifndef K3_R_addr_hash_value
#define K3_R_addr_hash_value
template <class _T0>
class R_addr {
  public:
      R_addr(): addr() {}
      R_addr(_T0 _addr): addr(_addr) {}
      R_addr(const R_addr<_T0>& __other): addr(__other.addr) {}
      bool operator==(const R_addr<_T0>& __other) {
        return addr == (__other.addr);
      }
      bool operator!=(const R_addr<_T0>& __other) {
        return std::tie(addr) != std::tie(__other.addr);
      }
      bool operator<(const R_addr<_T0>& __other) {
        return std::tie(addr) < std::tie(__other.addr);
      }
      template <class archive>
      void serialize(archive& _archive, const unsigned int _version) {
        _archive & addr;
      }
      _T0 addr;
};
#endif
template <class C> class _Collection;

template <class C> class _Vector;
template <class C> class _Seq;
template <class C> class _Map;

namespace K3 {
  template <class _T0>
  class patcher<R_addr<_T0>> {
    public:
        static void patch(std::string _input, R_addr<_T0>& _record) {
          shallow<string::iterator> _shallow;
          qi::rule<string::iterator,
          qi::space_type> _addr = (qi::lit("addr") >> (':') >> _shallow)[([&_record] (std::string _partial)  {
            do_patch(_partial, _record.addr);
          })];
          qi::rule<string::iterator, qi::space_type> _field = _addr;
          qi::rule<string::iterator, qi::space_type> _parser = ('{') >> _field % (',') >> ('}');
          qi::phrase_parse(std::begin(_input), std::end(_input), _parser, qi::space);
        }
  };
}
#ifndef K3_R_addr
#define K3_R_addr
template <class _T0>
std::size_t hash_value(const R_addr<_T0>& r) {
  boost::hash<std::tuple<_T0>> hasher;
  return hasher(std::tie(r.addr));
}
#endif
#ifndef K3_R_arg_hash_value
#define K3_R_arg_hash_value
template <class _T0>
class R_arg {
  public:
      R_arg(): arg() {}
      R_arg(_T0 _arg): arg(_arg) {}
      R_arg(const R_arg<_T0>& __other): arg(__other.arg) {}
      bool operator==(const R_arg<_T0>& __other) {
        return arg == (__other.arg);
      }
      bool operator!=(const R_arg<_T0>& __other) {
        return std::tie(arg) != std::tie(__other.arg);
      }
      bool operator<(const R_arg<_T0>& __other) {
        return std::tie(arg) < std::tie(__other.arg);
      }
      template <class archive>
      void serialize(archive& _archive, const unsigned int _version) {
        _archive & arg;
      }
      _T0 arg;
};
#endif
namespace K3 {
  template <class _T0>
  class patcher<R_arg<_T0>> {
    public:
        static void patch(std::string _input, R_arg<_T0>& _record) {
          shallow<string::iterator> _shallow;
          qi::rule<string::iterator,
          qi::space_type> _arg = (qi::lit("arg") >> (':') >> _shallow)[([&_record] (std::string _partial)  {
            do_patch(_partial, _record.arg);
          })];
          qi::rule<string::iterator, qi::space_type> _field = _arg;
          qi::rule<string::iterator, qi::space_type> _parser = ('{') >> _field % (',') >> ('}');
          qi::phrase_parse(std::begin(_input), std::end(_input), _parser, qi::space);
        }
  };
}
#ifndef K3_R_arg
#define K3_R_arg
template <class _T0>
std::size_t hash_value(const R_arg<_T0>& r) {
  boost::hash<std::tuple<_T0>> hasher;
  return hasher(std::tie(r.arg));
}
#endif
#ifndef K3_R_count_sum_hash_value
#define K3_R_count_sum_hash_value
template <class _T0, class _T1>
class R_count_sum {
  public:
      R_count_sum(): count(), sum() {}
      R_count_sum(_T0 _count, _T1 _sum): count(_count), sum(_sum) {}
      R_count_sum(const R_count_sum<_T0, _T1>& __other): count(__other.count), sum(__other.sum) {}
      bool operator==(const R_count_sum<_T0, _T1>& __other) {
        return count == (__other.count) && sum == (__other.sum);
      }
      bool operator!=(const R_count_sum<_T0, _T1>& __other) {
        return std::tie(count, sum) != std::tie(__other.count, __other.sum);
      }
      bool operator<(const R_count_sum<_T0, _T1>& __other) {
        return std::tie(count, sum) < std::tie(__other.count, __other.sum);
      }
      template <class archive>
      void serialize(archive& _archive, const unsigned int _version) {
        _archive & count;
        _archive & sum;
      }
      _T0 count;
      _T1 sum;
};
#endif
namespace K3 {
  template <class _T0, class _T1>
  class patcher<R_count_sum<_T0, _T1>> {
    public:
        static void patch(std::string _input, R_count_sum<_T0, _T1>& _record) {
          shallow<string::iterator> _shallow;
          qi::rule<string::iterator,
          qi::space_type> _count = (qi::lit("count") >> (':') >> _shallow)[([&_record] (std::string _partial)  {
            do_patch(_partial, _record.count);
          })];
          qi::rule<string::iterator,
          qi::space_type> _sum = (qi::lit("sum") >> (':') >> _shallow)[([&_record] (std::string _partial)  {
            do_patch(_partial, _record.sum);
          })];
          qi::rule<string::iterator, qi::space_type> _field = _count | _sum;
          qi::rule<string::iterator, qi::space_type> _parser = ('{') >> _field % (',') >> ('}');
          qi::phrase_parse(std::begin(_input), std::end(_input), _parser, qi::space);
        }
  };
}
#ifndef K3_R_count_sum
#define K3_R_count_sum
template <class _T0, class _T1>
std::size_t hash_value(const R_count_sum<_T0, _T1>& r) {
  boost::hash<std::tuple<_T0, _T1>> hasher;
  return hasher(std::tie(r.count, r.sum));
}
#endif
#ifndef K3_R_distance_mean_hash_value
#define K3_R_distance_mean_hash_value
template <class _T0, class _T1>
class R_distance_mean {
  public:
      R_distance_mean(): distance(), mean() {}
      R_distance_mean(_T0 _distance, _T1 _mean): distance(_distance), mean(_mean) {}
      R_distance_mean(const R_distance_mean<_T0, _T1>& __other): distance(__other.distance),
      mean(__other.mean) {}
      bool operator==(const R_distance_mean<_T0, _T1>& __other) {
        return distance == (__other.distance) && mean == (__other.mean);
      }
      bool operator!=(const R_distance_mean<_T0, _T1>& __other) {
        return std::tie(distance, mean) != std::tie(__other.distance, __other.mean);
      }
      bool operator<(const R_distance_mean<_T0, _T1>& __other) {
        return std::tie(distance, mean) < std::tie(__other.distance, __other.mean);
      }
      template <class archive>
      void serialize(archive& _archive, const unsigned int _version) {
        _archive & distance;
        _archive & mean;
      }
      _T0 distance;
      _T1 mean;
};
#endif
namespace K3 {
  template <class _T0, class _T1>
  class patcher<R_distance_mean<_T0, _T1>> {
    public:
        static void patch(std::string _input, R_distance_mean<_T0, _T1>& _record) {
          shallow<string::iterator> _shallow;
          qi::rule<string::iterator,
          qi::space_type> _distance = (qi::lit("distance") >> (':') >> _shallow)[([&_record] (std::string _partial)  {
            do_patch(_partial, _record.distance);
          })];
          qi::rule<string::iterator,
          qi::space_type> _mean = (qi::lit("mean") >> (':') >> _shallow)[([&_record] (std::string _partial)  {
            do_patch(_partial, _record.mean);
          })];
          qi::rule<string::iterator, qi::space_type> _field = _distance | _mean;
          qi::rule<string::iterator, qi::space_type> _parser = ('{') >> _field % (',') >> ('}');
          qi::phrase_parse(std::begin(_input), std::end(_input), _parser, qi::space);
        }
  };
}
#ifndef K3_R_distance_mean
#define K3_R_distance_mean
template <class _T0, class _T1>
std::size_t hash_value(const R_distance_mean<_T0, _T1>& r) {
  boost::hash<std::tuple<_T0, _T1>> hasher;
  return hasher(std::tie(r.distance, r.mean));
}
#endif
#ifndef K3_R_elem_hash_value
#define K3_R_elem_hash_value
template <class _T0>
class R_elem {
  public:
      R_elem(): elem() {}
      R_elem(_T0 _elem): elem(_elem) {}
      R_elem(const R_elem<_T0>& __other): elem(__other.elem) {}
      bool operator==(const R_elem<_T0>& __other) {
        return elem == (__other.elem);
      }
      bool operator!=(const R_elem<_T0>& __other) {
        return std::tie(elem) != std::tie(__other.elem);
      }
      bool operator<(const R_elem<_T0>& __other) {
        return std::tie(elem) < std::tie(__other.elem);
      }
      template <class archive>
      void serialize(archive& _archive, const unsigned int _version) {
        _archive & elem;
      }
      _T0 elem;
};
#endif
namespace K3 {
  template <class _T0>
  class patcher<R_elem<_T0>> {
    public:
        static void patch(std::string _input, R_elem<_T0>& _record) {
          shallow<string::iterator> _shallow;
          qi::rule<string::iterator,
          qi::space_type> _elem = (qi::lit("elem") >> (':') >> _shallow)[([&_record] (std::string _partial)  {
            do_patch(_partial, _record.elem);
          })];
          qi::rule<string::iterator, qi::space_type> _field = _elem;
          qi::rule<string::iterator, qi::space_type> _parser = ('{') >> _field % (',') >> ('}');
          qi::phrase_parse(std::begin(_input), std::end(_input), _parser, qi::space);
        }
  };
}
#ifndef K3_R_elem
#define K3_R_elem
template <class _T0>
std::size_t hash_value(const R_elem<_T0>& r) {
  boost::hash<std::tuple<_T0>> hasher;
  return hasher(std::tie(r.elem));
}
#endif
#ifndef K3_R_key_value_hash_value
#define K3_R_key_value_hash_value
template <class _T0, class _T1>
class R_key_value {
  public:
      R_key_value(): key(), value() {}
      R_key_value(_T0 _key, _T1 _value): key(_key), value(_value) {}
      R_key_value(const R_key_value<_T0, _T1>& __other): key(__other.key), value(__other.value) {}
      bool operator==(const R_key_value<_T0, _T1>& __other) {
        return key == (__other.key) && value == (__other.value);
      }
      bool operator!=(const R_key_value<_T0, _T1>& __other) {
        return std::tie(key, value) != std::tie(__other.key, __other.value);
      }
      bool operator<(const R_key_value<_T0, _T1>& __other) {
        return std::tie(key, value) < std::tie(__other.key, __other.value);
      }
      template <class archive>
      void serialize(archive& _archive, const unsigned int _version) {
        _archive & key;
        _archive & value;
      }
      _T0 key;
      _T1 value;
};
#endif
namespace K3 {
  template <class _T0, class _T1>
  class patcher<R_key_value<_T0, _T1>> {
    public:
        static void patch(std::string _input, R_key_value<_T0, _T1>& _record) {
          shallow<string::iterator> _shallow;
          qi::rule<string::iterator,
          qi::space_type> _key = (qi::lit("key") >> (':') >> _shallow)[([&_record] (std::string _partial)  {
            do_patch(_partial, _record.key);
          })];
          qi::rule<string::iterator,
          qi::space_type> _value = (qi::lit("value") >> (':') >> _shallow)[([&_record] (std::string _partial)  {
            do_patch(_partial, _record.value);
          })];
          qi::rule<string::iterator, qi::space_type> _field = _key | _value;
          qi::rule<string::iterator, qi::space_type> _parser = ('{') >> _field % (',') >> ('}');
          qi::phrase_parse(std::begin(_input), std::end(_input), _parser, qi::space);
        }
  };
}
#ifndef K3_R_key_value
#define K3_R_key_value
template <class _T0, class _T1>
std::size_t hash_value(const R_key_value<_T0, _T1>& r) {
  boost::hash<std::tuple<_T0, _T1>> hasher;
  return hasher(std::tie(r.key, r.value));
}
#endif


template <class __CONTENT>
class _Vector: public K3::Vector<__CONTENT> {
  public:
      _Vector(): K3::Vector<__CONTENT>() {}
      _Vector(const _Vector<__CONTENT>& __other): K3::Vector<__CONTENT>(__other) {}
      _Vector(const K3::Vector<__CONTENT>& __other1): K3::Vector<__CONTENT>(__other1) {}
      template <class archive>
      void serialize(archive& _archive, const unsigned int _version) {
        _archive & boost::serialization::base_object<K3::Vector<__CONTENT>>(*this);
      }
};

namespace K3 {
  template <class __CONTENT>
  class patcher<_Vector<__CONTENT>> {
    public:
        static void patch(std::string _input, _Vector<__CONTENT>& _c) {
          collection_patcher<_Vector, __CONTENT>::patch(_input, _c);
        }
  };
}
template <class __CONTENT>
class _Collection: public K3::Collection<__CONTENT> {
  public:
      _Collection(): K3::Collection<__CONTENT>() {}
      _Collection(const _Collection<__CONTENT>& __other): K3::Collection<__CONTENT>(__other) {}
      _Collection(const K3::Collection<__CONTENT>& __other1): K3::Collection<__CONTENT>(__other1) {}
      template <class archive>
      void serialize(archive& _archive, const unsigned int _version) {
        _archive & boost::serialization::base_object<K3::Collection<__CONTENT>>(*this);
      }
};
namespace K3 {
  template <class __CONTENT>
  class patcher<_Collection<__CONTENT>> {
    public:
        static void patch(std::string _input, _Collection<__CONTENT>& _c) {
          collection_patcher<_Collection, __CONTENT>::patch(_input, _c);
        }
  };
}
template <class __CONTENT>
class _Map: public K3::Map<__CONTENT> {
  public:
      _Map(): K3::Map<__CONTENT>() {}
      _Map(const _Map<__CONTENT>& __other): K3::Map<__CONTENT>(__other) {}
      _Map(const K3::Map<__CONTENT>& __other1): K3::Map<__CONTENT>(__other1) {}
      template <class archive>
      void serialize(archive& _archive, const unsigned int _version) {
        _archive & boost::serialization::base_object<K3::Map<__CONTENT>>(*this);
      }
};
namespace K3 {
  template <class __CONTENT>
  class patcher<_Map<__CONTENT>> {
    public:
        static void patch(std::string _input, _Map<__CONTENT>& _c) {
          collection_patcher<_Map, __CONTENT>::patch(_input, _c);
        }
  };
}
template <class __CONTENT>
class _Seq: public K3::Seq<__CONTENT> {
  public:
      _Seq(): K3::Seq<__CONTENT>() {}
      _Seq(const _Seq<__CONTENT>& __other): K3::Seq<__CONTENT>(__other) {}
      _Seq(const K3::Seq<__CONTENT>& __other1): K3::Seq<__CONTENT>(__other1) {}
      template <class archive>
      void serialize(archive& _archive, const unsigned int _version) {
        _archive & boost::serialization::base_object<K3::Seq<__CONTENT>>(*this);
      }
};
namespace K3 {
  template <class __CONTENT>
  class patcher<_Seq<__CONTENT>> {
    public:
        static void patch(std::string _input, _Seq<__CONTENT>& _c) {
          collection_patcher<_Seq, __CONTENT>::patch(_input, _c);
        }
  };
}
class __global_context: public K3::__standard_context, public K3::__string_context,
public K3::__time_context {
  public:
      __global_context(Engine& __engine): K3::__standard_context(__engine), K3::__string_context(),
      K3::__time_context() {
        k = 3;
        dimensionality = 3;
        iterations_remaining = 5;
        peers_ready = 0;
        requests = 0;
        dispatch_table[0] = [this] (void* payload)  {
          shutdown_(*(static_cast<unit_t*>(payload)));
        };
        dispatch_table[1] = [this] (void* payload)  {
          maximize(*(static_cast<unit_t*>(payload)));
        };
        dispatch_table[2] = [this] (void* payload)  {
          aggregate(*(static_cast<_Seq<R_key_value<int, R_count_sum<int,
          _Vector<R_elem<double>>>>>*>(payload)));
        };
        dispatch_table[3] = [this] (void* payload)  {
          assign(*(static_cast<_Collection<R_key_value<int, _Vector<R_elem<double>>>>*>(payload)));
        };
        dispatch_table[4] = [this] (void* payload)  {
          ready(*(static_cast<unit_t*>(payload)));
        };
        dispatch_table[5] = [this] (void* payload)  {
          load(*(static_cast<unit_t*>(payload)));
        };
      }
      Address me;
      _Collection<R_addr<Address>> peers;
      std::tuple<_Collection<R_arg<std::string>>, _Collection<R_key_value<std::string,
      std::string>>> args;
      std::string role;
      int k;
      int dimensionality;
      Address master;
      std::string data_file;
      int iterations_remaining;
      int peers_ready;
      int requests;
      _Seq<R_elem<_Vector<R_elem<double>>>> data;
      _Collection<R_key_value<int, _Vector<R_elem<double>>>> means;
      _Map<R_key_value<int, R_count_sum<int, _Vector<R_elem<double>>>>> aggregates;
      unit_t broadcastMeans(unit_t _) {
        printLine("Broadcasting current means to each peer");
        return peers.iterate([this] (R_addr<Address> p) -> unit_t {
          requests = requests + (1);
          auto d = std::make_shared<K3::ValDispatcher<_Collection<R_key_value<int,
          _Vector<R_elem<double>>>>>>(means);
          __engine.send(p.addr, 3, d);
          return unit_t {};
        });
      }
      unit_t printMeans(unit_t _) {
        return means.iterate([this] (R_key_value<int, _Vector<R_elem<double>>> kv) -> unit_t {
          return printLine((kv.value).toString(unit_t {}));
        });
      }
      unit_t start(unit_t _) {
        (range(k)).iterate([this] (R_elem<int> i) -> unit_t {
          return means.insert(R_key_value<int, _Vector<R_elem<double>>> {(i.elem) + (1),
          (data.at(i.elem)).elem});
        });
        printLine("Intial Means: ");
        printMeans(unit_t {});
        broadcastMeans(unit_t {});
        return printLine("---------------");
      }
      int nearest_mean(_Vector<R_elem<double>> p) {
        shared_ptr<R_key_value<int, _Vector<R_elem<double>>>> __0;
        __0 = means.peek(unit_t {});
        if (__0) {
          R_key_value<int, _Vector<R_elem<double>>> first_mean;
          first_mean = *(__0);
          {
            R_distance_mean<double, R_key_value<int, _Vector<R_elem<double>>>> nearest;
            nearest = means.fold([this, p] (R_distance_mean<double, R_key_value<int,
            _Vector<R_elem<double>>>> acc)  {
              return [this, p, acc] (R_key_value<int,
              _Vector<R_elem<double>>> curr_mean) -> R_distance_mean<double, R_key_value<int,
              _Vector<R_elem<double>>>> {
                {
                  double dist;
                  dist = p.distance(curr_mean.value);
                  if (dist < (acc.distance)) {
                    return R_distance_mean<double, R_key_value<int, _Vector<R_elem<double>>>> {dist,
                    curr_mean};
                  } else {
                    return acc;
                  }
                }
              };
            },R_distance_mean<double, R_key_value<int,
            _Vector<R_elem<double>>>> {p.distance(first_mean.value), first_mean});
            return (nearest.mean).key;
          }
        } else {
          return -(1);
        }
      }
      unit_t merge_results(_Seq<R_key_value<int, R_count_sum<int,
      _Vector<R_elem<double>>>>> peer_aggs) {
        requests = requests - (1);
        return peer_aggs.iterate([this] (R_key_value<int, R_count_sum<int,
        _Vector<R_elem<double>>>> v) -> unit_t {
          shared_ptr<R_key_value<int, R_count_sum<int, _Vector<R_elem<double>>>>> __1;
          __1 = aggregates.lookup(v);
          if (__1) {
            R_key_value<int, R_count_sum<int, _Vector<R_elem<double>>>> a;
            a = *(__1);
            return aggregates.insert(R_key_value<int, R_count_sum<int,
            _Vector<R_elem<double>>>> {v.key, R_count_sum<int,
            _Vector<R_elem<double>>> {((v.value).count) + ((a.value).count),
            ((v.value).sum).add((a.value).sum)}});
          } else {
            return aggregates.insert(v);
          }
        });
      }
      unit_t load(unit_t _) {
        loadVector(data_file,data);
        auto d = std::make_shared<K3::ValDispatcher<unit_t>>(unit_t {});
        __engine.send(master, 4, d);
        return unit_t {};
      }
      unit_t ready(unit_t _) {
        peers_ready = peers_ready + (1);
        if (peers_ready == peers.size(unit_t {})) {
          return start(unit_t {});
        } else {
          return unit_t {};
        }
      }
      unit_t assign(_Collection<R_key_value<int, _Vector<R_elem<double>>>> new_means) {
        means = new_means;
        {
          _Seq<R_key_value<int, R_count_sum<int, _Vector<R_elem<double>>>>> aggs;
          aggs = data.groupBy([this] (R_elem<_Vector<R_elem<double>>> p) -> int {
            return nearest_mean(p.elem);
          },[this] (R_count_sum<int, _Vector<R_elem<double>>> sc)  {
            return [this, sc] (R_elem<_Vector<R_elem<double>>> p) -> R_count_sum<int,
            _Vector<R_elem<double>>> {
              return R_count_sum<int, _Vector<R_elem<double>>> {(sc.count) + (1),
              (sc.sum).add(p.elem)};
            };
          },R_count_sum<int, _Vector<R_elem<double>>> {0, zeroVector(dimensionality)});
       
	  auto d = std::make_shared<K3::ValDispatcher<_Seq<R_key_value<int, R_count_sum<int,
          _Vector<R_elem<double>>>>>>>(aggs);
          
	  __engine.send(master, 2, d);
          return unit_t {};
        }
      }
      unit_t aggregate(_Seq<R_key_value<int, R_count_sum<int,
      _Vector<R_elem<double>>>>> peer_aggs) {
        merge_results(peer_aggs);
        if (requests == (0)) {
          auto d = std::make_shared<K3::ValDispatcher<unit_t>>(unit_t {});
          __engine.send(master, 1, d);
          return unit_t {};
        } else {
          return unit_t {};
        }
      }
      unit_t maximize(unit_t _) {
        printLine("Received all partial aggregates from peers, computing new means");
        means = _Collection<R_key_value<int, _Vector<R_elem<double>>>> {};
        aggregates.iterate([this] (R_key_value<int, R_count_sum<int,
        _Vector<R_elem<double>>>> x) -> unit_t {

          return means.insert(R_key_value<int, _Vector<R_elem<double>>> {x.key,
          ((x.value).sum).map([this, x] (R_elem<double> e) -> double {
            return (1.0) / ((x.value).count) * (e.elem);
          })});
        });
        iterations_remaining = iterations_remaining - (1);
        if (iterations_remaining == (0)) {
          printLine("Zero iterations remaining. Final means: ");
          printMeans(unit_t {});
          return peers.iterate([this] (R_addr<Address> p) -> unit_t {
            auto d = std::make_shared<K3::ValDispatcher<unit_t>>(unit_t {});
            __engine.send(p.addr, 0, d);
            return unit_t {};
          });
        } else {
          aggregates = _Map<R_key_value<int, R_count_sum<int, _Vector<R_elem<double>>>>> {};
          printLine("New means: ");
          printMeans(unit_t {});
          peers.iterate([this] (R_addr<Address> p) -> unit_t {
            requests = requests + (1);
            auto d = std::make_shared<K3::ValDispatcher<_Collection<R_key_value<int,
            _Vector<R_elem<double>>>>>>(means);
            __engine.send(p.addr, 3, d);
            return unit_t {};
          });
          return printLine("---------------");
        }
      }
      unit_t shutdown_(unit_t _) {
        return haltEngine(unit_t {});
      }
      unit_t s1Process(unit_t _) {
        return [this] (unit_t next) -> unit_t {
          auto d = std::make_shared<K3::ValDispatcher<unit_t>>(next);
          __engine.send(me, 5, d);
          return unit_t {};
        }(unit_t {});
      }
      unit_t initDecls(unit_t _) {
        return unit_t {};
      }
      unit_t processRole(unit_t _) {
        if (role == ("s1")) {
          return s1Process(unit_t {});
        } else {
          return unit_t {};
        }
      }
      unit_t atInit(unit_t _) {
        initDecls(unit_t {});
        return processRole(unit_t {});
      }
      unit_t atExit(unit_t _) {
        return unit_t {};
      }
      std::map<std::string, std::string> __prettify() {
        std::map<std::string, std::string> result;
        result["aggregates"] = [] (_Map<R_key_value<int, R_count_sum<int,
        _Vector<R_elem<double>>>>> x)  {
          std::ostringstream oss;
          x.iterate([&oss] (const R_key_value<int, R_count_sum<int,
          _Vector<R_elem<double>>>>& elem)  {
            std::string s = ("{") + std::string("key:") + std::to_string(elem.key) + (",") + std::string("value:") + ("{") + std::string("count:") + std::to_string((elem.value).count) + (",") + std::string("sum:") + [] (_Vector<R_elem<double>> x)  {
              std::ostringstream oss;
              x.iterate([&oss] (const R_elem<double>& elem)  {
                std::string s = ("{") + std::string("elem:") + std::to_string(elem.elem) + ("}");
                oss << s << (",");
                return unit_t {};
              });
              return ("[") + oss.str();
            }((elem.value).sum) + ("}") + ("}");
            oss << s << (",");
            return unit_t {};
          });
          return ("[") + oss.str();
        }(aggregates);
        result["means"] = [] (_Collection<R_key_value<int, _Vector<R_elem<double>>>> x)  {
          std::ostringstream oss;
          x.iterate([&oss] (const R_key_value<int, _Vector<R_elem<double>>>& elem)  {
            std::string s = ("{") + std::string("key:") + std::to_string(elem.key) + (",") + std::string("value:") + [] (_Vector<R_elem<double>> x)  {
              std::ostringstream oss;
              x.iterate([&oss] (const R_elem<double>& elem)  {
                std::string s = ("{") + std::string("elem:") + std::to_string(elem.elem) + ("}");
                oss << s << (",");
                return unit_t {};
              });
              return ("[") + oss.str();
            }(elem.value) + ("}");
            oss << s << (",");
            return unit_t {};
          });
          return ("[") + oss.str();
        }(means);
        result["data"] = [] (_Seq<R_elem<_Vector<R_elem<double>>>> x)  {
          std::ostringstream oss;
          x.iterate([&oss] (const R_elem<_Vector<R_elem<double>>>& elem)  {
            std::string s = ("{") + std::string("elem:") + [] (_Vector<R_elem<double>> x)  {
              std::ostringstream oss;
              x.iterate([&oss] (const R_elem<double>& elem)  {
                std::string s = ("{") + std::string("elem:") + std::to_string(elem.elem) + ("}");
                oss << s << (",");
                return unit_t {};
              });
              return ("[") + oss.str();
            }(elem.elem) + ("}");
            oss << s << (",");
            return unit_t {};
          });
          return ("[") + oss.str();
        }(data);
        result["requests"] = std::to_string(requests);
        result["peers_ready"] = std::to_string(peers_ready);
        result["iterations_remaining"] = std::to_string(iterations_remaining);
        result["data_file"] = data_file;
        result["master"] = K3::addressAsString(master);
        result["dimensionality"] = std::to_string(dimensionality);
        result["k"] = std::to_string(k);
        result["role"] = role;
        result["args"] = ("(") + [] (_Collection<R_arg<std::string>> x)  {
          std::ostringstream oss;
          x.iterate([&oss] (const R_arg<std::string>& elem)  {
            std::string s = ("{") + std::string("arg:") + (elem.arg) + ("}");
            oss << s << (",");
            return unit_t {};
          });
          return ("[") + oss.str();
        }(get<0>(args)) + (",") + [] (_Collection<R_key_value<std::string, std::string>> x)  {
          std::ostringstream oss;
          x.iterate([&oss] (const R_key_value<std::string, std::string>& elem)  {
            std::string s = ("{") + std::string("key:") + (elem.key) + (",") + std::string("value:") + (elem.value) + ("}");
            oss << s << (",");
            return unit_t {};
          });
          return ("[") + oss.str();
        }(get<1>(args)) + (")");
        result["peers"] = [] (_Collection<R_addr<Address>> x)  {
          std::ostringstream oss;
          x.iterate([&oss] (const R_addr<Address>& elem)  {
            std::string s = ("{") + std::string("addr:") + K3::addressAsString(elem.addr) + ("}");
            oss << s << (",");
            return unit_t {};
          });
          return ("[") + oss.str();
        }(peers);
        result["me"] = K3::addressAsString(me);
        return result;
      }
      void __patch(std::map<std::string, std::string> bindings) {
        if (bindings.count("aggregates") > (0)) {
          do_patch(bindings["aggregates"], aggregates);
        }
        if (bindings.count("means") > (0)) {
          do_patch(bindings["means"], means);
        }
        if (bindings.count("data") > (0)) {
          do_patch(bindings["data"], data);
        }
        if (bindings.count("requests") > (0)) {
          do_patch(bindings["requests"], requests);
        }
        if (bindings.count("peers_ready") > (0)) {
          do_patch(bindings["peers_ready"], peers_ready);
        }
        if (bindings.count("iterations_remaining") > (0)) {
          do_patch(bindings["iterations_remaining"], iterations_remaining);
        }
        if (bindings.count("data_file") > (0)) {
          do_patch(bindings["data_file"], data_file);
        }
        if (bindings.count("master") > (0)) {
          do_patch(bindings["master"], master);
        }
        if (bindings.count("dimensionality") > (0)) {
          do_patch(bindings["dimensionality"], dimensionality);
        }
        if (bindings.count("k") > (0)) {
          do_patch(bindings["k"], k);
        }
        if (bindings.count("role") > (0)) {
          do_patch(bindings["role"], role);
        }
        if (bindings.count("args") > (0)) {
          do_patch(bindings["args"], args);
        }
        if (bindings.count("peers") > (0)) {
          do_patch(bindings["peers"], peers);
        }
        if (bindings.count("me") > (0)) {
          do_patch(bindings["me"], me);
        }
      }
      void __dispatch(int trigger_id, void* payload) {
        dispatch_table[trigger_id](payload);
      }
  protected:
      std::map<int, std::function<void(void*)>> dispatch_table;
};
int main(int argc, char** argv) {
  Engine engine;
  Options opt;
  if (opt.parse(argc, argv)) {
    return 0;
  }
  auto contexts = createContexts<__global_context>(opt.peer_strings, engine);
  {
    __k3_context::__trigger_names[0] = "shutdown_";
    __k3_context::__trigger_names[1] = "maximize";
    __k3_context::__trigger_names[2] = "aggregate";
    __k3_context::__trigger_names[3] = "assign";
    __k3_context::__trigger_names[4] = "ready";
    __k3_context::__trigger_names[5] = "load";
    __k3_context::__clonable_dispatchers[0] = make_shared<ValDispatcher<unit_t>>();
    __k3_context::__clonable_dispatchers[1] = make_shared<ValDispatcher<unit_t>>();
    __k3_context::__clonable_dispatchers[2] = make_shared<ValDispatcher<_Seq<R_key_value<int,
    R_count_sum<int, _Vector<R_elem<double>>>>>>>();
    __k3_context::__clonable_dispatchers[3] = make_shared<ValDispatcher<_Collection<R_key_value<int,
    _Vector<R_elem<double>>>>>>();
    __k3_context::__clonable_dispatchers[4] = make_shared<ValDispatcher<unit_t>>();
    __k3_context::__clonable_dispatchers[5] = make_shared<ValDispatcher<unit_t>>();
  }
  SystemEnvironment se = defaultEnvironment(getAddrs(contexts));
  engine.configure(opt.simulation, se, make_shared<DefaultInternalCodec>(), opt.log_level);
  processRoles(contexts);
  engine.runEngine(make_shared<virtualizing_message_processor>(contexts));
}
