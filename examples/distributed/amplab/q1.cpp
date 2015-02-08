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
      R_addr(): addr()  {}
      R_addr(const _T0& _addr): addr(_addr)  {}
      R_addr(_T0&& _addr): addr(std::move(_addr))  {}
      R_addr(const R_addr<_T0>& __other): addr(__other.addr)  {}
      R_addr(R_addr<_T0>&& __other): addr(std::move(__other.addr))  {}
      template <class archive>
      void serialize(archive& _archive, const unsigned int _version)  {
        _archive & addr;
      }
      R_addr<_T0>& operator=(const R_addr<_T0>& __other)  {
        addr = (__other.addr);
        return *(this);
      }
      R_addr<_T0>& operator=(R_addr<_T0>&& __other)  {
        addr = std::move(__other.addr);
        return *(this);
      }
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
      _T0 addr;
};
#endif
namespace K3 {
  template <class _T0>
  class patcher<R_addr<_T0>> {
    public:
        static void patch(std::string _input, R_addr<_T0>& _record)  {
          shallow<string::iterator> _shallow;
          qi::rule<string::iterator,
          qi::space_type> _addr = (qi::lit("addr") >> (':') >> _shallow)[([&_record] (std::string _partial)   {
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
std::size_t hash_value(const R_addr<_T0>& r)  {
  boost::hash<std::tuple<_T0>> hasher;
  return hasher(std::tie(r.addr));
}
#endif
#ifndef K3_R_arg_hash_value
#define K3_R_arg_hash_value
template <class _T0>
class R_arg {
  public:
      R_arg(): arg()  {}
      R_arg(const _T0& _arg): arg(_arg)  {}
      R_arg(_T0&& _arg): arg(std::move(_arg))  {}
      R_arg(const R_arg<_T0>& __other): arg(__other.arg)  {}
      R_arg(R_arg<_T0>&& __other): arg(std::move(__other.arg))  {}
      template <class archive>
      void serialize(archive& _archive, const unsigned int _version)  {
        _archive & arg;
      }
      R_arg<_T0>& operator=(const R_arg<_T0>& __other)  {
        arg = (__other.arg);
        return *(this);
      }
      R_arg<_T0>& operator=(R_arg<_T0>&& __other)  {
        arg = std::move(__other.arg);
        return *(this);
      }
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
      _T0 arg;
};
#endif
namespace K3 {
  template <class _T0>
  class patcher<R_arg<_T0>> {
    public:
        static void patch(std::string _input, R_arg<_T0>& _record)  {
          shallow<string::iterator> _shallow;
          qi::rule<string::iterator,
          qi::space_type> _arg = (qi::lit("arg") >> (':') >> _shallow)[([&_record] (std::string _partial)   {
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
std::size_t hash_value(const R_arg<_T0>& r)  {
  boost::hash<std::tuple<_T0>> hasher;
  return hasher(std::tie(r.arg));
}
#endif
#ifndef K3_R_avgDuration_pageRank_pageURL_hash_value
#define K3_R_avgDuration_pageRank_pageURL_hash_value
template <class _T0, class _T1, class _T2>
class R_avgDuration_pageRank_pageURL {
  public:
      R_avgDuration_pageRank_pageURL(): avgDuration(), pageRank(), pageURL()  {}
      template <class __T0, class __T1, class __T2>
      R_avgDuration_pageRank_pageURL(__T0&& _avgDuration, __T1&& _pageRank,
      __T2&& _pageURL): avgDuration(std::forward<__T0>(_avgDuration)),
      pageRank(std::forward<__T1>(_pageRank)), pageURL(std::forward<__T2>(_pageURL))  {}
      R_avgDuration_pageRank_pageURL(const R_avgDuration_pageRank_pageURL<_T0, _T1,
      _T2>& __other): avgDuration(__other.avgDuration), pageRank(__other.pageRank),
      pageURL(__other.pageURL)  {}
      R_avgDuration_pageRank_pageURL(R_avgDuration_pageRank_pageURL<_T0, _T1,
      _T2>&& __other): avgDuration(std::move(__other.avgDuration)),
      pageRank(std::move(__other.pageRank)), pageURL(std::move(__other.pageURL))  {}
      template <class archive>
      void serialize(archive& _archive, const unsigned int _version)  {
        _archive & avgDuration;
        _archive & pageRank;
        _archive & pageURL;
      }
      R_avgDuration_pageRank_pageURL<_T0, _T1,
      _T2>& operator=(const R_avgDuration_pageRank_pageURL<_T0, _T1, _T2>& __other)  {
        avgDuration = (__other.avgDuration);
        pageRank = (__other.pageRank);
        pageURL = (__other.pageURL);
        return *(this);
      }
      R_avgDuration_pageRank_pageURL<_T0, _T1, _T2>& operator=(R_avgDuration_pageRank_pageURL<_T0,
      _T1, _T2>&& __other)  {
        avgDuration = std::move(__other.avgDuration);
        pageRank = std::move(__other.pageRank);
        pageURL = std::move(__other.pageURL);
        return *(this);
      }
      bool operator==(const R_avgDuration_pageRank_pageURL<_T0, _T1, _T2>& __other) const {
        return avgDuration == (__other.avgDuration) && pageRank == (__other.pageRank) && pageURL == (__other.pageURL);
      }
      bool operator!=(const R_avgDuration_pageRank_pageURL<_T0, _T1, _T2>& __other) const {
        return std::tie(avgDuration, pageRank, pageURL) != std::tie(__other.avgDuration,
        __other.pageRank, __other.pageURL);
      }
      bool operator<(const R_avgDuration_pageRank_pageURL<_T0, _T1, _T2>& __other) const {
        return std::tie(avgDuration, pageRank, pageURL) < std::tie(__other.avgDuration,
        __other.pageRank, __other.pageURL);
      }
      bool operator>(const R_avgDuration_pageRank_pageURL<_T0, _T1, _T2>& __other) const {
        return std::tie(avgDuration, pageRank, pageURL) > std::tie(__other.avgDuration,
        __other.pageRank, __other.pageURL);
      }
      _T0 avgDuration;
      _T1 pageRank;
      _T2 pageURL;
};
#endif
namespace K3 {
  template <class _T0, class _T1, class _T2>
  class patcher<R_avgDuration_pageRank_pageURL<_T0, _T1, _T2>> {
    public:
        static void patch(std::string _input, R_avgDuration_pageRank_pageURL<_T0, _T1,
        _T2>& _record)  {
          shallow<string::iterator> _shallow;
          qi::rule<string::iterator,
          qi::space_type> _avgDuration = (qi::lit("avgDuration") >> (':') >> _shallow)[([&_record] (std::string _partial)   {
            do_patch(_partial, _record.avgDuration);
          })];
          qi::rule<string::iterator,
          qi::space_type> _pageRank = (qi::lit("pageRank") >> (':') >> _shallow)[([&_record] (std::string _partial)   {
            do_patch(_partial, _record.pageRank);
          })];
          qi::rule<string::iterator,
          qi::space_type> _pageURL = (qi::lit("pageURL") >> (':') >> _shallow)[([&_record] (std::string _partial)   {
            do_patch(_partial, _record.pageURL);
          })];
          qi::rule<string::iterator, qi::space_type> _field = _avgDuration | _pageRank | _pageURL;
          qi::rule<string::iterator, qi::space_type> _parser = ('{') >> _field % (',') >> ('}');
          qi::phrase_parse(std::begin(_input), std::end(_input), _parser, qi::space);
        }
  };
}
#ifndef K3_R_avgDuration_pageRank_pageURL
#define K3_R_avgDuration_pageRank_pageURL
template <class _T0, class _T1, class _T2>
std::size_t hash_value(const R_avgDuration_pageRank_pageURL<_T0, _T1, _T2>& r)  {
  boost::hash<std::tuple<_T0, _T1, _T2>> hasher;
  return hasher(std::tie(r.avgDuration, r.pageRank, r.pageURL));
}
#endif
#ifndef K3_R_key_value_hash_value
#define K3_R_key_value_hash_value
template <class _T0, class _T1>
class R_key_value {
  public:
      R_key_value(): key(), value()  {}
      template <class __T0, class __T1>
      R_key_value(__T0&& _key, __T1&& _value): key(std::forward<__T0>(_key)),
      value(std::forward<__T1>(_value))  {}
      R_key_value(const R_key_value<_T0, _T1>& __other): key(__other.key), value(__other.value)  {}
      R_key_value(R_key_value<_T0, _T1>&& __other): key(std::move(__other.key)),
      value(std::move(__other.value))  {}
      template <class archive>
      void serialize(archive& _archive, const unsigned int _version)  {
        _archive & key;
        _archive & value;
      }
      R_key_value<_T0, _T1>& operator=(const R_key_value<_T0, _T1>& __other)  {
        key = (__other.key);
        value = (__other.value);
        return *(this);
      }
      R_key_value<_T0, _T1>& operator=(R_key_value<_T0, _T1>&& __other)  {
        key = std::move(__other.key);
        value = std::move(__other.value);
        return *(this);
      }
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
      _T0 key;
      _T1 value;
};
#endif
namespace K3 {
  template <class _T0, class _T1>
  class patcher<R_key_value<_T0, _T1>> {
    public:
        static void patch(std::string _input, R_key_value<_T0, _T1>& _record)  {
          shallow<string::iterator> _shallow;
          qi::rule<string::iterator,
          qi::space_type> _key = (qi::lit("key") >> (':') >> _shallow)[([&_record] (std::string _partial)   {
            do_patch(_partial, _record.key);
          })];
          qi::rule<string::iterator,
          qi::space_type> _value = (qi::lit("value") >> (':') >> _shallow)[([&_record] (std::string _partial)   {
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
std::size_t hash_value(const R_key_value<_T0, _T1>& r)  {
  boost::hash<std::tuple<_T0, _T1>> hasher;
  return hasher(std::tie(r.key, r.value));
}
#endif
#ifndef K3_R_pageRank_pageURL_hash_value
#define K3_R_pageRank_pageURL_hash_value
template <class _T0, class _T1>
class R_pageRank_pageURL {
  public:
      R_pageRank_pageURL(): pageRank(), pageURL()  {}
      template <class __T0, class __T1>
      R_pageRank_pageURL(__T0&& _pageRank,
      __T1&& _pageURL): pageRank(std::forward<__T0>(_pageRank)),
      pageURL(std::forward<__T1>(_pageURL))  {}
      R_pageRank_pageURL(const R_pageRank_pageURL<_T0, _T1>& __other): pageRank(__other.pageRank),
      pageURL(__other.pageURL)  {}
      R_pageRank_pageURL(R_pageRank_pageURL<_T0,
      _T1>&& __other): pageRank(std::move(__other.pageRank)),
      pageURL(std::move(__other.pageURL))  {}
      template <class archive>
      void serialize(archive& _archive, const unsigned int _version)  {
        _archive & pageRank;
        _archive & pageURL;
      }
      R_pageRank_pageURL<_T0, _T1>& operator=(const R_pageRank_pageURL<_T0, _T1>& __other)  {
        pageRank = (__other.pageRank);
        pageURL = (__other.pageURL);
        return *(this);
      }
      R_pageRank_pageURL<_T0, _T1>& operator=(R_pageRank_pageURL<_T0, _T1>&& __other)  {
        pageRank = std::move(__other.pageRank);
        pageURL = std::move(__other.pageURL);
        return *(this);
      }
      bool operator==(const R_pageRank_pageURL<_T0, _T1>& __other) const {
        return pageRank == (__other.pageRank) && pageURL == (__other.pageURL);
      }
      bool operator!=(const R_pageRank_pageURL<_T0, _T1>& __other) const {
        return std::tie(pageRank, pageURL) != std::tie(__other.pageRank, __other.pageURL);
      }
      bool operator<(const R_pageRank_pageURL<_T0, _T1>& __other) const {
        return std::tie(pageRank, pageURL) < std::tie(__other.pageRank, __other.pageURL);
      }
      bool operator>(const R_pageRank_pageURL<_T0, _T1>& __other) const {
        return std::tie(pageRank, pageURL) > std::tie(__other.pageRank, __other.pageURL);
      }
      _T0 pageRank;
      _T1 pageURL;
};
#endif
namespace K3 {
  template <class _T0, class _T1>
  class patcher<R_pageRank_pageURL<_T0, _T1>> {
    public:
        static void patch(std::string _input, R_pageRank_pageURL<_T0, _T1>& _record)  {
          shallow<string::iterator> _shallow;
          qi::rule<string::iterator,
          qi::space_type> _pageRank = (qi::lit("pageRank") >> (':') >> _shallow)[([&_record] (std::string _partial)   {
            do_patch(_partial, _record.pageRank);
          })];
          qi::rule<string::iterator,
          qi::space_type> _pageURL = (qi::lit("pageURL") >> (':') >> _shallow)[([&_record] (std::string _partial)   {
            do_patch(_partial, _record.pageURL);
          })];
          qi::rule<string::iterator, qi::space_type> _field = _pageRank | _pageURL;
          qi::rule<string::iterator, qi::space_type> _parser = ('{') >> _field % (',') >> ('}');
          qi::phrase_parse(std::begin(_input), std::end(_input), _parser, qi::space);
        }
  };
}
#ifndef K3_R_pageRank_pageURL
#define K3_R_pageRank_pageURL
template <class _T0, class _T1>
std::size_t hash_value(const R_pageRank_pageURL<_T0, _T1>& r)  {
  boost::hash<std::tuple<_T0, _T1>> hasher;
  return hasher(std::tie(r.pageRank, r.pageURL));
}
#endif
#ifndef K3_R_path_hash_value
#define K3_R_path_hash_value
template <class _T0>
class R_path {
  public:
      R_path(): path()  {}
      R_path(const _T0& _path): path(_path)  {}
      R_path(_T0&& _path): path(std::move(_path))  {}
      R_path(const R_path<_T0>& __other): path(__other.path)  {}
      R_path(R_path<_T0>&& __other): path(std::move(__other.path))  {}
      template <class archive>
      void serialize(archive& _archive, const unsigned int _version)  {
        _archive & path;
      }
      R_path<_T0>& operator=(const R_path<_T0>& __other)  {
        path = (__other.path);
        return *(this);
      }
      R_path<_T0>& operator=(R_path<_T0>&& __other)  {
        path = std::move(__other.path);
        return *(this);
      }
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
      _T0 path;
};
#endif
namespace K3 {
  template <class _T0>
  class patcher<R_path<_T0>> {
    public:
        static void patch(std::string _input, R_path<_T0>& _record)  {
          shallow<string::iterator> _shallow;
          qi::rule<string::iterator,
          qi::space_type> _path = (qi::lit("path") >> (':') >> _shallow)[([&_record] (std::string _partial)   {
            do_patch(_partial, _record.path);
          })];
          qi::rule<string::iterator, qi::space_type> _field = _path;
          qi::rule<string::iterator, qi::space_type> _parser = ('{') >> _field % (',') >> ('}');
          qi::phrase_parse(std::begin(_input), std::end(_input), _parser, qi::space);
        }
  };
}
#ifndef K3_R_path
#define K3_R_path
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
      _Collection(const _Collection<__CONTENT>& __other): K3::Collection<__CONTENT>(__other)  {}
      _Collection(const K3::Collection<__CONTENT>& __other1): K3::Collection<__CONTENT>(__other1)  {}
      _Collection(K3::Collection<__CONTENT>&& __other1): K3::Collection<__CONTENT>(std::move(__other1))  {}
      template <class archive>
      void serialize(archive& _archive, const unsigned int _version)  {
        _archive & boost::serialization::base_object<K3::Collection<__CONTENT>>(*this);
      }
};
namespace K3 {
  template <class __CONTENT>
  class patcher<_Collection<__CONTENT>> {
    public:
        static void patch(std::string _input, _Collection<__CONTENT>& _c)  {
          collection_patcher<_Collection, __CONTENT>::patch(_input, _c);
        }
  };
}
class __global_context: public K3::__standard_context, public K3::__string_context,
public K3::__time_context {
  public:
      __global_context(Engine& __engine): K3::__standard_context(__engine), K3::__string_context(),
      K3::__time_context()  {
        master = make_address("127.0.0.1", 40000);
        x = 10;
        peers_ready = 0;
        peers_finished = 0;
        num_results = 0;
        start_ms = 0;
        end_ms = 0;
        elapsed_ms = 0;
        load_start_ms = 0;
        load_end_ms = 0;
        load_elapsed_ms = 0;
        dispatch_table[0] = [this] (void* payload)   {
          load_all(*(static_cast<unit_t*>(payload)));
        };
        dispatch_table[1] = [this] (void* payload)   {
          ready(*(static_cast<unit_t*>(payload)));
        };
        dispatch_table[2] = [this] (void* payload)   {
          shutdown_(*(static_cast<unit_t*>(payload)));
        };
        dispatch_table[3] = [this] (void* payload)   {
          finished(*(static_cast<int*>(payload)));
        };
        dispatch_table[4] = [this] (void* payload)   {
          q1_local(*(static_cast<unit_t*>(payload)));
        };
        dispatch_table[5] = [this] (void* payload)   {
          hello(*(static_cast<unit_t*>(payload)));
        };
      }
      Address me;
      _Collection<R_addr<Address>> peers;
      std::tuple<_Collection<R_arg<std::string>>, _Collection<R_key_value<std::string,
      std::string>>> args;
      std::string role;
      unit_t dataLoader(string file, _Collection<R_avgDuration_pageRank_pageURL<int, int,
      std::string>>& c)  {
        R_avgDuration_pageRank_pageURL<int, int, std::string> rec;
        strtk::for_each_line(file, [&rec, &c] (const std::string& str)   {
          if (strtk::parse(str, ",", rec.pageURL, rec.pageRank, rec.avgDuration)) {
            c.insert(rec);
          } else {
            std::cout << ("Failed to parse a row!\n");
          }
        });
        return unit_t {};
      }
      _Collection<R_path<std::string>> dataFiles;
      Address master;
      int x;
      int peers_ready;
      int peers_finished;
      int num_results;
      _Collection<R_avgDuration_pageRank_pageURL<int, int, std::string>> local_rankings;
      _Collection<R_pageRank_pageURL<int, std::string>> local_q1_results;
      int start_ms;
      int end_ms;
      int elapsed_ms;
      int load_start_ms;
      int load_end_ms;
      int load_elapsed_ms;
      unit_t hello(const unit_t& _)  {
        return unit_t {};
      }
      unit_t q1_local(const unit_t& _)  {
        local_rankings.iterate([this] (const R_avgDuration_pageRank_pageURL<int, int,
        std::string>& row) mutable  {
          if ((row.pageRank) > x) {
            return local_q1_results.insert(R_pageRank_pageURL<int, std::string> {row.pageRank,
            row.pageURL});
          } else {
            return unit_t {};
          }
        });
        auto d = std::make_shared<K3::ValDispatcher<int>>(local_q1_results.size(unit_t {}));
        __engine.send(master, 3, d);
        return unit_t {};
      }
      unit_t finished(const int& num_peer_results)  {
        num_results = num_results + num_peer_results;
        peers_finished = peers_finished + (1);
        if (peers_finished == peers.size(unit_t {})) {
          end_ms = now_int(unit_t {});
          elapsed_ms = end_ms - start_ms;
          print(concat("# Results: ", itos(num_results)));
          print(concat("Elapsed time (ms):", itos(elapsed_ms)));
          return peers.iterate([this] (const R_addr<Address>& p) mutable  {
            auto d = std::make_shared<K3::ValDispatcher<unit_t>>(unit_t {});
            __engine.send(p.addr, 2, d);
            return unit_t {};
          });
        } else {
          return unit_t {};
        }
      }
      unit_t shutdown_(const unit_t& _)  {
        return haltEngine(unit_t {});
      }
      unit_t ready(const unit_t& _)  {
        peers_ready = peers_ready + (1);
        if (peers_ready == peers.size(unit_t {})) {
          peers.iterate([this] (const R_addr<Address>& p) mutable  {
            auto d = std::make_shared<K3::ValDispatcher<unit_t>>(unit_t {});
            __engine.send(p.addr, 5, d);
            return unit_t {};
          });
          start_ms = now_int(unit_t {});
          return peers.iterate([this] (const R_addr<Address>& p) mutable  {
            auto d = std::make_shared<K3::ValDispatcher<unit_t>>(unit_t {});
            __engine.send(p.addr, 4, d);
            return unit_t {};
          });
        } else {
          return unit_t {};
        }
      }
      unit_t load_all(const unit_t& _)  {
        load_start_ms = now_int(unit_t {});
        dataFiles.iterate([this] (const R_path<std::string>& e) mutable  {
          return dataLoader(e.path, local_rankings);
        });
        load_end_ms = now_int(unit_t {});
        load_elapsed_ms = load_end_ms - load_start_ms;
        print(concat("Load time: ", itos(load_elapsed_ms)));
        auto d = std::make_shared<K3::ValDispatcher<unit_t>>(unit_t {});
        __engine.send(master, 1, d);
        return unit_t {};
      }
      unit_t rowsProcess(const unit_t& _)  {
        return [this] (const unit_t& next) mutable  {
          auto d = std::make_shared<K3::ValDispatcher<unit_t>>(next);
          __engine.send(me, 0, d);
          return unit_t {};
        }(unit_t {});
      }
      unit_t initDecls(const unit_t& _)  {
        return unit_t {};
      }
      unit_t processRole(const unit_t& _)  {
        if (role == ("rows")) {
          return rowsProcess(unit_t {});
        } else {
          return unit_t {};
        }
      }
      unit_t atInit(const unit_t& _)  {
        initDecls(unit_t {});
        return processRole(unit_t {});
      }
      unit_t atExit(const unit_t& _)  {
        return unit_t {};
      }
      std::map<std::string, std::string> __prettify()  {
        std::map<std::string, std::string> result;
        result["load_elapsed_ms"] = std::to_string(load_elapsed_ms);
        result["load_end_ms"] = std::to_string(load_end_ms);
        result["load_start_ms"] = std::to_string(load_start_ms);
        result["elapsed_ms"] = std::to_string(elapsed_ms);
        result["end_ms"] = std::to_string(end_ms);
        result["start_ms"] = std::to_string(start_ms);
        result["local_q1_results"] = [] (_Collection<R_pageRank_pageURL<int, std::string>> x)   {
          std::ostringstream oss;
          x.iterate([&oss] (const R_pageRank_pageURL<int, std::string>& elem)   {
            std::string s = ("{") + std::string("pageRank:") + std::to_string(elem.pageRank) + (",") + std::string("pageURL:") + (elem.pageURL) + ("}");
            oss << s << (",");
            return unit_t {};
          });
          return ("[") + oss.str();
        }(local_q1_results);
        result["local_rankings"] = [] (_Collection<R_avgDuration_pageRank_pageURL<int, int,
        std::string>> x)   {
          std::ostringstream oss;
          x.iterate([&oss] (const R_avgDuration_pageRank_pageURL<int, int, std::string>& elem)   {
            std::string s = ("{") + std::string("pageRank:") + std::to_string(elem.pageRank) + (",") + std::string("pageURL:") + (elem.pageURL) + (",") + std::string("avgDuration:") + std::to_string(elem.avgDuration) + ("}");
            oss << s << (",");
            return unit_t {};
          });
          return ("[") + oss.str();
        }(local_rankings);
        result["num_results"] = std::to_string(num_results);
        result["peers_finished"] = std::to_string(peers_finished);
        result["peers_ready"] = std::to_string(peers_ready);
        result["x"] = std::to_string(x);
        result["master"] = K3::addressAsString(master);
        result["dataFiles"] = [] (_Collection<R_path<std::string>> x)   {
          std::ostringstream oss;
          x.iterate([&oss] (const R_path<std::string>& elem)   {
            std::string s = ("{") + std::string("path:") + (elem.path) + ("}");
            oss << s << (",");
            return unit_t {};
          });
          return ("[") + oss.str();
        }(dataFiles);
        result["role"] = role;
        result["args"] = ("(") + [] (_Collection<R_arg<std::string>> x)   {
          std::ostringstream oss;
          x.iterate([&oss] (const R_arg<std::string>& elem)   {
            std::string s = ("{") + std::string("arg:") + (elem.arg) + ("}");
            oss << s << (",");
            return unit_t {};
          });
          return ("[") + oss.str();
        }(get<0>(args)) + (",") + [] (_Collection<R_key_value<std::string, std::string>> x)   {
          std::ostringstream oss;
          x.iterate([&oss] (const R_key_value<std::string, std::string>& elem)   {
            std::string s = ("{") + std::string("key:") + (elem.key) + (",") + std::string("value:") + (elem.value) + ("}");
            oss << s << (",");
            return unit_t {};
          });
          return ("[") + oss.str();
        }(get<1>(args)) + (")");
        result["peers"] = [] (_Collection<R_addr<Address>> x)   {
          std::ostringstream oss;
          x.iterate([&oss] (const R_addr<Address>& elem)   {
            std::string s = ("{") + std::string("addr:") + K3::addressAsString(elem.addr) + ("}");
            oss << s << (",");
            return unit_t {};
          });
          return ("[") + oss.str();
        }(peers);
        result["me"] = K3::addressAsString(me);
        return result;
      }
      void __patch(std::map<std::string, std::string> bindings)  {
        if (bindings.count("load_elapsed_ms") > (0)) {
          do_patch(bindings["load_elapsed_ms"], load_elapsed_ms);
        }
        if (bindings.count("load_end_ms") > (0)) {
          do_patch(bindings["load_end_ms"], load_end_ms);
        }
        if (bindings.count("load_start_ms") > (0)) {
          do_patch(bindings["load_start_ms"], load_start_ms);
        }
        if (bindings.count("elapsed_ms") > (0)) {
          do_patch(bindings["elapsed_ms"], elapsed_ms);
        }
        if (bindings.count("end_ms") > (0)) {
          do_patch(bindings["end_ms"], end_ms);
        }
        if (bindings.count("start_ms") > (0)) {
          do_patch(bindings["start_ms"], start_ms);
        }
        if (bindings.count("local_q1_results") > (0)) {
          do_patch(bindings["local_q1_results"], local_q1_results);
        }
        if (bindings.count("local_rankings") > (0)) {
          do_patch(bindings["local_rankings"], local_rankings);
        }
        if (bindings.count("num_results") > (0)) {
          do_patch(bindings["num_results"], num_results);
        }
        if (bindings.count("peers_finished") > (0)) {
          do_patch(bindings["peers_finished"], peers_finished);
        }
        if (bindings.count("peers_ready") > (0)) {
          do_patch(bindings["peers_ready"], peers_ready);
        }
        if (bindings.count("x") > (0)) {
          do_patch(bindings["x"], x);
        }
        if (bindings.count("master") > (0)) {
          do_patch(bindings["master"], master);
        }
        if (bindings.count("dataFiles") > (0)) {
          do_patch(bindings["dataFiles"], dataFiles);
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
      void __dispatch(int trigger_id, void* payload)  {
        dispatch_table[trigger_id](payload);
      }
  protected:
      std::map<int, std::function<void(void*)>> dispatch_table;
};
int main(int argc, char** argv)  {
  Engine engine;
  Options opt;
  if (opt.parse(argc, argv)) {
    return 0;
  }
  auto contexts = createContexts<__global_context>(opt.peer_strings, engine);
  {
    __k3_context::__trigger_names[0] = "load_all";
    __k3_context::__trigger_names[1] = "ready";
    __k3_context::__trigger_names[2] = "shutdown_";
    __k3_context::__trigger_names[3] = "finished";
    __k3_context::__trigger_names[4] = "q1_local";
    __k3_context::__trigger_names[5] = "hello";
    __k3_context::__clonable_dispatchers[0] = make_shared<ValDispatcher<unit_t>>();
    __k3_context::__clonable_dispatchers[1] = make_shared<ValDispatcher<unit_t>>();
    __k3_context::__clonable_dispatchers[2] = make_shared<ValDispatcher<unit_t>>();
    __k3_context::__clonable_dispatchers[3] = make_shared<ValDispatcher<int>>();
    __k3_context::__clonable_dispatchers[4] = make_shared<ValDispatcher<unit_t>>();
    __k3_context::__clonable_dispatchers[5] = make_shared<ValDispatcher<unit_t>>();
  }
  SystemEnvironment se = defaultEnvironment(getAddrs(contexts));
  engine.configure(opt.simulation, se, make_shared<DefaultInternalCodec>(), opt.log_level);
  processRoles(contexts);
  engine.runEngine(make_shared<virtualizing_message_processor>(contexts));
}