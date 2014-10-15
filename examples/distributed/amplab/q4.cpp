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
#ifndef K3_R_count_destPage_sourcePage_hash_value
#define K3_R_count_destPage_sourcePage_hash_value
template <class _T0, class _T1, class _T2>
class R_count_destPage_sourcePage {
  public:
      R_count_destPage_sourcePage(): count(), destPage(), sourcePage()  {}
      template <class __T0, class __T1, class __T2>
      R_count_destPage_sourcePage(__T0&& _count, __T1&& _destPage,
      __T2&& _sourcePage): count(std::forward<__T0>(_count)),
      destPage(std::forward<__T1>(_destPage)), sourcePage(std::forward<__T2>(_sourcePage))  {}
      R_count_destPage_sourcePage(const R_count_destPage_sourcePage<_T0, _T1,
      _T2>& __other): count(__other.count), destPage(__other.destPage),
      sourcePage(__other.sourcePage)  {}
      R_count_destPage_sourcePage(R_count_destPage_sourcePage<_T0, _T1,
      _T2>&& __other): count(std::move(__other.count)), destPage(std::move(__other.destPage)),
      sourcePage(std::move(__other.sourcePage))  {}
      template <class archive>
      void serialize(archive& _archive, const unsigned int _version)  {
        _archive & count;
        _archive & destPage;
        _archive & sourcePage;
      }
      R_count_destPage_sourcePage<_T0, _T1, _T2>& operator=(const R_count_destPage_sourcePage<_T0,
      _T1, _T2>& __other)  {
        count = (__other.count);
        destPage = (__other.destPage);
        sourcePage = (__other.sourcePage);
        return *(this);
      }
      R_count_destPage_sourcePage<_T0, _T1, _T2>& operator=(R_count_destPage_sourcePage<_T0, _T1,
      _T2>&& __other)  {
        count = std::move(__other.count);
        destPage = std::move(__other.destPage);
        sourcePage = std::move(__other.sourcePage);
        return *(this);
      }
      bool operator==(const R_count_destPage_sourcePage<_T0, _T1, _T2>& __other) const {
        return count == (__other.count) && destPage == (__other.destPage) && sourcePage == (__other.sourcePage);
      }
      bool operator!=(const R_count_destPage_sourcePage<_T0, _T1, _T2>& __other) const {
        return std::tie(count, destPage, sourcePage) != std::tie(__other.count, __other.destPage,
        __other.sourcePage);
      }
      bool operator<(const R_count_destPage_sourcePage<_T0, _T1, _T2>& __other) const {
        return std::tie(count, destPage, sourcePage) < std::tie(__other.count, __other.destPage,
        __other.sourcePage);
      }
      bool operator>(const R_count_destPage_sourcePage<_T0, _T1, _T2>& __other) const {
        return std::tie(count, destPage, sourcePage) > std::tie(__other.count, __other.destPage,
        __other.sourcePage);
      }
      _T0 count;
      _T1 destPage;
      _T2 sourcePage;
};
#endif
namespace K3 {
  template <class _T0, class _T1, class _T2>
  class patcher<R_count_destPage_sourcePage<_T0, _T1, _T2>> {
    public:
        static void patch(std::string _input, R_count_destPage_sourcePage<_T0, _T1,
        _T2>& _record)  {
          shallow<string::iterator> _shallow;
          qi::rule<string::iterator,
          qi::space_type> _count = (qi::lit("count") >> (':') >> _shallow)[([&_record] (std::string _partial)   {
            do_patch(_partial, _record.count);
          })];
          qi::rule<string::iterator,
          qi::space_type> _destPage = (qi::lit("destPage") >> (':') >> _shallow)[([&_record] (std::string _partial)   {
            do_patch(_partial, _record.destPage);
          })];
          qi::rule<string::iterator,
          qi::space_type> _sourcePage = (qi::lit("sourcePage") >> (':') >> _shallow)[([&_record] (std::string _partial)   {
            do_patch(_partial, _record.sourcePage);
          })];
          qi::rule<string::iterator, qi::space_type> _field = _count | _destPage | _sourcePage;
          qi::rule<string::iterator, qi::space_type> _parser = ('{') >> _field % (',') >> ('}');
          qi::phrase_parse(std::begin(_input), std::end(_input), _parser, qi::space);
        }
  };
}
#ifndef K3_R_count_destPage_sourcePage
#define K3_R_count_destPage_sourcePage
template <class _T0, class _T1, class _T2>
std::size_t hash_value(const R_count_destPage_sourcePage<_T0, _T1, _T2>& r)  {
  boost::hash<std::tuple<_T0, _T1, _T2>> hasher;
  return hasher(std::tie(r.count, r.destPage, r.sourcePage));
}
#endif
#ifndef K3_R_elem_hash_value
#define K3_R_elem_hash_value
template <class _T0>
class R_elem {
  public:
      R_elem(): elem()  {}
      R_elem(const _T0& _elem): elem(_elem)  {}
      R_elem(_T0&& _elem): elem(std::move(_elem))  {}
      R_elem(const R_elem<_T0>& __other): elem(__other.elem)  {}
      R_elem(R_elem<_T0>&& __other): elem(std::move(__other.elem))  {}
      template <class archive>
      void serialize(archive& _archive, const unsigned int _version)  {
        _archive & elem;
      }
      R_elem<_T0>& operator=(const R_elem<_T0>& __other)  {
        elem = (__other.elem);
        return *(this);
      }
      R_elem<_T0>& operator=(R_elem<_T0>&& __other)  {
        elem = std::move(__other.elem);
        return *(this);
      }
      bool operator==(const R_elem<_T0>& __other) const {
        return elem == (__other.elem);
      }
      bool operator!=(const R_elem<_T0>& __other) const {
        return std::tie(elem) != std::tie(__other.elem);
      }
      bool operator<(const R_elem<_T0>& __other) const {
        return std::tie(elem) < std::tie(__other.elem);
      }
      bool operator>(const R_elem<_T0>& __other) const {
        return std::tie(elem) > std::tie(__other.elem);
      }
      _T0 elem;
};
#endif
namespace K3 {
  template <class _T0>
  class patcher<R_elem<_T0>> {
    public:
        static void patch(std::string _input, R_elem<_T0>& _record)  {
          shallow<string::iterator> _shallow;
          qi::rule<string::iterator,
          qi::space_type> _elem = (qi::lit("elem") >> (':') >> _shallow)[([&_record] (std::string _partial)   {
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
std::size_t hash_value(const R_elem<_T0>& r)  {
  boost::hash<std::tuple<_T0>> hasher;
  return hasher(std::tie(r.elem));
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
template <class __CONTENT>
class _Map: public K3::Map<__CONTENT> {
  public:
      _Map(): K3::Map<__CONTENT>()  {}
      _Map(const _Map<__CONTENT>& __other): K3::Map<__CONTENT>(__other)  {}
      _Map(const K3::Map<__CONTENT>& __other1): K3::Map<__CONTENT>(__other1)  {}
      _Map(K3::Map<__CONTENT>&& __other1): K3::Map<__CONTENT>(std::move(__other1))  {}
      template <class archive>
      void serialize(archive& _archive, const unsigned int _version)  {
        _archive & boost::serialization::base_object<K3::Map<__CONTENT>>(*this);
      }
};
namespace K3 {
  template <class __CONTENT>
  class patcher<_Map<__CONTENT>> {
    public:
        static void patch(std::string _input, _Map<__CONTENT>& _c)  {
          collection_patcher<_Map, __CONTENT>::patch(_input, _c);
        }
  };
}
template <class __CONTENT>
class _Seq: public K3::Seq<__CONTENT> {
  public:
      _Seq(): K3::Seq<__CONTENT>()  {}
      _Seq(const _Seq<__CONTENT>& __other): K3::Seq<__CONTENT>(__other)  {}
      _Seq(const K3::Seq<__CONTENT>& __other1): K3::Seq<__CONTENT>(__other1)  {}
      _Seq(K3::Seq<__CONTENT>&& __other1): K3::Seq<__CONTENT>(std::move(__other1))  {}
      template <class archive>
      void serialize(archive& _archive, const unsigned int _version)  {
        _archive & boost::serialization::base_object<K3::Seq<__CONTENT>>(*this);
      }
};
namespace K3 {
  template <class __CONTENT>
  class patcher<_Seq<__CONTENT>> {
    public:
        static void patch(std::string _input, _Seq<__CONTENT>& _c)  {
          collection_patcher<_Seq, __CONTENT>::patch(_input, _c);
        }
  };
}
class __global_context: public K3::__standard_context, public K3::__string_context,
public K3::__time_context {
  public:
      __global_context(Engine& __engine): K3::__standard_context(__engine), K3::__string_context(),
      K3::__time_context()  {
        master = make_address("127.0.0.1", 40000);
        x = 3;
        peers_ready = 0;
        peers_finished = 0;
        start_ms = 0;
        end_ms = 0;
        elapsed_ms = 0;
        cur_page = "NONE";
        received = 0;
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
          aggregate(*(static_cast<_Collection<R_key_value<std::string, int>>*>(payload)));
        };
        dispatch_table[4] = [this] (void* payload)   {
          local(*(static_cast<unit_t*>(payload)));
        };
      }
      Address me;
      _Collection<R_addr<Address>> peers;
      std::tuple<_Collection<R_arg<std::string>>, _Collection<R_key_value<std::string,
      std::string>>> args;
      std::string role;
      unit_t stringLoader(string file, _Seq<R_elem<std::string>>& c)  {
        R_elem<std::string> rec;
        strtk::for_each_line(file, [&rec, &c] (const std::string& str)   {
          if (strtk::parse(str, ",", rec.elem)) {
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
      int start_ms;
      int end_ms;
      int elapsed_ms;
      _Seq<R_elem<std::string>> inputData;
      _Map<R_key_value<std::string, int>> url_count;
      _Map<R_key_value<std::string, int>> url_regex;
      std::string cur_page;
      _Collection<R_count_destPage_sourcePage<int, std::string, std::string>> url_counts_partial;
      unit_t get_line(const std::string& line)  {
        {
          _Seq<R_elem<std::string>> sp;
          sp = splitString(line)(" ");
          if (slice_string(line, 0, 4) == ("http") && sp.size(unit_t {}) == (5)) {
            cur_page = (sp.at(0)).elem;
            url_count.iterate([this] (const R_key_value<std::string, int>& v) mutable  {
              return url_counts_partial.insert(R_count_destPage_sourcePage<int, std::string,
              std::string> {v.value, v.key, cur_page});
            });
            url_count.iterate([this] (const R_key_value<std::string, int>& v) mutable  {
              return url_count.erase(v);
            });
          }
          return url_regex.iterate([this] (const R_key_value<std::string, int>& r) mutable  {
            shared_ptr<R_key_value<std::string, int>> __0;
            __0 = url_count.lookup(R_key_value<std::string, int> {r.key, 0});
            if (__0) {
              auto& x = *(__0);
              return url_count.insert(R_key_value<std::string, int> {r.key, (x.value) + (1)});
            } else {
              return url_count.insert(R_key_value<std::string, int> {r.key, 0});
            }
          });
        }
      }
      unit_t local(const unit_t& _)  {
        inputData.iterate([this] (const R_elem<std::string>& s) mutable  {
          return get_line(s.elem);
        });
        url_count.iterate([this] (const R_key_value<std::string, int>& v) mutable  {
          return url_counts_partial.insert(R_count_destPage_sourcePage<int, std::string,
          std::string> {v.value, v.key, cur_page});
        });
        {
          _Collection<R_key_value<std::string, int>> url_counts_total;
          url_counts_total = url_counts_partial.groupBy([this] (const R_count_destPage_sourcePage<int,
          std::string, std::string>& v) mutable  {
            return v.destPage;
          }, [this] (const int& acc) mutable  {
            return [this, &acc] (const R_count_destPage_sourcePage<int, std::string,
            std::string>& v) mutable  {
              return acc + (v.count);
            };
          }, 0);
          auto d = std::make_shared<K3::ValDispatcher<_Collection<R_key_value<std::string,
          int>>>>(url_counts_total);
          __engine.send(master, 3, d);
          return unit_t {};
        }
      }
      _Map<R_key_value<std::string, int>> url_counts_agg;
      int received;
      unit_t aggregate(const _Collection<R_key_value<std::string, int>>& newVals)  {
        newVals.iterate([this] (const R_key_value<std::string, int>& v) mutable  {
          shared_ptr<R_key_value<std::string, int>> __1;
          __1 = url_counts_agg.lookup(R_key_value<std::string, int> {v.key, 0});
          if (__1) {
            auto& x = *(__1);
            return url_counts_agg.insert(R_key_value<std::string, int> {v.key,
            (v.value) + (x.value)});
          } else {
            return url_counts_agg.insert(v);
          }
        });
        peers_finished = peers_finished + (1);
        if (peers_finished == peers.size(unit_t {})) {
          end_ms = now_int(unit_t {});
          elapsed_ms = end_ms - start_ms;
          print(itos(elapsed_ms));
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
        dataFiles.iterate([this] (const R_path<std::string>& e) mutable  {
          return stringLoader(e.path, inputData);
        });
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
        result["received"] = std::to_string(received);
        result["url_counts_agg"] = [] (_Map<R_key_value<std::string, int>> x)   {
          std::ostringstream oss;
          x.iterate([&oss] (const R_key_value<std::string, int>& elem)   {
            std::string s = ("{") + std::string("key:") + (elem.key) + (",") + std::string("value:") + std::to_string(elem.value) + ("}");
            oss << s << (",");
            return unit_t {};
          });
          return ("[") + oss.str();
        }(url_counts_agg);
        result["url_counts_partial"] = [] (_Collection<R_count_destPage_sourcePage<int, std::string,
        std::string>> x)   {
          std::ostringstream oss;
          x.iterate([&oss] (const R_count_destPage_sourcePage<int, std::string,
          std::string>& elem)   {
            std::string s = ("{") + std::string("sourcePage:") + (elem.sourcePage) + (",") + std::string("destPage:") + (elem.destPage) + (",") + std::string("count:") + std::to_string(elem.count) + ("}");
            oss << s << (",");
            return unit_t {};
          });
          return ("[") + oss.str();
        }(url_counts_partial);
        result["cur_page"] = cur_page;
        result["url_regex"] = [] (_Map<R_key_value<std::string, int>> x)   {
          std::ostringstream oss;
          x.iterate([&oss] (const R_key_value<std::string, int>& elem)   {
            std::string s = ("{") + std::string("key:") + (elem.key) + (",") + std::string("value:") + std::to_string(elem.value) + ("}");
            oss << s << (",");
            return unit_t {};
          });
          return ("[") + oss.str();
        }(url_regex);
        result["url_count"] = [] (_Map<R_key_value<std::string, int>> x)   {
          std::ostringstream oss;
          x.iterate([&oss] (const R_key_value<std::string, int>& elem)   {
            std::string s = ("{") + std::string("key:") + (elem.key) + (",") + std::string("value:") + std::to_string(elem.value) + ("}");
            oss << s << (",");
            return unit_t {};
          });
          return ("[") + oss.str();
        }(url_count);
        result["inputData"] = [] (_Seq<R_elem<std::string>> x)   {
          std::ostringstream oss;
          x.iterate([&oss] (const R_elem<std::string>& elem)   {
            std::string s = ("{") + std::string("elem:") + (elem.elem) + ("}");
            oss << s << (",");
            return unit_t {};
          });
          return ("[") + oss.str();
        }(inputData);
        result["elapsed_ms"] = std::to_string(elapsed_ms);
        result["end_ms"] = std::to_string(end_ms);
        result["start_ms"] = std::to_string(start_ms);
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
        if (bindings.count("received") > (0)) {
          do_patch(bindings["received"], received);
        }
        if (bindings.count("url_counts_agg") > (0)) {
          do_patch(bindings["url_counts_agg"], url_counts_agg);
        }
        if (bindings.count("url_counts_partial") > (0)) {
          do_patch(bindings["url_counts_partial"], url_counts_partial);
        }
        if (bindings.count("cur_page") > (0)) {
          do_patch(bindings["cur_page"], cur_page);
        }
        if (bindings.count("url_regex") > (0)) {
          do_patch(bindings["url_regex"], url_regex);
        }
        if (bindings.count("url_count") > (0)) {
          do_patch(bindings["url_count"], url_count);
        }
        if (bindings.count("inputData") > (0)) {
          do_patch(bindings["inputData"], inputData);
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
    __k3_context::__trigger_names[3] = "aggregate";
    __k3_context::__trigger_names[4] = "local";
    __k3_context::__clonable_dispatchers[0] = make_shared<ValDispatcher<unit_t>>();
    __k3_context::__clonable_dispatchers[1] = make_shared<ValDispatcher<unit_t>>();
    __k3_context::__clonable_dispatchers[2] = make_shared<ValDispatcher<unit_t>>();
    __k3_context::__clonable_dispatchers[3] = make_shared<ValDispatcher<_Collection<R_key_value<std::string,
    int>>>>();
    __k3_context::__clonable_dispatchers[4] = make_shared<ValDispatcher<unit_t>>();
  }
  SystemEnvironment se = defaultEnvironment(getAddrs(contexts));
  engine.configure(opt.simulation, se, make_shared<DefaultInternalCodec>(), opt.log_level);
  processRoles(contexts);
  engine.runEngine(make_shared<virtualizing_message_processor>(contexts));
}