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
#ifndef K3_R_adRevenue_countryCode_destURL_duration_languageCode_searchWord_sourceIP_userAgent_visitDate_hash_value
#define K3_R_adRevenue_countryCode_destURL_duration_languageCode_searchWord_sourceIP_userAgent_visitDate_hash_value
template <class _T0, class _T1, class _T2, class _T3, class _T4, class _T5, class _T6, class _T7,
class _T8>
class R_adRevenue_countryCode_destURL_duration_languageCode_searchWord_sourceIP_userAgent_visitDate {
  public:
      R_adRevenue_countryCode_destURL_duration_languageCode_searchWord_sourceIP_userAgent_visitDate(): adRevenue(),
      countryCode(), destURL(), duration(), languageCode(), searchWord(), sourceIP(), userAgent(),
      visitDate()  {}
      template <class __T0, class __T1, class __T2, class __T3, class __T4, class __T5, class __T6,
      class __T7, class __T8>
      R_adRevenue_countryCode_destURL_duration_languageCode_searchWord_sourceIP_userAgent_visitDate(__T0&& _adRevenue,
      __T1&& _countryCode, __T2&& _destURL, __T3&& _duration, __T4&& _languageCode,
      __T5&& _searchWord, __T6&& _sourceIP, __T7&& _userAgent,
      __T8&& _visitDate): adRevenue(std::forward<__T0>(_adRevenue)),
      countryCode(std::forward<__T1>(_countryCode)), destURL(std::forward<__T2>(_destURL)),
      duration(std::forward<__T3>(_duration)), languageCode(std::forward<__T4>(_languageCode)),
      searchWord(std::forward<__T5>(_searchWord)), sourceIP(std::forward<__T6>(_sourceIP)),
      userAgent(std::forward<__T7>(_userAgent)), visitDate(std::forward<__T8>(_visitDate))  {}
      R_adRevenue_countryCode_destURL_duration_languageCode_searchWord_sourceIP_userAgent_visitDate(const R_adRevenue_countryCode_destURL_duration_languageCode_searchWord_sourceIP_userAgent_visitDate<_T0,
      _T1, _T2, _T3, _T4, _T5, _T6, _T7, _T8>& __other): adRevenue(__other.adRevenue),
      countryCode(__other.countryCode), destURL(__other.destURL), duration(__other.duration),
      languageCode(__other.languageCode), searchWord(__other.searchWord),
      sourceIP(__other.sourceIP), userAgent(__other.userAgent), visitDate(__other.visitDate)  {}
      R_adRevenue_countryCode_destURL_duration_languageCode_searchWord_sourceIP_userAgent_visitDate(R_adRevenue_countryCode_destURL_duration_languageCode_searchWord_sourceIP_userAgent_visitDate<_T0,
      _T1, _T2, _T3, _T4, _T5, _T6, _T7, _T8>&& __other): adRevenue(std::move(__other.adRevenue)),
      countryCode(std::move(__other.countryCode)), destURL(std::move(__other.destURL)),
      duration(std::move(__other.duration)), languageCode(std::move(__other.languageCode)),
      searchWord(std::move(__other.searchWord)), sourceIP(std::move(__other.sourceIP)),
      userAgent(std::move(__other.userAgent)), visitDate(std::move(__other.visitDate))  {}
      template <class archive>
      void serialize(archive& _archive, const unsigned int _version)  {
        _archive & adRevenue;
        _archive & countryCode;
        _archive & destURL;
        _archive & duration;
        _archive & languageCode;
        _archive & searchWord;
        _archive & sourceIP;
        _archive & userAgent;
        _archive & visitDate;
      }
      R_adRevenue_countryCode_destURL_duration_languageCode_searchWord_sourceIP_userAgent_visitDate<_T0,
      _T1, _T2, _T3, _T4, _T5, _T6, _T7,
      _T8>& operator=(const R_adRevenue_countryCode_destURL_duration_languageCode_searchWord_sourceIP_userAgent_visitDate<_T0,
      _T1, _T2, _T3, _T4, _T5, _T6, _T7, _T8>& __other)  {
        adRevenue = (__other.adRevenue);
        countryCode = (__other.countryCode);
        destURL = (__other.destURL);
        duration = (__other.duration);
        languageCode = (__other.languageCode);
        searchWord = (__other.searchWord);
        sourceIP = (__other.sourceIP);
        userAgent = (__other.userAgent);
        visitDate = (__other.visitDate);
        return *(this);
      }
      R_adRevenue_countryCode_destURL_duration_languageCode_searchWord_sourceIP_userAgent_visitDate<_T0,
      _T1, _T2, _T3, _T4, _T5, _T6, _T7,
      _T8>& operator=(R_adRevenue_countryCode_destURL_duration_languageCode_searchWord_sourceIP_userAgent_visitDate<_T0,
      _T1, _T2, _T3, _T4, _T5, _T6, _T7, _T8>&& __other)  {
        adRevenue = std::move(__other.adRevenue);
        countryCode = std::move(__other.countryCode);
        destURL = std::move(__other.destURL);
        duration = std::move(__other.duration);
        languageCode = std::move(__other.languageCode);
        searchWord = std::move(__other.searchWord);
        sourceIP = std::move(__other.sourceIP);
        userAgent = std::move(__other.userAgent);
        visitDate = std::move(__other.visitDate);
        return *(this);
      }
      bool operator==(const R_adRevenue_countryCode_destURL_duration_languageCode_searchWord_sourceIP_userAgent_visitDate<_T0,
      _T1, _T2, _T3, _T4, _T5, _T6, _T7, _T8>& __other) const {
        return adRevenue == (__other.adRevenue) && countryCode == (__other.countryCode) && destURL == (__other.destURL) && duration == (__other.duration) && languageCode == (__other.languageCode) && searchWord == (__other.searchWord) && sourceIP == (__other.sourceIP) && userAgent == (__other.userAgent) && visitDate == (__other.visitDate);
      }
      bool operator!=(const R_adRevenue_countryCode_destURL_duration_languageCode_searchWord_sourceIP_userAgent_visitDate<_T0,
      _T1, _T2, _T3, _T4, _T5, _T6, _T7, _T8>& __other) const {
        return std::tie(adRevenue, countryCode, destURL, duration, languageCode, searchWord,
        sourceIP, userAgent, visitDate) != std::tie(__other.adRevenue, __other.countryCode,
        __other.destURL, __other.duration, __other.languageCode, __other.searchWord,
        __other.sourceIP, __other.userAgent, __other.visitDate);
      }
      bool operator<(const R_adRevenue_countryCode_destURL_duration_languageCode_searchWord_sourceIP_userAgent_visitDate<_T0,
      _T1, _T2, _T3, _T4, _T5, _T6, _T7, _T8>& __other) const {
        return std::tie(adRevenue, countryCode, destURL, duration, languageCode, searchWord,
        sourceIP, userAgent, visitDate) < std::tie(__other.adRevenue, __other.countryCode,
        __other.destURL, __other.duration, __other.languageCode, __other.searchWord,
        __other.sourceIP, __other.userAgent, __other.visitDate);
      }
      bool operator>(const R_adRevenue_countryCode_destURL_duration_languageCode_searchWord_sourceIP_userAgent_visitDate<_T0,
      _T1, _T2, _T3, _T4, _T5, _T6, _T7, _T8>& __other) const {
        return std::tie(adRevenue, countryCode, destURL, duration, languageCode, searchWord,
        sourceIP, userAgent, visitDate) > std::tie(__other.adRevenue, __other.countryCode,
        __other.destURL, __other.duration, __other.languageCode, __other.searchWord,
        __other.sourceIP, __other.userAgent, __other.visitDate);
      }
      _T0 adRevenue;
      _T1 countryCode;
      _T2 destURL;
      _T3 duration;
      _T4 languageCode;
      _T5 searchWord;
      _T6 sourceIP;
      _T7 userAgent;
      _T8 visitDate;
};
#endif
namespace K3 {
  template <class _T0, class _T1, class _T2, class _T3, class _T4, class _T5, class _T6, class _T7,
  class _T8>
  class patcher<R_adRevenue_countryCode_destURL_duration_languageCode_searchWord_sourceIP_userAgent_visitDate<_T0,
  _T1, _T2, _T3, _T4, _T5, _T6, _T7, _T8>> {
    public:
        static void patch(std::string _input,
        R_adRevenue_countryCode_destURL_duration_languageCode_searchWord_sourceIP_userAgent_visitDate<_T0,
        _T1, _T2, _T3, _T4, _T5, _T6, _T7, _T8>& _record)  {
          shallow<string::iterator> _shallow;
          qi::rule<string::iterator,
          qi::space_type> _adRevenue = (qi::lit("adRevenue") >> (':') >> _shallow)[([&_record] (std::string _partial)   {
            do_patch(_partial, _record.adRevenue);
          })];
          qi::rule<string::iterator,
          qi::space_type> _countryCode = (qi::lit("countryCode") >> (':') >> _shallow)[([&_record] (std::string _partial)   {
            do_patch(_partial, _record.countryCode);
          })];
          qi::rule<string::iterator,
          qi::space_type> _destURL = (qi::lit("destURL") >> (':') >> _shallow)[([&_record] (std::string _partial)   {
            do_patch(_partial, _record.destURL);
          })];
          qi::rule<string::iterator,
          qi::space_type> _duration = (qi::lit("duration") >> (':') >> _shallow)[([&_record] (std::string _partial)   {
            do_patch(_partial, _record.duration);
          })];
          qi::rule<string::iterator,
          qi::space_type> _languageCode = (qi::lit("languageCode") >> (':') >> _shallow)[([&_record] (std::string _partial)   {
            do_patch(_partial, _record.languageCode);
          })];
          qi::rule<string::iterator,
          qi::space_type> _searchWord = (qi::lit("searchWord") >> (':') >> _shallow)[([&_record] (std::string _partial)   {
            do_patch(_partial, _record.searchWord);
          })];
          qi::rule<string::iterator,
          qi::space_type> _sourceIP = (qi::lit("sourceIP") >> (':') >> _shallow)[([&_record] (std::string _partial)   {
            do_patch(_partial, _record.sourceIP);
          })];
          qi::rule<string::iterator,
          qi::space_type> _userAgent = (qi::lit("userAgent") >> (':') >> _shallow)[([&_record] (std::string _partial)   {
            do_patch(_partial, _record.userAgent);
          })];
          qi::rule<string::iterator,
          qi::space_type> _visitDate = (qi::lit("visitDate") >> (':') >> _shallow)[([&_record] (std::string _partial)   {
            do_patch(_partial, _record.visitDate);
          })];
          qi::rule<string::iterator,
          qi::space_type> _field = _adRevenue | _countryCode | _destURL | _duration | _languageCode | _searchWord | _sourceIP | _userAgent | _visitDate;
          qi::rule<string::iterator, qi::space_type> _parser = ('{') >> _field % (',') >> ('}');
          qi::phrase_parse(std::begin(_input), std::end(_input), _parser, qi::space);
        }
  };
}
#ifndef K3_R_adRevenue_countryCode_destURL_duration_languageCode_searchWord_sourceIP_userAgent_visitDate
#define K3_R_adRevenue_countryCode_destURL_duration_languageCode_searchWord_sourceIP_userAgent_visitDate
template <class _T0, class _T1, class _T2, class _T3, class _T4, class _T5, class _T6, class _T7,
class _T8>
std::size_t hash_value(const R_adRevenue_countryCode_destURL_duration_languageCode_searchWord_sourceIP_userAgent_visitDate<_T0,
_T1, _T2, _T3, _T4, _T5, _T6, _T7, _T8>& r)  {
  boost::hash<std::tuple<_T0, _T1, _T2, _T3, _T4, _T5, _T6, _T7, _T8>> hasher;
  return hasher(std::tie(r.adRevenue, r.countryCode, r.destURL, r.duration, r.languageCode,
  r.searchWord, r.sourceIP, r.userAgent, r.visitDate));
}
#endif
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
        master_peers_finished = 0;
        start_ms = 0;
        end_ms = 0;
        elapsed_ms = 0;
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
          master_done(*(static_cast<unit_t*>(payload)));
        };
        dispatch_table[4] = [this] (void* payload)   {
          peer_barrier(*(static_cast<unit_t*>(payload)));
        };
        dispatch_table[5] = [this] (void* payload)   {
          aggregate(*(static_cast<_Collection<R_key_value<std::string, double>>*>(payload)));
        };
        dispatch_table[6] = [this] (void* payload)   {
          q2_local(*(static_cast<unit_t*>(payload)));
        };
      }
      Address me;
      _Collection<R_addr<Address>> peers;
      std::tuple<_Collection<R_arg<std::string>>, _Collection<R_key_value<std::string,
      std::string>>> args;
      std::string role;
      unit_t dataLoader(string file,
      _Collection<R_adRevenue_countryCode_destURL_duration_languageCode_searchWord_sourceIP_userAgent_visitDate<double,
      std::string, std::string, int, std::string, std::string, std::string, std::string,
      std::string>>& c)  {
        R_adRevenue_countryCode_destURL_duration_languageCode_searchWord_sourceIP_userAgent_visitDate<double,
        std::string, std::string, int, std::string, std::string, std::string, std::string,
        std::string> rec;
        strtk::for_each_line(file, [&rec, &c] (const std::string& str)   {
          if (strtk::parse(str, ",", rec.sourceIP, rec.destURL, rec.visitDate, rec.adRevenue,
          rec.userAgent, rec.countryCode, rec.languageCode, rec.searchWord, rec.duration)) {
            c.insert(rec);
          } else {
            std::cout << ("Failed to parse a row!\n");
          }
        });
        return unit_t {};
      }
      _Collection<R_path<std::string>> dataFiles;
      _Seq<R_addr<Address>> peers_seq;
      Address master;
      int index_by_hash(const std::string& s)  {
        return hash(s) % peers_seq.size(unit_t {});
      }
      int x;
      int peers_ready;
      int peers_finished;
      int master_peers_finished;
      _Collection<R_adRevenue_countryCode_destURL_duration_languageCode_searchWord_sourceIP_userAgent_visitDate<double,
      std::string, std::string, int, std::string, std::string, std::string, std::string,
      std::string>> local_uservisits;
      _Map<R_key_value<std::string, double>> local_q2_results;
      int start_ms;
      int end_ms;
      int elapsed_ms;
      _Collection<R_key_value<std::string, double>> agg_vals;
      _Collection<R_key_value<int, _Collection<R_key_value<std::string, double>>>> peer_aggs;
      unit_t q2_local(const unit_t& _)  {
        agg_vals = local_uservisits.groupBy([this] (const R_adRevenue_countryCode_destURL_duration_languageCode_searchWord_sourceIP_userAgent_visitDate<double,
        std::string, std::string, int, std::string, std::string, std::string, std::string,
        std::string>& r) mutable  {
          return slice_string(r.sourceIP, 0, x);
        }, [this] (const double& acc) mutable  {
          return [this,
          &acc] (const R_adRevenue_countryCode_destURL_duration_languageCode_searchWord_sourceIP_userAgent_visitDate<double,
          std::string, std::string, int, std::string, std::string, std::string, std::string,
          std::string>& r) mutable  {
            return acc + (r.adRevenue);
          };
        }, 0.0);
        peer_aggs = agg_vals.groupBy([this] (const R_key_value<std::string, double>& v) mutable  {
          return index_by_hash(v.key);
        }, [this] (_Collection<R_key_value<std::string, double>> acc) mutable  {
          return [this, acc = std::move(acc)] (const R_key_value<std::string, double>& v) mutable  {
            acc.insert(v);
            return acc;
          };
        }, _Collection<R_key_value<std::string, double>> {});
        peer_aggs.iterate([this] (const R_key_value<int, _Collection<R_key_value<std::string,
        double>>>& v) mutable  {
          auto d = std::make_shared<K3::ValDispatcher<_Collection<R_key_value<std::string,
          double>>>>(v.value);
          __engine.send((peers_seq.at(v.key)).addr, 5, d);
          return unit_t {};
        });
        return peers.iterate([this] (const R_addr<Address>& p) mutable  {
          auto d = std::make_shared<K3::ValDispatcher<unit_t>>(unit_t {});
          __engine.send(p.addr, 4, d);
          return unit_t {};
        });
      }
      unit_t merge_results(const _Collection<R_key_value<std::string, double>>& vals)  {
        return vals.iterate([this] (const R_key_value<std::string, double>& v) mutable  {
          shared_ptr<R_key_value<std::string, double>> __0;
          __0 = local_q2_results.lookup(R_key_value<std::string, double> {v.key, 0.0});
          if (__0) {
            auto& kv = *(__0);
            return local_q2_results.insert(R_key_value<std::string, double> {v.key,
            (kv.value) + (v.value)});
          } else {
            return local_q2_results.insert(v);
          }
        });
      }
      unit_t aggregate(const _Collection<R_key_value<std::string, double>>& vals)  {
        return merge_results(vals);
      }
      unit_t peer_barrier(const unit_t& _)  {
        peers_finished = peers_finished + (1);
        if (peers_finished == peers.size(unit_t {})) {
          auto d = std::make_shared<K3::ValDispatcher<unit_t>>(unit_t {});
          __engine.send(master, 3, d);
          return unit_t {};
        } else {
          return unit_t {};
        }
      }
      unit_t master_done(const unit_t& _)  {
        master_peers_finished = master_peers_finished + (1);
        if (master_peers_finished == peers.size(unit_t {})) {
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
        print(concat("Num local results: ", itos(local_q2_results.size(unit_t {}))));
        return haltEngine(unit_t {});
      }
      unit_t ready(const unit_t& _)  {
        peers_ready = peers_ready + (1);
        if (peers_ready == peers.size(unit_t {})) {
          start_ms = now_int(unit_t {});
          return peers.iterate([this] (const R_addr<Address>& p) mutable  {
            auto d = std::make_shared<K3::ValDispatcher<unit_t>>(unit_t {});
            __engine.send(p.addr, 6, d);
            return unit_t {};
          });
        } else {
          return unit_t {};
        }
      }
      unit_t load_all(const unit_t& _)  {
        peers.iterate([this] (const R_addr<Address>& i) mutable  {
          return peers_seq.insert(i);
        });
        dataFiles.iterate([this] (const R_path<std::string>& e) mutable  {
          return dataLoader(e.path, local_uservisits);
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
        result["peer_aggs"] = [] (_Collection<R_key_value<int, _Collection<R_key_value<std::string,
        double>>>> x)   {
          std::ostringstream oss;
          x.iterate([&oss] (const R_key_value<int, _Collection<R_key_value<std::string,
          double>>>& elem)   {
            std::string s = ("{") + std::string("key:") + std::to_string(elem.key) + (",") + std::string("value:") + [] (_Collection<R_key_value<std::string,
            double>> x)   {
              std::ostringstream oss;
              x.iterate([&oss] (const R_key_value<std::string, double>& elem)   {
                std::string s = ("{") + std::string("key:") + (elem.key) + (",") + std::string("value:") + std::to_string(elem.value) + ("}");
                oss << s << (",");
                return unit_t {};
              });
              return ("[") + oss.str();
            }(elem.value) + ("}");
            oss << s << (",");
            return unit_t {};
          });
          return ("[") + oss.str();
        }(peer_aggs);
        result["agg_vals"] = [] (_Collection<R_key_value<std::string, double>> x)   {
          std::ostringstream oss;
          x.iterate([&oss] (const R_key_value<std::string, double>& elem)   {
            std::string s = ("{") + std::string("key:") + (elem.key) + (",") + std::string("value:") + std::to_string(elem.value) + ("}");
            oss << s << (",");
            return unit_t {};
          });
          return ("[") + oss.str();
        }(agg_vals);
        result["elapsed_ms"] = std::to_string(elapsed_ms);
        result["end_ms"] = std::to_string(end_ms);
        result["start_ms"] = std::to_string(start_ms);
        result["local_q2_results"] = [] (_Map<R_key_value<std::string, double>> x)   {
          std::ostringstream oss;
          x.iterate([&oss] (const R_key_value<std::string, double>& elem)   {
            std::string s = ("{") + std::string("key:") + (elem.key) + (",") + std::string("value:") + std::to_string(elem.value) + ("}");
            oss << s << (",");
            return unit_t {};
          });
          return ("[") + oss.str();
        }(local_q2_results);
        result["local_uservisits"] = [] (_Collection<R_adRevenue_countryCode_destURL_duration_languageCode_searchWord_sourceIP_userAgent_visitDate<double,
        std::string, std::string, int, std::string, std::string, std::string, std::string,
        std::string>> x)   {
          std::ostringstream oss;
          x.iterate([&oss] (const R_adRevenue_countryCode_destURL_duration_languageCode_searchWord_sourceIP_userAgent_visitDate<double,
          std::string, std::string, int, std::string, std::string, std::string, std::string,
          std::string>& elem)   {
            std::string s = ("{") + std::string("adRevenue:") + std::to_string(elem.adRevenue) + (",") + std::string("countryCode:") + (elem.countryCode) + (",") + std::string("destURL:") + (elem.destURL) + (",") + std::string("duration:") + std::to_string(elem.duration) + (",") + std::string("languageCode:") + (elem.languageCode) + (",") + std::string("searchWord:") + (elem.searchWord) + (",") + std::string("sourceIP:") + (elem.sourceIP) + (",") + std::string("userAgent:") + (elem.userAgent) + (",") + std::string("visitDate:") + (elem.visitDate) + ("}");
            oss << s << (",");
            return unit_t {};
          });
          return ("[") + oss.str();
        }(local_uservisits);
        result["master_peers_finished"] = std::to_string(master_peers_finished);
        result["peers_finished"] = std::to_string(peers_finished);
        result["peers_ready"] = std::to_string(peers_ready);
        result["x"] = std::to_string(x);
        result["master"] = K3::addressAsString(master);
        result["peers_seq"] = [] (_Seq<R_addr<Address>> x)   {
          std::ostringstream oss;
          x.iterate([&oss] (const R_addr<Address>& elem)   {
            std::string s = ("{") + std::string("addr:") + K3::addressAsString(elem.addr) + ("}");
            oss << s << (",");
            return unit_t {};
          });
          return ("[") + oss.str();
        }(peers_seq);
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
        if (bindings.count("peer_aggs") > (0)) {
          do_patch(bindings["peer_aggs"], peer_aggs);
        }
        if (bindings.count("agg_vals") > (0)) {
          do_patch(bindings["agg_vals"], agg_vals);
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
        if (bindings.count("local_q2_results") > (0)) {
          do_patch(bindings["local_q2_results"], local_q2_results);
        }
        if (bindings.count("local_uservisits") > (0)) {
          do_patch(bindings["local_uservisits"], local_uservisits);
        }
        if (bindings.count("master_peers_finished") > (0)) {
          do_patch(bindings["master_peers_finished"], master_peers_finished);
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
        if (bindings.count("peers_seq") > (0)) {
          do_patch(bindings["peers_seq"], peers_seq);
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
    __k3_context::__trigger_names[3] = "master_done";
    __k3_context::__trigger_names[4] = "peer_barrier";
    __k3_context::__trigger_names[5] = "aggregate";
    __k3_context::__trigger_names[6] = "q2_local";
    __k3_context::__clonable_dispatchers[0] = make_shared<ValDispatcher<unit_t>>();
    __k3_context::__clonable_dispatchers[1] = make_shared<ValDispatcher<unit_t>>();
    __k3_context::__clonable_dispatchers[2] = make_shared<ValDispatcher<unit_t>>();
    __k3_context::__clonable_dispatchers[3] = make_shared<ValDispatcher<unit_t>>();
    __k3_context::__clonable_dispatchers[4] = make_shared<ValDispatcher<unit_t>>();
    __k3_context::__clonable_dispatchers[5] = make_shared<ValDispatcher<_Collection<R_key_value<std::string,
    double>>>>();
    __k3_context::__clonable_dispatchers[6] = make_shared<ValDispatcher<unit_t>>();
  }
  SystemEnvironment se = defaultEnvironment(getAddrs(contexts));
  engine.configure(opt.simulation, se, make_shared<DefaultInternalCodec>(), opt.log_level);
  processRoles(contexts);
  engine.runEngine(make_shared<virtualizing_message_processor>(contexts));
}