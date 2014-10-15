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
#ifndef K3_R_arSum_prCount_prSum_hash_value
#define K3_R_arSum_prCount_prSum_hash_value
template <class _T0, class _T1, class _T2>
class R_arSum_prCount_prSum {
  public:
      R_arSum_prCount_prSum(): arSum(), prCount(), prSum()  {}
      template <class __T0, class __T1, class __T2>
      R_arSum_prCount_prSum(__T0&& _arSum, __T1&& _prCount,
      __T2&& _prSum): arSum(std::forward<__T0>(_arSum)), prCount(std::forward<__T1>(_prCount)),
      prSum(std::forward<__T2>(_prSum))  {}
      R_arSum_prCount_prSum(const R_arSum_prCount_prSum<_T0, _T1,
      _T2>& __other): arSum(__other.arSum), prCount(__other.prCount), prSum(__other.prSum)  {}
      R_arSum_prCount_prSum(R_arSum_prCount_prSum<_T0, _T1,
      _T2>&& __other): arSum(std::move(__other.arSum)), prCount(std::move(__other.prCount)),
      prSum(std::move(__other.prSum))  {}
      template <class archive>
      void serialize(archive& _archive, const unsigned int _version)  {
        _archive & arSum;
        _archive & prCount;
        _archive & prSum;
      }
      R_arSum_prCount_prSum<_T0, _T1, _T2>& operator=(const R_arSum_prCount_prSum<_T0, _T1,
      _T2>& __other)  {
        arSum = (__other.arSum);
        prCount = (__other.prCount);
        prSum = (__other.prSum);
        return *(this);
      }
      R_arSum_prCount_prSum<_T0, _T1, _T2>& operator=(R_arSum_prCount_prSum<_T0, _T1,
      _T2>&& __other)  {
        arSum = std::move(__other.arSum);
        prCount = std::move(__other.prCount);
        prSum = std::move(__other.prSum);
        return *(this);
      }
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
      _T0 arSum;
      _T1 prCount;
      _T2 prSum;
};
#endif
namespace K3 {
  template <class _T0, class _T1, class _T2>
  class patcher<R_arSum_prCount_prSum<_T0, _T1, _T2>> {
    public:
        static void patch(std::string _input, R_arSum_prCount_prSum<_T0, _T1, _T2>& _record)  {
          shallow<string::iterator> _shallow;
          qi::rule<string::iterator,
          qi::space_type> _arSum = (qi::lit("arSum") >> (':') >> _shallow)[([&_record] (std::string _partial)   {
            do_patch(_partial, _record.arSum);
          })];
          qi::rule<string::iterator,
          qi::space_type> _prCount = (qi::lit("prCount") >> (':') >> _shallow)[([&_record] (std::string _partial)   {
            do_patch(_partial, _record.prCount);
          })];
          qi::rule<string::iterator,
          qi::space_type> _prSum = (qi::lit("prSum") >> (':') >> _shallow)[([&_record] (std::string _partial)   {
            do_patch(_partial, _record.prSum);
          })];
          qi::rule<string::iterator, qi::space_type> _field = _arSum | _prCount | _prSum;
          qi::rule<string::iterator, qi::space_type> _parser = ('{') >> _field % (',') >> ('}');
          qi::phrase_parse(std::begin(_input), std::end(_input), _parser, qi::space);
        }
  };
}
#ifndef K3_R_arSum_prCount_prSum
#define K3_R_arSum_prCount_prSum
template <class _T0, class _T1, class _T2>
std::size_t hash_value(const R_arSum_prCount_prSum<_T0, _T1, _T2>& r)  {
  boost::hash<std::tuple<_T0, _T1, _T2>> hasher;
  return hasher(std::tie(r.arSum, r.prCount, r.prSum));
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
class __global_context: public K3::__standard_context, public K3::__string_context,
public K3::__time_context {
  public:
      __global_context(Engine& __engine): K3::__standard_context(__engine), K3::__string_context(),
      K3::__time_context()  {
        master = make_address("127.0.0.1", 30001);
        peer_count = 0;
        date_lb = 0;
        date_ub = 100;
        merged_peers = 0;
        peers_done = 0;
        peers_ready = 0;
        dispatch_table[0] = [this] (void* payload)   {
          loadAll(*(static_cast<unit_t*>(payload)));
        };
        dispatch_table[1] = [this] (void* payload)   {
          ready(*(static_cast<unit_t*>(payload)));
        };
        dispatch_table[2] = [this] (void* payload)   {
          finished(*(static_cast<unit_t*>(payload)));
        };
        dispatch_table[3] = [this] (void* payload)   {
          find_global_max(*(static_cast<R_key_value<std::string, R_arSum_prCount_prSum<double, int,
          int>>*>(payload)));
        };
        dispatch_table[4] = [this] (void* payload)   {
          find_local_max(*(static_cast<unit_t*>(payload)));
        };
        dispatch_table[5] = [this] (void* payload)   {
          count_merges(*(static_cast<unit_t*>(payload)));
        };
        dispatch_table[6] = [this] (void* payload)   {
          merge(*(static_cast<_Map<R_key_value<std::string, R_arSum_prCount_prSum<double, int,
          int>>>*>(payload)));
        };
        dispatch_table[7] = [this] (void* payload)   {
          q3_local(*(static_cast<unit_t*>(payload)));
        };
      }
      Address me;
      _Collection<R_addr<Address>> peers;
      std::tuple<_Collection<R_arg<std::string>>, _Collection<R_key_value<std::string,
      std::string>>> args;
      std::string role;
      Address master;
      int peer_count;
      int start_ms;
      int end_ms;
      int elapsed_ms;
      _Collection<R_adRevenue_countryCode_destURL_duration_languageCode_searchWord_sourceIP_userAgent_visitDate<double,
      std::string, std::string, int, std::string, std::string, std::string, std::string,
      int>> user_visits;
      _Map<R_key_value<std::string, int>> rankingsMap;
      _Map<R_key_value<std::string, R_arSum_prCount_prSum<double, int,
      int>>> empty_source_ip_to_aggs_C;
      R_arSum_prCount_prSum<double, int, int> empty_aggs;
      _Map<R_key_value<Address, _Map<R_key_value<std::string, R_arSum_prCount_prSum<double, int,
      int>>>>> partial_aggs;
      _Map<R_key_value<std::string, R_arSum_prCount_prSum<double, int, int>>> merged_partials;
      int date_lb;
      int date_ub;
      bool valid_date(const int& i)  {
        return i <= date_ub && i >= date_lb;
      }
      unit_t q3_local(const unit_t& _)  {
        user_visits.iterate([this] (const R_adRevenue_countryCode_destURL_duration_languageCode_searchWord_sourceIP_userAgent_visitDate<double,
        std::string, std::string, int, std::string, std::string, std::string, std::string,
        int>& uv) mutable  {
          if (valid_date(uv.visitDate)) {
            {
              std::string ip;
              ip = uv.sourceIP;
              {
                Address a;
                a = peer_by_hash(ip);
                shared_ptr<R_key_value<Address, _Map<R_key_value<std::string,
                R_arSum_prCount_prSum<double, int, int>>>>> __0;
                __0 = partial_aggs.lookup(R_key_value<Address, _Map<R_key_value<std::string,
                R_arSum_prCount_prSum<double, int, int>>>> {a, empty_source_ip_to_aggs_C});
                if (__0) {
                  auto& aggs = *(__0);
                  shared_ptr<R_key_value<std::string, int>> __1;
                  __1 = rankingsMap.lookup(R_key_value<std::string, int> {ip, 0});
                  if (__1) {
                    auto& r = *(__1);
                    shared_ptr<R_key_value<std::string, R_arSum_prCount_prSum<double, int,
                    int>>> __2;
                    __2 = (aggs.value).lookup(R_key_value<std::string, R_arSum_prCount_prSum<double,
                    int, int>> {ip, empty_aggs});
                    if (__2) {
                      auto& pa_v = *(__2);
                      (aggs.value).insert(R_key_value<std::string, R_arSum_prCount_prSum<double,
                      int, int>> {ip, R_arSum_prCount_prSum<double, int,
                      int> {((pa_v.value).arSum) + (uv.adRevenue), ((pa_v.value).prCount) + (1),
                      ((pa_v.value).prSum) + (r.value)}});
                    } else {
                      (aggs.value).insert(R_key_value<std::string, R_arSum_prCount_prSum<double,
                      int, int>> {ip, R_arSum_prCount_prSum<double, int, int> {uv.adRevenue, 1,
                      r.value}});
                    }
                    return partial_aggs.insert(R_key_value<Address, _Map<R_key_value<std::string,
                    R_arSum_prCount_prSum<double, int, int>>>> {a, aggs.value});
                  } else {
                    return unit_t {};
                  }
                } else {
                  shared_ptr<R_key_value<std::string, int>> __3;
                  __3 = rankingsMap.lookup(R_key_value<std::string, int> {ip, 0});
                  if (__3) {
                    auto& r = *(__3);
                    _Map<R_key_value<std::string, R_arSum_prCount_prSum<double, int, int>>> __4;
                    {
                      _Map<R_key_value<std::string, R_arSum_prCount_prSum<double, int,
                      int>>> __collection;
                      __collection = _Map<R_key_value<std::string, R_arSum_prCount_prSum<double,
                      int, int>>> {};
                      __collection.insert(R_key_value<std::string, R_arSum_prCount_prSum<double,
                      int, int>> {ip, R_arSum_prCount_prSum<double, int, int> {uv.adRevenue, 1,
                      r.value}});
                      __4 = __collection;
                    }
                    return partial_aggs.insert(R_key_value<Address, _Map<R_key_value<std::string,
                    R_arSum_prCount_prSum<double, int, int>>>> {a, __4});
                  } else {
                    return unit_t {};
                  }
                }
              }
            }
          } else {
            return unit_t {};
          }
        });
        partial_aggs.iterate([this] (const R_key_value<Address, _Map<R_key_value<std::string,
        R_arSum_prCount_prSum<double, int, int>>>>& kv) mutable  {
          auto d = std::make_shared<K3::ValDispatcher<_Map<R_key_value<std::string,
          R_arSum_prCount_prSum<double, int, int>>>>>(kv.value);
          __engine.send(kv.key, 6, d);
          return unit_t {};
        });
        return peers.iterate([this] (const R_addr<Address>& p) mutable  {
          auto d = std::make_shared<K3::ValDispatcher<unit_t>>(unit_t {});
          __engine.send(p.addr, 5, d);
          return unit_t {};
        });
      }
      unit_t merge(const _Map<R_key_value<std::string, R_arSum_prCount_prSum<double, int,
      int>>>& aggs_map)  {
        return aggs_map.iterate([this] (const R_key_value<std::string, R_arSum_prCount_prSum<double,
        int, int>>& kv) mutable  {
          shared_ptr<R_key_value<std::string, R_arSum_prCount_prSum<double, int, int>>> __5;
          __5 = merged_partials.lookup(R_key_value<std::string, R_arSum_prCount_prSum<double, int,
          int>> {kv.key, kv.value});
          if (__5) {
            auto& agg = *(__5);
            return merged_partials.insert(R_key_value<std::string, R_arSum_prCount_prSum<double,
            int, int>> {kv.key, R_arSum_prCount_prSum<double, int,
            int> {((agg.value).arSum) + ((kv.value).arSum),
            ((agg.value).prCount) + ((kv.value).prCount),
            ((agg.value).prSum) + ((kv.value).prSum)}});
          } else {
            return merged_partials.insert(kv);
          }
        });
      }
      int merged_peers;
      unit_t count_merges(const unit_t& _)  {
        merged_peers = merged_peers + (1);
        if (merged_peers == peers.size(unit_t {})) {
          auto d = std::make_shared<K3::ValDispatcher<unit_t>>(unit_t {});
          __engine.send(me, 4, d);
          return unit_t {};
        } else {
          return unit_t {};
        }
      }
      R_key_value<std::string, R_arSum_prCount_prSum<double, int, int>> local_max;
      unit_t find_local_max(const unit_t& _)  {
        merged_partials.iterate([this] (const R_key_value<std::string, R_arSum_prCount_prSum<double,
        int, int>>& kv) mutable  {
          if (((kv.value).arSum) > ((local_max.value).arSum)) {
            local_max = kv;
            return unit_t {};
          } else {
            return unit_t {};
          }
        });
        auto d = std::make_shared<K3::ValDispatcher<R_key_value<std::string,
        R_arSum_prCount_prSum<double, int, int>>>>(local_max);
        __engine.send(master, 3, d);
        return unit_t {};
      }
      int peers_done;
      R_key_value<std::string, R_arSum_prCount_prSum<double, int, int>> global_max;
      unit_t find_global_max(const R_key_value<std::string, R_arSum_prCount_prSum<double, int,
      int>>& kv)  {
        if (((kv.value).arSum) > ((global_max.value).arSum)) {
          global_max = kv;
          return unit_t {};
        } else {
          peers_done = peers_done + (1);
          if (peers_done == peers.size(unit_t {})) {
            auto d = std::make_shared<K3::ValDispatcher<unit_t>>(unit_t {});
            __engine.send(master, 2, d);
            return unit_t {};
          } else {
            return unit_t {};
          }
        }
      }
      unit_t finished(const unit_t& _)  {
        end_ms = now_int(unit_t {});
        elapsed_ms = end_ms - start_ms;
        return print(concat("Elapsed: ", itos(elapsed_ms)));
      }
      int peers_ready;
      unit_t ready(const unit_t& _)  {
        peers_ready = peers_ready + (1);
        if (peers_ready == peers.size(unit_t {})) {
          start_ms = now_int(unit_t {});
          return peers.iterate([this] (const R_addr<Address>& p) mutable  {
            auto d = std::make_shared<K3::ValDispatcher<unit_t>>(unit_t {});
            __engine.send(p.addr, 7, d);
            return unit_t {};
          });
        } else {
          return unit_t {};
        }
      }
      _Collection<R_path<std::string>> uvFiles;
      unit_t uvLoader(string file,
      _Collection<R_adRevenue_countryCode_destURL_duration_languageCode_searchWord_sourceIP_userAgent_visitDate<double,
      std::string, std::string, int, std::string, std::string, std::string, std::string,
      int>>& c)  {
        R_adRevenue_countryCode_destURL_duration_languageCode_searchWord_sourceIP_userAgent_visitDate<double,
        std::string, std::string, int, std::string, std::string, std::string, std::string, int> rec;
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
      _Collection<R_path<std::string>> rkFiles;
      unit_t loadAll(const unit_t& _)  {
        uvFiles.iterate([this] (const R_path<std::string>& e) mutable  {
          return uvLoader(e.path, user_visits);
        });
        rkFiles.iterate([this] (const R_path<std::string>& e) mutable  {
          return rkLoaderMap(e.path, rankingsMap);
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
        result["rkFiles"] = [] (_Collection<R_path<std::string>> x)   {
          std::ostringstream oss;
          x.iterate([&oss] (const R_path<std::string>& elem)   {
            std::string s = ("{") + std::string("path:") + (elem.path) + ("}");
            oss << s << (",");
            return unit_t {};
          });
          return ("[") + oss.str();
        }(rkFiles);
        result["uvFiles"] = [] (_Collection<R_path<std::string>> x)   {
          std::ostringstream oss;
          x.iterate([&oss] (const R_path<std::string>& elem)   {
            std::string s = ("{") + std::string("path:") + (elem.path) + ("}");
            oss << s << (",");
            return unit_t {};
          });
          return ("[") + oss.str();
        }(uvFiles);
        result["peers_ready"] = std::to_string(peers_ready);
        result["global_max"] = ("{") + std::string("key:") + (global_max.key) + (",") + std::string("value:") + ("{") + std::string("prSum:") + std::to_string((global_max.value).prSum) + (",") + std::string("prCount:") + std::to_string((global_max.value).prCount) + (",") + std::string("arSum:") + std::to_string((global_max.value).arSum) + ("}") + ("}");
        result["peers_done"] = std::to_string(peers_done);
        result["local_max"] = ("{") + std::string("key:") + (local_max.key) + (",") + std::string("value:") + ("{") + std::string("prSum:") + std::to_string((local_max.value).prSum) + (",") + std::string("prCount:") + std::to_string((local_max.value).prCount) + (",") + std::string("arSum:") + std::to_string((local_max.value).arSum) + ("}") + ("}");
        result["merged_peers"] = std::to_string(merged_peers);
        result["date_ub"] = std::to_string(date_ub);
        result["date_lb"] = std::to_string(date_lb);
        result["merged_partials"] = [] (_Map<R_key_value<std::string, R_arSum_prCount_prSum<double,
        int, int>>> x)   {
          std::ostringstream oss;
          x.iterate([&oss] (const R_key_value<std::string, R_arSum_prCount_prSum<double, int,
          int>>& elem)   {
            std::string s = ("{") + std::string("key:") + (elem.key) + (",") + std::string("value:") + ("{") + std::string("prSum:") + std::to_string((elem.value).prSum) + (",") + std::string("prCount:") + std::to_string((elem.value).prCount) + (",") + std::string("arSum:") + std::to_string((elem.value).arSum) + ("}") + ("}");
            oss << s << (",");
            return unit_t {};
          });
          return ("[") + oss.str();
        }(merged_partials);
        result["partial_aggs"] = [] (_Map<R_key_value<Address, _Map<R_key_value<std::string,
        R_arSum_prCount_prSum<double, int, int>>>>> x)   {
          std::ostringstream oss;
          x.iterate([&oss] (const R_key_value<Address, _Map<R_key_value<std::string,
          R_arSum_prCount_prSum<double, int, int>>>>& elem)   {
            std::string s = ("{") + std::string("key:") + K3::addressAsString(elem.key) + (",") + std::string("value:") + [] (_Map<R_key_value<std::string,
            R_arSum_prCount_prSum<double, int, int>>> x)   {
              std::ostringstream oss;
              x.iterate([&oss] (const R_key_value<std::string, R_arSum_prCount_prSum<double, int,
              int>>& elem)   {
                std::string s = ("{") + std::string("key:") + (elem.key) + (",") + std::string("value:") + ("{") + std::string("prSum:") + std::to_string((elem.value).prSum) + (",") + std::string("prCount:") + std::to_string((elem.value).prCount) + (",") + std::string("arSum:") + std::to_string((elem.value).arSum) + ("}") + ("}");
                oss << s << (",");
                return unit_t {};
              });
              return ("[") + oss.str();
            }(elem.value) + ("}");
            oss << s << (",");
            return unit_t {};
          });
          return ("[") + oss.str();
        }(partial_aggs);
        result["empty_aggs"] = ("{") + std::string("prSum:") + std::to_string(empty_aggs.prSum) + (",") + std::string("prCount:") + std::to_string(empty_aggs.prCount) + (",") + std::string("arSum:") + std::to_string(empty_aggs.arSum) + ("}");
        result["empty_source_ip_to_aggs_C"] = [] (_Map<R_key_value<std::string,
        R_arSum_prCount_prSum<double, int, int>>> x)   {
          std::ostringstream oss;
          x.iterate([&oss] (const R_key_value<std::string, R_arSum_prCount_prSum<double, int,
          int>>& elem)   {
            std::string s = ("{") + std::string("key:") + (elem.key) + (",") + std::string("value:") + ("{") + std::string("prSum:") + std::to_string((elem.value).prSum) + (",") + std::string("prCount:") + std::to_string((elem.value).prCount) + (",") + std::string("arSum:") + std::to_string((elem.value).arSum) + ("}") + ("}");
            oss << s << (",");
            return unit_t {};
          });
          return ("[") + oss.str();
        }(empty_source_ip_to_aggs_C);
        result["rankingsMap"] = [] (_Map<R_key_value<std::string, int>> x)   {
          std::ostringstream oss;
          x.iterate([&oss] (const R_key_value<std::string, int>& elem)   {
            std::string s = ("{") + std::string("key:") + (elem.key) + (",") + std::string("value:") + std::to_string(elem.value) + ("}");
            oss << s << (",");
            return unit_t {};
          });
          return ("[") + oss.str();
        }(rankingsMap);
        result["user_visits"] = [] (_Collection<R_adRevenue_countryCode_destURL_duration_languageCode_searchWord_sourceIP_userAgent_visitDate<double,
        std::string, std::string, int, std::string, std::string, std::string, std::string,
        int>> x)   {
          std::ostringstream oss;
          x.iterate([&oss] (const R_adRevenue_countryCode_destURL_duration_languageCode_searchWord_sourceIP_userAgent_visitDate<double,
          std::string, std::string, int, std::string, std::string, std::string, std::string,
          int>& elem)   {
            std::string s = ("{") + std::string("sourceIP:") + (elem.sourceIP) + (",") + std::string("destURL:") + (elem.destURL) + (",") + std::string("visitDate:") + std::to_string(elem.visitDate) + (",") + std::string("adRevenue:") + std::to_string(elem.adRevenue) + (",") + std::string("userAgent:") + (elem.userAgent) + (",") + std::string("countryCode:") + (elem.countryCode) + (",") + std::string("languageCode:") + (elem.languageCode) + (",") + std::string("searchWord:") + (elem.searchWord) + (",") + std::string("duration:") + std::to_string(elem.duration) + ("}");
            oss << s << (",");
            return unit_t {};
          });
          return ("[") + oss.str();
        }(user_visits);
        result["elapsed_ms"] = std::to_string(elapsed_ms);
        result["end_ms"] = std::to_string(end_ms);
        result["start_ms"] = std::to_string(start_ms);
        result["peer_count"] = std::to_string(peer_count);
        result["master"] = K3::addressAsString(master);
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
        if (bindings.count("rkFiles") > (0)) {
          do_patch(bindings["rkFiles"], rkFiles);
        }
        if (bindings.count("uvFiles") > (0)) {
          do_patch(bindings["uvFiles"], uvFiles);
        }
        if (bindings.count("peers_ready") > (0)) {
          do_patch(bindings["peers_ready"], peers_ready);
        }
        if (bindings.count("global_max") > (0)) {
          do_patch(bindings["global_max"], global_max);
        }
        if (bindings.count("peers_done") > (0)) {
          do_patch(bindings["peers_done"], peers_done);
        }
        if (bindings.count("local_max") > (0)) {
          do_patch(bindings["local_max"], local_max);
        }
        if (bindings.count("merged_peers") > (0)) {
          do_patch(bindings["merged_peers"], merged_peers);
        }
        if (bindings.count("date_ub") > (0)) {
          do_patch(bindings["date_ub"], date_ub);
        }
        if (bindings.count("date_lb") > (0)) {
          do_patch(bindings["date_lb"], date_lb);
        }
        if (bindings.count("merged_partials") > (0)) {
          do_patch(bindings["merged_partials"], merged_partials);
        }
        if (bindings.count("partial_aggs") > (0)) {
          do_patch(bindings["partial_aggs"], partial_aggs);
        }
        if (bindings.count("empty_aggs") > (0)) {
          do_patch(bindings["empty_aggs"], empty_aggs);
        }
        if (bindings.count("empty_source_ip_to_aggs_C") > (0)) {
          do_patch(bindings["empty_source_ip_to_aggs_C"], empty_source_ip_to_aggs_C);
        }
        if (bindings.count("rankingsMap") > (0)) {
          do_patch(bindings["rankingsMap"], rankingsMap);
        }
        if (bindings.count("user_visits") > (0)) {
          do_patch(bindings["user_visits"], user_visits);
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
        if (bindings.count("peer_count") > (0)) {
          do_patch(bindings["peer_count"], peer_count);
        }
        if (bindings.count("master") > (0)) {
          do_patch(bindings["master"], master);
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
    __k3_context::__trigger_names[0] = "loadAll";
    __k3_context::__trigger_names[1] = "ready";
    __k3_context::__trigger_names[2] = "finished";
    __k3_context::__trigger_names[3] = "find_global_max";
    __k3_context::__trigger_names[4] = "find_local_max";
    __k3_context::__trigger_names[5] = "count_merges";
    __k3_context::__trigger_names[6] = "merge";
    __k3_context::__trigger_names[7] = "q3_local";
    __k3_context::__clonable_dispatchers[0] = make_shared<ValDispatcher<unit_t>>();
    __k3_context::__clonable_dispatchers[1] = make_shared<ValDispatcher<unit_t>>();
    __k3_context::__clonable_dispatchers[2] = make_shared<ValDispatcher<unit_t>>();
    __k3_context::__clonable_dispatchers[3] = make_shared<ValDispatcher<R_key_value<std::string,
    R_arSum_prCount_prSum<double, int, int>>>>();
    __k3_context::__clonable_dispatchers[4] = make_shared<ValDispatcher<unit_t>>();
    __k3_context::__clonable_dispatchers[5] = make_shared<ValDispatcher<unit_t>>();
    __k3_context::__clonable_dispatchers[6] = make_shared<ValDispatcher<_Map<R_key_value<std::string,
    R_arSum_prCount_prSum<double, int, int>>>>>();
    __k3_context::__clonable_dispatchers[7] = make_shared<ValDispatcher<unit_t>>();
  }
  SystemEnvironment se = defaultEnvironment(getAddrs(contexts));
  engine.configure(opt.simulation, se, make_shared<DefaultInternalCodec>(), opt.log_level);
  processRoles(contexts);
  engine.runEngine(make_shared<virtualizing_message_processor>(contexts));
}