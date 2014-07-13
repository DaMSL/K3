#define MAIN_PROGRAM

#include <functional>
#include <memory>
#include <sstream>
#include <string>
#include <strtk.hpp>
#include <json_spirit_reader_template.h>
#include <boost/serialization/list.hpp>
#include <boost/serialization/vector.hpp>

#include "Collections.hpp"
#include "Common.hpp"
#include "Dispatch.hpp"
#include "Engine.hpp"
#include "Literals.hpp"
#include "MessageProcessor.hpp"
#include "Serialization.hpp"

using namespace std;
using namespace K3;
using namespace K3::BoostSerializer;
using std::begin;
using std::end;

Engine engine = Engine();

#include "Builtins.hpp"

using K3::Collection;

template <class _T0> class R_sourceIP;

template <class _T0> class R_revenue;

template <class _T0,class _T1> class R_pageRank_pageURL;

template <class _T0,class _T1> class R_key_value;

template <class _T0,class _T1,class _T2> class R_count_revenue_total;

template <class _T0,class _T1,class _T2> class R_avgRank_sourceIP_totalRevenue;

template <class _T0,class _T1,class _T2> class R_avgDuration_pageRank_pageURL;

template <class _T0> class R_arg;

template <class _T0> class R_addr;

template <class _T0,class _T1,class _T2> class R_adRevenue_sourceIP_visitDate;

template <class _T0,class _T1,class _T2> class R_adRevenue_pageRank_sourceIP;

template <class _T0,class _T1> class R_adRevenue_pageRank;

template <class _T0,class _T1,class _T2,class _T3,class _T4,class _T5,class _T6,class _T7,class _T8> class R_adRevenue_countryCode_destURL_duration_languageCode_searchWord_sourceIP_userAgent_visitDate;

template <class CONTENT> class _Collection;
template <class CONTENT> class _Map;

unit_t atExit(unit_t);

unit_t atInit(unit_t);

unit_t processRole(unit_t);

unit_t initDecls(unit_t);

unit_t aggregate_global(_Map<R_key_value<string, R_count_revenue_total<int, double, int>>>);

unit_t aggregate_local(unit_t);

template <class CONTENT>
class _Collection: public K3::Collection<CONTENT> {
    public:
        _Collection(): K3::Collection<CONTENT>(&engine) {}

        _Collection(const _Collection& c): K3::Collection<CONTENT>(c) {}

        _Collection(const K3::Collection<CONTENT>& c): K3::Collection<CONTENT>(c) {}

        template <class archive>
        void serialize(archive& _archive,const unsigned int) {

            _archive & boost::serialization::base_object<K3::Collection<CONTENT>>(*this);
        }

};
template <class CONTENT>
class _Map: public K3::Map<CONTENT> {
    public:
        _Map(): K3::Map<CONTENT>(&engine) {}

        _Map(const _Map& c): K3::Map<CONTENT>(c) {}

        _Map(const K3::Map<CONTENT>& c): K3::Map<CONTENT>(c) {}

        template <class archive>
        void serialize(archive& _archive,const unsigned int) {
            _archive & boost::serialization::base_object<K3::Map<CONTENT>>(*this);
        }

};

namespace K3 {
    template <class E>
    struct patcher<_Map<E>> {
        static void patch(string s,_Map<E>& c) {
            collection_patcher<_Map,E>::patch(s,c);
        }
    };
}
namespace K3 {
    template <class E>
    struct patcher<_Collection<E>> {
        static void patch(string s,_Collection<E>& c) {
            collection_patcher<_Collection,E>::patch(s,c);
        }
    };
}

template <class _T0,class _T1,class _T2,class _T3,class _T4,class _T5,class _T6,class _T7,class _T8>
class R_adRevenue_countryCode_destURL_duration_languageCode_searchWord_sourceIP_userAgent_visitDate {
    public:
        R_adRevenue_countryCode_destURL_duration_languageCode_searchWord_sourceIP_userAgent_visitDate() {}
        R_adRevenue_countryCode_destURL_duration_languageCode_searchWord_sourceIP_userAgent_visitDate(_T0 _adRevenue
                                                                                                     ,_T1 _countryCode
                                                                                                     ,_T2 _destURL
                                                                                                     ,_T3 _duration
                                                                                                     ,_T4 _languageCode
                                                                                                     ,_T5 _searchWord
                                                                                                     ,_T6 _sourceIP
                                                                                                     ,_T7 _userAgent
                                                                                                     ,_T8 _visitDate): adRevenue(_adRevenue), countryCode(_countryCode), destURL(_destURL), duration(_duration), languageCode(_languageCode), searchWord(_searchWord), sourceIP(_sourceIP), userAgent(_userAgent), visitDate(_visitDate) {}
        R_adRevenue_countryCode_destURL_duration_languageCode_searchWord_sourceIP_userAgent_visitDate(const R_adRevenue_countryCode_destURL_duration_languageCode_searchWord_sourceIP_userAgent_visitDate<_T0, _T1, _T2, _T3, _T4, _T5, _T6, _T7, _T8>& _r): adRevenue(_r.adRevenue), countryCode(_r.countryCode), destURL(_r.destURL), duration(_r.duration), languageCode(_r.languageCode), searchWord(_r.searchWord), sourceIP(_r.sourceIP), userAgent(_r.userAgent), visitDate(_r.visitDate) {}
        bool operator==(R_adRevenue_countryCode_destURL_duration_languageCode_searchWord_sourceIP_userAgent_visitDate _r) {
            if (adRevenue == _r.adRevenue&& countryCode == _r.countryCode&& destURL == _r.destURL&& duration == _r.duration&& languageCode == _r.languageCode&& searchWord == _r.searchWord&& sourceIP == _r.sourceIP&& userAgent == _r.userAgent&& visitDate == _r.visitDate)
                return true;
            return false;
        }
        template <class archive>
        void serialize(archive& _archive,const unsigned int) {
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
namespace K3 {
    template <class _T0,class _T1,class _T2,class _T3,class _T4,class _T5,class _T6,class _T7,class _T8>
    struct patcher<R_adRevenue_countryCode_destURL_duration_languageCode_searchWord_sourceIP_userAgent_visitDate<_T0, _T1, _T2, _T3, _T4, _T5, _T6, _T7, _T8>> {
        static void patch(string s,
        R_adRevenue_countryCode_destURL_duration_languageCode_searchWord_sourceIP_userAgent_visitDate<_T0, _T1, _T2, _T3, _T4, _T5, _T6, _T7, _T8>& r) {
            shallow<string::iterator> _shallow;;
            qi::rule<string::iterator, qi::space_type> _adRevenue = (qi::lit("adRevenue") >> ':' >> _shallow)[([&r] (string _string) {do_patch(_string
                                                                                                                                              ,r.adRevenue);})];
            qi::rule<string::iterator, qi::space_type> _countryCode = (qi::lit("countryCode") >> ':' >> _shallow)[([&r] (string _string) {do_patch(_string
                                                                                                                                                  ,r.countryCode);})];
            qi::rule<string::iterator, qi::space_type> _destURL = (qi::lit("destURL") >> ':' >> _shallow)[([&r] (string _string) {do_patch(_string
                                                                                                                                          ,r.destURL);})];
            qi::rule<string::iterator, qi::space_type> _duration = (qi::lit("duration") >> ':' >> _shallow)[([&r] (string _string) {do_patch(_string
                                                                                                                                            ,r.duration);})];
            qi::rule<string::iterator, qi::space_type> _languageCode = (qi::lit("languageCode") >> ':' >> _shallow)[([&r] (string _string) {do_patch(_string
                                                                                                                                                    ,r.languageCode);})];
            qi::rule<string::iterator, qi::space_type> _searchWord = (qi::lit("searchWord") >> ':' >> _shallow)[([&r] (string _string) {do_patch(_string
                                                                                                                                                ,r.searchWord);})];
            qi::rule<string::iterator, qi::space_type> _sourceIP = (qi::lit("sourceIP") >> ':' >> _shallow)[([&r] (string _string) {do_patch(_string
                                                                                                                                            ,r.sourceIP);})];
            qi::rule<string::iterator, qi::space_type> _userAgent = (qi::lit("userAgent") >> ':' >> _shallow)[([&r] (string _string) {do_patch(_string
                                                                                                                                              ,r.userAgent);})];
            qi::rule<string::iterator, qi::space_type> _visitDate = (qi::lit("visitDate") >> ':' >> _shallow)[([&r] (string _string) {do_patch(_string
                                                                                                                                              ,r.visitDate);})];
            qi::rule<string::iterator, qi::space_type> _field = _adRevenue |
            _countryCode |
            _destURL |
            _duration |
            _languageCode |
            _searchWord |
            _sourceIP |
            _userAgent |
            _visitDate;
            qi::rule<string::iterator, qi::space_type> _parser = '{' >> (_field % ',') >> '}';
            qi::phrase_parse(std::begin(s),std::end(s),_parser,qi::space);
        }
    };
}

template <class _T0,class _T1>
class R_adRevenue_pageRank {
    public:
        R_adRevenue_pageRank() {}
        R_adRevenue_pageRank(_T0 _adRevenue
                            ,_T1 _pageRank): adRevenue(_adRevenue), pageRank(_pageRank) {}
        R_adRevenue_pageRank(const R_adRevenue_pageRank<_T0, _T1>& _r): adRevenue(_r.adRevenue), pageRank(_r.pageRank) {}
        bool operator==(R_adRevenue_pageRank _r) {
            if (adRevenue == _r.adRevenue&& pageRank == _r.pageRank)
                return true;
            return false;
        }
        template <class archive>
        void serialize(archive& _archive,const unsigned int) {
            _archive & adRevenue;
            _archive & pageRank;

        }
        _T0 adRevenue;
        _T1 pageRank;
};
namespace K3 {
    template <class _T0,class _T1>
    struct patcher<R_adRevenue_pageRank<_T0, _T1>> {
        static void patch(string s,R_adRevenue_pageRank<_T0, _T1>& r) {
            shallow<string::iterator> _shallow;;
            qi::rule<string::iterator, qi::space_type> _adRevenue = (qi::lit("adRevenue") >> ':' >> _shallow)[([&r] (string _string) {do_patch(_string
                                                                                                                                              ,r.adRevenue);})];
            qi::rule<string::iterator, qi::space_type> _pageRank = (qi::lit("pageRank") >> ':' >> _shallow)[([&r] (string _string) {do_patch(_string
                                                                                                                                            ,r.pageRank);})];
            qi::rule<string::iterator, qi::space_type> _field = _adRevenue | _pageRank;
            qi::rule<string::iterator, qi::space_type> _parser = '{' >> (_field % ',') >> '}';
            qi::phrase_parse(std::begin(s),std::end(s),_parser,qi::space);
        }
    };
}

template <class _T0,class _T1,class _T2>
class R_adRevenue_pageRank_sourceIP {
    public:
        R_adRevenue_pageRank_sourceIP() {}
        R_adRevenue_pageRank_sourceIP(_T0 _adRevenue
                                     ,_T1 _pageRank
                                     ,_T2 _sourceIP): adRevenue(_adRevenue), pageRank(_pageRank), sourceIP(_sourceIP) {}
        R_adRevenue_pageRank_sourceIP(const R_adRevenue_pageRank_sourceIP<_T0, _T1, _T2>& _r): adRevenue(_r.adRevenue), pageRank(_r.pageRank), sourceIP(_r.sourceIP) {}
        bool operator==(R_adRevenue_pageRank_sourceIP _r) {
            if (adRevenue == _r.adRevenue&& pageRank == _r.pageRank&& sourceIP == _r.sourceIP)
                return true;
            return false;
        }
        template <class archive>
        void serialize(archive& _archive,const unsigned int) {
            _archive & adRevenue;
            _archive & pageRank;
            _archive & sourceIP;

        }
        _T0 adRevenue;
        _T1 pageRank;
        _T2 sourceIP;
};
namespace K3 {
    template <class _T0,class _T1,class _T2>
    struct patcher<R_adRevenue_pageRank_sourceIP<_T0, _T1, _T2>> {
        static void patch(string s,R_adRevenue_pageRank_sourceIP<_T0, _T1, _T2>& r) {
            shallow<string::iterator> _shallow;;
            qi::rule<string::iterator, qi::space_type> _adRevenue = (qi::lit("adRevenue") >> ':' >> _shallow)[([&r] (string _string) {do_patch(_string
                                                                                                                                              ,r.adRevenue);})];
            qi::rule<string::iterator, qi::space_type> _pageRank = (qi::lit("pageRank") >> ':' >> _shallow)[([&r] (string _string) {do_patch(_string
                                                                                                                                            ,r.pageRank);})];
            qi::rule<string::iterator, qi::space_type> _sourceIP = (qi::lit("sourceIP") >> ':' >> _shallow)[([&r] (string _string) {do_patch(_string
                                                                                                                                            ,r.sourceIP);})];
            qi::rule<string::iterator, qi::space_type> _field = _adRevenue | _pageRank | _sourceIP;
            qi::rule<string::iterator, qi::space_type> _parser = '{' >> (_field % ',') >> '}';
            qi::phrase_parse(std::begin(s),std::end(s),_parser,qi::space);
        }
    };
}

template <class _T0,class _T1,class _T2>
class R_adRevenue_sourceIP_visitDate {
    public:
        R_adRevenue_sourceIP_visitDate() {}
        R_adRevenue_sourceIP_visitDate(_T0 _adRevenue
                                      ,_T1 _sourceIP
                                      ,_T2 _visitDate): adRevenue(_adRevenue), sourceIP(_sourceIP), visitDate(_visitDate) {}
        R_adRevenue_sourceIP_visitDate(const R_adRevenue_sourceIP_visitDate<_T0, _T1, _T2>& _r): adRevenue(_r.adRevenue), sourceIP(_r.sourceIP), visitDate(_r.visitDate) {}
        bool operator==(R_adRevenue_sourceIP_visitDate _r) {
            if (adRevenue == _r.adRevenue&& sourceIP == _r.sourceIP&& visitDate == _r.visitDate)
                return true;
            return false;
        }
        template <class archive>
        void serialize(archive& _archive,const unsigned int) {
            _archive & adRevenue;
            _archive & sourceIP;
            _archive & visitDate;

        }
        _T0 adRevenue;
        _T1 sourceIP;
        _T2 visitDate;
};
namespace K3 {
    template <class _T0,class _T1,class _T2>
    struct patcher<R_adRevenue_sourceIP_visitDate<_T0, _T1, _T2>> {
        static void patch(string s,R_adRevenue_sourceIP_visitDate<_T0, _T1, _T2>& r) {
            shallow<string::iterator> _shallow;;
            qi::rule<string::iterator, qi::space_type> _adRevenue = (qi::lit("adRevenue") >> ':' >> _shallow)[([&r] (string _string) {do_patch(_string
                                                                                                                                              ,r.adRevenue);})];
            qi::rule<string::iterator, qi::space_type> _sourceIP = (qi::lit("sourceIP") >> ':' >> _shallow)[([&r] (string _string) {do_patch(_string
                                                                                                                                            ,r.sourceIP);})];
            qi::rule<string::iterator, qi::space_type> _visitDate = (qi::lit("visitDate") >> ':' >> _shallow)[([&r] (string _string) {do_patch(_string
                                                                                                                                              ,r.visitDate);})];
            qi::rule<string::iterator, qi::space_type> _field = _adRevenue | _sourceIP | _visitDate;
            qi::rule<string::iterator, qi::space_type> _parser = '{' >> (_field % ',') >> '}';
            qi::phrase_parse(std::begin(s),std::end(s),_parser,qi::space);
        }
    };
}

template <class _T0>
class R_addr {
    public:
        R_addr() {}
        R_addr(_T0 _addr): addr(_addr) {}
        R_addr(const R_addr<_T0>& _r): addr(_r.addr) {}
        bool operator==(R_addr _r) {
            if (addr == _r.addr)
                return true;
            return false;
        }
        template <class archive>
        void serialize(archive& _archive,const unsigned int) {
            _archive & addr;

        }
        _T0 addr;
};
namespace K3 {
    template <class _T0>
    struct patcher<R_addr<_T0>> {
        static void patch(string s,R_addr<_T0>& r) {
            shallow<string::iterator> _shallow;;
            qi::rule<string::iterator, qi::space_type> _addr = (qi::lit("addr") >> ':' >> _shallow)[([&r] (string _string) {do_patch(_string
                                                                                                                                    ,r.addr);})];
            qi::rule<string::iterator, qi::space_type> _field = _addr;
            qi::rule<string::iterator, qi::space_type> _parser = '{' >> (_field % ',') >> '}';
            qi::phrase_parse(std::begin(s),std::end(s),_parser,qi::space);
        }
    };
}

template <class _T0>
class R_arg {
    public:
        R_arg() {}
        R_arg(_T0 _arg): arg(_arg) {}
        R_arg(const R_arg<_T0>& _r): arg(_r.arg) {}
        bool operator==(R_arg _r) {
            if (arg == _r.arg)
                return true;
            return false;
        }
        template <class archive>
        void serialize(archive& _archive,const unsigned int) {
            _archive & arg;

        }
        _T0 arg;
};
namespace K3 {
    template <class _T0>
    struct patcher<R_arg<_T0>> {
        static void patch(string s,R_arg<_T0>& r) {
            shallow<string::iterator> _shallow;;
            qi::rule<string::iterator, qi::space_type> _arg = (qi::lit("arg") >> ':' >> _shallow)[([&r] (string _string) {do_patch(_string
                                                                                                                                  ,r.arg);})];
            qi::rule<string::iterator, qi::space_type> _field = _arg;
            qi::rule<string::iterator, qi::space_type> _parser = '{' >> (_field % ',') >> '}';
            qi::phrase_parse(std::begin(s),std::end(s),_parser,qi::space);
        }
    };
}

template <class _T0,class _T1,class _T2>
class R_avgDuration_pageRank_pageURL {
    public:
        R_avgDuration_pageRank_pageURL() {}
        R_avgDuration_pageRank_pageURL(_T0 _avgDuration
                                      ,_T1 _pageRank
                                      ,_T2 _pageURL): avgDuration(_avgDuration), pageRank(_pageRank), pageURL(_pageURL) {}
        R_avgDuration_pageRank_pageURL(const R_avgDuration_pageRank_pageURL<_T0, _T1, _T2>& _r): avgDuration(_r.avgDuration), pageRank(_r.pageRank), pageURL(_r.pageURL) {}
        bool operator==(R_avgDuration_pageRank_pageURL _r) {
            if (avgDuration == _r.avgDuration&& pageRank == _r.pageRank&& pageURL == _r.pageURL)
                return true;
            return false;
        }
        template <class archive>
        void serialize(archive& _archive,const unsigned int) {
            _archive & avgDuration;
            _archive & pageRank;
            _archive & pageURL;

        }
        _T0 avgDuration;
        _T1 pageRank;
        _T2 pageURL;
};
namespace K3 {
    template <class _T0,class _T1,class _T2>
    struct patcher<R_avgDuration_pageRank_pageURL<_T0, _T1, _T2>> {
        static void patch(string s,R_avgDuration_pageRank_pageURL<_T0, _T1, _T2>& r) {
            shallow<string::iterator> _shallow;;
            qi::rule<string::iterator, qi::space_type> _avgDuration = (qi::lit("avgDuration") >> ':' >> _shallow)[([&r] (string _string) {do_patch(_string
                                                                                                                                                  ,r.avgDuration);})];
            qi::rule<string::iterator, qi::space_type> _pageRank = (qi::lit("pageRank") >> ':' >> _shallow)[([&r] (string _string) {do_patch(_string
                                                                                                                                            ,r.pageRank);})];
            qi::rule<string::iterator, qi::space_type> _pageURL = (qi::lit("pageURL") >> ':' >> _shallow)[([&r] (string _string) {do_patch(_string
                                                                                                                                          ,r.pageURL);})];
            qi::rule<string::iterator, qi::space_type> _field = _avgDuration | _pageRank | _pageURL;
            qi::rule<string::iterator, qi::space_type> _parser = '{' >> (_field % ',') >> '}';
            qi::phrase_parse(std::begin(s),std::end(s),_parser,qi::space);
        }
    };
}

template <class _T0,class _T1,class _T2>
class R_avgRank_sourceIP_totalRevenue {
    public:
        R_avgRank_sourceIP_totalRevenue() {}
        R_avgRank_sourceIP_totalRevenue(_T0 _avgRank
                                       ,_T1 _sourceIP
                                       ,_T2 _totalRevenue): avgRank(_avgRank), sourceIP(_sourceIP), totalRevenue(_totalRevenue) {}
        R_avgRank_sourceIP_totalRevenue(const R_avgRank_sourceIP_totalRevenue<_T0, _T1, _T2>& _r): avgRank(_r.avgRank), sourceIP(_r.sourceIP), totalRevenue(_r.totalRevenue) {}
        bool operator==(R_avgRank_sourceIP_totalRevenue _r) {
            if (avgRank == _r.avgRank&& sourceIP == _r.sourceIP&& totalRevenue == _r.totalRevenue)
                return true;
            return false;
        }
        template <class archive>
        void serialize(archive& _archive,const unsigned int) {
            _archive & avgRank;
            _archive & sourceIP;
            _archive & totalRevenue;

        }
        _T0 avgRank;
        _T1 sourceIP;
        _T2 totalRevenue;
};
namespace K3 {
    template <class _T0,class _T1,class _T2>
    struct patcher<R_avgRank_sourceIP_totalRevenue<_T0, _T1, _T2>> {
        static void patch(string s,R_avgRank_sourceIP_totalRevenue<_T0, _T1, _T2>& r) {
            shallow<string::iterator> _shallow;;
            qi::rule<string::iterator, qi::space_type> _avgRank = (qi::lit("avgRank") >> ':' >> _shallow)[([&r] (string _string) {do_patch(_string
                                                                                                                                          ,r.avgRank);})];
            qi::rule<string::iterator, qi::space_type> _sourceIP = (qi::lit("sourceIP") >> ':' >> _shallow)[([&r] (string _string) {do_patch(_string
                                                                                                                                            ,r.sourceIP);})];
            qi::rule<string::iterator, qi::space_type> _totalRevenue = (qi::lit("totalRevenue") >> ':' >> _shallow)[([&r] (string _string) {do_patch(_string
                                                                                                                                                    ,r.totalRevenue);})];
            qi::rule<string::iterator, qi::space_type> _field = _avgRank |
            _sourceIP |
            _totalRevenue;
            qi::rule<string::iterator, qi::space_type> _parser = '{' >> (_field % ',') >> '}';
            qi::phrase_parse(std::begin(s),std::end(s),_parser,qi::space);
        }
    };
}

template <class _T0,class _T1,class _T2>
class R_count_revenue_total {
    public:
        R_count_revenue_total() {}
        R_count_revenue_total(_T0 _count
                             ,_T1 _revenue
                             ,_T2 _total): count(_count), revenue(_revenue), total(_total) {}
        R_count_revenue_total(const R_count_revenue_total<_T0, _T1, _T2>& _r): count(_r.count), revenue(_r.revenue), total(_r.total) {}
        bool operator==(R_count_revenue_total _r) {
            if (count == _r.count&& revenue == _r.revenue&& total == _r.total)
                return true;
            return false;
        }
        template <class archive>
        void serialize(archive& _archive,const unsigned int) {
            _archive & count;
            _archive & revenue;
            _archive & total;

        }
        _T0 count;
        _T1 revenue;
        _T2 total;
};
namespace K3 {
    template <class _T0,class _T1,class _T2>
    struct patcher<R_count_revenue_total<_T0, _T1, _T2>> {
        static void patch(string s,R_count_revenue_total<_T0, _T1, _T2>& r) {
            shallow<string::iterator> _shallow;;
            qi::rule<string::iterator, qi::space_type> _count = (qi::lit("count") >> ':' >> _shallow)[([&r] (string _string) {do_patch(_string
                                                                                                                                      ,r.count);})];
            qi::rule<string::iterator, qi::space_type> _revenue = (qi::lit("revenue") >> ':' >> _shallow)[([&r] (string _string) {do_patch(_string
                                                                                                                                          ,r.revenue);})];
            qi::rule<string::iterator, qi::space_type> _total = (qi::lit("total") >> ':' >> _shallow)[([&r] (string _string) {do_patch(_string
                                                                                                                                      ,r.total);})];
            qi::rule<string::iterator, qi::space_type> _field = _count | _revenue | _total;
            qi::rule<string::iterator, qi::space_type> _parser = '{' >> (_field % ',') >> '}';
            qi::phrase_parse(std::begin(s),std::end(s),_parser,qi::space);
        }
    };
}

#ifndef K3_R_key_value
#define K3_R_key_value
template <class _T0,class _T1>
class R_key_value {
    public:
        R_key_value() {}
        R_key_value(_T0 _key,_T1 _value): key(_key), value(_value) {}
        R_key_value(const R_key_value<_T0, _T1>& _r): key(_r.key), value(_r.value) {}
        bool operator==(R_key_value _r) {
            if (key == _r.key&& value == _r.value)
                return true;
            return false;
        }
        template <class archive>
        void serialize(archive& _archive,const unsigned int) {
            _archive & key;
            _archive & value;

        }
        _T0 key;
        _T1 value;
  using KeyType = _T0;
  using ValueType = _T1;
};
#endif

namespace K3 {
    template <class _T0,class _T1>
    struct patcher<R_key_value<_T0, _T1>> {
        static void patch(string s,R_key_value<_T0, _T1>& r) {
            shallow<string::iterator> _shallow;;
            qi::rule<string::iterator, qi::space_type> _key = (qi::lit("key") >> ':' >> _shallow)[([&r] (string _string) {do_patch(_string
                                                                                                                                  ,r.key);})];
            qi::rule<string::iterator, qi::space_type> _value = (qi::lit("value") >> ':' >> _shallow)[([&r] (string _string) {do_patch(_string
                                                                                                                                      ,r.value);})];
            qi::rule<string::iterator, qi::space_type> _field = _key | _value;
            qi::rule<string::iterator, qi::space_type> _parser = '{' >> (_field % ',') >> '}';
            qi::phrase_parse(std::begin(s),std::end(s),_parser,qi::space);
        }
    };
}

template <class _T0,class _T1>
class R_pageRank_pageURL {
    public:
        R_pageRank_pageURL() {}
        R_pageRank_pageURL(_T0 _pageRank,_T1 _pageURL): pageRank(_pageRank), pageURL(_pageURL) {}
        R_pageRank_pageURL(const R_pageRank_pageURL<_T0, _T1>& _r): pageRank(_r.pageRank), pageURL(_r.pageURL) {}
        bool operator==(R_pageRank_pageURL _r) {
            if (pageRank == _r.pageRank&& pageURL == _r.pageURL)
                return true;
            return false;
        }
        template <class archive>
        void serialize(archive& _archive,const unsigned int) {
            _archive & pageRank;
            _archive & pageURL;

        }
        _T0 pageRank;
        _T1 pageURL;
};
namespace K3 {
    template <class _T0,class _T1>
    struct patcher<R_pageRank_pageURL<_T0, _T1>> {
        static void patch(string s,R_pageRank_pageURL<_T0, _T1>& r) {
            shallow<string::iterator> _shallow;;
            qi::rule<string::iterator, qi::space_type> _pageRank = (qi::lit("pageRank") >> ':' >> _shallow)[([&r] (string _string) {do_patch(_string
                                                                                                                                            ,r.pageRank);})];
            qi::rule<string::iterator, qi::space_type> _pageURL = (qi::lit("pageURL") >> ':' >> _shallow)[([&r] (string _string) {do_patch(_string
                                                                                                                                          ,r.pageURL);})];
            qi::rule<string::iterator, qi::space_type> _field = _pageRank | _pageURL;
            qi::rule<string::iterator, qi::space_type> _parser = '{' >> (_field % ',') >> '}';
            qi::phrase_parse(std::begin(s),std::end(s),_parser,qi::space);
        }
    };
}

template <class _T0>
class R_revenue {
    public:
        R_revenue() {}
        R_revenue(_T0 _revenue): revenue(_revenue) {}
        R_revenue(const R_revenue<_T0>& _r): revenue(_r.revenue) {}
        bool operator==(R_revenue _r) {
            if (revenue == _r.revenue)
                return true;
            return false;
        }
        template <class archive>
        void serialize(archive& _archive,const unsigned int) {
            _archive & revenue;

        }
        _T0 revenue;
};
namespace K3 {
    template <class _T0>
    struct patcher<R_revenue<_T0>> {
        static void patch(string s,R_revenue<_T0>& r) {
            shallow<string::iterator> _shallow;;
            qi::rule<string::iterator, qi::space_type> _revenue = (qi::lit("revenue") >> ':' >> _shallow)[([&r] (string _string) {do_patch(_string
                                                                                                                                          ,r.revenue);})];
            qi::rule<string::iterator, qi::space_type> _field = _revenue;
            qi::rule<string::iterator, qi::space_type> _parser = '{' >> (_field % ',') >> '}';
            qi::phrase_parse(std::begin(s),std::end(s),_parser,qi::space);
        }
    };
}

template <class _T0>
class R_sourceIP {
    public:
        R_sourceIP() {}
        R_sourceIP(_T0 _sourceIP): sourceIP(_sourceIP) {}
        R_sourceIP(const R_sourceIP<_T0>& _r): sourceIP(_r.sourceIP) {}
        bool operator==(R_sourceIP _r) {
            if (sourceIP == _r.sourceIP)
                return true;
            return false;
        }
        template <class archive>
        void serialize(archive& _archive,const unsigned int) {
            _archive & sourceIP;

        }
        _T0 sourceIP;
};
namespace K3 {
    template <class _T0>
    struct patcher<R_sourceIP<_T0>> {
        static void patch(string s,R_sourceIP<_T0>& r) {
            shallow<string::iterator> _shallow;;
            qi::rule<string::iterator, qi::space_type> _sourceIP = (qi::lit("sourceIP") >> ':' >> _shallow)[([&r] (string _string) {do_patch(_string
                                                                                                                                            ,r.sourceIP);})];
            qi::rule<string::iterator, qi::space_type> _field = _sourceIP;
            qi::rule<string::iterator, qi::space_type> _parser = '{' >> (_field % ',') >> '}';
            qi::phrase_parse(std::begin(s),std::end(s),_parser,qi::space);
        }
    };
}

Address me;

_Collection<R_addr<Address>> peers;

tuple<_Collection<R_arg<string>>, _Collection<R_key_value<string, string>>> args;

string role;

int lowerDate;

int upperDate;

Address master;

int peer_count;

_Collection<R_adRevenue_countryCode_destURL_duration_languageCode_searchWord_sourceIP_userAgent_visitDate<double, string, string, int, string, string, string, string, string>> userVisits_raw;

_Collection<R_adRevenue_countryCode_destURL_duration_languageCode_searchWord_sourceIP_userAgent_visitDate<double, string, string, int, string, string, string, string, int>> userVisits;

_Collection<R_avgDuration_pageRank_pageURL<int, int, string>> rankings;

_Collection<R_adRevenue_pageRank_sourceIP<double, int, string>> matches;

unit_t aggregate_local(unit_t _) {
    userVisits.iterate([] (const R_adRevenue_countryCode_destURL_duration_languageCode_searchWord_sourceIP_userAgent_visitDate<double, string, string, int, string, string, string, string, int>& u) -> unit_t {
        return rankings.iterate([u] (R_avgDuration_pageRank_pageURL<int, int, string> r) -> unit_t {
            if (u.destURL == r.pageURL && lowerDate < u.visitDate && u.visitDate < upperDate) {

                return matches.insert(R_adRevenue_pageRank_sourceIP<double, int, string>{u.adRevenue,
                r.pageRank,
                u.sourceIP});
            } else {
                return unit_t();
            }
        });
    });
    {
        _Map<R_key_value<string, R_count_revenue_total<int, double, int>>> local_aggregate;

        local_aggregate = matches.groupBy<string, R_count_revenue_total<int, double, int>>([] (const R_adRevenue_pageRank_sourceIP<double, int, string>& r) -> string {
            return r.sourceIP;
          })([] (const R_count_revenue_total<int, double, int>& a) -> std::function<R_count_revenue_total<int, double, int>(R_adRevenue_pageRank_sourceIP<double, int, string>)> {

              return [a] (const R_adRevenue_pageRank_sourceIP<double, int, string>& r) -> R_count_revenue_total<int, double, int> {

                return R_count_revenue_total<int, double, int>{a.count + 1,
                a.revenue + r.adRevenue,
                a.total + r.pageRank};
            };
        })(R_count_revenue_total<int, double, int>{0,0,0});

        auto d = make_shared<DispatcherImpl<_Map<R_key_value<string, R_count_revenue_total<int, double, int>>>>>(aggregate_global, local_aggregate);
        engine.send(master,0,d);return unit_t();
    }
}

int local_aggregate_complete;

_Map<R_key_value<string, R_count_revenue_total<int, double, double>>> global_partial_result;

R_avgRank_sourceIP_totalRevenue<double, string, double> global_result;

unit_t aggregate_global(_Map<R_key_value<string, R_count_revenue_total<int, double, int>>> c) {

    c.iterate([] (const R_key_value<string, R_count_revenue_total<int, double, int>>& r) -> unit_t {

                    unit_t __0;

            shared_ptr<R_count_revenue_total<int, double, double>> found = lookup(global_partial_result)(r.key);

            if (found == nullptr) {
              global_partial_result.insert(
                  R_key_value<string, R_count_revenue_total<int, double, double>> {
                    r.key, R_count_revenue_total<int, double, double> { r.value.count, r.value.revenue, double(r.value.total) }
                  }
              );
            } else {
              global_partial_result.insert(
                  R_key_value<string, R_count_revenue_total<int, double, double>> {
                    r.key, R_count_revenue_total<int, double, double> {
                      found->count + r.value.count,
                      found->revenue + r.value.revenue,
                      found->total + double(r.value.total)
                    }
                  }
              );
            }

            return __0;
    });
    local_aggregate_complete = local_aggregate_complete + 1;
    if (local_aggregate_complete == peer_count) {

        return global_partial_result.iterate([] (const R_key_value<string, R_count_revenue_total<int, double, double>>& g) -> unit_t {

                            unit_t __1;
                if (g.value.revenue > global_result.totalRevenue) {

                    global_result = R_avgRank_sourceIP_totalRevenue<double, string, double>{g.value.total / g.value.count,
                    g.key,
                    g.value.revenue};__1 = unit_t();
                } else {
                    __1 = unit_t();
                }
                return __1;
        });
    } else {
        return unit_t();
    }
}

unit_t initDecls(unit_t _) {
    return unit_t();
}

unit_t processRole(unit_t _) {
    return unit_t();
}

unit_t atInit(unit_t _) {

    initDecls(unit_t());

    return processRole(unit_t());
}

unit_t atExit(unit_t _) {
    return unit_t();
}

unit_t initGlobalDecls() {

    master = make_address(string("127.0.0.1"),20000);peer_count = 5;local_aggregate_complete = 0;

    global_result = R_avgRank_sourceIP_totalRevenue<double, string, double>{0.0,
    string(""),
    0};return unit_t();
}

void populate_dispatch() {
    dispatch_table.resize(2);
    dispatch_table[0] = make_tuple(make_shared<DispatcherImpl<_Map<R_key_value<string, R_count_revenue_total<int, double, int>>>>>(aggregate_global), "aggregate_global");
    dispatch_table[1] = make_tuple(make_shared<DispatcherImpl<unit_t>>(aggregate_local), "aggregate_local");
}

map<string,string> show_globals() {
    map<string,string> result;
    result["global_result"] = "{" + ("sourceIP:" + global_result.sourceIP + "," + "avgRank:" + to_string(global_result.avgRank) + "," + "totalRevenue:" + to_string(global_result.totalRevenue) + "}");
    result["global_partial_result"] = ([] (_Map<R_key_value<string, R_count_revenue_total<int, double, double>>> coll) {
        ostringstream oss;
        auto f = [&] (R_key_value<string, R_count_revenue_total<int, double, double>> elem) {oss << "{" + ("key:" + elem.key + "," + "value:" + "{" + ("count:" + to_string(elem.value.count) + "," + "total:" + to_string(elem.value.total) + "," + "revenue:" + to_string(elem.value.revenue) + "}") + "}") << ",";
        return unit_t();};
        coll.iterate(f);
        return "[" + oss.str() + "]";
    }(global_partial_result));
    result["local_aggregate_complete"] = to_string(local_aggregate_complete);
    result["matches"] = ([] (_Collection<R_adRevenue_pageRank_sourceIP<double, int, string>> coll) {
        ostringstream oss;
        auto f = [&] (R_adRevenue_pageRank_sourceIP<double, int, string> elem) {oss << "{" + ("sourceIP:" + elem.sourceIP + "," + "pageRank:" + to_string(elem.pageRank) + "," + "adRevenue:" + to_string(elem.adRevenue) + "}") << ",";
        return unit_t();};
        coll.iterate(f);
        return "[" + oss.str() + "]";
    }(matches));
    result["rankings"] = ([] (_Collection<R_avgDuration_pageRank_pageURL<int, int, string>> coll) {
        ostringstream oss;
        auto f = [&] (R_avgDuration_pageRank_pageURL<int, int, string> elem) {oss << "{" + ("pageURL:" + elem.pageURL + "," + "pageRank:" + to_string(elem.pageRank) + "," + "avgDuration:" + to_string(elem.avgDuration) + "}") << ",";
        return unit_t();};
        coll.iterate(f);
        return "[" + oss.str() + "]";
    }(rankings));
    result["userVisits"] = ([] (_Collection<R_adRevenue_countryCode_destURL_duration_languageCode_searchWord_sourceIP_userAgent_visitDate<double, string, string, int, string, string, string, string, int>> coll) {
        ostringstream oss;
        auto f = [&] (R_adRevenue_countryCode_destURL_duration_languageCode_searchWord_sourceIP_userAgent_visitDate<double, string, string, int, string, string, string, string, int> elem) {oss << "{" + ("sourceIP:" + elem.sourceIP + "," + "destURL:" + elem.destURL + "," + "visitDate:" + to_string(elem.visitDate) + "," + "adRevenue:" + to_string(elem.adRevenue) + "," + "userAgent:" + elem.userAgent + "," + "countryCode:" + elem.countryCode + "," + "languageCode:" + elem.languageCode + "," + "searchWord:" + elem.searchWord + "," + "duration:" + to_string(elem.duration) + "}") << ",";
        return unit_t();};
        coll.iterate(f);
        return "[" + oss.str() + "]";
    }(userVisits));
    result["userVisits_raw"] = ([] (_Collection<R_adRevenue_countryCode_destURL_duration_languageCode_searchWord_sourceIP_userAgent_visitDate<double, string, string, int, string, string, string, string, string>> coll) {
        ostringstream oss;
        auto f = [&] (R_adRevenue_countryCode_destURL_duration_languageCode_searchWord_sourceIP_userAgent_visitDate<double, string, string, int, string, string, string, string, string> elem) {oss << "{" + ("sourceIP:" + elem.sourceIP + "," + "destURL:" + elem.destURL + "," + "visitDate:" + elem.visitDate + "," + "adRevenue:" + to_string(elem.adRevenue) + "," + "userAgent:" + elem.userAgent + "," + "countryCode:" + elem.countryCode + "," + "languageCode:" + elem.languageCode + "," + "searchWord:" + elem.searchWord + "," + "duration:" + to_string(elem.duration) + "}") << ",";
        return unit_t();};
        coll.iterate(f);
        return "[" + oss.str() + "]";
    }(userVisits_raw));
    result["peer_count"] = to_string(peer_count);
    result["master"] = addressAsString(master);
    result["upperDate"] = to_string(upperDate);
    result["lowerDate"] = to_string(lowerDate);
    result["role"] = role;
    result["args"] = "("
    +
    ([] (_Collection<R_arg<string>> coll) {
        ostringstream oss;
        auto f = [&] (R_arg<string> elem) {oss << "{" + ("arg:" + elem.arg + "}") << ",";
        return unit_t();};
        coll.iterate(f);
        return "[" + oss.str() + "]";
    }(get<0>(args))) + "," + ([] (_Collection<R_key_value<string, string>> coll) {
        ostringstream oss;
        auto f = [&] (R_key_value<string, string> elem) {oss << "{" + ("key:" + elem.key + "," + "value:" + elem.value + "}") << ",";
        return unit_t();};
        coll.iterate(f);
        return "[" + oss.str() + "]";
    }(get<1>(args)))
    +
    ")";
    result["peers"] = ([] (_Collection<R_addr<Address>> coll) {
        ostringstream oss;
        auto f = [&] (R_addr<Address> elem) {oss << "{" + ("addr:" + addressAsString(elem.addr) + "}") << ",";
        return unit_t();};
        coll.iterate(f);
        return "[" + oss.str() + "]";
    }(peers));
    result["me"] = addressAsString(me);
    return result;
}

int main(int argc,char** argv) {
    initGlobalDecls();
    Options opt;
    if (opt.parse(argc,argv)) return 0;
    populate_dispatch();
    map<string,std::function<void(string)>> matchers;
    matchers["global_result"] = [] (string _s) {do_patch(_s,global_result);};
    matchers["global_partial_result"] = [] (string _s) {do_patch(_s,global_partial_result);};
    matchers["local_aggregate_complete"] = [] (string _s) {do_patch(_s,local_aggregate_complete);};
    matchers["matches"] = [] (string _s) {do_patch(_s,matches);};
    matchers["rankings"] = [] (string _s) {do_patch(_s,rankings);};
    matchers["userVisits"] = [] (string _s) {do_patch(_s,userVisits);};
    matchers["userVisits_raw"] = [] (string _s) {do_patch(_s,userVisits_raw);};
    matchers["peer_count"] = [] (string _s) {do_patch(_s,peer_count);};
    matchers["master"] = [] (string _s) {do_patch(_s,master);};
    matchers["upperDate"] = [] (string _s) {do_patch(_s,upperDate);};
    matchers["lowerDate"] = [] (string _s) {do_patch(_s,lowerDate);};
    matchers["role"] = [] (string _s) {do_patch(_s,role);};
    matchers["args"] = [] (string _s) {do_patch(_s,args);};
    matchers["peers"] = [] (string _s) {do_patch(_s,peers);};
    matchers["me"] = [] (string _s) {do_patch(_s,me);};
    string parse_arg = opt.peer_strings[0];;
    map<string,string> bindings = parse_bindings(parse_arg);
    match_patchers(bindings,matchers);
    list<Address> addr_l;
    addr_l.push_back(me);
    SystemEnvironment se = defaultEnvironment(addr_l);
    engine.configure(opt.simulation
                    ,se
                    ,make_shared<DefaultInternalCodec>(DefaultInternalCodec())
                    ,opt.log_level);
    processRole(unit_t());
    DispatchMessageProcessor dmp = DispatchMessageProcessor(show_globals);;
    engine.runEngine(make_shared<DispatchMessageProcessor>(dmp));
    return 0;
}
