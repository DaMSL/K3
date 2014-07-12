#define MAIN_PROGRAM

#include <functional>
#include <memory>
#include <sstream>
#include <string>
#include <external/strtk.hpp>
#include <external/json_spirit_reader_template.h>

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

using K3::Collection;

template <class _T0,class _T1> class R_key_value;

template <class _T0> class R_arg;

template <class _T0> class R_addr;

template <class _T0,class _T1,class _T2,class _T3,class _T4,class _T5,class _T6,class _T7,class _T8> class R_adRevenue_countryCode_destURL_duration_languageCode_searchWord_sourceIP_userAgent_visitDate;

template <class CONTENT> class _Map;

template <class CONTENT> class _Collection;

unit_t atExit(unit_t);

unit_t atInit(unit_t);

unit_t processRole(unit_t);

unit_t initDecls(unit_t);

unit_t rowsProcess(unit_t);

unit_t load_all(unit_t);

unit_t ready(unit_t);

unit_t shutdown_(unit_t);

unit_t aggregate(_Map<R_key_value<string, double>>);

unit_t q2_local(unit_t);

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
namespace K3 {
    template <class E>
    struct patcher<_Collection<E>> {
        static void patch(string s,_Collection<E>& c) {
            collection_patcher<_Collection,E>::patch(s,c);
        }
    };
}

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

#ifndef K3_R_adRevenue_countryCode_destURL_duration_languageCode_searchWord_sourceIP_userAgent_visitDate
#define K3_R_adRevenue_countryCode_destURL_duration_languageCode_searchWord_sourceIP_userAgent_visitDate
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
#endif
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

#ifndef K3_R_addr
#define K3_R_addr
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
#endif
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

#ifndef K3_R_arg
#define K3_R_arg
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
#endif
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

#include "Builtins.hpp"





































Address me;

_Collection<R_addr<Address>> peers;

tuple<_Collection<R_arg<string>>, _Collection<R_key_value<string, string>>> args;

string role;

F<unit_t(K3::Collection<R_adRevenue_countryCode_destURL_duration_languageCode_searchWord_sourceIP_userAgent_visitDate<double, string, string, int, string, string, string, string, string>>&)>dataLoader(string filepath){
    F<unit_t(K3::Collection<R_adRevenue_countryCode_destURL_duration_languageCode_searchWord_sourceIP_userAgent_visitDate<double, string, string, int, string, string, string, string, string>>&)> r = [filepath] (K3::Collection<R_adRevenue_countryCode_destURL_duration_languageCode_searchWord_sourceIP_userAgent_visitDate<double, string, string, int, string, string, string, string, string>> & c){
        R_adRevenue_countryCode_destURL_duration_languageCode_searchWord_sourceIP_userAgent_visitDate<double, string, string, int, string, string, string, string, string> rec;
        strtk::for_each_line(filepath,
        [&](const std::string& str){
            if (strtk::parse(str,",",rec.adRevenue,rec.countryCode,rec.destURL,rec.duration,rec.languageCode,rec.searchWord,rec.sourceIP,rec.userAgent,rec.visitDate)){
                c.insert(rec);
            }
            else{
                std::cout << "Failed to parse a row" << std::endl;
            }
        });
        return unit_t();
    };
    return r;
}

Address master;

int x;

int num_peers;

string data_file;

int peers_ready;

int peers_finished;

_Collection<R_adRevenue_countryCode_destURL_duration_languageCode_searchWord_sourceIP_userAgent_visitDate<double, string, string, int, string, string, string, string, string>> local_uservisits;

_Map<R_key_value<string, double>> local_q2_results;

int start_ms;

int end_ms;

int elapsed_ms;

unit_t q2_local(unit_t _) {
    {
        _Map<R_key_value<string, double>> agg_vals;
        
        
        
        
        agg_vals = local_uservisits.groupBy<string, double>([] (R_adRevenue_countryCode_destURL_duration_languageCode_searchWord_sourceIP_userAgent_visitDate<double, string, string, int, string, string, string, string, string> r) -> string {
            
            
            
            return substring(r.sourceIP)(0)(x);
        })([] (double acc) -> std::function<double(R_adRevenue_countryCode_destURL_duration_languageCode_searchWord_sourceIP_userAgent_visitDate<double, string, string, int, string, string, string, string, string>)> {
            
            return [acc] (R_adRevenue_countryCode_destURL_duration_languageCode_searchWord_sourceIP_userAgent_visitDate<double, string, string, int, string, string, string, string, string> r) -> double {
                return acc + r.adRevenue;
            };
        })(0.0);
        
        
        
        auto d = make_shared<DispatcherImpl<_Map<R_key_value<string, double>>>>(aggregate,agg_vals);
        engine.send(master,3,d);return unit_t();
    }
}

unit_t aggregate(_Map<R_key_value<string, double>> vals) {
    {
        _Map<R_key_value<string, double>> new_agg;
        
        
        
        
        
        new_agg = local_q2_results.combine(vals).groupBy<string, double>([] (R_key_value<string, double> r) -> string {
            return r.key;
        })([] (double acc) -> std::function<double(R_key_value<string, double>)> {
            return [acc] (R_key_value<string, double> r) -> double {
                return acc + r.value;
            };
        })(0.0);
        local_q2_results = new_agg;
        peers_finished = peers_finished + 1;
        if (peers_finished == num_peers) {
            
            end_ms = now(unit_t());
            elapsed_ms = end_ms - start_ms;
            
            
            printLine(itos(elapsed_ms));
            
            return peers.iterate([] (R_addr<Address> p) -> unit_t {
                
                
                
                auto d = make_shared<DispatcherImpl<unit_t>>(shutdown_,unit_t());
                engine.send(p.addr,2,d);return unit_t();
            });
        } else {
            return unit_t();
        }
    }
}

unit_t shutdown_(unit_t _) {
    
    return haltEngine(unit_t());
}

unit_t ready(unit_t _) {
    peers_ready = peers_ready + 1;
    if (peers_ready == num_peers) {
        
        start_ms = now(unit_t());
        
        return peers.iterate([] (R_addr<Address> p) -> unit_t {
            
            
            
            auto d = make_shared<DispatcherImpl<unit_t>>(q2_local,unit_t());
            engine.send(p.addr,4,d);return unit_t();
        });
    } else {
        return unit_t();
    }
}

unit_t load_all(unit_t _) {
    
    
    dataLoader(data_file)(local_uservisits);
    
    
    
    auto d = make_shared<DispatcherImpl<unit_t>>(ready,unit_t());
    engine.send(master,1,d);return unit_t();
}



unit_t rowsProcess(unit_t _) {
    
    return [] (unit_t next) -> unit_t {
        
        
        
        auto d = make_shared<DispatcherImpl<unit_t>>(load_all,next);
        engine.send(me,0,d);return unit_t();
    }(unit_t());
}

unit_t initDecls(unit_t _) {
    return unit_t();
}

unit_t processRole(unit_t _) {
    if (role == string("rows")) {
        
        return rowsProcess(unit_t());
    } else {
        return unit_t();
    }
}

unit_t atInit(unit_t _) {
    
    initDecls(unit_t());
    
    return processRole(unit_t());
}

unit_t atExit(unit_t _) {
    return unit_t();
}

unit_t initGlobalDecls() {
    
    master = make_address(string("127.0.0.1"),40000);x = 3;num_peers = 2;
    data_file = string("/k3/data/amplab/rankings_10.k3");peers_ready = 0;peers_finished = 0;
    start_ms = 0;end_ms = 0;elapsed_ms = 0;return unit_t();
}

void populate_dispatch() {
    dispatch_table.resize(5);
    dispatch_table[0] = make_tuple(make_shared<DispatcherImpl<unit_t>>(load_all), "load_all");
    dispatch_table[1] = make_tuple(make_shared<DispatcherImpl<unit_t>>(ready), "ready");
    dispatch_table[2] = make_tuple(make_shared<DispatcherImpl<unit_t>>(shutdown_), "shutdown_");
    dispatch_table[3] = make_tuple(make_shared<DispatcherImpl<_Map<R_key_value<string, double>>>>(aggregate), "aggregate");
    dispatch_table[4] = make_tuple(make_shared<DispatcherImpl<unit_t>>(q2_local), "q2_local");
}

map<string,string> show_globals() {
    map<string,string> result;
    result["elapsed_ms"] = to_string(elapsed_ms);
    result["end_ms"] = to_string(end_ms);
    result["start_ms"] = to_string(start_ms);
    result["local_q2_results"] = ([] (_Map<R_key_value<string, double>> coll) {
        ostringstream oss;
        auto f = [&] (R_key_value<string, double> elem) {oss << "{" + ("key:" + elem.key + "," + "value:" + to_string(elem.value) + "}") << ",";
        return unit_t();};
        coll.iterate(f);
        return "[" + oss.str() + "]";
    }(local_q2_results));
    result["local_uservisits"] = ([] (_Collection<R_adRevenue_countryCode_destURL_duration_languageCode_searchWord_sourceIP_userAgent_visitDate<double, string, string, int, string, string, string, string, string>> coll) {
        ostringstream oss;
        auto f = [&] (R_adRevenue_countryCode_destURL_duration_languageCode_searchWord_sourceIP_userAgent_visitDate<double, string, string, int, string, string, string, string, string> elem) {oss << "{" + ("adRevenue:" + to_string(elem.adRevenue) + "," + "countryCode:" + elem.countryCode + "," + "destURL:" + elem.destURL + "," + "duration:" + to_string(elem.duration) + "," + "languageCode:" + elem.languageCode + "," + "searchWord:" + elem.searchWord + "," + "sourceIP:" + elem.sourceIP + "," + "userAgent:" + elem.userAgent + "," + "visitDate:" + elem.visitDate + "}") << ",";
        return unit_t();};
        coll.iterate(f);
        return "[" + oss.str() + "]";
    }(local_uservisits));
    result["peers_finished"] = to_string(peers_finished);
    result["peers_ready"] = to_string(peers_ready);
    result["data_file"] = data_file;
    result["num_peers"] = to_string(num_peers);
    result["x"] = to_string(x);
    result["master"] = addressAsString(master);
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
    matchers["elapsed_ms"] = [] (string _s) {do_patch(_s,elapsed_ms);};
    matchers["end_ms"] = [] (string _s) {do_patch(_s,end_ms);};
    matchers["start_ms"] = [] (string _s) {do_patch(_s,start_ms);};
    matchers["local_q2_results"] = [] (string _s) {do_patch(_s,local_q2_results);};
    matchers["local_uservisits"] = [] (string _s) {do_patch(_s,local_uservisits);};
    matchers["peers_finished"] = [] (string _s) {do_patch(_s,peers_finished);};
    matchers["peers_ready"] = [] (string _s) {do_patch(_s,peers_ready);};
    matchers["data_file"] = [] (string _s) {do_patch(_s,data_file);};
    matchers["num_peers"] = [] (string _s) {do_patch(_s,num_peers);};
    matchers["x"] = [] (string _s) {do_patch(_s,x);};
    matchers["master"] = [] (string _s) {do_patch(_s,master);};
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
