#define MAIN_PROGRAM

#include <chrono>
#include <thread>
#include <functional>
#include <memory>
#include <sstream>
#include <string>
#include <external/strtk.hpp>
#include <external/json_spirit_reader_template.h>
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




#include "Builtins.hpp"

using K3::Collection;

template <class _T0,class _T1> class R_pageRank_pageURL;

template <class _T0,class _T1> class R_key_value;

template <class _T0,class _T1,class _T2> class R_avgDuration_pageRank_pageURL;

template <class _T0> class R_arg;

template <class _T0> class R_addr;

template <class CONTENT> class _Collection;

unit_t atExit(unit_t);

unit_t atInit(unit_t);

unit_t processRole(unit_t);

unit_t initDecls(unit_t);

unit_t rowsProcess(unit_t);

unit_t load_all(unit_t);

unit_t hello(unit_t);
unit_t ready(unit_t);

unit_t shutdown_(unit_t);

unit_t finished(unit_t);

unit_t q1_local(unit_t);

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

































Address me;

_Collection<R_addr<Address>> peers;

tuple<_Collection<R_arg<string>>, _Collection<R_key_value<string, string>>> args;

string role;

F<unit_t(K3::Collection<R_avgDuration_pageRank_pageURL<int, int, string>>&)>dataLoader(string filepath){
    F<unit_t(K3::Collection<R_avgDuration_pageRank_pageURL<int, int, string>>&)> r = [filepath] (K3::Collection<R_avgDuration_pageRank_pageURL<int, int, string>> & c){
        R_avgDuration_pageRank_pageURL<int, int, string> rec;
        strtk::for_each_line(filepath,
        [&](const std::string& str){
            if (strtk::parse(str,",",rec.pageURL, rec.pageRank, rec.avgDuration)){
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

string rankings_file;

int peers_ready;

int peers_finished;

_Collection<R_avgDuration_pageRank_pageURL<int, int, string>> local_rankings;

_Collection<R_pageRank_pageURL<int, string>> local_q1_results;

int start_ms;

int end_ms;

int elapsed_ms;

unit_t q1_local(unit_t _) {
    
    local_rankings.iterate([] (R_avgDuration_pageRank_pageURL<int, int, string> row) -> unit_t {
        if (row.pageRank > x) {
            
            
            return local_q1_results.insert(R_pageRank_pageURL<int, string>{row.pageRank,
            row.pageURL});
        } else {
            return unit_t();
        }
    });
    
    
    
    auto d = make_shared<ValDispatcher<unit_t>>(finished,unit_t());
    engine.send(master,3,d);return unit_t();
}

unit_t finished(unit_t _) {
    peers_finished = peers_finished + 1;
    if (peers_finished == num_peers) {
        
        end_ms = now(unit_t());
        elapsed_ms = end_ms - start_ms;
        
        
        printLine("time:" + itos(elapsed_ms));
        
        return peers.iterate([] (R_addr<Address> p) -> unit_t {
            
            
            
            auto d = make_shared<ValDispatcher<unit_t>>(shutdown_,unit_t());
            engine.send(p.addr,2,d);return unit_t();
        });
    } else {
        return unit_t();
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
            
            
            
            auto d = make_shared<ValDispatcher<unit_t>>(q1_local,unit_t());
            engine.send(p.addr,4,d);return unit_t();
        });
    } else {
        return unit_t();
    }
}

unit_t load_all(unit_t _) {
    
    
    dataLoader(rankings_file)(local_rankings);
 
    std::chrono::milliseconds dura( 10000 );
    std::this_thread::sleep_for( dura );

    for (const auto& p : peers.getConstContainer()) {
      auto d = make_shared<ValDispatcher<unit_t>>(hello,unit_t());
      engine.send(p.addr,5,d);
    } 
    
    
    auto d = make_shared<ValDispatcher<unit_t>>(ready,unit_t());
    engine.send(master,1,d);return unit_t();
}


unit_t hello(unit_t _) {
    return unit_t();
}

unit_t rowsProcess(unit_t _) {
    
    return [] (unit_t next) -> unit_t {
        
        
        
        auto d = make_shared<ValDispatcher<unit_t>>(load_all,next);
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
    
    master = make_address(string("127.0.0.1"),40000);x = 10;num_peers = 2;
    rankings_file = string("/k3/data/amplab/rankings_10.k3");peers_ready = 0;peers_finished = 0;
    start_ms = 0;end_ms = 0;elapsed_ms = 0;return unit_t();
}

void populate_dispatch() {
    dispatch_table.resize(6);
    dispatch_table[0] = make_tuple(make_shared<ValDispatcher<unit_t>>(load_all), "load_all");
    dispatch_table[1] = make_tuple(make_shared<ValDispatcher<unit_t>>(ready), "ready");
    dispatch_table[2] = make_tuple(make_shared<ValDispatcher<unit_t>>(shutdown_), "shutdown_");
    dispatch_table[3] = make_tuple(make_shared<ValDispatcher<unit_t>>(finished), "finished");
    dispatch_table[4] = make_tuple(make_shared<ValDispatcher<unit_t>>(q1_local), "q1_local");
    dispatch_table[5] = make_tuple(make_shared<ValDispatcher<unit_t>>(hello), "hello");
}

map<string,string> show_globals() {
    map<string,string> result;
    result["elapsed_ms"] = to_string(elapsed_ms);
    result["end_ms"] = to_string(end_ms);
    result["start_ms"] = to_string(start_ms);
    result["local_q1_results"] = ([] (_Collection<R_pageRank_pageURL<int, string>> coll) {
        ostringstream oss;
        auto f = [&] (R_pageRank_pageURL<int, string> elem) {oss << "{" + ("pageRank:" + to_string(elem.pageRank) + "," + "pageURL:" + elem.pageURL + "}") << ",";
        return unit_t();};
        coll.iterate(f);
        return "[" + oss.str() + "]";
    }(local_q1_results));
    result["local_rankings"] = ([] (_Collection<R_avgDuration_pageRank_pageURL<int, int, string>> coll) {
        ostringstream oss;
        auto f = [&] (R_avgDuration_pageRank_pageURL<int, int, string> elem) {oss << "{" + ("avgDuration:" + to_string(elem.avgDuration) + "," + "pageRank:" + to_string(elem.pageRank) + "," + "pageURL:" + elem.pageURL + "}") << ",";
        return unit_t();};
        coll.iterate(f);
        return "[" + oss.str() + "]";
    }(local_rankings));
    result["peers_finished"] = to_string(peers_finished);
    result["peers_ready"] = to_string(peers_ready);
    result["rankings_file"] = rankings_file;
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
    matchers["local_q1_results"] = [] (string _s) {do_patch(_s,local_q1_results);};
    matchers["local_rankings"] = [] (string _s) {do_patch(_s,local_rankings);};
    matchers["peers_finished"] = [] (string _s) {do_patch(_s,peers_finished);};
    matchers["peers_ready"] = [] (string _s) {do_patch(_s,peers_ready);};
    matchers["rankings_file"] = [] (string _s) {do_patch(_s,rankings_file);};
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
