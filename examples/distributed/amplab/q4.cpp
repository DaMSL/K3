#define MAIN_PROGRAM

#include <functional>
#include <fstream>
#include <memory>
#include <sstream>
#include <string>
#include <external/strtk.hpp>
#include <external/json_spirit_reader_template.h>
#include <re2/re2.h>

#include "Collections.hpp"
#include "Common.hpp"
#include "Dispatch.hpp"
#include "Engine.hpp"
#include "Literals.hpp"
#include "MessageProcessor.hpp"
#include "Serialization.hpp"
#include "Builtins.hpp"

using namespace std;
using namespace K3;
using namespace K3::BoostSerializer;
using std::begin;
using std::end;

using K3::Collection;

template <class _T0,class _T1> class R_key_value;

template <class _T0> class R_elem;

template <class _T0,class _T1,class _T2> class R_count_destPage_sourcePage;

template <class _T0> class R_arg;

template <class _T0> class R_addr;

template <class CONTENT> class _Seq;

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

unit_t aggregateShared(const shared_ptr<_Map<R_key_value<string, int>>>);
unit_t aggregate(const _Map<R_key_value<string, int>> &);

unit_t local(unit_t);

unit_t get_line(const string&);

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

template <class CONTENT>
class _Seq: public K3::Seq<CONTENT> {
    public:
        _Seq(): K3::Seq<CONTENT>(&engine) {}

        _Seq(const _Seq& c): K3::Seq<CONTENT>(c) {}

        _Seq(const K3::Seq<CONTENT>& c): K3::Seq<CONTENT>(c) {}

        template <class archive>
        void serialize(archive& _archive,const unsigned int) {

            _archive & boost::serialization::base_object<K3::Seq<CONTENT>>(*this);
        }

};
namespace K3 {
    template <class E>
    struct patcher<_Seq<E>> {
        static void patch(string s,_Seq<E>& c) {
            collection_patcher<_Seq,E>::patch(s,c);
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
        template <class archive>
        void serialize(archive& _archive,const unsigned int) {
            _archive & addr;

        }
        _T0 addr;
};
#endif // K3_R_addr
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
        template <class archive>
        void serialize(archive& _archive,const unsigned int) {
            _archive & arg;

        }
        _T0 arg;
};
#endif // K3_R_arg
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

#ifndef K3_R_count_destPage_sourcePage
#define K3_R_count_destPage_sourcePage
template <class _T0,class _T1,class _T2>
class R_count_destPage_sourcePage {
    public:
        R_count_destPage_sourcePage() {}
        R_count_destPage_sourcePage(_T0 _count
                                   ,_T1 _destPage
                                   ,_T2 _sourcePage): count(_count), destPage(_destPage), sourcePage(_sourcePage) {}
        R_count_destPage_sourcePage(const R_count_destPage_sourcePage<_T0, _T1, _T2>& _r): count(_r.count), destPage(_r.destPage), sourcePage(_r.sourcePage) {}
        template <class archive>
        void serialize(archive& _archive,const unsigned int) {
            _archive & count;
            _archive & destPage;
            _archive & sourcePage;

        }
        _T0 count;
        _T1 destPage;
        _T2 sourcePage;
};
#endif // K3_R_count_destPage_sourcePage
namespace K3 {
    template <class _T0,class _T1,class _T2>
    struct patcher<R_count_destPage_sourcePage<_T0, _T1, _T2>> {
        static void patch(string s,R_count_destPage_sourcePage<_T0, _T1, _T2>& r) {
            shallow<string::iterator> _shallow;;
            qi::rule<string::iterator, qi::space_type> _count = (qi::lit("count") >> ':' >> _shallow)[([&r] (string _string) {do_patch(_string
                                                                                                                                      ,r.count);})];
            qi::rule<string::iterator, qi::space_type> _destPage = (qi::lit("destPage") >> ':' >> _shallow)[([&r] (string _string) {do_patch(_string
                                                                                                                                            ,r.destPage);})];
            qi::rule<string::iterator, qi::space_type> _sourcePage = (qi::lit("sourcePage") >> ':' >> _shallow)[([&r] (string _string) {do_patch(_string
                                                                                                                                                ,r.sourcePage);})];
            qi::rule<string::iterator, qi::space_type> _field = _count | _destPage | _sourcePage;
            qi::rule<string::iterator, qi::space_type> _parser = '{' >> (_field % ',') >> '}';
            qi::phrase_parse(std::begin(s),std::end(s),_parser,qi::space);
        }
    };
}

#ifndef K3_R_elem
#define K3_R_elem
template <class _T0>
class R_elem {
    public:
        R_elem() {}
        R_elem(_T0 _elem): elem(_elem) {}
        R_elem(const R_elem<_T0>& _r): elem(_r.elem) {}
        template <class archive>
        void serialize(archive& _archive,const unsigned int) {
            _archive & elem;

        }
        _T0 elem;
};
#endif // K3_R_elem
namespace K3 {
    template <class _T0>
    struct patcher<R_elem<_T0>> {
        static void patch(string s,R_elem<_T0>& r) {
            shallow<string::iterator> _shallow;;
            qi::rule<string::iterator, qi::space_type> _elem = (qi::lit("elem") >> ':' >> _shallow)[([&r] (string _string) {do_patch(_string
                                                                                                                                    ,r.elem);})];
            qi::rule<string::iterator, qi::space_type> _field = _elem;
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
        template <class archive>
        void serialize(archive& _archive,const unsigned int) {
            _archive & key;
            _archive & value;

        }
        _T0 key;
        _T1 value;
};
#endif // K3_R_key_value
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

Address me;

_Collection<R_addr<Address>> peers;

tuple<_Collection<R_arg<string>>, _Collection<R_key_value<string, string>>> args;

string role;

F<unit_t(_Seq<R_elem<Str>>&)>stringLoader(string filepath){
    F<unit_t(_Seq<R_elem<Str>>&)> r = [filepath] (_Seq<R_elem<Str>> & c){
        std::ifstream infile(filepath);

        std::string line;
        while (std::getline(infile, line))
        {
          R_elem<Str> rec(line);
          c.insert(rec);
        }
        return unit_t();
    };
    return r;
}

Address master;

int x;

int num_peers;


int peers_ready;

int peers_finished;

int start_ms;

int end_ms;

int elapsed_ms;

string crawl_file;

_Seq<R_elem<Str>> inputData;

_Map<R_key_value<string, int>> url_count;

_Map<R_key_value<string, int>> url_regex;

string cur_page;

_Collection<R_count_destPage_sourcePage<int, string, string>> url_counts_partial;

RE2 regex_query("(https?://[^\\s]+)");

int time_pre_split = 0;
int time_split = 0;
int time_http_do = 0;
int time_regex = 0;
int time_local_last = 0;

unit_t get_line(Str& line) {
  int time_start = now();

  std::vector<string> words;
  string s(line.c_str());
  auto it = s.begin();
  auto last = s.begin();

  int time_split_start = now();

  // do split ourselves
  for (; it != s.end(); it++) {
    if (*it == ' ') {
      words.push_back(string(last, it));
      last = it + 1;
    }
  }
  // get the last string
  if (last != s.end()) {
    words.push_back(string(last, s.end()));
  }

  int time_split_end = now();

  if (s.substr(0,4) == "http" && words.size() == 5) {

      cur_page = words[0];

      for (const auto &v : url_count.getConstContainer()) {
          url_counts_partial.insert(
              R_count_destPage_sourcePage<int, string, string>{v.second, v.first, cur_page});
      }

      url_count.getContainer().clear();
  }

  int time_http_do_end = now();

  re2::StringPiece sp(s);
  string word;
  while (RE2::FindAndConsume(&sp, regex_query, &word)) {
      url_count.getContainer()[word] += 1;
  }

  int time_regex_end = now();

  int time_local_end = now();

  // Calculate times
  time_pre_split += time_split_start - time_start;
  time_split += time_split_end - time_split_start;
  time_http_do += time_http_do_end - time_split_end;
  time_regex += time_regex_end - time_http_do_end;
  time_local_last += time_local_end - time_regex_end;

  return unit_t();
}

_Map<R_key_value<string, int>> url_counts_total;

unit_t local(unit_t _) {

    int count = 0;
    for (auto &s : inputData.getContainer()) {
        get_line(s.elem);
        s = Str("");
        count++;
        /*if (!(count % 1000)) {
          printf("pre_split[%d], split[%d], http_do[%d], regex[%d], last[%d]\n",
                 time_pre_split, time_split, time_http_do, time_regex, time_local_last);
          time_pre_split=0;
          time_split=0;
          time_http_do=0;
          time_regex=0;
          time_local_last=0;
        } */
    }

    for (const auto &v : url_count.getConstContainer()) {
        url_counts_partial.insert(
            R_count_destPage_sourcePage<int, string, string>{v.second, v.first, cur_page});
    }

    for (const auto &v : url_counts_partial.getConstContainer()) {
      url_counts_total.getContainer()[v.destPage] += v.count;
    }

    auto d = make_shared<RefDispatcher<_Map<R_key_value<string, int>>>>
               (aggregate, url_counts_total);
    engine.send(master,3,d);
    return unit_t();
}

_Map<R_key_value<string, int>> url_counts_agg;

int received;

unit_t aggregateShared(const shared_ptr<_Map<R_key_value<string, int>>> newVals) {
  return aggregate(*newVals);
}

unit_t aggregate(const _Map<R_key_value<string, int>>& newVals) {

    for (const auto &v : newVals.getConstContainer()) {
      url_counts_agg.getContainer()[v.first] += v.second;
    }

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



            auto d = make_shared<ValDispatcher<unit_t>>(local,unit_t());
            engine.send(p.addr,4,d);return unit_t();
        });
    } else {
        return unit_t();
    }
}

int load_time = 0;

unit_t load_all(unit_t _) {

    int last = now(unit_t());
    stringLoader(crawl_file)(inputData);
    load_time = now(unit_t()) - last;

    auto d = make_shared<ValDispatcher<unit_t>>(ready,unit_t());
    engine.send(master,1,d);
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

    master = make_address(string("127.0.0.1"),40000);x = 3;num_peers = 2;
    start_ms = 0;end_ms = 0;elapsed_ms = 0;crawl_file = string("data.txt");cur_page = string("NONE");
    received = 0;return unit_t();
}

void populate_dispatch() {
    dispatch_table.resize(5);
    dispatch_table[0] = make_tuple(make_shared<ValDispatcher<unit_t>>(load_all), "load_all");
    dispatch_table[1] = make_tuple(make_shared<ValDispatcher<unit_t>>(ready), "ready");
    dispatch_table[2] = make_tuple(make_shared<ValDispatcher<unit_t>>(shutdown_), "shutdown_");
    dispatch_table[3] = make_tuple(make_shared<SharedDispatcher<_Map<R_key_value<string, int>>>>(aggregateShared), "aggregate");
    dispatch_table[4] = make_tuple(make_shared<ValDispatcher<unit_t>>(local), "local");
}

map<string,string> show_globals() {
    map<string,string> result;
    result["received"] = to_string(received);
    result["url_counts_agg"] = ([] (_Map<R_key_value<string, int>> coll) {
        ostringstream oss;
        auto f = [&] (R_key_value<string, int> elem) {oss << "{" + ("key:" + elem.key + "," + "value:" + to_string(elem.value) + "}") << ",";
        return unit_t();};
        coll.iterate(f);
        return "[" + oss.str() + "]";
    }(url_counts_agg));
    result["url_counts_partial"] = ([] (_Collection<R_count_destPage_sourcePage<int, string, string>> coll) {
        ostringstream oss;
        auto f = [&] (R_count_destPage_sourcePage<int, string, string> elem) {oss << "{" + ("sourcePage:" + elem.sourcePage + "," + "destPage:" + elem.destPage + "," + "count:" + to_string(elem.count) + "}") << ",";
        return unit_t();};
        coll.iterate(f);
        return "[" + oss.str() + "]";
    }(url_counts_partial));
    result["cur_page"] = cur_page;
    result["url_regex"] = ([] (_Map<R_key_value<string, int>> coll) {
        ostringstream oss;
        auto f = [&] (R_key_value<string, int> elem) {oss << "{" + ("key:" + elem.key + "," + "value:" + to_string(elem.value) + "}") << ",";
        return unit_t();};
        coll.iterate(f);
        return "[" + oss.str() + "]";
    }(url_regex));
    result["url_count"] = ([] (_Map<R_key_value<string, int>> coll) {
        ostringstream oss;
        auto f = [&] (R_key_value<string, int> elem) {oss << "{" + ("key:" + elem.key + "," + "value:" + to_string(elem.value) + "}") << ",";
        return unit_t();};
        coll.iterate(f);
        return "[" + oss.str() + "]";
    }(url_count));
    result["crawl_file"] = crawl_file;
    result["elapsed_ms"] = to_string(elapsed_ms);
    result["end_ms"] = to_string(end_ms);
    result["start_ms"] = to_string(start_ms);
    result["peers_finished"] = to_string(peers_finished);
    result["peers_ready"] = to_string(peers_ready);
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
    matchers["received"] = [] (string _s) {do_patch(_s,received);};
    matchers["url_counts_agg"] = [] (string _s) {do_patch(_s,url_counts_agg);};
    matchers["url_counts_partial"] = [] (string _s) {do_patch(_s,url_counts_partial);};
    matchers["cur_page"] = [] (string _s) {do_patch(_s,cur_page);};
    matchers["url_regex"] = [] (string _s) {do_patch(_s,url_regex);};
    matchers["url_count"] = [] (string _s) {do_patch(_s,url_count);};
    matchers["inputData"] = [] (string _s) {do_patch(_s,inputData);};
    matchers["crawl_file"] = [] (string _s) {do_patch(_s,crawl_file);};
    matchers["elapsed_ms"] = [] (string _s) {do_patch(_s,elapsed_ms);};
    matchers["end_ms"] = [] (string _s) {do_patch(_s,end_ms);};
    matchers["start_ms"] = [] (string _s) {do_patch(_s,start_ms);};
    matchers["peers_finished"] = [] (string _s) {do_patch(_s,peers_finished);};
    matchers["peers_ready"] = [] (string _s) {do_patch(_s,peers_ready);};
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
