#define MAIN_PROGRAM

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



Engine engine = Engine();

using K3::Collection;


template <class _T0,class _T1> class R_key_value;


template <class _T0> class R_elem;

template <class _T0,class _T1> class R_count_sum;

template <class _T0> class R_arg;

template <class _T0> class R_addr;

template <class CONTENT> class _Collection;

unit_t atExit(unit_t);

unit_t atInit(unit_t);

unit_t processRole(unit_t);

unit_t initDecls(unit_t);

unit_t pointsProcess(unit_t);

unit_t load_all(unit_t);

unit_t shutdown_(unit_t);

unit_t start(unit_t);

unit_t ready(unit_t);

unit_t maximize(unit_t);

unit_t aggregate(_Collection<R_key_value<int, R_count_sum<int, _Collection<R_elem<double>>>>>);

unit_t assign(_Collection<R_key_value<int, _Collection<R_elem<double>>>>);

std::function<int(_Collection<R_key_value<int, _Collection<R_elem<double>>>>)> nearest_neighbor(_Collection<R_elem<double>>);

unit_t print_means(unit_t);

unit_t foo(unit_t);

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
#endif K3_R_addr
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
#endif K3_R_arg
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

#ifndef K3_R_count_sum
#define K3_R_count_sum
template <class _T0,class _T1>
class R_count_sum {
    public:
        R_count_sum() {}
        R_count_sum(_T0 _count,_T1 _sum): count(_count), sum(_sum) {}
        R_count_sum(const R_count_sum<_T0, _T1>& _r): count(_r.count), sum(_r.sum) {}
        bool operator==(R_count_sum _r) {
            if (count == _r.count&& sum == _r.sum)
                return true;
            return false;
        }
        template <class archive>
        void serialize(archive& _archive,const unsigned int) {
            _archive & count;
            _archive & sum;
            
        }
        _T0 count;
        _T1 sum;
};
#endif K3_R_count_sum
namespace K3 {
    template <class _T0,class _T1>
    struct patcher<R_count_sum<_T0, _T1>> {
        static void patch(string s,R_count_sum<_T0, _T1>& r) {
            shallow<string::iterator> _shallow;;
            qi::rule<string::iterator, qi::space_type> _count = (qi::lit("count") >> ':' >> _shallow)[([&r] (string _string) {do_patch(_string
                                                                                                                                      ,r.count);})];
            qi::rule<string::iterator, qi::space_type> _sum = (qi::lit("sum") >> ':' >> _shallow)[([&r] (string _string) {do_patch(_string
                                                                                                                                  ,r.sum);})];
            qi::rule<string::iterator, qi::space_type> _field = _count | _sum;
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
        bool operator==(R_elem _r) {
            if (elem == _r.elem)
                return true;
            return false;
        }
        template <class archive>
        void serialize(archive& _archive,const unsigned int) {
            _archive & elem;
            
        }
        _T0 elem;
};
#endif K3_R_elem
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
#endif K3_R_key_value
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

Address master;

int k;

int dimensionality;

int num_peers;

int iterations_remaining;

int requests;

F<unit_t(K3::Collection<R_elem<_Collection<R_elem<double>>>>&)>LoaderVector(string filepath){
    F<unit_t(K3::Collection<R_elem<_Collection<R_elem<double>>>>&)> r = [filepath] (K3::Collection<R_elem<_Collection<R_elem<double>>>> & c){
        std::string line;
        std::ifstream infile(filepath);
        while (std::getline(infile, line)){
            char * pch;
            pch = strtok (&line[0],",");
            _Collection<R_elem<double>> c2 = _Collection<R_elem<double>>();
            while (pch != NULL){
                R_elem<double> rec;
                rec.elem = std::atof(pch);
                c2.insert(rec);
                pch = strtok (NULL,",");
            }
            R_elem<_Collection<R_elem<double>>> rec2;
            rec2.elem = c2;
            c.insert(rec2);
        }
        return unit_t();
    };
    return r;
}

_Collection<R_key_value<int, _Collection<R_elem<double>>>> means;

_Collection<R_key_value<int, R_count_sum<int, _Collection<R_elem<double>>>>> aggregates;

_Collection<R_elem<_Collection<R_elem<double>>>> data;

unit_t foo(unit_t _) {
    return unit_t();
}

unit_t print_means(unit_t _) {
    
    return means.iterate([] (R_key_value<int, _Collection<R_elem<double>>> m) -> unit_t {
        
        printLine(string("Mean: "));
        
        return m.value.iterate([] (R_elem<double> p) -> unit_t {
            
            
            return printLine(rtos(p.elem));
        });
    });
}

std::function<int(_Collection<R_key_value<int, _Collection<R_elem<double>>>>)> nearest_neighbor(_Collection<R_elem<double>> p) {
    return [p] (_Collection<R_key_value<int, _Collection<R_elem<double>>>> means) -> int {
        std::shared_ptr<R_key_value<int, _Collection<R_elem<double>>>> __0;
        
        __0 = means.peek(unit_t());
        if (__0) {
            R_key_value<int, _Collection<R_elem<double>>> first_mean;
            first_mean = *__0;{
                R_key_value<int,_Collection<R_elem<double>>> nearest;
                
                
                
                nearest = means.fold<R_key_value<int,_Collection<R_elem<double>>>>([p] (R_key_value<int,_Collection<R_elem<double>>> acc) -> std::function<R_key_value<int,_Collection<R_elem<double>>>(R_key_value<int,_Collection<R_elem<double>>>)> {
                    return [p
                           ,acc] (R_key_value<int,_Collection<R_elem<double>>> next) -> R_key_value<int,_Collection<R_elem<double>>> {
                        
                        
                        
                        
                        if (squared_distance(p)(next.value) < squared_distance(p)(acc.value)) {
                            return next;
                        } else {
                            return acc;
                        }
                    };
                })(first_mean);
                return nearest.key;
            }
        } else {
            return -1;
        }
    };
}

unit_t assign(_Collection<R_key_value<int, _Collection<R_elem<double>>>> current_means) {
    
    
    
    
    
    
    
    
    auto d = make_shared<DispatcherImpl<_Collection<R_key_value<int, R_count_sum<int, _Collection<R_elem<double>>>>>>>(aggregate
                                                                                                                      ,data.groupBy<int, R_count_sum<int, _Collection<R_elem<double>>>>([current_means] (R_elem<_Collection<R_elem<double>>> p) -> int {
                                                                                                                          
                                                                                                                          
                                                                                                                          
                                                                                                                          return nearest_neighbor(p.elem)(current_means);
                                                                                                                      })([] (R_count_sum<int, _Collection<R_elem<double>>> sc) -> std::function<R_count_sum<int, _Collection<R_elem<double>>>(R_elem<_Collection<R_elem<double>>>)> {
                                                                                                                          
                                                                                                                          return [sc] (R_elem<_Collection<R_elem<double>>> p) -> R_count_sum<int, _Collection<R_elem<double>>> {
                                                                                                                              
                                                                                                                              
                                                                                                                              
                                                                                                                              
                                                                                                                              
                                                                                                                              return R_count_sum<int, _Collection<R_elem<double>>>{sc.count + 1,
                                                                                                                              vector_add(sc.sum)(p.elem)};
                                                                                                                          };
                                                                                                                      })(R_count_sum<int, _Collection<R_elem<double>>>{0,
                                                                                                                      zero_vector(dimensionality)}));
    engine.send(master,5,d);return unit_t();
}

unit_t aggregate(_Collection<R_key_value<int, R_count_sum<int, _Collection<R_elem<double>>>>> ags) {
    {
        _Collection<R_key_value<int, R_count_sum<int, _Collection<R_elem<double>>>>> combined;
        
        combined = aggregates.combine(ags);
        {
            _Collection<R_key_value<int, R_count_sum<int, _Collection<R_elem<double>>>>> new_ags;
            
            
            
            
            
            
            new_ags = combined.groupBy<int, R_count_sum<int, _Collection<R_elem<double>>>>([] (R_key_value<int, R_count_sum<int, _Collection<R_elem<double>>>> ag) -> int {
                return ag.key;
            })([] (R_count_sum<int, _Collection<R_elem<double>>> acc) -> std::function<R_count_sum<int, _Collection<R_elem<double>>>(R_key_value<int, R_count_sum<int, _Collection<R_elem<double>>>>)> {
                
                return [acc] (R_key_value<int, R_count_sum<int, _Collection<R_elem<double>>>> ag) -> R_count_sum<int, _Collection<R_elem<double>>> {
                    
                    
                    
                    return R_count_sum<int, _Collection<R_elem<double>>>{acc.count + ag.value.count,
                    vector_add(acc.sum)(ag.value.sum)};
                };
            })(R_count_sum<int, _Collection<R_elem<double>>>{0,zero_vector(dimensionality)});
            aggregates = new_ags;
            requests = requests - 1;
            if (requests == 0) {
                
                
                
                auto d = make_shared<DispatcherImpl<unit_t>>(maximize,unit_t());
                engine.send(master,4,d);return unit_t();
            } else {
                return unit_t();
            }
        }
    }
}

unit_t maximize(unit_t _) {
    means = _Collection<R_key_value<int, _Collection<R_elem<double>>>>();
    
    
    aggregates.iterate([] (R_key_value<int, R_count_sum<int, _Collection<R_elem<double>>>> x) -> unit_t {
        
        
        
        
        return means.insert(R_key_value<int, _Collection<R_elem<double>>>{x.key,
        scalar_mult(1.0 / x.value.count)(x.value.sum)});
    });
    iterations_remaining = iterations_remaining - 1;
    if (iterations_remaining == 0) {
        
        print_means(unit_t());
        
        return peers.iterate([] (R_addr<Address> p) -> unit_t {
            
            
            
            auto d = make_shared<DispatcherImpl<unit_t>>(shutdown_,unit_t());
            engine.send(p.addr,1,d);return unit_t();
        });
    } else {
        aggregates = _Collection<R_key_value<int, R_count_sum<int, _Collection<R_elem<double>>>>>();
        
        return peers.iterate([] (R_addr<Address> p) -> unit_t {
            requests = requests + 1;
            
            
            
            auto d = make_shared<DispatcherImpl<_Collection<R_key_value<int, _Collection<R_elem<double>>>>>>(assign
                                                                                                            ,means);
            engine.send(p.addr,6,d);return unit_t();
        });
    }
}

unit_t ready(unit_t _) {
    num_peers = num_peers - 1;
    if (num_peers == 0) {
        
        
        
        auto d = make_shared<DispatcherImpl<unit_t>>(start,unit_t());
        engine.send(master,2,d);return unit_t();
    } else {
        return unit_t();
    }
}

unit_t start(unit_t _) {
    {
        int foo;
        
        
        foo = data.fold<int>([] (int a) -> std::function<int(R_elem<_Collection<R_elem<double>>>)> {
            return [a] (R_elem<_Collection<R_elem<double>>> p) -> int {
                if (a == 0) {
                    return a;
                } else {
                    
                    
                    means.insert(R_key_value<int, _Collection<R_elem<double>>>{a,p.elem});
                    return a - 1;
                }
            };
        })(k);
        
        return peers.iterate([] (R_addr<Address> p) -> unit_t {
            requests = requests + 1;
            
            
            
            auto d = make_shared<DispatcherImpl<_Collection<R_key_value<int, _Collection<R_elem<double>>>>>>(assign
                                                                                                            ,means);
            engine.send(p.addr,6,d);return unit_t();
        });
    }
}

unit_t shutdown_(unit_t _) {
    
    return haltEngine(unit_t());
}

unit_t load_all(unit_t _) {
    
    
    LoaderVector(string("/Users/joshwheeler/foo.vector"))(data);
    
    
    
    auto d = make_shared<DispatcherImpl<unit_t>>(ready,unit_t());
    engine.send(master,3,d);return unit_t();
}



unit_t pointsProcess(unit_t _) {
    
    return [] (unit_t next) -> unit_t {
        
        
        
        auto d = make_shared<DispatcherImpl<unit_t>>(load_all,next);
        engine.send(me,0,d);return unit_t();
    }(unit_t());
}

unit_t initDecls(unit_t _) {
    return unit_t();
}

unit_t processRole(unit_t _) {
    if (role == string("points")) {
        
        return pointsProcess(unit_t());
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
    
    master = make_address(string("127.0.0.1"),40000);k = 1;dimensionality = 3;num_peers = 1;
    iterations_remaining = 10;requests = 0;return unit_t();
}

void populate_dispatch() {
    dispatch_table.resize(8);
    dispatch_table[0] = make_tuple(make_shared<DispatcherImpl<unit_t>>(load_all), "load_all");
    dispatch_table[1] = make_tuple(make_shared<DispatcherImpl<unit_t>>(shutdown_), "shutdown_");
    dispatch_table[2] = make_tuple(make_shared<DispatcherImpl<unit_t>>(start), "start");
    dispatch_table[3] = make_tuple(make_shared<DispatcherImpl<unit_t>>(ready), "ready");
    dispatch_table[4] = make_tuple(make_shared<DispatcherImpl<unit_t>>(maximize), "maximize");
    dispatch_table[5] = make_tuple(make_shared<DispatcherImpl<_Collection<R_key_value<int, R_count_sum<int, _Collection<R_elem<double>>>>>>>(aggregate), "aggregate");
    dispatch_table[6] = make_tuple(make_shared<DispatcherImpl<_Collection<R_key_value<int, _Collection<R_elem<double>>>>>>(assign), "assign");
    dispatch_table[7] = make_tuple(make_shared<DispatcherImpl<unit_t>>(foo), "foo");
}

map<string,string> show_globals() {
    map<string,string> result;
    result["data"] = ([] (_Collection<R_elem<_Collection<R_elem<double>>>> coll) {
        ostringstream oss;
        auto f = [&] (R_elem<_Collection<R_elem<double>>> elem) {oss << "{" + ("elem:" + ([] (_Collection<R_elem<double>> coll) {
            ostringstream oss;
            auto f = [&] (R_elem<double> elem) {oss << "{" + ("elem:" + to_string(elem.elem) + "}") << ",";
            return unit_t();};
            coll.iterate(f);
            return "[" + oss.str() + "]";
        }(elem.elem)) + "}") << ",";
        return unit_t();};
        coll.iterate(f);
        return "[" + oss.str() + "]";
    }(data));
    result["aggregates"] = ([] (_Collection<R_key_value<int, R_count_sum<int, _Collection<R_elem<double>>>>> coll) {
        ostringstream oss;
        auto f = [&] (R_key_value<int, R_count_sum<int, _Collection<R_elem<double>>>> elem) {oss << "{" + ("key:" + to_string(elem.key) + "," + "value:" + "{" + ("count:" + to_string(elem.value.count) + "," + "sum:" + ([] (_Collection<R_elem<double>> coll) {
            ostringstream oss;
            auto f = [&] (R_elem<double> elem) {oss << "{" + ("elem:" + to_string(elem.elem) + "}") << ",";
            return unit_t();};
            coll.iterate(f);
            return "[" + oss.str() + "]";
        }(elem.value.sum)) + "}") + "}") << ",";
        return unit_t();};
        coll.iterate(f);
        return "[" + oss.str() + "]";
    }(aggregates));
    result["means"] = ([] (_Collection<R_key_value<int, _Collection<R_elem<double>>>> coll) {
        ostringstream oss;
        auto f = [&] (R_key_value<int, _Collection<R_elem<double>>> elem) {oss << "{" + ("key:" + to_string(elem.key) + "," + "value:" + ([] (_Collection<R_elem<double>> coll) {
            ostringstream oss;
            auto f = [&] (R_elem<double> elem) {oss << "{" + ("elem:" + to_string(elem.elem) + "}") << ",";
            return unit_t();};
            coll.iterate(f);
            return "[" + oss.str() + "]";
        }(elem.value)) + "}") << ",";
        return unit_t();};
        coll.iterate(f);
        return "[" + oss.str() + "]";
    }(means));
    result["requests"] = to_string(requests);
    result["iterations_remaining"] = to_string(iterations_remaining);
    result["num_peers"] = to_string(num_peers);
    result["dimensionality"] = to_string(dimensionality);
    result["k"] = to_string(k);
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
    matchers["data"] = [] (string _s) {do_patch(_s,data);};
    matchers["aggregates"] = [] (string _s) {do_patch(_s,aggregates);};
    matchers["means"] = [] (string _s) {do_patch(_s,means);};
    matchers["requests"] = [] (string _s) {do_patch(_s,requests);};
    matchers["iterations_remaining"] = [] (string _s) {do_patch(_s,iterations_remaining);};
    matchers["num_peers"] = [] (string _s) {do_patch(_s,num_peers);};
    matchers["dimensionality"] = [] (string _s) {do_patch(_s,dimensionality);};
    matchers["k"] = [] (string _s) {do_patch(_s,k);};
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
