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


string bpti_file;

using K3::Collection;

template <class _T0> class R_value;

template <class _T0> class R_mean;

template <class _T0,class _T1> class R_key_value;

template <class _T0> class R_key;

template <class _T0> class R_elem;

template <class _T0,class _T1> class R_distance_mean;

template <class _T0,class _T1> class R_count_sum;

template <class _T0> class R_arg;

template <class _T0> class R_addr;

template <class CONTENT> class _Map;

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

unit_t aggregate(const _Map<R_key_value<int, R_count_sum<int, _Collection<R_elem<double>>>>>&);
unit_t aggregateShared(const shared_ptr<_Map<R_key_value<int, R_count_sum<int, _Collection<R_elem<double>>>>>>);

unit_t assign(const _Collection<R_key_value<int, _Collection<R_elem<double>>>>&);
unit_t assignShared(const shared_ptr<_Collection<R_key_value<int, _Collection<R_elem<double>>>>>);

int nearest_neighbor(_Collection<R_elem<double>>&, 
    _Collection<R_key_value<int, _Collection<R_elem<double>>>>&);

unit_t print_results(unit_t);

unit_t merge_results(const _Map<R_key_value<int, R_count_sum<int, _Collection<R_elem<double>>>>>&);

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
#endif // K3_R_count_sum
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

#ifndef K3_R_distance_mean
#define K3_R_distance_mean
template <class _T0,class _T1>
class R_distance_mean {
    public:
        R_distance_mean() {}
        R_distance_mean(_T0 _distance,_T1 _mean): distance(_distance), mean(_mean) {}
        R_distance_mean(const R_distance_mean<_T0, _T1>& _r): distance(_r.distance), mean(_r.mean) {}
        bool operator==(R_distance_mean _r) {
            if (distance == _r.distance&& mean == _r.mean)
                return true;
            return false;
        }
        template <class archive>
        void serialize(archive& _archive,const unsigned int) {
            _archive & distance;
            _archive & mean;

        }
        _T0 distance;
        _T1 mean;
};
#endif // K3_R_distance_mean
namespace K3 {
    template <class _T0,class _T1>
    struct patcher<R_distance_mean<_T0, _T1>> {
        static void patch(string s,R_distance_mean<_T0, _T1>& r) {
            shallow<string::iterator> _shallow;;
            qi::rule<string::iterator, qi::space_type> _distance = (qi::lit("distance") >> ':' >> _shallow)[([&r] (string _string) {do_patch(_string
                                                                                                                                            ,r.distance);})];
            qi::rule<string::iterator, qi::space_type> _mean = (qi::lit("mean") >> ':' >> _shallow)[([&r] (string _string) {do_patch(_string
                                                                                                                                    ,r.mean);})];
            qi::rule<string::iterator, qi::space_type> _field = _distance | _mean;
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

#ifndef K3_R_key
#define K3_R_key
template <class _T0>
class R_key {
    public:
        R_key() {}
        R_key(_T0 _key): key(_key) {}
        R_key(const R_key<_T0>& _r): key(_r.key) {}
        bool operator==(R_key _r) {
            if (key == _r.key)
                return true;
            return false;
        }
        template <class archive>
        void serialize(archive& _archive,const unsigned int) {
            _archive & key;

        }
        _T0 key;
};
#endif // K3_R_key
namespace K3 {
    template <class _T0>
    struct patcher<R_key<_T0>> {
        static void patch(string s,R_key<_T0>& r) {
            shallow<string::iterator> _shallow;;
            qi::rule<string::iterator, qi::space_type> _key = (qi::lit("key") >> ':' >> _shallow)[([&r] (string _string) {do_patch(_string
                                                                                                                                  ,r.key);})];
            qi::rule<string::iterator, qi::space_type> _field = _key;
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

#ifndef K3_R_mean
#define K3_R_mean
template <class _T0>
class R_mean {
    public:
        R_mean() {}
        R_mean(_T0 _mean): mean(_mean) {}
        R_mean(const R_mean<_T0>& _r): mean(_r.mean) {}
        bool operator==(R_mean _r) {
            if (mean == _r.mean)
                return true;
            return false;
        }
        template <class archive>
        void serialize(archive& _archive,const unsigned int) {
            _archive & mean;

        }
        _T0 mean;
};
#endif // K3_R_mean
namespace K3 {
    template <class _T0>
    struct patcher<R_mean<_T0>> {
        static void patch(string s,R_mean<_T0>& r) {
            shallow<string::iterator> _shallow;;
            qi::rule<string::iterator, qi::space_type> _mean = (qi::lit("mean") >> ':' >> _shallow)[([&r] (string _string) {do_patch(_string
                                                                                                                                    ,r.mean);})];
            qi::rule<string::iterator, qi::space_type> _field = _mean;
            qi::rule<string::iterator, qi::space_type> _parser = '{' >> (_field % ',') >> '}';
            qi::phrase_parse(std::begin(s),std::end(s),_parser,qi::space);
        }
    };
}

#ifndef K3_R_value
#define K3_R_value
template <class _T0>
class R_value {
    public:
        R_value() {}
        R_value(_T0 _value): value(_value) {}
        R_value(const R_value<_T0>& _r): value(_r.value) {}
        bool operator==(R_value _r) {
            if (value == _r.value)
                return true;
            return false;
        }
        template <class archive>
        void serialize(archive& _archive,const unsigned int) {
            _archive & value;

        }
        _T0 value;
};
#endif // K3_R_value
namespace K3 {
    template <class _T0>
    struct patcher<R_value<_T0>> {
        static void patch(string s,R_value<_T0>& r) {
            shallow<string::iterator> _shallow;;
            qi::rule<string::iterator, qi::space_type> _value = (qi::lit("value") >> ':' >> _shallow)[([&r] (string _string) {do_patch(_string
                                                                                                                                      ,r.value);})];
            qi::rule<string::iterator, qi::space_type> _field = _value;
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

int start_ms;

int end_ms;

int elapsed_ms;

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

_Map<R_key_value<int, R_count_sum<int, _Collection<R_elem<double>>>>> aggregates;

_Collection<R_elem<_Collection<R_elem<double>>>> data;

unit_t merge_results(const _Map<R_key_value<int, R_count_sum<int, _Collection<R_elem<double>>>>>& vals) {


    return vals.iterate([] (R_key_value<int, R_count_sum<int, _Collection<R_elem<double>>>> v) -> unit_t {
        R_count_sum<int, _Collection<R_elem<double>>>* __0;
        __0 = lookup<int, R_count_sum<int, _Collection<R_elem<double>>>>(aggregates)(v.key);
        if (__0) {
            R_count_sum<int, _Collection<R_elem<double>>> a;
            a = *__0;



            return aggregates.insert(R_key_value<int, R_count_sum<int, _Collection<R_elem<double>>>>{v.key,
            R_count_sum<int, _Collection<R_elem<double>>>{v.value.count + a.count,
            vector_add(v.value.sum)(a.sum)}});
        } else {

            return aggregates.insert(v);
        }
    });
}

unit_t print_results(unit_t _) {

    return means.iterate([] (R_key_value<int, _Collection<R_elem<double>>> m) -> unit_t {

        end_ms = now(unit_t());
        elapsed_ms = end_ms - start_ms;

        printLine(string("Mean: "));

        m.value.iterate([] (R_elem<double> p) -> unit_t {


            return printLine(rtos(p.elem));
        });


        return printLine("time:" + itos(elapsed_ms));
    });
}

int nearest_neighbor(_Collection<R_elem<double>>& p, 
  _Collection<R_key_value<int, _Collection<R_elem<double>>>>& means) {
    if (means.getCollection().empty()) { return -1; }

    auto first_mean = *(means.getCollection().begin());
    auto nearest = squared_distance(p)(first_mean.value);
    auto *nearestMean = &first_mean;

    for (auto &m : means) {
      if (squared_distance(p)(m.value) < nearest) {
         nearest = squared_distance(p)(m.value);
         nearestMean = &m;
      }
    }
    return nearestMean->key;
}

unit_t assignShared(const shared_ptr<_Collection<R_key_value<int, _Collection<R_elem<double>>>>> current_means) {
  return assign(*current_means);
}

unit_t assign(const _Collection<R_key_value<int, _Collection<R_elem<double>>>>& current_means) {

   _Map<R_key_value<int, R_count_sum<int, _Collection<R_elem<double>>>>> local_aggs;
   // Initialize local aggs
   for (int i =1; i<= k; i++) {
     R_count_sum<int, _Collection<R_elem<double>>> r;
     r.count = 0;
     r.sum = zero_vector(dimensionality);
     local_aggs.getContainer()[i] = r;
   }   

   for (const auto &r : data.getConstContainer()) {
     // Assign each data point to the closest mean.
     int which_k = nearest_neighbor(r.elem, current_means);
     // Update aggregates for this mean
     auto &agg = local_aggs.getContainer()[which_k];
     agg.count += 1;
     for (int i=0; i<dimensionality; i++) {
       agg.sum[i] += r.elem;
     }
   }

    auto d = make_shared<RefDispatcher<_Map<R_key_value<int, R_count_sum<int, _Collection<R_elem<double>>>>>>>(aggregate, local_aggs);
    engine.send(master,5,d);
    return unit_t();
}

unit_t aggregateShared(const shared_ptr<_Map<R_key_value<int, R_count_sum<int, _Collection<R_elem<double>>>>>> ags) {
  return aggregate(*ags);
}

unit_t aggregate(const _Map<R_key_value<int, R_count_sum<int, _Collection<R_elem<double>>>>> &ags) {

    merge_results(ags);
    requests = requests - 1;
    if (requests == 0) {

        auto d = make_shared<ValDispatcher<unit_t>>(maximize,unit_t());
        engine.send(master,4,d);return unit_t();
    } else {
        return unit_t();
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

        print_results(unit_t());

        return peers.iterate([] (R_addr<Address> p) -> unit_t {



            auto d = make_shared<ValDispatcher<unit_t>>(shutdown_,unit_t());
            engine.send(p.addr,1,d);return unit_t();
        });
    } else {
        aggregates = _Map<R_key_value<int, R_count_sum<int, _Collection<R_elem<double>>>>>();

        return peers.iterate([] (R_addr<Address> p) -> unit_t {
            requests = requests + 1;



            auto d = make_shared<RefDispatcher<_Collection<R_key_value<int, _Collection<R_elem<double>>>>>>
                     (assign, means);
            engine.send(p.addr,6,d);
            return unit_t();
        });
    }
}

unit_t ready(unit_t _) {
    num_peers = num_peers - 1;
    if (num_peers == 0) {

        start_ms = now(unit_t());



        auto d = make_shared<ValDispatcher<unit_t>>(start,unit_t());
        engine.send(master,2,d);return unit_t();
    } else {
        return unit_t();
    }
}

unit_t start(unit_t _) {
        int foo;

        int a = k;
        for (const auto &p : data.getConstContainer()) {
          if (a > 0) {
            means.insert(R_key_value<int, _Collection<R_elem<double>>>{a,p.elem});
            a = a - 1;
          }
        }
              
        for (const auto &peer : peers.getConstContainer()) {
          requests = requests + 1;
          auto d = make_shared<RefDispatcher<_Collection<R_key_value<int, _Collection<R_elem<double>>>>>>
            (assign ,means);
            engine.send(peer.addr,6,d);
        }

        return unit_t();
}


unit_t shutdown_(unit_t _) {

    return haltEngine(unit_t());
}

unit_t load_all(unit_t _) {


    LoaderVector(string(bpti_file))(data);



    auto d = make_shared<ValDispatcher<unit_t>>(ready,unit_t());
    engine.send(master,3,d);return unit_t();
}



unit_t pointsProcess(unit_t _) {

    return [] (unit_t next) -> unit_t {



        auto d = make_shared<ValDispatcher<unit_t>>(load_all,next);
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
    dispatch_table.resize(7);
    dispatch_table[0] = make_tuple(make_shared<ValDispatcher<unit_t>>(load_all), "load_all");
    dispatch_table[1] = make_tuple(make_shared<ValDispatcher<unit_t>>(shutdown_), "shutdown_");
    dispatch_table[2] = make_tuple(make_shared<ValDispatcher<unit_t>>(start), "start");
    dispatch_table[3] = make_tuple(make_shared<ValDispatcher<unit_t>>(ready), "ready");
    dispatch_table[4] = make_tuple(make_shared<ValDispatcher<unit_t>>(maximize), "maximize");
    dispatch_table[5] = make_tuple(make_shared<SharedDispatcher<_Map<R_key_value<int, R_count_sum<int, _Collection<R_elem<double>>>>>>>(aggregateShared), "aggregate");
    dispatch_table[6] = make_tuple(make_shared<SharedDispatcher<_Collection<R_key_value<int, _Collection<R_elem<double>>>>>>(assignShared), "assign");
}

map<string,string> show_globals() {
    map<string,string> result;
    result["bpti_file"] = bpti_file;
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
    result["aggregates"] = ([] (_Map<R_key_value<int, R_count_sum<int, _Collection<R_elem<double>>>>> coll) {
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
    result["elapsed_ms"] = to_string(elapsed_ms);
    result["end_ms"] = to_string(end_ms);
    result["start_ms"] = to_string(start_ms);
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
    matchers["elapsed_ms"] = [] (string _s) {do_patch(_s,elapsed_ms);};
    matchers["end_ms"] = [] (string _s) {do_patch(_s,end_ms);};
    matchers["start_ms"] = [] (string _s) {do_patch(_s,start_ms);};
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
    matchers["bpti_file"] = [] (string _s) {do_patch(_s,bpti_file);};
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
