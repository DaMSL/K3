#ifndef K3_RUNTIME_LITERALS_H
#define K3_RUNTIME_LITERALS_H

#include <list>
#include <map>
#include <memory>
#include <string>
#include <tuple>
#include <vector>

#include "boost/asio.hpp"
#include "boost/fusion/include/std_pair.hpp"
#include "boost/spirit/include/qi.hpp"

namespace K3 {
  namespace qi = boost::spirit::qi;


  template <class iterator>
  class shallow: public qi::grammar<iterator, qi::space_type, string()> {
   public:
    shallow(): shallow::base_type(start) {
      start = qi::raw[indirection | option | angles | braces | brackets | parens | quotes | other];

      indirection = qi::lit("ind") >> start;
      option = qi::lit("none") | qi::lit("some") >> start;

      angles = '<' >> *(qi::char_ - '>') >> '>';
      braces = '{' >> *(qi::char_ - '}') >> '}';
      brackets = '[' >> *(qi::char_ - ']') >> ']';
      parens = '(' >> *(qi::char_ - ')') >> ')';
      quotes = '"' >> *(escape - '"') >> '"';
      other = *(qi::char_ - ',');

      escape = '\\' >> qi::char_ | qi::char_;
    }

   private:
    qi::rule<iterator, qi::space_type, string()> start;

    qi::rule<iterator, qi::space_type> indirection;
    qi::rule<iterator, qi::space_type> option;

    qi::rule<iterator, qi::space_type> angles;
    qi::rule<iterator, qi::space_type> braces;
    qi::rule<iterator, qi::space_type> brackets;
    qi::rule<iterator, qi::space_type> parens;
    qi::rule<iterator, qi::space_type> quotes;
    qi::rule<iterator, qi::space_type> other;

    qi::rule<iterator, qi::space_type> escape;
  };

  template <class iterator>
  class literal: public qi::grammar<iterator, map<string, string>(), qi::space_type> {
   public:
    literal(): literal::base_type(start) {

      start = binding % ',';
      binding = key >> ':' >> value;

      key = qi::char_("a-zA-Z_") >> *qi::char_("a-zA-Z0-9_");
    }

   private:
    qi::rule<iterator, map<string, string>(), qi::space_type> start;
    qi::rule<iterator, pair<string, string>(), qi::space_type> binding;
    qi::rule<iterator, string(), qi::space_type> key;
    shallow<iterator> value;
  };

  // Built-in literal patchers.

  template <class T> struct patcher;
  template <class T, size_t i, size_t n> struct tuple_patcher;

  template <class T> struct patcher {
    static void patch(string, T&);
  };

  template <> struct patcher<bool> {
    static void patch(string s, bool& b) {
      qi::parse(begin(s), end(s), qi::bool_[([&b] (bool q) { b = q; })]);
    }
  };

  template <> struct patcher<int> {
    static void patch(string s, int& i) {
      qi::parse(begin(s), end(s), qi::int_[([&i] (int j) { i = j; })]);
    }
  };

  template <> struct patcher<double> {
    static void patch(string s, double& d) {
      qi::parse(begin(s), end(s), qi::double_[([&d] (double f) { d = f; })]);
    }
  };

  template <> struct patcher<string> {
    static void patch(string s, string& t) {
      string r;

      if (qi::parse(begin(s), end(s), ('"' >> *(('\\' >> qi::char_) | (qi::char_ - '"')) >> '"'), r)) {
        t = r;
      }
    }
  };

  template <> struct patcher<unsigned short> {
    static void patch(string s, unsigned short& u) {
      qi::parse(std::begin(s), std::end(s), qi::ushort_, u);
    }
  };

  template <> struct patcher<boost::asio::ip::address> {
    static void patch(string s, boost::asio::ip::address& a) {
      a = boost::asio::ip::address::from_string(s);
    }
  };

  template <class T> struct patcher<shared_ptr<T>> {
    static void patch(string s, shared_ptr<T> p) {
      shallow<string::iterator> _shallow;

      shared_ptr<T> tp = make_shared<T>(T());

      qi::rule<string::iterator, qi::space_type> ind_rule
        = qi::lit("ind") >> _shallow[([&tp] (string t) { patcher<T>::patch(t, *tp); })];
      qi::rule<string::iterator, qi::space_type> opt_rule
        = qi::lit("none")[([&p, &tp] () { tp = p = nullptr; })]
        | qi::lit("some") >> _shallow[([&tp] (string t) { patcher<T>::patch(t, *tp); })];

      qi::phrase_parse(begin(s), end(s), ind_rule | opt_rule, qi::space);

      if (tp) {
        p = tp;
      }
    }
  };

  template <class T> struct patcher<shared_ptr<list<T>>> {
    static void patch(string s, shared_ptr<list<T>>& p) {
      shallow<string::iterator> _shallow;
      list<T> lp;
      qi::rule<string::iterator, qi::space_type> item_rule
        = _shallow[([&lp] (string t) { lp.push_back(T()); patcher<T>::patch(t, lp.back()); })];

      qi::rule<string::iterator, qi::space_type> list_rule = '[' >> (item_rule % ',') >> ']';

      if (qi::phrase_parse(begin(s), end(s), list_rule, qi::space)) {
        p = make_shared<list<T>>(lp);
      }
    }
  };

  template <class ... Ts> struct patcher<tuple<Ts...>> {
    static void patch(string s, tuple<Ts...>& t) {
      list<string> v;
      shallow<string::iterator> _shallow;

      qi::rule<string::iterator, qi::space_type, list<string>()> tuple_rule = '(' >> (_shallow % ',') >> ')';

      qi::rule<string::iterator, qi::space_type, string()> ip_rule = qi::raw[(qi::int_ % '.')];
      qi::rule<string::iterator, qi::space_type, string()> port_rule = qi::raw[qi::int_];
      qi::rule<string::iterator, qi::space_type, list<string>()> address_rule
        = '<' >> ip_rule >> ':' >> port_rule >> '>';

      qi::phrase_parse(begin(s), end(s), tuple_rule | address_rule, qi::space, v);
      tuple_patcher<tuple<Ts...>, 0, sizeof...(Ts)>::patch(v, t);
    }
  };

  template <class T, size_t i> struct tuple_patcher<T, i, i> {
    static void patch(list<string>&, T&) {}
  };

  template <class T, size_t i, size_t n> struct tuple_patcher {
    static void patch(list<string>& v, T& t) {
      if (v.empty()) {
        return;
      }

      patcher<typename std::tuple_element<i, T>::type>::patch(v.front(), std::get<i>(t));

      v.pop_front();
      tuple_patcher<T, i + 1, n>::patch(v, t);
    }
  };

  template <template <class> class C, class E> struct collection_patcher {
    static void patch(string s, C<E>& c) {
      shared_ptr<list<E>> tmp_dsp = nullptr;
      patcher<shared_ptr<list<E>>>::patch(s, tmp_dsp);

      if (tmp_dsp) {
        C<E> new_c;
        for (E e: *tmp_dsp) {
          new_c.insert(e);
        }

        c = new_c;
      }
    }
  };

  map<string, string> parse_bindings(string s) {
    map<string, string> bindings;
    literal<string::iterator> parser;
    qi::phrase_parse(begin(s), end(s), parser, qi::space, bindings);
    return bindings;
  }

  template <class T>
  void do_patch(string s, T& t) {
    patcher<T>::patch(s, t);
  }

  void match_patchers(map<string, string>& m, map<string, function<void(string)>>& f) {
    for (pair<string, string> p: m) {
      if (f.find(p.first) != end(f)) {
        f[p.first](p.second);
      }
    }
  }

  string preprocess_argv(int argc, char** argv) {
    if (argc < 2)
      return "";

    return argv[1];
  }
}

#endif
