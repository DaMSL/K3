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

  using boost::asio::ip::address;

  using std::begin;
  using std::end;
  using std::function;
  using std::list;
  using std::make_shared;
  using std::map;
  using std::pair;
  using std::shared_ptr;
  using std::string;
  using std::tuple;
  using std::vector;

  template <class iterator>
  class shallow: public qi::grammar<iterator, qi::space_type, string()> {
   public:
    shallow(): shallow::base_type(start) {
      start = qi::raw[indirection | option | angles | braces | parens | quotes | other];

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

  template <> void refresh<bool>(string s, bool& b) {
    qi::parse(begin(s), end(s), qi::bool_[([&b] (bool b_) { b = b_; })]);
  }

  template <> void refresh<int>(string s, int& i) {
    qi::parse(begin(s), end(s), qi::int_[([&i] (int i_) { i = i_; })]);
  }

  template <> void refresh<double>(string s, double& d) {
    qi::parse(begin(s), end(s), qi::double_[([&d] (double d_) { d = d_; })]);
  }

  template <> void refresh<string>(string s, string& t) {
    string t_;
    bool result = qi::parse(begin(s), end(s), ('"' >> *(('\\' >> qi::char_) | (qi::char_ - '"')) >> '"'), t_);

    if (result) {
      t = t_;
    }
  }

  template <class T> void refresh(string s, shared_ptr<T>& p) {
    shallow<string::iterator> _shallow;
    qi::rule<string::iterator, qi::space_type> raw_rule = _shallow[([&p] (string s_) {
      if (!p) {
        p = make_shared<T>();
      }

      refresh(s_, *p);
    })];
    qi::rule<string::iterator, qi::space_type> nullptr_rule = qi::lit("none")[([&p] () { p = nullptr; })];
    qi::rule<string::iterator, qi::space_type> someptr_rule = (qi::lit("some") | qi::lit("ind")) >> raw_rule;
    qi::phrase_parse(begin(s), end(s), nullptr_rule | someptr_rule, qi::space);
  }

  template <> void refresh<address>(string s, address& a) {
    a = address::from_string(s);
  }

  template <> void refresh<unsigned short>(string s, unsigned short& i) {
    qi::parse(begin(s), end(s), qi::ushort_, i);
  }

  template <class ... Ts> void refresh(string s, tuple<Ts...>& t) {
    list<string> v;
    shallow<string::iterator> _shallow;

    qi::rule<string::iterator, qi::space_type, list<string>()> tuple_rule = '(' >> (_shallow % ',') >> ')';

    qi::rule<string::iterator, qi::space_type, string()> ip_rule = qi::raw[(qi::int_ % '.')];
    qi::rule<string::iterator, qi::space_type, string()> port_rule = qi::raw[qi::int_];
    qi::rule<string::iterator, qi::space_type, list<string>()> address_rule
      = '<' >> ip_rule >> ':' >> port_rule >> '>';

    qi::phrase_parse(begin(s), end(s), tuple_rule | address_rule, qi::space, v);
    refresh_many<tuple<Ts...>, 0, sizeof...(Ts)>()(v, t);
  }

  template <class T, size_t i> struct refresh_many<T, i, i> {
    void operator()(list<string>&, T&) {}
  };

  template <class T, size_t i, size_t n> struct refresh_many {
    void operator()(list<string>& v, T& t) {
      if (v.empty()) {
        return;
      }

      refresh<typename std::tuple_element<i, T>::type>(v.front(), std::get<i>(t));

      v.pop_front();
      refresh_many<T, i + 1, n>()(v, t);
    }
  };

  map<string, string> parse_bindings(string s) {
    map<string, string> bindings;
    literal<string::iterator> parser;
    qi::phrase_parse(begin(s), end(s), parser, qi::space, bindings);
    return bindings;
  }

  void consolidate_refreshments(map<string, string>& m, map<string, function<void(string)>>& f) {
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
