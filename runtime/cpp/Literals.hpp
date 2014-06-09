#ifndef K3_RUNTIME_LITERALS_H
#define K3_RUNTIME_LITERALS_H

#include <list>
#include <map>
#include <memory>
#include <string>
#include <tuple>
#include <vector>

#include "boost/fusion/include/std_pair.hpp"
#include "boost/spirit/include/qi.hpp"

namespace K3 {
  namespace qi = boost::spirit::qi;

  using std::list;
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
    qi::rule<iterator, qi::space_type> parens;
    qi::rule<iterator, qi::space_type> quotes;
    qi::rule<iterator, qi::space_type> other;

    qi::rule<iterator, qi::space_type> escape;
  };

  template <class iterator>
  class literal: public qi::grammar<iterator, map<string, string>(), qi::space_type> {
   public:
    literal(): literal::base_type(start) {

      start = binding % ';';
      binding = key >> '=' >> value;

      key = qi::char_("a-zA-Z_") >> *qi::char_("a-zA-Z0-9_");
      value = shallow<iterator>();
    }

   private:
    qi::rule<iterator, map<string, string>(), qi::space_type> start;
    qi::rule<iterator, pair<string, string>(), qi::space_type> binding;
    qi::rule<iterator, string(), qi::space_type> key;
    qi::rule<iterator, string(), qi::space_type> value;
  };

  // Built-in type refresh overloads.

  template <class T> void refresh(string, T&);
  template <class T, size_t i, size_t n> struct refresh_many;

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
    qi::parse(
      begin(s), end(s),
      ('"' >> *('\\' >> qi::char_ | qi::char_) >> '"')
        [([&t] (std::vector<char> t_) { t = string(begin(t_), end(t_)); })]
    );
  }

  template <class T> void refresh(string s, shared_ptr<T> p) {
    shallow<string::iterator> _shallow;
    qi::rule<string::iterator, qi::space_type> raw_rule = _shallow[([&p] (string s_) { refresh(s_, *p); })];
    qi::rule<string::iterator, qi::space_type> nullptr_rule = qi::lit("none")[([&p] () { p = nullptr; })];
    qi::rule<string::iterator, qi::space_type> someptr_rule = (qi::lit("some") | qi::lit("ind")) >> raw_rule;
    qi::phrase_parse(begin(s), end(s), nullptr_rule | someptr_rule, qi::space);
  }


    qi::rule<iterator, string()> parse_string;
    qi::rule<iterator, string(), qi::space_type> parse_indirection;
    qi::rule<iterator, string(), qi::space_type> parse_option;
    qi::rule<iterator, string(), qi::space_type> parse_tuple;
    qi::rule<iterator, string(), qi::space_type> parse_record;

    qi::rule<iterator, string()> parse_address;
    qi::rule<iterator, string()> parse_host;
    qi::rule<iterator, string()> parse_port;

    qi::rule<iterator, string(), qi::space_type> parse_collection;
  };
}

#endif
