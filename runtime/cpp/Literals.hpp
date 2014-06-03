#ifndef K3_RUNTIME_LITERALS_H
#define K3_RUNTIME_LITERALS_H

#include <map>
#include <string>

#include "boost/fusion/include/std_pair.hpp"
#include "boost/spirit/include/qi.hpp"

namespace K3 {
  namespace qi = boost::spirit::qi;

  using std::map;
  using std::pair;
  using std::string;

  template <class iterator>
  class literal: public qi::grammar<iterator, map<string, string>(), qi::space_type> {
   public:
    literal(): literal::base_type(start) {
      start = binding % ';';
      binding = key >> '=' >> value;

      key = qi::char_("a-zA-Z_") >> *qi::char_("a-zA-Z0-9_");
      value
        = qi::raw[qi::bool_]
        | qi::raw[qi::double_]
        | qi::raw[parse_string]
        | qi::raw[parse_indirection]
        | qi::raw[parse_option]
        | qi::raw[parse_tuple]
        | qi::raw[parse_record]
        | qi::raw[parse_address]
        | key;

      escape = (qi::lit('\\') >> qi::char_) | qi::char_;

      parse_string = qi::lit('"') >> *(escape - '"') >> qi::lit('"');
      parse_indirection = qi::string("ind") >> value;
      parse_option = qi::string("none") | (qi::string("some") >> value);
      parse_tuple = qi::lit('(') >> qi::raw[value % ','] >> qi::lit(')');
      parse_record = qi::lit('{') >> qi::raw[(key >> ':' >> value) % ','] >> qi::lit('}');

      parse_address = qi::raw['<' >> host >> ':' >> port >> '>'];
      host = qi::raw[qi::int_ >> '.' >> qi::int_ >> '.' >> qi::int_ >> '.' >> qi::int_];
      port = qi::raw[qi::int_];
    }

   private:
    qi::rule<iterator, map<string, string>(), qi::space_type> start;
    qi::rule<iterator, pair<string, string>(), qi::space_type> binding;
    qi::rule<iterator, string(), qi::space_type> key;
    qi::rule<iterator, string(), qi::space_type> value;

    qi::rule<iterator, char()> escape;

    qi::rule<iterator, string()> parse_string;
    qi::rule<iterator, string(), qi::space_type> parse_indirection;
    qi::rule<iterator, string(), qi::space_type> parse_option;
    qi::rule<iterator, string(), qi::space_type> parse_tuple;
    qi::rule<iterator, string(), qi::space_type> parse_record;

    qi::rule<iterator, string()> parse_address;
    qi::rule<iterator, string()> host;
    qi::rule<iterator, string()> port;
  };
}

#endif
