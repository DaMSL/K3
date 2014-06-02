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
      key = qi::char_("a-zA-Z_") >> *qi::char_("a-zA-Z0-9_");
      value = quoted_string | +qi::char_("a-zA-Z0-9_");
      assignment = key >> '=' >> value;
      start = assignment % ';';

      escape = (qi::lit('\\') >> qi::char_) | qi::char_;
      quoted_string = qi::lit('"') >> *(escape - '"') >> qi::lit('"');
    }

   private:
    qi::rule<iterator, string(), qi::space_type> key;
    qi::rule<iterator, string(), qi::space_type> value;
    qi::rule<iterator, pair<string, string>(), qi::space_type> assignment;
    qi::rule<iterator, map<string, string>(), qi::space_type> start;

    qi::rule<iterator, string()> quoted_string;
    qi::rule<iterator, char()> escape;
  };
}

#endif
