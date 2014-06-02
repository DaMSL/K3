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
      key = +qi::char_("a-zA-Z0-9_");
      value = +qi::char_("a-zA-Z0-9_");
      assignment = key >> '=' >> value;
      start = assignment % ';';
    }

   private:
    qi::rule<iterator, string()> key;
    qi::rule<iterator, string()> value;
    qi::rule<iterator, pair<string, string>(), qi::space_type> assignment;
    qi::rule<iterator, map<string, string>(), qi::space_type> start;
  };
}

#endif
