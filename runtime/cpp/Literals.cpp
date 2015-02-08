#include <map>
#include <string>
#include <functional>
#include "boost/fusion/include/std_pair.hpp"
#include "Literals.hpp"

namespace K3 {
  using std::string;
  using std::map;
  using std::pair;

  map<string, string> parse_bindings(string s) {
    map<string, string> bindings;
    literal<string::iterator> parser;
    qi::phrase_parse(begin(s), end(s), parser, qi::space, bindings);
    return bindings;
  }

  void match_patchers(map<string, string>& m, map<string, std::function<void(string)>>& f) {
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
