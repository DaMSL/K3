#ifndef K3_OPTIONS
#define K3_OPTIONS

#include <string>
#include <sstream>
#include <vector>
#include <boost/program_options.hpp>

using std::string;
using std::vector;

namespace K3 {

class Options {
 public:
  int parse(int argc, const char *const argv[]);

  vector<string> peer_strs_;
};
}
namespace po = boost::program_options;

#endif
