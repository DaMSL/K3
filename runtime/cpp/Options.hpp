#ifndef K3_RUNTIME_OPTIONS_H
#define K3_RUNTIME_OPTIONS_H

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

    std::vector<std::string> peer_strings;
    bool simulation = false;
    string log_level = "";
    bool directory_master;
    string directory_upstream;
    string name;
};

}

namespace po = boost::program_options;

#endif /* K3_RUNTIME_OPTIONS_H */
