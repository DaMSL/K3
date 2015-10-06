#ifndef K3_OPTIONS
#define K3_OPTIONS

#include <string>
#include <sstream>
#include <vector>

#include "boost/program_options.hpp"

using std::string;
using std::vector;

namespace K3 {

class JSONOptions {
 public:
  JSONOptions() {
    final_state_only_ = false;
    output_folder_ = "";
    globals_regex_ = "";
    messages_regex_ = "";
  }

  bool final_state_only_;
  string output_folder_;
  string globals_regex_;
  string messages_regex_;
};

class Options {
 public:
  Options() {
    log_level_ = 0;
    local_sends_enabled_ = true;
  }

  int parse(int argc, const char* const argv[]);

  vector<string> peer_strs_;
  int log_level_;
  bool local_sends_enabled_;
  JSONOptions json_;
};
}  // namespace K3

#endif
