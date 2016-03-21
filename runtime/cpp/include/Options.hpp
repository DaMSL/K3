#ifndef K3_OPTIONS
#define K3_OPTIONS

#include <string>
#include <sstream>
#include <vector>

#include "boost/program_options.hpp"

namespace K3 {

using std::string;
using std::vector;

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
    // default options
    log_level_ = 0;
    local_sends_enabled_ = true;
    num_threads_ = 4;
    profile_interval_ = 250;
  }

  int parse(int argc, const char* const argv[]);

  vector<string> peer_strs_;
  int log_level_;
  bool local_sends_enabled_;
  JSONOptions json_;
  int num_threads_; // number of threads for networking
  int profile_interval_;  // ms to wait between profiling instances
};
}  // namespace K3

#endif
