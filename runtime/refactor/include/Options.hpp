#ifndef K3_OPTIONS
#define K3_OPTIONS

#include <string>
#include <sstream>
#include <vector>

#include "boost/program_options.hpp"

using std::string;
using std::vector;

namespace K3 {

class Options {
 public:
  Options() { }
  Options(const vector<string>& strs, int log_level, const string& json_folder, bool json_final) {
    peer_strs_ = strs;
    log_level_ = log_level;
    json_folder_ = json_folder;
    json_final_state_only_ = json_final;
  }
  int parse(int argc, const char *const argv[]);

  vector<string> peer_strs_;
  int log_level_;
  string json_folder_;
  bool json_final_state_only_;
};
}  // namespace K3

#endif
