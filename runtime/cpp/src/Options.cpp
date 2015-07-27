#include <iostream>
#include <vector>
#include <string>

#include "Options.hpp"

namespace K3 {

int Options::parse(int argc, const char *const argv[]) {
  namespace po = boost::program_options;
  po::options_description desc("Allowed options");
  desc.add_options()("help,h", "produce help message")
      ("peer,p", po::value<vector<string> >(), "variables to set in peer (required)")
      ("log_level,l", po::value<int>(), "Engine log level: (1,2,3)")
      ("json,j", po::value<string>(), "Directory for json log files")
      ("json_final_only,f", "json log final state only")
      ("json_messages_regex,m", po::value<string>(),  "Regex to filter json messages based on destination trigger ")
      ("json_globals_regex,g", po::value<string>(), "Regex to filter json globals based on variable name")
      ("disable_local_messages,d", "force serialization of all messages)");

  po::variables_map vm;
  po::store(po::parse_command_line(argc, argv, desc), vm);
  po::notify(vm);

  if (vm.count("help") || vm.empty()) {
    std::cout << desc << "\n";
    return 1;
  }

  if (vm.count("peer")) {
    peer_strs_ = vm["peer"].as<vector<string> >();
  } else {  // required
    std::cout << desc << "\n";
    return 1;
  }

  if (vm.count("log_level")) {
    log_level_ = vm["log_level"].as<int>();
  } else {
    log_level_ = -1;
  }

  if (vm.count("json")) {
    json_folder_ = vm["json"].as<string>();
  } else {
    json_folder_ = "";
  }

  if (vm.count("json_final_only")) {
    json_final_state_only_ = true;
  } else {
    json_final_state_only_ = false;
  }

  if (vm.count("json_globals_regex")) {
    json_globals_regex_ = vm["json_globals_regex"].as<string>();
  } else {
    json_globals_regex_ = ".*";
  }

  if (vm.count("json_messages_regex")) {
    json_messages_regex_ = vm["json_messages_regex"].as<string>();
  } else {
    json_messages_regex_ = ".*";
  }

  if (vm.count("disable_local_messages")) {
    local_sends_enabled_ = false;
  } else {
    local_sends_enabled_ = true;
  }

  return 0;
}

}  // namespace K3
