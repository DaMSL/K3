#include <iostream>
#include <vector>
#include <string>

#include "Options.hpp"

namespace K3 {

int Options::parse(int argc, const char *const argv[]) {
  po::options_description desc("Allowed options");
  desc.add_options()
    ("help,h", "produce help message")
    ("peer,p", po::value<vector<string> >(), "variables to set in peer (required)")
    ("log_level,l", po::value<int>(), "Engine log level");

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

  if (vm.count("log level")) {
    log_level_ = vm["log level"].as<int>();
  } else {
    log_level_ = 10;
  }

  return 0;
}

}  // namespace K3
