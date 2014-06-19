#include "Options.hpp"

namespace K3 {

int Options::parse(int argc, const char *const argv[]) {
  po::options_description desc("Allowed options");
  desc.add_options()
    ("help,h", "produce help message")
    ("peer,p", po::value< vector<string> >(), "variables to set in peer (required)")
    ("simulation,s", "run in simulation mode");

  po::variables_map vm;
  po::store(po::parse_command_line(argc, argv, desc), vm);
  po::notify(vm);

  if (vm.count("help") || vm.empty()) {
    std::cout << desc << "\n";
    return 1;
  }
  if (vm.count("peer")) { 
    peer_strings = vm["peer"].as<vector<string>>();
  } else { // required
    std::cout << desc << "\n";
    return 1;
  }
  if (vm.count("simulation")) {
    simulation = true;
  }
  if (vm.count("log_level")) {
    log_level = vm["log_level"].as<string>();
  }

  return 0;
}

}
