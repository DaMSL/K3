#include <iostream>
#include "Options.hpp"
#include <sys/types.h>
#include <sys/stat.h>

namespace K3 {

int Options::parse(int argc, const char *const argv[]) {
  po::options_description desc("Allowed options");
  desc.add_options()
    ("help,h", "produce help message")
    ("peer,p", po::value< vector<string> >(), "variables to set in peer (required)")
    ("simulation,s", "run in simulation mode")
    ("log,l",po::value<string>(), "log directory (required to enable logging)")
    ("result_var,r", po::value<string>(), "result variable to log (must be a flat collection)")
    ("result_path,o", po::value<string>(), "path to store the result variable");

  po::variables_map vm;
  po::store(po::parse_command_line(argc, argv, desc), vm);
  po::notify(vm);

  if (vm.count("help") || vm.empty()) {
    std::cout << desc << "\n";
    return 1;
  }
  if (vm.count("peer")) {
    peer_strings = vm["peer"].as<vector<string> >();
  } else { // required
    std::cout << desc << "\n";
    return 1;
  }
  if (vm.count("simulation")) {
    simulation = true;
  } else {
    simulation = false;
  }

  if (vm.count("log")) {
    log_path = vm["log"].as<string>();
    struct stat info;
    if( stat( log_path.c_str(), &info ) != 0 ) {
      throw std::runtime_error("Log directory does not exist: " + log_path);
    }
  } else {
    log_path = "";
  }

  if (vm.count("result_var")) {
    result_var = vm["result_var"].as<string>();
  } else {
    result_var = "";
  }

  if (vm.count("result_path")) {
    if(result_var == "") {
      throw std::runtime_error("Invalid options: Cannot specify result_path without result_var" );
    }

    result_path= vm["result_path"].as<string>();
    struct stat info;
    if( stat( result_path.c_str(), &info ) != 0 ) {
      throw std::runtime_error("Result directory does not exist: " + result_path);
    }

  } else {
    result_path  = "";
  }

  return 0;
}

}
