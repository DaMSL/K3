#include <iostream>
#include <sys/types.h>
#include <sys/stat.h>

#include "Options.hpp"

namespace K3 {

int Options::parse(int argc, const char *const argv[]) {
  po::options_description desc("Allowed options");
  desc.add_options()
    ("help,h", "produce help message")
    ("peer,p", po::value< vector<string> >(), "variables to set in peer (required)")
    ("simulation,s", "run in simulation mode")
    ("log,l",po::value<string>(), "log level for stdout")
    ("json,j",po::value<string>(), "directory for json logging")
    ("result_var,r", po::value<string>(), "result variable to log (must be a flat collection)")
    ("result_path,o", po::value<string>(), "path to store the result variable")
    ("web_port,w", po::value<int>(), "webserver port")
    ("web_data_port", po::value<int>(), "webserver data port");

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

  if (vm.count("json")) {
    json_path = vm["json"].as<string>();
    struct stat info;
    if( stat( json_path.c_str(), &info ) != 0 ) {
      throw std::runtime_error("JSON Log directory does not exist: " + json_path);
    }
  }

  if (vm.count("log")) {
    log_level = vm["log"].as<string>();
  } else {
    log_level = "";
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

    result_path = vm["result_path"].as<string>();
    struct stat info;
    if( stat( result_path.c_str(), &info ) != 0 ) {
      throw std::runtime_error("Result directory does not exist: " + result_path);
    }

  } else {
    result_path  = "";
  }

  if (vm.count("web_port")) {
    web_server = true;
    web_port = vm["web_port"].as<int>();
  } else {
    web_server = false;
    web_port = -1;
  }

  if (web_server && vm.count("web_data_port")) {
    web_data_port = vm["web_data_port"].as<int>();
  } else {
    web_data_port = -1;
  }

  return 0;
}

}
