#include <iostream>
#include "Options.hpp"

namespace K3 {

static constexpr char NAME[]                = "name";
static constexpr char DIRECTORY_MASTER[]    = "directory_master";
static constexpr char DIRECTORY_UPSTREAM[]  = "directory_upstream";

int Options::parse(int argc, const char *const argv[]) {
  po::options_description desc("Allowed options");
  desc.add_options()
    ("help,h", "produce help message")
    ("simulation,s", "run in simulation mode")
    ("log,l",po::value<string>(), "log level")
    ("name", po::value<string>(), "unique name for this peer (required)")
    (DIRECTORY_MASTER, "host the master of the peer directory service (one of directory_master or directory_upstream is required.)")
    (DIRECTORY_UPSTREAM, po::value<string>(), "address and port of an instance of the directory service (one of directory_master or directory_upstream is required.)")
    ;

  po::variables_map vm;
  po::store(po::parse_command_line(argc, argv, desc), vm);
  po::notify(vm);

  if (vm.count("help") || vm.empty()) {
    std::cout << desc << "\n";
    return 1;
  }
  if (vm.count("simulation")) {
    simulation = true;
  } else {
    simulation = false;
  }
  if (vm.count("log")) {
    log_level = vm["log"].as<string>();
    std::cout << "log:" << log_level << std::endl;
  } else {
    log_level = "";
    std::cout << "logging disabled" << std::endl;
  }
  if (vm.count(NAME)) {
    name = vm[NAME].as<string>();
  } else {
    std::cout << desc << "\n";
    return 1;
  }
  if (vm.count(DIRECTORY_MASTER)) {
      directory_master = true;
      if (vm.count(DIRECTORY_UPSTREAM)) {
          std::cout << desc << "\n";
          return 1;
      }
  } else {
      directory_master = false;
      if (vm.count(DIRECTORY_UPSTREAM)) {
        directory_upstream = vm[DIRECTORY_UPSTREAM].as<string>();
      } else {
          std::cout << desc << "\n";
          return 1;
      }
  }

  return 0;
}

}
