#ifndef K3_RUNTIME_OPTIONS_H
#define K3_RUNTIME_OPTIONS_H

#include <string>
#include <sstream>
#include <vector>
#include <boost/program_options.hpp>

using std::string;
using std::vector;

class Options {
  public:
    int parse(int argc, const char *const argv[]);

    vector<string> peer_strings;
    bool simulation = false;
    string log_level = "";
};

namespace po = boost::program_options;

int Options::parse(int argc, const char *const argv[]) {
  po::options_description desc("Allowed options");
  desc.add_options()
    ("help,h", "produce help message")
    ("peer,p", po::value< vector<string> >(), "variables to set in peer")
    ("simulation,s", "run in simulation mode");

  po::variables_map vm;
  po::store(po::parse_command_line(argc, argv, desc), vm);
  po::notify(vm);

  if (vm.count("help")) {
    std::cout << desc << "\n";
  }
  else {
    if (vm.count("peer"))
      peer_strings = vm["peer"].as<vector<string>>();
    if (vm.count("simulation"))
      simulation = true;
    if (vm.count("log_level"))
      log_level = vm["log_level"].as<string>();
  }

  return 1;
}

#endif /* K3_RUNTIME_OPTIONS_H */

