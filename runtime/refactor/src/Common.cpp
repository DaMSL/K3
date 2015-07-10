#include "Common.hpp"

namespace K3 {
IOMode getIOMode(const string& s) {
  if (s == "r") {
    return IOMode::Read;
  }
  if (s == "w") {
    return IOMode::Write;
  }
  throw std::runtime_error("Unrecognized IO Mode string: " + s);
}
}
