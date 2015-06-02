#include <chrono>

#include "Common.hpp"

namespace K3 {

  int time_milli() {
    auto t = std::chrono::system_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(t.time_since_epoch());
    return elapsed.count();
  }

}
