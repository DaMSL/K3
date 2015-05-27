#include <time.h>
#include <chrono>

#include "builtins/TimeBuiltins.hpp"

namespace K3 {

TimeBuiltins::TimeBuiltins() {}

int TimeBuiltins::now_int(const unit_t&) {
  auto t = std::chrono::system_clock::now();
  auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
      t.time_since_epoch());
  return elapsed.count();
}

}  // namespace K3
