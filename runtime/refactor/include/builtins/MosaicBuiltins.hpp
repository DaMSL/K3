#ifndef K3_MOSAICBUILTINS
#define K3_MOSAICBUILTINS

#include <climits>
#include <boost/thread/mutex.hpp>
#include <boost/thread/thread.hpp>

#include "types/BaseString.hpp"
#include "Common.hpp"

namespace K3 {

class MosaicBuiltins {
  template <C>
  C max(C a, C b) {
    return (a > b ? a : b);
  }

  template <class C>
  C min(C a, C b) {
    return (a < b ? a : b);
  }

  int date_part(const string_impl& s, int y) {
    if (s == "day" || s == "DAY") {
      return y % 100;
    }
    if (s == "month" || s == "MONTH") {
      return (y % 10000) / 100;
    }
    if (s == "year" || s == "YEAR") {
      return (y / 10000);
    }
    throw std::runtime_error("Unrecognized date part key: " + s);
  }
  double real_of_int(int n) { return (double)n; }
  int get_max_int(unit_t) { return INT_MAX; }
  int truncate(double n) { return (int)n; }
};

}  // namespace K3

#endif
