#ifndef K3_MOSAICBUILTINS
#define K3_MOSAICBUILTINS

#include <climits>
#include <boost/thread/mutex.hpp>
#include <boost/thread/thread.hpp>

#include "types/BaseString.hpp"
#include "Common.hpp"

namespace K3 {

class MosaicBuiltins {
 public:
  template <class A, class B>
  auto max(A a, B b) {
    return (a > b ? a : b);
  }

  template <class A, class B>
  auto min(A a, B b) {
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

  double real_of_int(int n) { return static_cast<double>(n); }
  int get_max_int(unit_t) { return INT_MAX; }
  int truncate(double n) { return static_cast<int>(n); }
  int regex_match_int(const string_impl& s1, const string_impl& s2) {
    bool b = std::regex_match(s2.c_str(), std::regex(s1.c_str()));
    return b ? 1 : 0;
  }
};

}  // namespace K3

#endif
