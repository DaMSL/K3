#ifndef K3_TIMEBUILTINS
#define K3_TIMEBUILTINS

#include "Common.hpp"

namespace K3 {

class TimeBuiltins {
 public:
  TimeBuiltins();
  int now_int(const unit_t&);
};

}  // namespace K3
#endif
