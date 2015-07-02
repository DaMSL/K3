#ifndef K3_DISPATCHER
#define K3_DISPATCHER

#include "Common.hpp"

namespace K3 {
class Dispatcher {
 public:
  virtual void operator()() = 0;
};

class SentinelDispatcher : public Dispatcher {
  void operator()(); 
};

}  // namespace K3

#endif
