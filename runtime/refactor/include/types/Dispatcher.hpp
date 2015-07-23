#ifndef K3_DISPATCHER
#define K3_DISPATCHER

#include "Common.hpp"
#include "types/BaseString.hpp"

namespace K3 {
class Dispatcher {
 public:
  virtual void operator()() = 0;
  virtual base_string jsonify() const;
#ifdef K3DEBUG
  Address source_;
  Address destination_;
  TriggerID trigger_;
#endif
};

class SentinelDispatcher : public Dispatcher {
  void operator()();
  base_string jsonify() const;
};

}  // namespace K3

#endif
