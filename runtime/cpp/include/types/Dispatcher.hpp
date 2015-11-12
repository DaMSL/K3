#ifndef K3_DISPATCHER
#define K3_DISPATCHER

#include "Common.hpp"
#include "types/BaseString.hpp"

namespace K3 {
class Dispatcher {
 public:
  virtual ~Dispatcher();
  virtual void operator()() = 0;
  virtual base_string jsonify() const;
  #ifdef K3MESSAGETRACE
  Address source_;
  Address destination_;
  #endif
  #if defined(K3MESSAGETRACE) || defined(K3TRIGGERTIMES)
  TriggerID trigger_;
  #endif
};

class SentinelDispatcher : public Dispatcher {
  void operator()();
  base_string jsonify() const;
};

}  // namespace K3

#endif
