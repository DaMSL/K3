#ifndef K3_RUNTIME_LISTENER_H
#define K3_RUNTIME_LISTENER_H

#include <boost/shared_ptr.hpp>
#include <k3/runtime/Endpoint.hpp>

namespace K3 {
  
  using namespace boost;

  //------------
  // Listeners

  class Listener {
  public:
    void run() {}
    virtual void processor(shared_ptr<Endpoint> e) = 0;

    Identifier name;
    shared_ptr<EngineControl> engineSync;
    shared_ptr<Endpoint> endpoint;
  };

} 

#endif