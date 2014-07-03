#ifndef K3_RUNTIME_DISPATCH_H
#define K3_RUNTIME_DISPATCH_H

#include <functional>
#include <map>
#include <string>

#include "Common.hpp"

namespace K3 {
  
    //------------
    // Dispatcher class
    //
    // Every trigger type must have a Dispatcher that can be inserted
    // in a queue
    class Dispatcher {
      public:
        virtual void dispatch();
        virtual string& pack(); 
        virtual void unpack(string &msg); 
    }

    // A TriggerDispatch table maps trigger names to the corresponding generated TriggerWrapper
    // function.
    using TriggerDispatch = std::map<Identifier, Dispatcher>;
}
#endif

