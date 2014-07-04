#ifndef K3_RUNTIME_DISPATCH_H
#define K3_RUNTIME_DISPATCH_H

#include <map>
#include <string>
#include <memory>

#include "Common.hpp"

namespace K3 {
  
    //------------
    // Dispatcher class
    //
    // Every trigger type must have a Dispatcher that can be inserted
    // in a queue
    class Dispatcher {
      public:
        virtual void dispatch() const;
        virtual std::shared_ptr<std::string> pack() const; 
        virtual void unpack(const std::string &msg); 
    };

    // A TriggerDispatch table maps trigger names to the corresponding generated TriggerWrapper
    // function.
    using TriggerDispatch = std::map<Identifier, std::shared_ptr<Dispatcher> >;

    // Forward declare the global
    TriggerDispatch dispatch_table;

} // namespace K3
#endif

