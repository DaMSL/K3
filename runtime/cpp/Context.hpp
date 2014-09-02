#ifndef K3_RUNTIME_CONTEXT_H
#define K3_RUNTIME_CONTEXT_H

#include <map>
#include <string>

#include "Dispatch.hpp"

namespace K3 {

  class Engine;

  class __k3_context {
   public:
    __k3_context(Engine& e): __engine(e) {}

    virtual void __dispatch(std::shared_ptr<Dispatcher>) = 0;
    virtual std::map<std::string, std::string> __prettify() = 0;
    virtual void __patch(std::string) = 0;

   protected:
    Engine& __engine;
  };
}
#endif
