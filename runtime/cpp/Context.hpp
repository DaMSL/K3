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
    __k3_context(Engine& e, map<string, string> b): __engine(e), __patch(b) {}

    virtual void __dispatch(int, void *) = 0;
    virtual std::map<std::string, std::string> __prettify() = 0;
    virtual void __patch(map<string, string>) = 0;

   protected:
    Engine& __engine;
  };
}
#endif
