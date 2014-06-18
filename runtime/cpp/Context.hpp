#ifndef K3_RUNTIME_CONTEXT_H
#define K3_RUNTIME_CONTEXT_H

#include <map>
#include <string>

#include "Engine.hpp"

namespace K3 {
  using std::map;
  using std::string;

  class __k3_context {
   public:
    __k3_context(Engine& e): __engine(e) {}

    virtual void __dispatch(string) = 0;
    virtual map<string, string> __prettify() = 0;
    virtual void __patch(string) = 0;

   protected:
    Engine& __engine;
  };
}
#endif
