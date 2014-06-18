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

    virtual void __dispatch(string) {}
    virtual map<string, string> __prettify() { return map<string, string> {}; }
    virtual void __patch(string) {}

   protected:
    Engine& __engine;
  };
}
#endif
