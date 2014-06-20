#ifndef K3_RUNTIME_CONTEXT_H
#define K3_RUNTIME_CONTEXT_H

#include <map>
#include <string>

namespace K3 {

  class Engine;

  class __k3_context {
   public:
    __k3_context(Engine& e): __engine(e) {}

    virtual void __dispatch(std::string) {}
    virtual std::map<std::string, std::string> __prettify() { return std::map<std::string, std::string> {}; }
    virtual void __patch(std::string) {}

   protected:
    Engine& __engine;
  };
}
#endif
