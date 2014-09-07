#ifndef K3_RUNTIME_CONTEXT_H
#define K3_RUNTIME_CONTEXT_H

#include <map>
#include <string>

#include "Dispatch.hpp"
#include "Literals.hpp"

namespace K3 {

  class Engine;

  class __k3_context {
   public:
    __k3_context(Engine& e): __engine(e) {}
    //__k3_context(Engine& e, std::map<string, string> b): __engine(e) {__patch(b)}

    virtual void __dispatch(int, void *) = 0;
    virtual std::map<std::string, std::string> __prettify() = 0;
    virtual void __patch(std::map<string, string>) = 0;

   protected:
    Engine& __engine;
  };

  template<class Context>
  std::map<Address, shared_ptr<__k3_context>> createContexts(std::vector<string> peer_strs, const Engine& engine) {
    std::map<Address, shared_ptr<__k3_context>> contexts;
    for (auto &s : peer_strs) {
      std::map<std::string, std::string> bindings = parse_bindings(s);
      Context gc = Context(engine);
      match_patchers(bindings, gc.matchers);
      // TODO: call __patch instead?
      contexts[gc.me] = make_shared<__k3_context>(gc);
    }
    return contexts;
  }
  std::list<Address> getAddrs(std::map<Address, shared_ptr<__k3_context>> contexts) {
    std::list<Address> result;
    for (const auto& it: contexts) {
      result.push_back(it.first);
    }
    return result;
  }

}



#endif
