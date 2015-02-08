#ifndef K3_RUNTIME_CONTEXT_H
#define K3_RUNTIME_CONTEXT_H

#include <map>
#include <string>

#include "Literals.hpp"

namespace K3 {

  class Engine;
  class Dispatcher;

  class __k3_context {
   public:
    __k3_context(Engine& e): __engine(e) {}

    virtual void __dispatch(int, void *, const Address&) = 0;
    virtual std::map<std::string, std::string> __prettify() = 0;
    virtual std::map<std::string, std::string> __jsonify() = 0;
    virtual void __patch(std::string) = 0;
    virtual unit_t processRole(const unit_t&) = 0;

    static std::string __get_trigger_name(int trig_id);
    static shared_ptr<Dispatcher> __get_clonable_dispatcher(int trig_id);

    static std::map<int, std::string> __trigger_names;
    static std::map<int, shared_ptr<Dispatcher>> __clonable_dispatchers;
   protected:
    Engine& __engine;
  };

  template <class context>
  std::map<Address, shared_ptr<__k3_context>> createContexts(std::vector<string> peer_strs, Engine& engine) {
    std::map<Address, shared_ptr<__k3_context>> contexts;
    for (auto& s : peer_strs) {
      auto gc = make_shared<context>(engine);
      gc->__patch(s);
      std::cout << addressAsString(gc->me) << std::endl;
      contexts[gc->me] = gc;
    }
    return contexts;
  }

  std::list<Address> getAddrs(std::map<Address, shared_ptr<__k3_context>> contexts);
  void processRoles(std::map<Address, shared_ptr<__k3_context>> contexts);
}



#endif
