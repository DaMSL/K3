#include <map>
#include <list>
#include "Common.hpp"
#include "Context.hpp"
#include "Engine.hpp"
#include "Resolver.hpp"

namespace K3 {


using K3::Address;
using K3::__k3_context;

std::string __k3_context::__get_trigger_name(int trig_id) {
  return __k3_context::__trigger_names[trig_id];
}
shared_ptr<Dispatcher> __k3_context::__get_clonable_dispatcher(int trig_id) {
  return __k3_context::__clonable_dispatchers[trig_id];
}


std::list<Address> getAddrs(std::map<Address, shared_ptr<__k3_context>> contexts) {
  std::list<Address> result;
  for (const auto& it: contexts) {
    result.push_back(it.first);
  }
  return result;
}

void processRoles(std::map<Address, shared_ptr<__k3_context>> contexts) {
  for (const auto& it: contexts) {
    it.second->processRole(unit_t {});
  }
}


unit_t __k3_context::sayHello(std::tuple<const PeerId&, Address> data) {
    __engine.sayHello(std::get<0>(data), std::get<1>(data));
}

std::map<int, std::string> __k3_context::__trigger_names;
std::map<int, shared_ptr<Dispatcher>> __k3_context::__clonable_dispatchers;


unit_t __k3_context::registerPeerChangeTrigger(TriggerId trigger, Address me)
{
    __engine.registerPeerChangeTrigger(trigger, me);
}
}
