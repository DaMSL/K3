#include <iostream>
#include <memory>
#include <list>
#include <string>
#include <map>

#include "Common.hpp"
#include "core/Engine.hpp"
#include "core/ProgramContext.hpp"
#include "serialization/Codec.hpp"
#include "types/Message.hpp"
#include "types/Value.hpp"

namespace K3 {

ProgramContext::ProgramContext(Engine& e) : StandardBuiltins(e), FileBuiltins(e), __engine_(e) {}

void ProgramContext::__patch(const YAML::Node& node) {
  return;
}

std::list<string_impl> ProgramContext::__globalNames() {
  return std::list<string_impl>();
}

unit_t ProgramContext::initDecls(unit_t) { return unit_t{}; }

unit_t ProgramContext::processRole(const unit_t&) { return unit_t{}; }

map<string, string> ProgramContext::__prettify() {
  return map<string, string>();
}

string_impl ProgramContext::__jsonify(const string_impl& global) {
  return "";
}

string ProgramContext::__triggerName(int trig) {
  auto it = ProgramContext::__trigger_names_.find(trig);
  std::string s = (it != ProgramContext::__trigger_names_.end())
                      ? it->second
                      : "{Undefined Trigger}";
  return s;
}

string ProgramContext::__jsonifyMessage(const Message& m) {
  return "";
}

std::map<TriggerID, string> ProgramContext::__trigger_names_;

}  // namespace K3
