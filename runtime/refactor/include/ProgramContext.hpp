#ifndef K3_PROGRAMCONTEXT
#define K3_PROGRAMCONTEXT

// A ProgramContext contains all code and data associated with a K3 Program.
// A K3-Program specific implementation will be created by the code generator

#include <memory>
#include <string>

#include "boost/asio.hpp"

#include "Builtins.hpp"
#include "Common.hpp"
#include "Message.hpp"
#include "Yaml.hpp"

namespace K3 {

class NativeValue;
class Engine;

class ProgramContext : public StandardBuiltins {
 public:
  explicit ProgramContext(Engine& e);
  virtual void dispatch(NativeValue* nv, TriggerID trig) = 0;
  virtual void dispatch(PackedValue* pv, TriggerID trig) = 0;
  virtual void dispatch(SentinelValue* sv) = 0;
  virtual void __patch(const YAML::Node& node) = 0;
  virtual void __processRole() = 0;

 protected:
  Engine& engine_;
};

class DummyState {
 public:
  int my_int_ = 0;
  std::string my_string_ = "";
};

typedef std::function<shared_ptr<ProgramContext>()> ContextFactory;

class DummyContext : public ProgramContext {
 public:
  explicit DummyContext(Engine& e);
  virtual void dispatch(NativeValue* nv, TriggerID trig);
  virtual void dispatch(PackedValue* pv, TriggerID trig);
  virtual void dispatch(SentinelValue* sv);
  virtual void __patch(const YAML::Node& node);
  virtual void __processRole();
  void intTrigger(int i);
  void stringTrigger(std::string s);
  void startTrigger(int i);
  void loopTrigger(int i);
  Address me;
  std::string role;
  int max_loops;
  shared_ptr<DummyState> state_;
};

}  // namespace K3

namespace YAML {
template <>
struct convert<K3::DummyContext> {
 public:
  static Node encode(const K3::DummyContext& context)  {
    Node _node;
    _node["me"] = convert<K3::Address>::encode(context.me);
    _node["role"] = convert<std::string>::encode(context.role);
    _node["my_int"] = convert<int>::encode(context.state_->my_int_);
    _node["my_string"] = convert<std::string>::encode(context.state_->my_string_);
    _node["max_loops"] = convert<int>::encode(context.max_loops);
    return _node;
  }
  static bool decode(const Node& node, K3::DummyContext& context)  {
    if (!node.IsMap()) {
      return false;
    }
    if (node["me"]) {
      context.me = K3::make_address(node["me"]);
    }
    if (node["role"]) {
      context.role = node["role"].as<std::string>();
    }
    if (node["my_int"]) {
      context.state_->my_int_ = node["my_int"].as<int>();
    }
    if (node["my_string"]) {
      context.state_->my_string_ = node["my_string"].as<std::string>();
    }
    if (node["max_loops"]) {
      context.max_loops = node["max_loops"].as<int>();
    }
    return true;
  }
};
}  // namespace YAML

#endif
