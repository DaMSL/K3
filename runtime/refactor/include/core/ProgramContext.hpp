#ifndef K3_PROGRAMCONTEXT
#define K3_PROGRAMCONTEXT

#include <memory>
#include <string>
#include <map>

#include <boost/asio.hpp>

#include "builtins/Builtins.hpp"
#include "Common.hpp"
#include "types/Message.hpp"
#include "serialization/Yaml.hpp"
#include "serialization/Json.hpp"

namespace K3 {

class NativeValue;
class Engine;

class ProgramContext : public StandardBuiltins,
                       public TimeBuiltins,
                       public StringBuiltins,
                       public ProfilingBuiltins,
                       public VectorBuiltins,
                       public AmplabLoaders {
 public:
  explicit ProgramContext(Engine& e);
  virtual ~ProgramContext() { }

  // Dispatch overloads: One for each Value type
  virtual void __dispatch(NativeValue* nv, TriggerID trig, const Address& source) = 0;
  virtual void __dispatch(PackedValue* pv, TriggerID trig, const Address& source) = 0;
  void __dispatch(SentinelValue* sv);

  // Program initialization
  virtual void __patch(const YAML::Node& node);
  virtual unit_t initDecls(unit_t);
  virtual unit_t processRole(const unit_t&);

  // Logging Helper Functions
  virtual map<string, string> __prettify();
  virtual map<string, string> __jsonify();
  static string __triggerName(int trig);
  virtual string __jsonifyMessage(const Message& m);

  static map<TriggerID, string> __trigger_names_;
 protected:
  CodecFormat __internal_format_ = K3_INTERNAL_FORMAT;
  Engine& __engine_;
};

typedef std::function<shared_ptr<ProgramContext>()> ContextFactory;

// A Dummy Context implementation, for simple tests.
// TODO(jbw) move to separate file
class DummyState {
 public:
  int my_int_ = 0;
  std::string my_string_ = "";
};

class DummyContext : public ProgramContext {
 public:
  explicit DummyContext(Engine& e);
  virtual void __dispatch(NativeValue* nv, TriggerID trig, const Address& source);
  virtual void __dispatch(PackedValue* pv, TriggerID trig, const Address& source);
  virtual void __patch(const YAML::Node& node);
  virtual unit_t processRole(const unit_t&);
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
  static Node encode(const K3::DummyContext& context) {
    Node _node;
    _node["me"] = convert<K3::Address>::encode(context.me);
    _node["role"] = convert<std::string>::encode(context.role);
    _node["my_int"] = convert<int>::encode(context.state_->my_int_);
    _node["my_string"] =
        convert<std::string>::encode(context.state_->my_string_);
    _node["max_loops"] = convert<int>::encode(context.max_loops);
    return _node;
  }
  static bool decode(const Node& node, K3::DummyContext& context) {
    if (!node.IsMap()) {
      return false;
    }
    if (node["me"]) {
      context.me = node["me"].as<K3::Address>();
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
