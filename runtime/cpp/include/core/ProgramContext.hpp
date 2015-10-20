#ifndef K3_PROGRAMCONTEXT
#define K3_PROGRAMCONTEXT

#include <memory>
#include <string>
#include <map>
#include <vector>
#include <functional>
#include <list>

#include <boost/asio.hpp>

#include "builtins/Builtins.hpp"
#include "Common.hpp"
#include "types/Dispatcher.hpp"
#include "types/Message.hpp"
#include "serialization/Yaml.hpp"
#include "serialization/Json.hpp"
#include "types/Pool.hpp"

namespace K3 {

class NativeValue;
class PackedValue;
class Engine;

class ProgramContext : public StandardBuiltins,
                       public TimeBuiltins,
                       public StringBuiltins,
                       public ProfilingBuiltins,
                       public FileBuiltins,
                       public MosaicBuiltins,
                       public VectorBuiltins,
                       public AmplabLoaders {
 public:
  explicit ProgramContext(Engine& e, Peer& p);
  virtual ~ProgramContext() { }

  __attribute__((always_inline)) Pool::unique_ptr<Dispatcher> __getDispatcher(Pool::unique_ptr<NativeValue> payload, TriggerID trig) {
      return native_dispatch_table[trig](std::move(payload));
  }
  __attribute__((always_inline)) Pool::unique_ptr<Dispatcher> __getDispatcher(Pool::unique_ptr<PackedValue> payload, TriggerID trig) {
      return packed_dispatch_table[trig](std::move(payload));
  }

  // Program initialization
  virtual void __patch(const YAML::Node& node);
  virtual unit_t initDecls(unit_t) = 0;
  virtual unit_t processRole(const unit_t&) = 0;

  // Logging Helper Function
  virtual map<string, string> __prettify();
  virtual std::list<string_impl> __globalNames();
  virtual string_impl __jsonify(const string_impl& global);

  static string __triggerName(int trig);
  virtual string __jsonifyMessage(const Message& m);

  static map<TriggerID, string> __trigger_names_;
 protected:
  std::vector<std::function<Pool::unique_ptr<Dispatcher>(Pool::unique_ptr<NativeValue>)>> native_dispatch_table;
  std::vector<std::function<Pool::unique_ptr<Dispatcher>(Pool::unique_ptr<PackedValue>)>> packed_dispatch_table;
  CodecFormat __internal_format_ = K3_INTERNAL_FORMAT;
  Engine& __engine_;
  Peer& __peer_;
};


}  // namespace K3

#endif
