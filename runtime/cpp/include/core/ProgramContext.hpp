#ifndef K3_PROGRAMCONTEXT
#define K3_PROGRAMCONTEXT

#include <memory>
#include <string>
#include <map>

#include <boost/asio.hpp>

#include "builtins/Builtins.hpp"
#include "Common.hpp"
#include "types/Dispatcher.hpp"
#include "types/Message.hpp"
#include "serialization/Yaml.hpp"
#include "serialization/Json.hpp"

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
  explicit ProgramContext(Engine& e);
  virtual ~ProgramContext() { }

  virtual unique_ptr<Dispatcher> __getDispatcher(unique_ptr<NativeValue>, TriggerID trig) = 0;
  virtual unique_ptr<Dispatcher> __getDispatcher(unique_ptr<PackedValue>, TriggerID trig) = 0;

  // Program initialization
  virtual void __patch(const YAML::Node& node);
  virtual unit_t initDecls(unit_t);
  virtual unit_t processRole(const unit_t&);

  // Logging Helper Function
  virtual map<string, string> __prettify();
  virtual map<string, string> __jsonify();
  static string __triggerName(int trig);
  virtual string __jsonifyMessage(const Message& m);

  static map<TriggerID, string> __trigger_names_;
 protected:
  CodecFormat __internal_format_ = K3_INTERNAL_FORMAT;
  Engine& __engine_;
};


}  // namespace K3

#endif
