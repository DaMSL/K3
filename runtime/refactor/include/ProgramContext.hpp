#ifndef K3_PROGRAMCONTEXT
#define K3_PROGRAMCONTEXT

// A ProgramContext contains all code and data associated with a K3 Program.
// A K3-Program specific implementation will be created by the code generator

#include <memory>
#include <string>

#include "Common.hpp"
#include "Message.hpp"

using std::shared_ptr;
class NativeValue;
class ProgramContext {
 public:
  virtual void dispatch(NativeValue* nv, TriggerID trig) = 0;
  virtual void dispatch(PackedValue* pv, TriggerID trig) = 0;
  virtual void dispatch(SentinelValue* sv) = 0;
};

class DummyState {
 public:
  int my_int_;
  std::string my_string_;
};

class DummyContext : public ProgramContext {
 public:
  DummyContext();
  virtual void dispatch(NativeValue* nv, TriggerID trig);
  virtual void dispatch(PackedValue* pv, TriggerID trig);
  virtual void dispatch(SentinelValue* sv);
  void intTrigger(int i);
  void stringTrigger(std::string s);
  shared_ptr<DummyState> state_;
};

#endif
