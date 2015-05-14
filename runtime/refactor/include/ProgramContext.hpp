#ifndef K3_PROGRAMCONTEXT
#define K3_PROGRAMCONTEXT

#include <string>

#include "Common.hpp"

class NativeValue;

class ProgramContext {
 public:
  virtual void processNative(TriggerID t, NativeValue& v) = 0;
};

class DummyContext : public ProgramContext {
 public:
  virtual void processNative(TriggerID t, NativeValue& v);
  void intTrigger(int i);
  void stringTrigger(std::string s);

  int my_int_;
  std::string my_string_;
};

#endif
