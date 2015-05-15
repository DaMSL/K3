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
  virtual void dispatch(unique_ptr<Message> m) = 0;
};

class DummyState {
 public:
  int my_int_;
  std::string my_string_;
};

class DummyContext : public ProgramContext {
 public:
  explicit DummyContext(shared_ptr<DummyState>);
  virtual void dispatch(unique_ptr<Message> m);
  void intTrigger(int i);
  void stringTrigger(std::string s);
  shared_ptr<DummyState> state_;
};

#endif
