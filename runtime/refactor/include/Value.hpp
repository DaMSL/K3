#ifndef K3_VALUE
#define K3_VALUE

#include <memory>
#include <utility>

#include "Common.hpp"
#include "ProgramContext.hpp"

using std::shared_ptr;
using std::make_shared;

// Interface
// Can contain either native or packed values.
// Virtual function to dispatch based on type (native or packed)
class Value {
 public:
  virtual void dispatch(ProgramContext&, TriggerID) = 0;
};

// Boxes a native C++ value that is casted to the
// correct native type once dispatched in the ProgramContext
class NativeValue : public Value {
 public:
  template <class T>
  T* as() {
    return static_cast<T*>(this->materialize());
  }

  template <class T>
  const T* asConst() const {
    return static_cast<const T*>(this->materializeConst());
  }

  virtual void dispatch(ProgramContext& pc, TriggerID t);

 protected:
  virtual void* materialize() = 0;
  virtual const void* materializeConst() const = 0;
};

// Templated instantiation of the NativeValue class
template <class T>
class TNativeValue : public NativeValue {
 public:
  template<class X>
  TNativeValue(X&& x) : value_(std::forward<X>(x)) { }

 protected:
  virtual void* materialize() {
    return static_cast<void*>(&value_);
  }

  virtual const void* materializeConst() const {
    return static_cast<const void*>(&value_);
  }

  T value_;
};

// Interface
// Packed C++ value that is unpacked and casted to a native type
// once dispatched in the ProgramContext
// Holds a handle to a Codec to enable deferred unpacking
class Codec;
class PackedValue : public Value {
 public:
  PackedValue(Buffer b, shared_ptr<Codec> c);
  virtual void dispatch(ProgramContext& pc, TriggerID trig);

 protected:
  const char* buf();
  size_t length();
  shared_ptr<Codec> codec_;
  Buffer buffer_;
};

#endif
