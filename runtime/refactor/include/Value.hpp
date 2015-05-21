#ifndef K3_VALUE
#define K3_VALUE

#include <exception>
#include <memory>
#include <utility>
#include <string>

#include "Common.hpp"

class ProgramContext;

// Interface
// Can contain either native, packed, or sentinel values.
// The 'dispatchIntoContext' method calls the correct 'dispatch' overload on a ProgramContext
// based on the underlying value type (native, packed, or sentinel).
class Value {
 public:
  virtual void dispatchIntoContext(ProgramContext* pc, TriggerID trig) = 0;
};

// Boxes a native C++ value.
// The as<T>() and asConst<T>() functions enable casting to (any) native type.
// A ProgramContext knows the true underlying type based on the trigger
// that this value is intended for.
class NativeValue : public Value {
 public:
  virtual void dispatchIntoContext(ProgramContext* pc, TriggerID trig);

  template <class T>
  T* as() {
    return static_cast<T*>(this->materialize());
  }

  template <class T>
  const T* asConst() const {
    return static_cast<const T*>(this->materializeConst());
  }

 protected:
  virtual void* materialize() = 0;
  virtual const void* materializeConst() const = 0;
};

// Templated instantiation of the NativeValue class
template <class T>
class TNativeValue : public NativeValue {
 public:
  template<class X>
  explicit TNativeValue(X&& x) : value_(std::forward<X>(x)) { }

 protected:
  virtual void* materialize() {
    return static_cast<void*>(&value_);
  }

  virtual const void* materializeConst() const {
    return static_cast<const void*>(&value_);
  }

  T value_;
};

// Owns a buffer that represents a packed C++ value.
class Codec;
class PackedValue : public Value {
 public:
  PackedValue(Buffer&& b, CodecFormat format);
  virtual void dispatchIntoContext(ProgramContext* pc, TriggerID trig);
  CodecFormat format() const;
  const char* buf() const;
  size_t length() const;

 protected:
  CodecFormat format_;
  std::unique_ptr<Buffer> buffer_;
};

// Sentinel value throws EndofProgram exception
// when converted to NativeValue
class SentinelValue : public Value {
 public:
  SentinelValue();
  virtual void dispatchIntoContext(ProgramContext* pc, TriggerID trig);
};

#endif
