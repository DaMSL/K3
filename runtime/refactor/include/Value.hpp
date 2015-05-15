#ifndef K3_VALUE
#define K3_VALUE

#include <exception>
#include <memory>
#include <utility>

#include "Common.hpp"

using std::shared_ptr;
using std::make_shared;
using std::unique_ptr;

class NativeValue;

// Interface
// Can contain either native, packed, or sentinel values.
// asNative() provides a NativeValue pointer that is guaranteed to be valid
// for the lifetime of the Value
class Value {
 public:
  virtual NativeValue* asNative() = 0;
};

// Boxes a native C++ value.
// The as<T>() and asConst<T>() functions enable casting to (any) native type.
// A ProgramContext knows the true underlying type based on the trigger
// that this value is intended for.
class NativeValue : public Value {
 public:
  virtual NativeValue* asNative();

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
// Holds a handle to a Codec to enable deferred unpacking
// asNative() will unpack the buffer, producing a native value
// and destroy the underlying buffer.
class Codec;
class PackedValue : public Value {
 public:
  PackedValue(unique_ptr<Buffer> b, shared_ptr<Codec> c);
  virtual NativeValue* asNative();
  const char* buf() const;
  size_t length() const;

 protected:
  shared_ptr<Codec> codec_;
  unique_ptr<NativeValue> native_;
  unique_ptr<Buffer> buffer_;
};

// Sentinel value throws EndofProgram exception
// when converted to NativeValue
class SentinelValue : public Value {
 public:
  SentinelValue();
  virtual NativeValue* asNative();
};

#endif
