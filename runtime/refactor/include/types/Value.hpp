#ifndef K3_VALUE
#define K3_VALUE

#include <exception>
#include <memory>
#include <utility>
#include <string>

#include "yas/buffers.hpp"

#include "Common.hpp"

namespace K3 {

class ProgramContext;

// Interface
// Can contain either native, packed, or sentinel values.
// The 'dispatchIntoContext' method calls the correct 'dispatch' overload on a
// ProgramContext
// based on the underlying value type (native, packed, or sentinel).
class Value {
 public:
  virtual void dispatchIntoContext(ProgramContext* pc, TriggerID trig, const Address& source) = 0;
};

// Boxes a native C++ value.
// The as<T>() and asConst<T>() functions enable casting to (any) native type.
// A ProgramContext knows the true underlying type based on the trigger
// that this value is intended for.
class NativeValue : public Value {
 public:
  virtual void dispatchIntoContext(ProgramContext* pc, TriggerID trig, const Address& source);

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
  template <class X>
  explicit TNativeValue(X&& x)
      : value_(std::forward<X>(x)) {}

 protected:
  virtual void* materialize() { return static_cast<void*>(&value_); }

  virtual const void* materializeConst() const {
    return static_cast<const void*>(&value_);
  }

  T value_;
};

// Interface that represents a packed C++ value.
class Codec;
class PackedValue : public Value {
 public:
  PackedValue( CodecFormat format);
  virtual void dispatchIntoContext(ProgramContext* pc, TriggerID trig, const Address& source);
  CodecFormat format() const;
  virtual const char* buf() const = 0;
  virtual size_t length() const = 0;

 protected:
  CodecFormat format_;
};

class BufferPackedValue : public PackedValue {
 public:
  BufferPackedValue(Buffer&& b, CodecFormat format);
  CodecFormat format() const;
  virtual const char* buf() const;
  virtual size_t length() const;

 protected:
  std::unique_ptr<Buffer> buffer_;
};

class StringPackedValue : public PackedValue {
 public:
  StringPackedValue(string&& b, CodecFormat format);
  CodecFormat format() const;
  virtual const char* buf() const;
  virtual size_t length() const;

 protected:
  std::unique_ptr<string> string_;
};

class YASPackedValue : public PackedValue {
 public:
  YASPackedValue(yas::shared_buffer b, CodecFormat format);
  CodecFormat format() const;
  virtual const char* buf() const;
  virtual size_t length() const;

 protected:
  yas::shared_buffer buf_;
};



// Sentinel value throws EndofProgram exception
// when converted to NativeValue
class SentinelValue : public Value {
 public:
  SentinelValue();
  virtual void dispatchIntoContext(ProgramContext* pc, TriggerID trig, const Address& source);
};

}  // namespace K3

#endif
