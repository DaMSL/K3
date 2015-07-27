#ifndef K3_VALUE
#define K3_VALUE

#include <exception>
#include <memory>
#include <utility>
#include <string>

#include <yas/buffers.hpp>

#include "Common.hpp"

namespace K3 {

class ProgramContext;

// Boxes a native C++ value.
// The as<T>() and asConst<T>() functions enable casting to (any) native type.
// The generated program knows the correct choice of T given the destination trigger
class NativeValue {
 public:
  virtual ~NativeValue() { }
  template <class T> T* as();
  template <class T> const T* asConst() const;

 protected:
  virtual void* materialize() = 0;
  virtual const void* materializeConst() const = 0;
};

// Interface that represents a packed C++ value.
class PackedValue {
 public:
  virtual ~PackedValue() { }
  PackedValue(CodecFormat format);
  CodecFormat format() const;
  virtual const char* buf() const = 0;
  virtual size_t length() const = 0;

 protected:
  CodecFormat format_;
};

// Native Value Implementation
template <class T>
T* NativeValue::as() {
  return static_cast<T*>(this->materialize());
}

template <class T>
const T* NativeValue::asConst() const {
  return static_cast<const T*>(this->materializeConst());
}

// Templated instantiation of the NativeValue class
template <class T>
class TNativeValue : public NativeValue {
 public:
  template <class X>
  explicit TNativeValue(X&& x) : value_(std::forward<X>(x)) {}

 protected:
  virtual void* materialize() { return static_cast<void*>(&value_); }
  virtual const void* materializeConst() const { return static_cast<const void*>(&value_); }
  T value_;
};

// Packed Value Implementations
// Vector-based
class BufferPackedValue : public PackedValue {
 public:
  BufferPackedValue(Buffer&& b, CodecFormat format);
  CodecFormat format() const;
  virtual const char* buf() const;
  virtual size_t length() const;

 protected:
  std::unique_ptr<Buffer> buffer_;
};

// String-based
class StringPackedValue : public PackedValue {
 public:
  StringPackedValue(string&& b, CodecFormat format);
  CodecFormat format() const;
  virtual const char* buf() const;
  virtual size_t length() const;

 protected:
  std::unique_ptr<string> string_;
};

// YAS-shared-buffer-based
class YASPackedValue : public PackedValue {
 public:
  YASPackedValue(yas::shared_buffer b, CodecFormat format);
  CodecFormat format() const;
  virtual const char* buf() const;
  virtual size_t length() const;

 protected:
  yas::shared_buffer buf_;
};

}  // namespace K3

#endif
