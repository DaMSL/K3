#ifndef K3_RUNTIME_FRAMING_H
#define K3_RUNTIME_FRAMING_H

#include <string>

#include "Common.hpp"
#include "Message.hpp"

namespace K3 {

  // A generic exception that can be thrown by wire descriptor methods.
  class FramingException : public std::runtime_error {
  public:
    FramingException(const std::string& msg) : runtime_error(msg) {}
    FramingException(const char* msg) : runtime_error(msg) {}
  };

  // Message serializtion/deserialization abstract base class.
  // Implementations can encapsulate framing concerns as well as serdes operations.
  //
  // The unpack method may be supplied a complete or incomplete std::string corresponding
  // to a value. It is left to the implementation to determine the scope of functionality
  // supported, for example partial unpacking (e.g., for network sockets).
  // The semantics of repeated invocations are dependent on the actual implementation
  // of the wire description (including factors such as message loss).
  // This includes the conditions under which an exception is thrown.

  class Framing : public virtual LogMT {
    public:
      Framing(): LogMT("Framing") {}
      virtual ~Framing() {}

      // codec cloning
      virtual shared_ptr<Framing> freshClone() = 0;

      virtual Value encode(const Value&) = 0;
      virtual shared_ptr<Value> decode(const Value&) = 0;
      virtual shared_ptr<Value> decode(const char *, size_t) = 0;
      virtual bool decode_ready() = 0;
      virtual bool good() = 0;
  };

  class DefaultFraming : public virtual Framing, public virtual LogMT {
    public:
      DefaultFraming() : Framing(), LogMT("DefaultFraming"), good_(true) {}

      Value encode(const Value& v) { return v; }

      shared_ptr<Value> decode(const Value& v) {
        shared_ptr<Value> result;
        if (v != "") {
          result = make_shared<Value>(v);
        }
        return result;
      }

      shared_ptr<Value> decode(const char* v, size_t i) {
        shared_ptr<Value> result;
        if (v != nullptr) {
          result = make_shared<Value>(v, i);
        }
        return result;
      }

      bool decode_ready() { return true; }

      bool good() { return good_; }

      shared_ptr<Framing> freshClone() {
        shared_ptr<Framing> cdec = shared_ptr<DefaultFraming>(new DefaultFraming());
        return cdec;
      };

    protected:
      bool good_;
  };

  class InternalFraming: public virtual Framing {
    public:
      InternalFraming() : LogMT("InternalFraming") {}

      virtual RemoteMessage read_message(const Value&) = 0;
      virtual Value show_message(const RemoteMessage&) = 0;
  };

  class DelimiterFraming : public virtual Framing, public virtual LogMT {
    public:
      DelimiterFraming(char delimiter)
        : Framing(), LogMT("DelimiterFraming"), delimiter_(delimiter), good_(true), buf_(new std::string())
      {}

      Value encode(const Value& v);

      shared_ptr<Value> decode(const char *s, size_t len) {
        if (s != nullptr) {
          buf_->append(s, len);
        }
        return completeDecode();
      }

      shared_ptr<Value> decode(const Value& v) {
        buf_->append(v);
        return completeDecode();
      }

      shared_ptr<Value> completeDecode();

      bool decode_ready() {
       return buf_?
          find_delimiter() != std::string::npos : false;
      }

      bool good() { return good_; }

      shared_ptr<Framing> freshClone() {
        return make_shared<DelimiterFraming>(delimiter_);
      }

      char delimiter_;
    protected:
      size_t find_delimiter() { return buf_->find(delimiter_); }
      bool good_;
      shared_ptr<std::string> buf_;
  };

  class LengthHeaderFraming : public virtual Framing, public virtual LogMT {
    public:
      LengthHeaderFraming()
        : Framing(), LogMT("LengthHeaderFraming"), good_(true), buf_(new std::string()), next_size_(NULL)
      {}

      Value encode(const Value& s);

      shared_ptr<Value> decode(const char *v, size_t len) {
        if (v != nullptr) {
          buf_->append(v, len);
        }
        return completeDecode();
      }

      shared_ptr<Value> decode(const Value& v) {
        buf_->append(v);
        return completeDecode();
      }

      shared_ptr<Value> completeDecode();

      bool decode_ready() {
        return next_size_? buf_->length() >= *next_size_ : false;
      }

      bool good() { return good_; }

      shared_ptr<Framing> freshClone() {
        return make_shared<LengthHeaderFraming>();
      };

    protected:
      bool good_;
      shared_ptr<fixed_int> next_size_;
      shared_ptr<std::string> buf_;

      void strip_header();
  };

  class AbstractDefaultInternalFraming : public virtual InternalFraming, public virtual LogMT {
    public:
      AbstractDefaultInternalFraming() : InternalFraming(), LogMT("AbstractDefaultInternalFraming") {}

      RemoteMessage read_message(const Value& v);

      Value show_message(const RemoteMessage& m);
  };

  class DefaultInternalFraming : public AbstractDefaultInternalFraming, public DefaultFraming, public virtual LogMT {
    public:
      DefaultInternalFraming()
        : AbstractDefaultInternalFraming(), DefaultFraming(), LogMT("DefaultInternalFraming")
      {}

      shared_ptr<Framing> freshClone() {
        shared_ptr<Framing> cdec = shared_ptr<DefaultInternalFraming>(new DefaultInternalFraming());
        return cdec;
      };

  };

  class DelimiterInternalFraming : public AbstractDefaultInternalFraming, public DelimiterFraming, public virtual LogMT {
    public:
      DelimiterInternalFraming(char delimiter)
        : AbstractDefaultInternalFraming(), DelimiterFraming(delimiter), LogMT("DelimiterInternalFraming"), delimiter_(delimiter)
      {}

      shared_ptr<Framing> freshClone() {
        return make_shared<DelimiterInternalFraming>(delimiter_);
      };

    protected:
      char delimiter_;
  };

  class LengthHeaderInternalFraming : public AbstractDefaultInternalFraming, public LengthHeaderFraming, public virtual LogMT {
    public:
      LengthHeaderInternalFraming()
        : AbstractDefaultInternalFraming(), LengthHeaderFraming(), LogMT("LengthHeaderInternalFraming")
      {}

      shared_ptr<Framing> freshClone() {
        return make_shared<LengthHeaderInternalFraming>();
      };

  };

  using ExternalFraming = Framing;

} // namespace K3

#endif // K3_RUNTIME_FRAMING_H
