#ifndef K3_RUNTIME_CODEC_H
#define K3_RUNTIME_CODEC_H

#include <string>

#include "Message.hpp"

namespace K3 {

  //--------------------
  // Wire descriptions

  // A generic exception that can be thrown by wire descriptor methods.
  class CodecException : public std::runtime_error {
  public:
    CodecException(const std::string& msg) : runtime_error(msg) {}
    CodecException(const char* msg) : runtime_error(msg) {}
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

  class Codec: public virtual LogMT {
    public:
      Codec(): LogMT("Codec") {}

      virtual Value encode(const Value&) = 0;
      virtual std::shared_ptr<Value> decode(const Value&) = 0;
      virtual std::shared_ptr<Value> decode(const char *, size_t) = 0;
      virtual bool decode_ready() = 0;
      virtual bool good() = 0;

      // codec cloning
      virtual std::shared_ptr<Codec> freshClone() = 0;
      virtual ~Codec() {}

  };

  class DefaultCodec : public virtual Codec, public virtual LogMT {
    public:
      DefaultCodec() : Codec(), LogMT("DefaultCodec"), good_(true) {}

      Value encode(const Value& v) { return v; }

      std::shared_ptr<Value> decode(const Value& v) {
        std::shared_ptr<Value> result;
        if (v != "") {
          result = std::make_shared<Value>(v);
        }
        return result;
      }

      std::shared_ptr<Value> decode(const char* v, size_t i) {
        std::shared_ptr<Value> result;
        if (v != nullptr) {
          result = std::make_shared<Value>(v, i);
        }
        return result;
      }

      bool decode_ready() { return true; }

      bool good() { return good_; }

      std::shared_ptr<Codec> freshClone() {
        std::shared_ptr<Codec> cdec = std::shared_ptr<DefaultCodec>(new DefaultCodec());
        return cdec;
      };

    protected:
      bool good_;
  };

  class InternalCodec: public virtual Codec {
    public:
      InternalCodec() : LogMT("InternalCodec") {}

      virtual RemoteMessage read_message(const Value&) = 0;
      virtual Value show_message(const RemoteMessage&) = 0;
  };

  class DelimiterCodec : public virtual Codec, public virtual LogMT {
    public:
      DelimiterCodec(char delimiter)
        : Codec(), LogMT("DelimiterCodec"), delimiter_(delimiter), good_(true), buf_(new std::string())
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

      std::shared_ptr<Codec> freshClone() {
        return make_shared<DelimiterCodec>(delimiter_);
      }

      char delimiter_;
    protected:
      size_t find_delimiter() { return buf_->find(delimiter_); }
      bool good_;
      std::shared_ptr<std::string> buf_;
  };

  class LengthHeaderCodec : public virtual Codec, public virtual LogMT {
    public:
      LengthHeaderCodec()
        : Codec(), LogMT("LengthHeaderCodec"), good_(true), buf_(new std::string()), next_size_(NULL)
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

      std::shared_ptr<Value> completeDecode();

      bool decode_ready() {
        return next_size_? buf_->length() >= *next_size_ : false;
      }

      bool good() { return good_; }

      std::shared_ptr<Codec> freshClone() {
        return make_shared<LengthHeaderCodec>();
      };

    protected:
      bool good_;
      std::shared_ptr<fixed_int> next_size_;
      std::shared_ptr<std::string> buf_;

      void strip_header();
  };

  class AbstractDefaultInternalCodec : public virtual InternalCodec, public virtual LogMT {
    public:
      AbstractDefaultInternalCodec() : InternalCodec(), LogMT("AbstractDefaultInternalCodec") {}

      RemoteMessage read_message(const Value& v);

      Value show_message(const RemoteMessage& m);
  };

  class DefaultInternalCodec : public AbstractDefaultInternalCodec, public DefaultCodec, public virtual LogMT {
    public:
      DefaultInternalCodec()
        : AbstractDefaultInternalCodec(), DefaultCodec(), LogMT("DefaultInternalCodec")
      {}

      std::shared_ptr<Codec> freshClone() {
        std::shared_ptr<Codec> cdec = std::shared_ptr<DefaultInternalCodec>(new DefaultInternalCodec());
        return cdec;
      };

  };

  class DelimiterInternalCodec : public AbstractDefaultInternalCodec, public DelimiterCodec, public virtual LogMT {
    public:
      DelimiterInternalCodec(char delimiter)
        : AbstractDefaultInternalCodec(), DelimiterCodec(delimiter), LogMT("DelimiterInternalCodec"), delimiter_(delimiter)
      {}

      std::shared_ptr<Codec> freshClone() {
        return make_shared<DelimiterInternalCodec>(delimiter_);
      };

    protected:
      char delimiter_;
  };

  class LengthHeaderInternalCodec : public AbstractDefaultInternalCodec, public LengthHeaderCodec, public virtual LogMT {
    public:
      LengthHeaderInternalCodec()
        : AbstractDefaultInternalCodec(), LengthHeaderCodec(), LogMT("LengthHeaderInternalCodec")
      {}

      std::shared_ptr<Codec> freshClone() {
        return make_shared<LengthHeaderInternalCodec>();
      };

  };

  using ExternalCodec = Codec;

} // namespace K3

#endif // K3_RUNTIME_CODEC_H
