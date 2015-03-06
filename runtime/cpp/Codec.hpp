#ifndef K3_RUNTIME_FRAMING_H
#define K3_RUNTIME_FRAMING_H

#include <string>

#include "Common.hpp"
#include "Message.hpp"

namespace K3 {

  // A generic exception that can be thrown by wire descriptor methods.
  class CodecException : public std::runtime_error {
  public:
    CodecException(const std::string& msg) : runtime_error(msg) {}
    CodecException(const char* msg) : runtime_error(msg) {}
  };

  // Codecs are an abstraction around a message transformation (e.g., framing, serialization, compression, etc.).
  // We provide three toplevel codec classes, which all provide an encode/decode method.
  // We do not provide an abstract base class since we may wish to have template methods, which cannot be virtual in C++.
  // a. Codec: this provides an enum encapsulating all child variants. It provides no encode/decode in its base class
  //    leaving its children to do so in templatized fashion. The usage pattern is to store the base class in data
  //    structures, but to cast based on the enum prior to usage.
  //    The primary use case is for convertinng to and from data representations, binary/csv/json formats.
  // b. FrameCodec: for framing transformations, e.g., line-based or length-based delimitation.
  // c. MessageCodec: for message serialization, e.g., as a triple of trigger id, address and payload.

  class Codec : public virtual LogMT {
    public:
      // This enum should be extended as more data formats arise.
      enum class CodecFormat {K3, CSV, JSON, YAML};

      Codec(CodecFormat f): format_(f), LogMT("Codec") {}
      virtual ~Codec() {}

      virtual shared_ptr<Codec> freshClone() { return make_shared<Codec>(format_); }

      // No encode or decode method. These are template methods provided by inherited classes.
      CodecFormat format() { return format_; }
      virtual bool decode_ready() { return true; }
      virtual bool good() { return true; }

    protected:
      CodecFormat format_;
  };


  // A codec specialization that explicitly consumes and produces strings.
  // Note this does not inherit from a Codec; rather it is used directly
  // in the only place it is needed (an IOHandle).
  class FrameCodec : public virtual LogMT {
    public:
      FrameCodec(): LogMT("FrameCodec") {}
      virtual ~FrameCodec() {}

      // codec cloning
      virtual shared_ptr<FrameCodec> freshClone() = 0;

      virtual string encode(const string&) = 0;
      virtual shared_ptr<string> decode(const string&) = 0;
      virtual shared_ptr<string> decode(const char *, size_t) = 0;
      virtual bool decode_ready() = 0;
      virtual bool good() = 0;
  };

  // A codec specialization that explicitly consumes and produces RemoteMessages.
  class MessageCodec : public virtual LogMT {
    public:
      MessageCodec(): LogMT("MessageCodec") {}
      virtual ~MessageCodec() {}

      virtual shared_ptr<MessageCodec> freshClone() = 0;

      virtual string encode(const RemoteMessage&) = 0;
      virtual shared_ptr<RemoteMessage> decode(const string&) = 0;
      virtual shared_ptr<RemoteMessage> decode(const char *, size_t) = 0;
      virtual bool decode_ready() = 0;
      virtual bool good() = 0;
  };


  // ----------------------------
  // Frame codec implementations
  //

  // The "identity" framing transformation, returning the payload unchanged.
  class DefaultFrameCodec : public virtual FrameCodec, public virtual LogMT {
    public:
      DefaultFrameCodec() : FrameCodec(), LogMT("DefaultFrameCodec"), good_(true) {}

      string encode(const string& v) { return v; }

      shared_ptr<string> decode(const string& v) {
        shared_ptr<string> result;
        if (v != "") {
          result = make_shared<string>(v);
        }
        return result;
      }

      shared_ptr<string> decode(const char* v, size_t i) {
        shared_ptr<string> result;
        if (v != nullptr) {
          result = make_shared<string>(v, i);
        }
        return result;
      }

      bool decode_ready() { return true; }

      bool good() { return good_; }

      shared_ptr<FrameCodec> freshClone() {
        shared_ptr<FrameCodec> cdec = shared_ptr<DefaultFrameCodec>(new DefaultFrameCodec());
        return cdec;
      };

    protected:
      bool good_;
  };

  class DelimiterFrameCodec : public virtual FrameCodec, public virtual LogMT {
    public:
      DelimiterFrameCodec(char delimiter)
        : FrameCodec(), LogMT("DelimiterFrameCodec"), delimiter_(delimiter), good_(true), buf_(new std::string())
      {}

      string encode(const string& v);

      shared_ptr<string> decode(const char *s, size_t len) {
        if (s != nullptr) {
          buf_->append(s, len);
        }
        return completeDecode();
      }

      shared_ptr<string> decode(const string& v) {
        buf_->append(v);
        return completeDecode();
      }

      shared_ptr<string> completeDecode();

      bool decode_ready() {
       return buf_?
          find_delimiter() != std::string::npos : false;
      }

      bool good() { return good_; }

      shared_ptr<FrameCodec> freshClone() {
        return make_shared<DelimiterFrameCodec>(delimiter_);
      }

      char delimiter_;

    protected:
      size_t find_delimiter() { return buf_->find(delimiter_); }
      bool good_;
      shared_ptr<std::string> buf_;
  };

  class LengthHeaderFrameCodec : public virtual FrameCodec, public virtual LogMT {
    public:
      LengthHeaderFrameCodec()
        : FrameCodec(), LogMT("LengthHeaderFrameCodec"), good_(true), buf_(new std::string()), next_size_(NULL)
      {}

      string encode(const string& s);

      shared_ptr<string> decode(const char *v, size_t len) {
        if (v != nullptr) {
          buf_->append(v, len);
        }
        return completeDecode();
      }

      shared_ptr<string> decode(const string& v) {
        buf_->append(v);
        return completeDecode();
      }

      shared_ptr<string> completeDecode();

      bool decode_ready() {
        return next_size_? buf_->length() >= *next_size_ : false;
      }

      bool good() { return good_; }

      shared_ptr<FrameCodec> freshClone() {
        return make_shared<LengthHeaderFrameCodec>();
      };

    protected:
      bool good_;
      shared_ptr<fixed_int> next_size_;
      shared_ptr<std::string> buf_;

      void strip_header();
  };


  // ------------------------------
  // Message codec implementations
  //

  template<typename BaseFrameCodec, class... Types>
  class AbstractMessageCodec : public virtual MessageCodec, public virtual LogMT
  {
    public:
      AbstractMessageCodec(Types... args) : frame(args...), LogMT("AbstractMessageCodec") {}

      string encode(const RemoteMessage& m) {
        return frame.encode(show_message(m));
      }

      shared_ptr<RemoteMessage> decode(const string& s) { return decode(s.c_str(), s.size()); }

      shared_ptr<RemoteMessage> decode(const char *s, size_t len) {
        return read_message(frame.decode(s, len));
      }

      bool decode_ready() { frame.decode_ready(); }
      bool good() { frame.good(); }

    protected:
      shared_ptr<RemoteMessage> read_message(shared_ptr<string> v) {
        shared_ptr<RemoteMessage> r;
        if ( v ) { r = BoostSerializer::unpack<RemoteMessage>(*v); }
        return r;
      }

      string show_message(const RemoteMessage& m) {
        return BoostSerializer::pack<RemoteMessage>(m);
      }

    private:
      BaseFrameCodec frame;
  };

  class DefaultMessageCodec : public AbstractMessageCodec<DefaultFrameCodec>, public virtual LogMT {
    public:
      DefaultMessageCodec() : AbstractMessageCodec<DefaultFrameCodec>(), LogMT("DefaultMessageCodec") {}

      shared_ptr<MessageCodec> freshClone() {
        return make_shared<DefaultMessageCodec>();
      }
  };

  class DelimiterMessageCodec : public AbstractMessageCodec<DelimiterFrameCodec, char>, public virtual LogMT
  {
    public:
      DelimiterMessageCodec(char delimiter)
        : AbstractMessageCodec<DelimiterFrameCodec, char>(delimiter),
          LogMT("DelimiterMessageCodec"), delimiter_(delimiter)
      {}

      shared_ptr<MessageCodec> freshClone() {
        return make_shared<DelimiterMessageCodec>(delimiter_);
      };

    protected:
      char delimiter_;
  };

  class LengthHeaderMessageCodec : public AbstractMessageCodec<LengthHeaderFrameCodec>, public virtual LogMT
  {
    public:
      LengthHeaderMessageCodec()
        : AbstractMessageCodec<LengthHeaderFrameCodec>(), LogMT("LengthHeaderMessageCodec")
      {}

      shared_ptr<MessageCodec> freshClone() {
        return make_shared<LengthHeaderMessageCodec>();
      };
  };

  // ------------------------------
  // External codec implementations
  //

  class K3Codec : public virtual Codec, public virtual LogMT {
  public:
    K3Codec(CodecFormat f): Codec(f), LogMT("K3Codec") {}
    virtual ~K3Codec() {}

    shared_ptr<Codec> freshClone() {
      return std::dynamic_pointer_cast<Codec, K3Codec>(make_shared<K3Codec>(format_));
    }

    template<typename T> string encode(const T& v) {
      return BoostSerializer::pack<T>(v);
    }

    template<typename T> shared_ptr<T> decode(const string& s) {
      return BoostSerializer::unpack<T>(s);
    }

    template<typename T> shared_ptr<T> decode(const char *s, size_t sz) {
      string v(s, sz);
      return decode<T>(v);
    }
  };

  class CSVCodec : public virtual Codec, public virtual LogMT {
  public:
    CSVCodec(CodecFormat f): Codec(f), LogMT("CSVCodec") {}
    virtual ~CSVCodec() {}

    shared_ptr<Codec> freshClone() {
      return std::dynamic_pointer_cast<Codec, CSVCodec>(make_shared<CSVCodec>(format_));
    }

    template<typename T> string encode(const T& v) {
      return BoostSerializer::pack_csv<T>(v);
    }

    template<typename T> shared_ptr<T> decode(const string& s) {
      return BoostSerializer::unpack_csv<T>(s);
    }

    template<typename T> shared_ptr<T> decode(const char *s, size_t sz) {
      string v(s, sz);
      return decode<T>(v);
    }
  };

  // ---------
  // Aliases
  //
  using ExternalFrameCodec = FrameCodec;

} // namespace K3

#endif // K3_RUNTIME_FRAMING_H
