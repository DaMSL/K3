#ifndef K3_RUNTIME_COMMON_H
#define K3_RUNTIME_COMMON_H

#include <cstdint>
#include <list>
#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <stdexcept>
#include <tuple>
#include <utility>
#include <boost/algorithm/string.hpp>
#include <boost/any.hpp>
#include <boost/asio.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/log/core.hpp>
#include <boost/log/sources/record_ostream.hpp>
#include <boost/log/sources/severity_channel_logger.hpp>
#include <boost/log/sources/severity_feature.hpp>
#include <boost/log/trivial.hpp>
#include <boost/phoenix/core.hpp>
#include <boost/phoenix/stl/algorithm.hpp>
#include <boost/regex.hpp>

namespace K3 {

  using namespace std;
  using boost::any;

  using namespace boost::log;
  using namespace boost::log::sources;
  using namespace boost::log::trivial;
  using namespace boost::phoenix;

  typedef string Identifier;
  typedef string Value;

  typedef string Value;
  typedef string EValue;
  typedef string IValue;

  typedef int32_t fixed_int;

  typedef tuple<boost::asio::ip::address, unsigned short> Address;

  enum class Builtin { Stdin, Stdout, Stderr };
  enum class IOMode  { Read, Write, Append, ReadWrite };
  
  //---------------
  // Addresses.

  Address make_address(const string& host, unsigned short port) {
    return Address(boost::asio::ip::address::from_string(host), port);
  }

  Address make_address(const char* host, unsigned short port) {
    return Address(boost::asio::ip::address::from_string(host), port);
  }

  Address make_address(const string&& host, unsigned short port) {
    return Address(boost::asio::ip::address::from_string(host), port);
  }

  inline string addressHost(const Address& addr) { return get<0>(addr).to_string(); }
  inline string addressHost(Address&& addr) { return get<0>(std::forward<Address>(addr)).to_string(); }
  
  inline int addressPort(const Address& addr) { return get<1>(addr); }
  inline int addressPort(Address&& addr) { return get<1>(std::forward<Address>(addr)); }

  string addressAsString(const Address& addr) {
    return addressHost(addr) + ":" + to_string(addressPort(addr));
  }

  string addressAsString(Address&& addr) {
    return addressHost(std::forward<Address>(addr))
            + ":" + to_string(addressPort(std::forward<Address>(addr)));
  }

  Address internalSendAddress(const Address& addr) {
    return make_address(addressHost(addr), addressPort(addr)+1);
  }
  
  Address internalSendAddress(Address&& addr) {
    return make_address(addressHost(std::forward<Address>(addr)),
                        addressPort(std::forward<Address>(addr))+1);
  }

  Address externalSendAddress(const Address& addr) {
    return make_address(addressHost(addr), addressPort(addr)+2);
  }
  
  Address externalSendAddress(Address&& addr) {
    return make_address(addressHost(std::forward<Address>(addr)),
                        addressPort(std::forward<Address>(addr))+2);
  }

  Address defaultAddress = make_address("127.0.0.1", 40000);


  //-------------
  // Messages.

  class Message : public tuple<Address, Identifier, Value> {
  public:
    Message(Address addr, Identifier id, const Value& v)
      : tuple<Address, Identifier, Value>(std::move(addr), std::move(id), v)
    {}

    Message(Address addr, Identifier id, Value&& v)
      : tuple<Address, Identifier, Value>(std::move(addr), std::move(id), std::forward<Value>(v))
    {}

    Message(Address&& addr, Identifier&& id, Value&& v)
      : tuple<Address, Identifier, Value>(std::forward<Address>(addr),
                                          std::forward<Identifier>(id),
                                          std::forward<Value>(v))
    {}

    Address&    address()  { return get<0>(*this); }
    Identifier& id()       { return get<1>(*this); }
    Value&      contents() { return get<2>(*this); }
    string      target()   { return id() + "@" + addressAsString(address()); }
    const Address&    address()  const { return get<0>(*this); }
    const Identifier& id()       const { return get<1>(*this); }
    const Value&      contents() const { return get<2>(*this); }
    const string      target()   const { return id() + "@" + addressAsString(address()); }
  };

  //--------------------
  // System environment.

  // Literals are native values rather than an AST reprensentation as in Haskell.
  typedef any Literal;
  typedef map<Identifier, Literal> PeerBootstrap;
  typedef map<Address, PeerBootstrap> SystemEnvironment;

  list<Address> deployedNodes(const SystemEnvironment& sysEnv) {
    list<Address> r;
    for ( auto x : sysEnv ) { r.push_back(x.first); }
    return std::move(r);
  }

  bool isDeployedNode(const SystemEnvironment& sysEnv, Address addr) {
    return sysEnv.find(addr) != sysEnv.end();
  }

  //-------------
  // Logging.
  class Log {
  public:
    Log() {}
    Log(severity_level lvl) : defaultLevel(lvl) {}

    virtual void log(const string& msg) = 0;
    virtual void log(const char* msg) = 0;
    virtual void logAt(severity_level lvl, const string& msg) = 0;
    virtual void logAt(severity_level lvl, const char* msg) = 0;
  
  protected:
    severity_level defaultLevel;
  };

  class LogST : public severity_channel_logger<severity_level,string>, public Log
  {
  public:
    typedef severity_channel_logger<severity_level,string> logger;
    
    LogST(string chan) : logger(keywords::channel = chan), Log(severity_level::info) {}
    LogST(string chan, severity_level lvl) : logger(keywords::channel = chan), Log(lvl) {}
    
    void log(const string& msg) { BOOST_LOG_SEV(*this, defaultLevel) << msg; }
    void log(const char* msg) { BOOST_LOG_SEV(*this, defaultLevel) << msg; }
    
    void logAt(severity_level lvl, const string& msg) { BOOST_LOG_SEV(*this, lvl) << msg; }
    void logAt(severity_level lvl, const char& msg) { BOOST_LOG_SEV(*this, lvl) << msg; }
  };
  
  class LogMT : public severity_channel_logger_mt<severity_level,string>, public Log
  {
  public:
    typedef severity_channel_logger_mt<severity_level,string> logger;

    LogMT(string chan) : logger(keywords::channel = chan), Log(severity_level::info) {}
    LogMT(string chan, severity_level lvl) : logger(keywords::channel = chan), Log(lvl) {}

    void log(const string& msg) { BOOST_LOG_SEV(*this, defaultLevel) << msg; }
    void log(const char* msg) { BOOST_LOG_SEV(*this, defaultLevel) << msg; }

    void logAt(severity_level lvl, const string& msg) { BOOST_LOG_SEV(*this, lvl) << msg; }
    void logAt(severity_level lvl, const char* msg) { BOOST_LOG_SEV(*this, lvl) << msg; }
  };

  //--------------------
  // Wire descriptions

  // A generic exception that can be thrown by wire descriptor methods.
  class CodecException : public runtime_error {
  public:
    CodecException(const string& msg) : runtime_error(msg) {}
    CodecException(const char* msg) : runtime_error(msg) {}
  };

  // Message serializtion/deserialization abstract base class.
  // Implementations can encapsulate framing concerns as well as serdes operations.
  //
  // The unpack method may be supplied a complete or incomplete string corresponding
  // to a value. It is left to the implementation to determine the scope of functionality
  // supported, for example partial unpacking (e.g., for network sockets).
  // The semantics of repeated invocations are dependent on the actual implementation
  // of the wire description (including factors such as message loss). 
  // This includes the conditions under which an exception is thrown.

  class Codec: public virtual LogMT {
    public:
      Codec(): LogMT("Codec") {}

      virtual Value encode(const Value&) = 0;
      virtual shared_ptr<Value> decode(const Value&) = 0;
      virtual bool decode_ready() = 0;
      virtual bool good() = 0;
  };

  class DefaultCodec : public Codec, public virtual LogMT {
    public:
      DefaultCodec() : Codec(), LogMT("DefaultCodec"), good_(true) {}
      Value encode(const Value& v) { return v; }
      shared_ptr<Value> decode(const Value& v) { return std::make_shared<Value>(v); } 
      bool decode_ready() { return true; }
      bool good() { return good_; }
    
    protected:
      bool good_;
  };

  class InternalCodec: public Codec {
    public:
      InternalCodec() : LogMT("InternalCodec") {}

      virtual Message read_message(const Value&) = 0;
      virtual Value show_message(const Message&) = 0;
  };

  class DelimiterCodec : public Codec, public virtual LogMT {
    public:
      DelimiterCodec(char delimiter) 
        : Codec(), LogMT("DelimiterCodec"), delimiter_(delimiter), good_(true), buf_(new string())
      {}
      
      Value encode(const Value& v) { 
        string res = string(v);
        res.push_back(delimiter_);
        return res;
      }

      shared_ptr<Value> decode(const Value& v) { 
        // Append to buffer
        *buf_ = *buf_ + v;
        // Determine if there is a complete value in the buffer
        shared_ptr<Value> result = shared_ptr<Value>();
        size_t pos = find_delimiter();
        if (pos != std::string::npos) {
          // There is a complete value
          // Grab it from the buffer
          result = shared_ptr<string>(new string());
          *result = buf_->substr(0, pos); // ignore the delimiter at pos
          // Delete from the buffer
          *buf_ = buf_->substr(pos+1);
        }
        return result;
      }

      bool decode_ready() {
       return buf_? 
          find_delimiter() != std::string::npos : false;
      }

      bool good() { return good_; }

    protected:
      size_t find_delimiter() { return buf_->find(delimiter_); }
      char delimiter_;
      bool good_;
      shared_ptr<string> buf_;
  };

 class LengthHeaderCodec : public Codec, public virtual LogMT {
    public:
      LengthHeaderCodec()
        : Codec(), LogMT("LengthHeaderCodec"), good_(true), buf_(new string())
      {}

      Value encode(const Value& s) {
        // calculate size of encoded value
        fixed_int value_size = fixed_int(s.length());
        size_t header_size = sizeof(value_size);
        size_t enc_size = header_size + value_size;
        // pack data into a buffer
        char * buffer = new char[enc_size]();
        memcpy(buffer, &value_size, header_size);
        memcpy(buffer + header_size, s.c_str(), value_size);
        // copy into string and free buffer
        Value enc_v = string(buffer, enc_size);
        delete[] buffer;
        return enc_v;
      }

      shared_ptr<Value> decode(const Value& v) {
        // Append v to buffer
        *buf_ = *buf_ + v;
        if (!next_size_) {
          // See if there is enough data in buffer to unpack a header
          shared_ptr<fixed_int> value_size = read_header(*buf_);
          if (value_size) {
            next_size_ = value_size;
            // remove the header bytes from the buffer
            size_t header_size = sizeof(fixed_int);
            *buf_ = buf_->substr(header_size);
          }
          else {
            // failure: not enough data in buffer
            return nullptr;
          }
        }

        // Now that we know the size of the next incoming value
        // See if the buffer contains enough data to unpack
        if (decode_ready()) {
          // Unpack next value
          const char * bytes = buf_->c_str();
          shared_ptr<Value> result = shared_ptr<Value>(new string(bytes, *next_size_));
          // Setup for next round
          *buf_ = buf_->substr(*next_size_);
          next_size_.reset();
          return result;
        }
        else {
          // failure: not enough data in buffer
          return nullptr;
        }
      }

      bool decode_ready() {
       return next_size_?
         buf_->length() >= *next_size_ : false;
      }

      bool good() { return good_; }

    protected:
      size_t find_delimiter() { return buf_->find(delimiter_); }
      char delimiter_;
      bool good_;
      shared_ptr<fixed_int> next_size_;
      shared_ptr<string> buf_;

      shared_ptr<fixed_int> read_header(Value s) {
        size_t header_size = sizeof(fixed_int);
        if (s.length() < header_size) {
          // failure: input does not contain a full header
          return nullptr;
        }
        const char * bytes = s.c_str();
        fixed_int x = *bytes;
        return make_shared<fixed_int>(x);
      }
  };

  class DefaultInternalCodec : public InternalCodec, public virtual LogMT {
    public:
      DefaultInternalCodec() : InternalCodec(), LogMT("DefaultInternalCodec"), dc(DefaultCodec()) {}
      Value encode(const Value& v) { return dc.encode(v); }
      shared_ptr<Value> decode(const Value &v) { return dc.decode(v); }
      bool decode_ready() { return dc.decode_ready(); } 
      bool good() { return dc.good(); } 

      Message read_message(const Value& v) {
        // Values are of the form: "(Address, Identifier, Payload)"
        // Split value into components:
        static const boost::regex value_regex("\\( *(.+) *, *(.+) *, *(.+) *\\)");
        boost::cmatch value_match;
        if(boost::regex_match(v.c_str(), value_match, value_regex)){
          // Parse Address
          static const boost::regex address_regex("(.+):(.+)");
          boost::cmatch address_match;
          Address a;
          string temp = value_match[1];
          if(boost::regex_match(temp.c_str(), address_match, address_regex)) {
            string ip = address_match[1];
            temp = address_match[2];
            unsigned short port = (unsigned short) std::strtoul(temp.c_str(), NULL, 0);
            a = make_address(ip, port);
          }
          else {
            throw CodecException("Invalid Format for Value's Address: " + value_match[1]);
          }

          // Parse Identifier
          Identifier m = value_match[2];
          // Parse Payload
          Value payload = value_match[3];
          return Message(a,m,payload);
         }
        else {
          throw CodecException("Invalid Format for Value:" + v);
        }

      };
 
      Value show_message(const Message& m) {
        ostringstream os;
        os << "(" << addressAsString(m.address()) << "," << m.id() << "," << m.contents() << ")";
        return os.str();
      }
    protected:
      DefaultCodec dc;
  };

  using ExternalCodec = Codec;

}

#endif
// vim: set sw=2 ts=2 sts=2:
