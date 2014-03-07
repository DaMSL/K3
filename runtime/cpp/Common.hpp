#ifndef K3_RUNTIME_COMMON_H
#define K3_RUNTIME_COMMON_H

#include <list>
#include <map>
#include <memory>
#include <stdexcept>
#include <tuple>
#include <utility>
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

namespace K3 {

  using namespace std;
  using boost::any;

  using namespace boost::log;
  using namespace boost::log::sources;
  using namespace boost::log::trivial;
  using namespace boost::phoenix;

  typedef string Identifier;
  typedef string Value;

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

  template<typename Value>
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
  class WireDescException : public runtime_error {
  public:
    WireDescException(const string& msg) : runtime_error(msg) {}
    WireDescException(const char* msg) : runtime_error(msg) {}
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
  template<typename T> 
  class WireDesc : public virtual LogMT {
    public:
      WireDesc() : LogMT("WireDesc") {}
      virtual string pack(const T& payload) = 0;
      virtual shared_ptr<T> unpack(const string& message) = 0;
  };

  class DefaultWireDesc : public WireDesc<string> {
    public:
      DefaultWireDesc() : LogMT("DefaultWireDesc") {}
      string pack(const string& payload) { return payload; }
      shared_ptr<string> unpack(const string& message) { return shared_ptr<string>(new string(message)); }
  };

  template <class T>
  class BoostWireDesc : public virtual LogMT {
    public:
      BoostWireDesc() : LogMT("BoostWireDesc") {}

      static string pack(const T& payload) {
          ostringstream out_sstream;
          boost::archive::text_oarchive out_archive(out_sstream);
          out_archive << payload;
          return out_sstream.str();
      }

      static shared_ptr<T> unpack(const string& message) {
          istringstream in_sstream(message);
          boost::archive::text_iarchive in_archive(in_sstream);

          shared_ptr<T> p;
          in_archive >> *p;
          return p;
      }
  };

  /*
  template <template<class> class F>
  class WireDesc : public virtual LogMT {
      public:
          WireDesc() : LogMT("WireDesc") {}

          template <class T>
          string pack(const T& payload) {
              return F<T>::pack(payload);
          }

          template <class T>
          shared_ptr<T> unpack(const string& message) {
              return F<T>::unpack(message);
          }
  };

  template <class T>
  class BoostWireDesc : public WireDesc<BoostWireDesc> {
      public:
          BoostWireDesc() : WireDesc(), LogMT("BoostWireDesc") {}

          static string pack(const T& payload) {
              ostringstream out_sstream;
              boost::archive::text_oarchive out_archive(out_sstream);
              out_archive << payload;
              return out_sstream.str();
          }

          static shared_ptr<T> unpack(const string& message) {
              istringstream in_sstream(message);
              boost::archive::text_iarchive in_archive(in_sstream);

              shared_ptr<T> p;
              in_archive >> *p;
              return p;
          }
  };
  */

  // TODO: protobuf, msgpack, json WireDesc implementations.  

}

#endif
