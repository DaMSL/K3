#ifndef K3_RUNTIME_COMMON_H
#define K3_RUNTIME_COMMON_H

#include <list>
#include <map>
#include <tuple>
#include <boost/any.hpp>
#include <boost/log/core.hpp>
#include <boost/log/sources/record_ostream.hpp>
#include <boost/log/sources/severity_channel_logger.hpp>
#include <boost/log/sources/severity_feature.hpp>
#include <boost/log/trivial.hpp>
#include <boost/phoenix/core.hpp>
#include <boost/phoenix/stl/algorithm.hpp>

namespace K3 {

  using namespace std;
  using namespace boost;

  using namespace boost::log;
  using namespace boost::log::sources;
  using namespace boost::log::trivial;
  using namespace boost::phoenix;

  typedef string Identifier;
  typedef tuple<string, int> Address;

  //---------------
  // Addresses.
  Address defaultAddress("127.0.0.1", 40000);

  string addressHost(Address& addr) { return get<0>(addr); }
  int    addressPort(Address& addr) { return get<1>(addr); }
  
  string addressAsString(Address& addr) {
    return addressHost(addr) + to_string(addressPort(addr));
  }

  Address internalSendAddress(Address addr) {
    return Address(addressHost(addr), addressPort(addr)+1);
  }

  Address externalSendAddress(Address addr) {
    return Address(addressHost(addr), addressPort(addr)+2);
  }

  //-------------
  // Messages.
  template<typename Value>
  class Message : public tuple<Address, Identifier, Value> {
  public:
    Message(Address addr, Identifier id, Value v)
      : tuple<Address, Identifier, Value>(addr, id, v)
    {}
    
    Address    address()  { return get<0>(*this); }
    Identifier id()       { return get<1>(*this); }
    Value      contents() { return get<2>(*this); }
  };

  //--------------------
  // Wire descriptions

  // TODO: FrameDesc
  class FrameDesc;

  template<typename Value>
  class WireDesc {
  public:
    WireDesc() {}
    virtual string pack(Value& v)    = 0;
    virtual Value  unpack(string& s) = 0;
    virtual FrameDesc frame()        = 0;
  };

  // TODO: protobuf, msgpack, json WireDesc implementations.  

  //--------------------
  // System environment.

  // Literals are native values rather than an AST reprensentation as in Haskell.
  typedef any Literal;
  typedef map<Identifier, Literal> PeerBootstrap;
  typedef map<Address, PeerBootstrap> SystemEnvironment;

  list<Address> deployedNodes(SystemEnvironment& sysEnv) {
    list<Address> r;
    for ( auto x : sysEnv ) { r.push_back(x.first); }
    return r;
  }

  //-------------
  // Logging.
  class Log {
  public:
    Log() {}
    Log(severity_level lvl) : defaultLevel(lvl) {}

    virtual void log(string& msg) = 0;
    virtual void logAt(string& msg, severity_level lvl) = 0;
  
  protected:
    severity_level defaultLevel;
  };

  class LogST : public severity_channel_logger<severity_level,string>, public Log
  {
  public:
    typedef severity_channel_logger<severity_level,string> logger;
    
    LogST(string chan) : logger(keywords::channel = chan), Log(severity_level::info) {}
    LogST(string chan, severity_level lvl) : logger(keywords::channel = chan), Log(lvl) {}
    
    void log(string& msg) { BOOST_LOG_SEV(*this, defaultLevel) << msg; }
    void logAt(string& msg, severity_level lvl) { BOOST_LOG_SEV(*this, lvl) << msg; }
  };
  
  class LogMT : public severity_channel_logger_mt<severity_level,string>, public Log
  {
  public:
    typedef severity_channel_logger_mt<severity_level,string> logger;

    LogMT(string chan) : logger(keywords::channel = chan), Log(severity_level::info) {}
    LogMT(string chan, severity_level lvl) : logger(keywords::channel = chan), Log(lvl) {}

    void log(string& msg) { BOOST_LOG_SEV(*this, defaultLevel) << msg; }
    void logAt(string& msg, severity_level lvl) { BOOST_LOG_SEV(*this, lvl) << msg; }
  };

}

#endif