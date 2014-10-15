#ifndef K3_RUNTIME_COMMON_H
#define K3_RUNTIME_COMMON_H

#include <cstdint>
#include <list>
#include <vector>
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
#include <boost/log/core.hpp>
#include <boost/log/sources/record_ostream.hpp>
#include <boost/log/sources/severity_channel_logger.hpp>
#include <boost/log/sources/severity_feature.hpp>
#include <boost/log/trivial.hpp>
#include <boost/regex.hpp>

namespace K3 {

  template <class r> using F = std::function<r>;
  typedef std::string Identifier;
  typedef int TriggerId;
  typedef std::string Value;

  typedef std::string Value;
  typedef std::string EValue;
  typedef std::string IValue;

  typedef uint32_t fixed_int;

  typedef std::tuple<boost::asio::ip::address, unsigned short> Address;

  enum class Builtin { Stdin, Stdout, Stderr };
  enum class IOMode  { Read, Write, Append, ReadWrite };

  class unit_t {
    public:
    template <class archive>
    void serialize(archive&, const unsigned int) {}
    bool operator==(const unit_t& r) const { return true; }
    bool operator!=(const unit_t& r) const { return false; }
    bool operator<(const unit_t& r) const { return false; }
    bool operator>(const unit_t& r) const { return false; }
  };

  //---------------
  // Addresses.

  static inline Address make_address(const std::string& host, unsigned short port)   {
    return Address(boost::asio::ip::address::from_string(host), port);
  }

  static inline Address make_address(const char* host, unsigned short port) {
    return Address(boost::asio::ip::address::from_string(host), port);
  }

  static inline Address make_address(const std::string&& host, unsigned short port)  {
    return Address(boost::asio::ip::address::from_string(host), port);
  }

  inline std::string addressHost(const Address& addr) { return std::get<0>(addr).to_string(); }
  inline std::string addressHost(Address&& addr) { return std::get<0>(std::forward<Address>(addr)).to_string(); }

  inline int addressPort(const Address& addr) { return std::get<1>(addr); }
  inline int addressPort(Address&& addr) { return std::get<1>(std::forward<Address>(addr)); }

  static inline std::string addressAsString(const Address& addr) {
    return addressHost(addr) + ":" + std::to_string(addressPort(addr));
  }

  static inline std::string addressAsString(Address&& addr) {
    return addressHost(std::forward<Address>(addr))
            + ":" + std::to_string(addressPort(std::forward<Address>(addr)));
  }

  static inline Address internalSendAddress(const Address& addr) {
    return make_address(addressHost(addr), addressPort(addr)+1);
  }

  static inline Address internalSendAddress(Address&& addr) {
    return make_address(addressHost(std::forward<Address>(addr)),
                        addressPort(std::forward<Address>(addr))+1);
  }

  static inline Address externalSendAddress(const Address& addr) {
    return make_address(addressHost(addr), addressPort(addr)+2);
  }

  static inline Address externalSendAddress(Address&& addr) {
    return make_address(addressHost(std::forward<Address>(addr)),
                        addressPort(std::forward<Address>(addr))+2);
  }

  // TODO put the definition somewhere
  static Address defaultAddress = make_address("127.0.0.1", 40000);


  //--------------------
  // System environment.

  // Literals are native values rather than an AST reprensentation as in Haskell.
  typedef boost::any Literal;
  typedef std::map<Identifier, Literal> PeerBootstrap;
  typedef std::map<Address, PeerBootstrap> SystemEnvironment;

  static inline SystemEnvironment defaultEnvironment(Address addr) {
    PeerBootstrap bootstrap = PeerBootstrap();
    SystemEnvironment s_env = SystemEnvironment();
    s_env[addr] = bootstrap;
    return s_env;
  }

  static inline SystemEnvironment defaultEnvironment(std::list<Address> addrs) {
    SystemEnvironment s_env;
    for (Address addr : addrs) {
      PeerBootstrap bootstrap = PeerBootstrap();
      s_env[addr] = bootstrap;
    }
    return s_env;
  }

  static inline SystemEnvironment defaultEnvironment() {
    return defaultEnvironment(defaultAddress);
  }

  static inline std::list<Address> deployedNodes(const SystemEnvironment& sysEnv) {
    std::list<Address> r;
    for (auto x : sysEnv) { r.push_back(x.first); }
    return std::move(r);
  }

  static inline bool isDeployedNode(const SystemEnvironment& sysEnv, Address addr) {
    return sysEnv.find(addr) != sysEnv.end();
  }

  //-------------
  // Logging.
  class Log {
  public:
    Log() {}
    Log(boost::log::trivial::severity_level lvl) : defaultLevel(lvl) {}

    virtual void log(const std::string& msg) = 0;
    virtual void log(const char* msg) = 0;
    virtual void logAt(boost::log::trivial::severity_level lvl, const std::string& msg) = 0;
    virtual void logAt(boost::log::trivial::severity_level lvl, const char* msg) = 0;

  protected:
    boost::log::trivial::severity_level defaultLevel;
  };

  class LogST : public boost::log::sources::severity_channel_logger<boost::log::trivial::severity_level,std::string>, public Log
  {
  public:
    typedef severity_channel_logger<boost::log::trivial::severity_level,std::string> logger;

    LogST(std::string chan) : logger(boost::log::keywords::channel = chan), Log(boost::log::trivial::severity_level::info) {}
    LogST(std::string chan, boost::log::trivial::severity_level lvl) : logger(boost::log::keywords::channel = chan), Log(lvl) {}

    void log(const std::string& msg) { BOOST_LOG_SEV(*this, defaultLevel) << msg; }
    void log(const char* msg) { BOOST_LOG_SEV(*this, defaultLevel) << msg; }

    void logAt(boost::log::trivial::severity_level lvl, const std::string& msg) { BOOST_LOG_SEV(*this, lvl) << msg; }
    void logAt(boost::log::trivial::severity_level lvl, const char& msg) { BOOST_LOG_SEV(*this, lvl) << msg; }
  };

  class LogMT : public boost::log::sources::severity_channel_logger_mt<boost::log::trivial::severity_level,std::string>, public Log
  {
  public:
    typedef severity_channel_logger_mt<boost::log::trivial::severity_level,std::string> logger;

    LogMT(std::string chan) : logger(boost::log::keywords::channel = chan), Log(boost::log::trivial::severity_level::info) {}
    LogMT(std::string chan, boost::log::trivial::severity_level lvl) : logger(boost::log::keywords::channel = chan), Log(lvl) {}

    void log(const std::string& msg) { BOOST_LOG_SEV(*this, defaultLevel) << msg; }
    void log(const char* msg) { BOOST_LOG_SEV(*this, defaultLevel) << msg; }

    void logAt(boost::log::trivial::severity_level lvl, const std::string& msg) { BOOST_LOG_SEV(*this, lvl) << msg; }
    void logAt(boost::log::trivial::severity_level lvl, const char* msg) { BOOST_LOG_SEV(*this, lvl) << msg; }
  }; // Class LogMT

} // namespace K3

#endif
