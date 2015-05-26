#ifndef K3_COMMON
#define K3_COMMON

// Common contains common declarations and utilities for the K3 Runtime.

#include <vector>
#include <tuple>
#include <memory>
#include <map>
#include <set>
#include <string>

#include "Yaml.hpp"

#include "boost/asio.hpp"
#include "boost/thread/mutex.hpp"
#include "boost/thread/externally_locked.hpp"
#include "boost/thread/lockable_adapter.hpp"

using std::string;
using std::shared_ptr;
using std::make_shared;
using std::list;
using std::map;
using std::vector;
namespace asio = boost::asio;
typedef const boost::system::error_code& boost_error;

typedef int TriggerID;
typedef std::vector<char> Buffer;

typedef std::tuple<unsigned long, unsigned short> Address;

typedef std::string Identifier; 

// TODO(jbw) move to Message.hpp
class MessageHeader {
 public:
  MessageHeader() {}
  MessageHeader(Address src, Address dest, TriggerID trig) {
    source_ = src;
    destination_ = dest;
    trigger_ = trig;
  }

  Address source() const {
    return source_;
  }

  Address destination() const {
    return destination_;
  }

  TriggerID trigger() const {
    return trigger_;
  }

  // TODO(jbw) make protected and expose references
  Address source_;
  Address destination_;
  TriggerID trigger_;
};

inline Address make_address(const std::string& host, unsigned short port) {
  return Address(boost::asio::ip::address::from_string(host).to_v4().to_ulong(), port);
}

inline Address make_address(unsigned long host, unsigned short port) {
  return Address(host, port);
}

inline Address make_address(const YAML::Node& node) {
  unsigned long ip = boost::asio::ip::address::from_string(node["me"][0].as<std::string>())
                     .to_v4().to_ulong();
  unsigned short port = node["me"][1].as<unsigned short>();
  return Address(ip, port);
}

enum class CodecFormat { BoostBinary };

class EndOfProgramException: public std::runtime_error {
 public:
  EndOfProgramException() : runtime_error("Peer terminated.") { }
};

// Thread-safe map from Key to shared_ptr<Value>
template <class Key, class Val>
class ConcurrentMap : public boost::basic_lockable_adapter<boost::mutex> {
 public:
  ConcurrentMap() : boost::basic_lockable_adapter<boost::mutex>(), map_(*this) { }

  void insert(const Key& key, Val v) {
    boost::strict_lock<ConcurrentMap<Key, Val>> lock(*this);
    map_.get(lock)[key] = v;
  }

  Val lookup(const Key& key) {
    boost::strict_lock<ConcurrentMap<Key, Val>> lock(*this);
    Val result;
    auto it = map_.get(lock).find(key);
    if (it != map_.get(lock).end()) {
      result = it->second;
    }
    return result;
  }

 protected:
  boost::externally_locked<std::map<Key, Val>, ConcurrentMap<Key, Val>> map_;
};

// Thread-safe set of Val
template <class Val>
class ConcurrentSet : public boost::basic_lockable_adapter<boost::mutex> {
 public:
  ConcurrentSet() : set_(*this) { }

  // TODO(jbw) move overload
  void insert(const Val& value) {
    boost::strict_lock<ConcurrentSet<Val>> lock(*this);
    set_.get(lock).insert(value);
  }

  void erase(const Val& value) {
    boost::strict_lock<ConcurrentSet<Val>> lock(*this);
    set_.get(lock).erase(value);
  }

  int size() {
    boost::strict_lock<ConcurrentSet<Val>> lock(*this);
    return set_.get(lock).size();
  }

 protected:
  boost::externally_locked<std::set<Val>, ConcurrentSet<Val>> set_;
};

#endif
