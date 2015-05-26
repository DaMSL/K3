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

namespace K3 {

using std::string;
using std::shared_ptr;
using std::make_shared;
using std::weak_ptr;
using std::list;
using std::map;
using std::vector;
using std::enable_shared_from_this;

namespace asio = boost::asio;
typedef const boost::system::error_code& boost_error;

typedef int TriggerID;
typedef std::vector<char> Buffer;

typedef std::tuple<unsigned long, unsigned short> Address;

class unit_t {
 public:
  template <class archive>
  void serialize(archive&, const unsigned int) {}

  template <class archive>
  void serialize(archive&) {}

  bool operator==(const unit_t&) const {
    return true;
  }
  bool operator!=(const unit_t&) const {
    return false;
  }
  bool operator<(const unit_t&) const {
    return false;
  }
  bool operator>(const unit_t&) const {
    return false;
  }
};

#ifndef K3_R_elem
#define K3_R_elem

template <class _T0>
class R_elem {
 public:
  R_elem() {}
  R_elem(_T0 _elem): elem(_elem) {}

  bool operator==(const R_elem& _r) const {
    if (elem == _r.elem)
      return true;
    return false;
  }

  bool operator!=(const R_elem& _r) const {
    return !(*this == _r);
  }

  bool operator<(const R_elem& _r) const {
    return elem < _r.elem;
  }

  template <class archive>
  void serialize(archive& _archive,const unsigned int) {
    _archive & BOOST_SERIALIZATION_NVP(elem);
  }

  template <class archive>
  void serialize(archive& _archive) {
    _archive & elem;
  }
  _T0 elem;
};
#endif // K3_R_elem

#ifndef K3_R_key_value
#define K3_R_key_value

template <class _T0, class _T1>
class R_key_value {
 public:
  typedef _T0 KeyType;
  typedef _T1 ValueType;
  R_key_value(): key(), value()  {}
  template <class __T0, class __T1>
  R_key_value(__T0&& _key, __T1&& _value): key(std::forward<__T0>(_key)), value(std::forward<__T1>(_value))  {}
  template <class archive>
  void serialize(archive& _archive, const unsigned int)  {
    _archive & BOOST_SERIALIZATION_NVP(key);
    _archive & BOOST_SERIALIZATION_NVP(value);
  }
  template <class archive>
  void serialize(archive& _archive)  {
    _archive & key;
    _archive & value;
  }
  bool operator==(const R_key_value<_T0, _T1>& __other) const {
    return key == (__other.key) && value == (__other.value);
  }
  bool operator!=(const R_key_value<_T0, _T1>& __other) const {
    return std::tie(key, value) != std::tie(__other.key, __other.value);
  }
  bool operator<(const R_key_value<_T0, _T1>& __other) const {
    return std::tie(key, value) < std::tie(__other.key, __other.value);
  }
  bool operator>(const R_key_value<_T0, _T1>& __other) const {
    return std::tie(key, value) > std::tie(__other.key, __other.value);
  }
  bool operator<=(const R_key_value<_T0, _T1>& __other) const {
    return std::tie(key, value) <= std::tie(__other.key, __other.value);
  }
  bool operator>=(const R_key_value<_T0, _T1>& __other) const {
    return std::tie(key, value) >= std::tie(__other.key, __other.value);
  }
  _T0 key;
  _T1 value;
};
#endif // K3_R_key_value




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
  unsigned long ip = boost::asio::ip::address::from_string(node[0].as<std::string>()).to_v4().to_ulong();
  unsigned short port = node[1].as<unsigned short>();
  return Address(ip, port);
}

enum class CodecFormat { BoostBinary };

class EndOfProgramException: public std::runtime_error {
 public:
  EndOfProgramException() : runtime_error("Peer terminated.") { }
};

// Thread-safe map from Key to Val.
// Val should be a pointer type.
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

  void erase(const Key& key) {
    boost::strict_lock<ConcurrentMap<Key, Val>> lock(*this);
    map_.get(lock).erase(key);
    return;
  }

  int size() {
    boost::strict_lock<ConcurrentMap<Key, Val>> lock(*this);
    return map_.get(lock).size();
  }

 protected:
  boost::externally_locked<std::map<Key, Val>, ConcurrentMap<Key, Val>> map_;
};

}  // namespace K3

#endif
