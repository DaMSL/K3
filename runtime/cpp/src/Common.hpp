#ifndef K3_COMMON
#define K3_COMMON

// Common contains common declarations and utilities for the K3 Runtime.

#include <list>
#include <vector>
#include <tuple>
#include <memory>
#include <map>
#include <set>
#include <string>

#include "boost/asio.hpp"
#include "boost/thread/mutex.hpp"
#include "boost/thread/externally_locked.hpp"
#include "boost/thread/lockable_adapter.hpp"
#include "boost/serialization/nvp.hpp"
#include "boost/serialization/array.hpp"
#include "boost/functional/hash.hpp"
#include "serialization/YAS.hpp"

#define _F(X) (std::forward<decltype((X))>(X))

namespace K3 {
  #ifdef BSL_ALLOC
    #ifdef BLOCAL 
    constexpr size_t lsz = 2<<14;
    #endif
  #endif

using std::string;
using std::shared_ptr;
using std::get;
using std::make_shared;
using std::weak_ptr;
using std::list;
using std::map;
using std::vector;
using std::enable_shared_from_this;
using std::unique_ptr;
using std::make_unique;

#define K3_LOG_ID "K3"
#define USE_CUSTOM_HASHMAPS 1
#define HAS_LIBDYNAMIC 1
#define K3_INTERNAL_FORMAT CodecFormat::YASBinary

class TriggerStatistics {
 public:
  std::string trig_id;
  int total_count;
  std::chrono::nanoseconds total_time;
};

namespace asio = boost::asio;
typedef const boost::system::error_code& boost_error;

typedef int TriggerID;
typedef std::vector<char> Buffer;
class ProgramContext;
typedef std::function<shared_ptr<ProgramContext>()> ContextFactory;

struct Address {
 public:
  Address() : ip(0), port(0) {}
  Address(unsigned long i, unsigned short p) : ip(i), port(p) {}

  bool operator==(const Address& other) const {
    return (ip == other.ip) && (port == other.port);
  }
  bool operator!=(const Address& other) const {
    return (ip != other.ip) || (port != other.port);
  }
  bool operator<=(const Address& other) const {
    return std::tie(ip, port) <= std::tie(other.ip, other.port);
  }
  bool operator<(const Address& other) const {
    return std::tie(ip, port) < std::tie(other.ip, other.port);
  }
  bool operator>=(const Address& other) const {
    return std::tie(ip, port) >= std::tie(other.ip, other.port);
  }
  bool operator>(const Address& other) const {
    return std::tie(ip, port) > std::tie(other.ip, other.port);
  }

  string toString() const {
    auto p1 = asio::ip::address_v4(ip).to_string();
    auto p2 = std::to_string(port);
    return p1 + ":" + p2;
  }

  template <class archive>
  void serialize(archive& ar) const {
    const char* buf = reinterpret_cast<const char*>(&ip);
    ar.write(buf, sizeof(ip));
    ar& port;
  }

  template <class Archive>
  void serialize(Archive& ar) {
    char* buf = reinterpret_cast<char*>(&ip);
    ar.read(buf, sizeof(ip));
    ar& port;
  }

  template <class Archive>
  void serialize(Archive& ar, const unsigned int) {
    char* buf = reinterpret_cast<char*>(&ip);
    ar& boost::serialization::make_array(buf, sizeof(ip));
    ar& port;
  }

  template <class T>
  Address& internalize(T& internalizer) {
    internalizer.internalize(ip);
    internalizer.internalize(port);
  }

  template <class T>
  Address& externalize(T& externalizer) {
    externalizer.externalize(ip);
    externalizer.externalize(port);
  }

  unsigned long ip;
  unsigned short port;
};


class unit_t {
 public:
  template <class archive>
  void serialize(archive&, const unsigned int) {}

  template <class archive>
  void serialize(archive&) {}

  bool operator==(const unit_t&) const { return true; }
  bool operator!=(const unit_t&) const { return false; }
  bool operator<(const unit_t&) const { return false; }
  bool operator>(const unit_t&) const { return false; }
};

typedef std::string Identifier;

inline Address make_address(const std::string& host, unsigned short port) {
  return Address(boost::asio::ip::address::from_string(host).to_v4().to_ulong(),
                 port);
}

inline Address make_address(unsigned long host, unsigned short port) {
  return Address(host, port);
}

inline std::string addressAsString(const Address& addr) {
  return addr.toString();
}

enum class CodecFormat { BoostBinary, CSV, PSV, YASBinary, Raw };
enum class StorageFormat { Binary, Text };
enum class IOMode { Read, Write };

IOMode getIOMode(const string& s);

static inline std::string currentTime() {
  std::chrono::nanoseconds ns =
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::high_resolution_clock::now().time_since_epoch());
  return std::to_string(ns.count());
}

class EndOfProgramException : public std::runtime_error {
 public:
  EndOfProgramException() : runtime_error("Peer terminated.") {}
};

// Thread-safe map from Key to Val.
// Val should be a pointer type.
template <class Key, class Val, typename Compare = std::less<Key>>
class ConcurrentMap : public boost::basic_lockable_adapter<boost::mutex> {
  using Container = std::map<Key, Val, Compare>;
  using CContainer = ConcurrentMap<Key, Val, Compare>;

 public:
  ConcurrentMap()
      : boost::basic_lockable_adapter<boost::mutex>(), map_(*this) {}

  void insert(const Key& key, const Val& v) {
    boost::strict_lock<CContainer> lock(*this);
    map_.get(lock)[key] = v;
  }

  void insert(const Key& key, Val&& v) {
    boost::strict_lock<CContainer> lock(*this);
    map_.get(lock)[key] = std::move(v);
  }

  void insert(Key&& key, Val&& v) {
    boost::strict_lock<CContainer> lock(*this);
    map_.get(lock)[std::move(key)] = std::move(v);
  }

  Val lookup(const Key& key) {
    boost::strict_lock<CContainer> lock(*this);
    Val result;
    auto it = map_.get(lock).find(key);
    if (it != map_.get(lock).end()) {
      result = it->second;
    }
    return result;
  }

  template<typename F>
  void apply(F f) {
    boost::strict_lock<CContainer> lock(*this);
    auto& map = map_.get(lock);
    f(map);
  }

  template<typename F>
  void apply(const Key& key, F f) {
    boost::strict_lock<CContainer> lock(*this);
    auto it = map_.get(lock).find(key);
    if (it != map_.get(lock).end()) {
      f(it->second);
    }
  }

  void erase(const Key& key) {
    boost::strict_lock<CContainer> lock(*this);
    map_.get(lock).erase(key);
    return;
  }

  int size() {
    boost::strict_lock<CContainer> lock(*this);
    return map_.get(lock).size();
  }

 protected:
  boost::externally_locked<Container, CContainer> map_;
};

// A hashtable-backed concurrent map.
template <class Key, class Val, typename Hash = std::hash<Key>, typename Equal = std::equal_to<Key>>
class ConcurrentHashMap : public boost::basic_lockable_adapter<boost::mutex>
{
  using Container = std::unordered_map<Key, Val, Hash, Equal>;
  using CContainer = ConcurrentHashMap<Key, Val, Hash, Equal>;

 public:
  ConcurrentHashMap()
      : boost::basic_lockable_adapter<boost::mutex>(), map_(*this) {}

  void insert(const Key& key, const Val& v) {
    boost::strict_lock<CContainer> lock(*this);
    map_.get(lock)[key] = v;
  }

  void insert(const Key& key, Val&& v) {
    boost::strict_lock<CContainer> lock(*this);
    map_.get(lock)[key] = std::move(v);
  }

  void insert(Key&& key, Val&& v) {
    boost::strict_lock<CContainer> lock(*this);
    map_.get(lock)[std::move(key)] = std::move(v);
  }

  Val lookup(const Key& key) {
    boost::strict_lock<CContainer> lock(*this);
    Val result;
    auto it = map_.get(lock).find(key);
    if (it != map_.get(lock).end()) {
      result = it->second;
    }
    return result;
  }

  template<typename F>
  void apply(F f) {
    boost::strict_lock<CContainer> lock(*this);
    auto& map = map_.get(lock);
    f(map);
  }

  template<typename F>
  void apply(const Key& key, F f) {
    boost::strict_lock<CContainer> lock(*this);
    auto it = map_.get(lock).find(key);
    if (it != map_.get(lock).end()) {
      f(it->second);
    }
  }

  void erase(const Key& key) {
    boost::strict_lock<CContainer> lock(*this);
    map_.get(lock).erase(key);
    return;
  }

  int size() {
    boost::strict_lock<CContainer> lock(*this);
    return map_.get(lock).size();
  }

 protected:
  boost::externally_locked<Container, CContainer> map_;
};

// Fast lock based on no sleep: every operation must be short
// Supports NUM-1 readers, and 1 writer
class FastLock {
  public:
  FastLock() : lock(0), intent(0) {}
  const int NUM = 5000;
  void read_lock() {
    // Try to take the intent. If anyone is trying to write, we can't take intent
    while (intent.fetch_add(1) < 0) {
      --intent;
    }
    --lock;
  }
  void read_unlock() {
    ++lock;
    ++intent;
  }
  void write_lock() {
    // Announce intent to lock. Can only do so if nobody else has write-locked
    while (intent.fetch_sub(NUM) < 0) {
      intent += NUM;
    }
    // All readers and other writers should exit now
    while (lock.fetch_sub(NUM) != 0) {
      lock += NUM;
    }
    lock -= NUM; // Take write lock
  }
  void write_unlock() {
    lock += NUM;
    intent += NUM;
  }

  protected:
  std::atomic_int intent;
  std::atomic_int lock;
};

class FastLockReader {
  public:
  FastLockReader(FastLock &f) : lock_(f) { lock_.read_lock(); }
  ~FastLockReader() { if (release_) lock_.read_unlock(); }
  void release() {
    release_ = false;
    lock_.read_unlock();
  }
  protected:
  bool release_ = true;
  FastLock& lock_;
};
class FastLockWriter {
  public:
  FastLockWriter(FastLock &f) : lock_(f) { lock_.write_lock(); }
  ~FastLockWriter() { if (release_) lock_.write_unlock(); }
  void release() {
    release_ = false;
    lock_.write_unlock();
  }
  protected:
  bool release_ = true;
  FastLock& lock_;
};

// A fast hashtable-based lock
// Only useful for really fast operations that require no sleep
template <class Key, class Val, typename Hash = std::hash<Key>, typename Equal = std::equal_to<Key>>
class FastConcurrentHashMap
{
  using Container = std::unordered_map<Key, Val, Hash, Equal>;

 public:
  FastConcurrentHashMap() {}

  void insert(Key&& key, Val&& v) {
    FastLockWriter flw(lock_);
    map_[std::forward<Key>(key)] = std::forward<Val>(v);
  }

  // Has to copy out of the map to allow for concurrency
  std::pair<Val,bool> lookup(const Key& key) {
    FastLockReader flr(lock_);
    auto it = map_.find(key);
    if (it != map_.end()) {
      return {it->second, true};
    } else {
      Val result;
      return {result, false};
    }
  }

  // Has to copy out of the map to allow for concurrency
  template <class F>
  Val lookupOrNew(Key&& key, F fn) {
    FastLockReader flr(lock_);
    auto it = map_.find(key);
    if (it != map_.end()) {
      return it->second;
    } else {
      flr.release();
      FastLockWriter flw(lock_);
      Val result = fn();
      map_[std::forward<Key>(key)] = result;
      return result;
    }
  }

  void erase(const Key& key) {
    FastLockWriter flw(lock_);
    map_.erase(key);
  }

  int size() {
    FastLockReader flr(lock_);
    return map_.size();
  }

 protected:
  Container map_;
  FastLock lock_;
};

class base_string;
typedef base_string string_impl;  // Toggle string implementations

}  // namespace K3

template <class T>
inline void hash_combine(std::size_t& seed, const T& v) {
  std::hash<T> hasher;
  seed ^= hasher(v) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
}

// TODO(jbw) hash
#ifndef K3_R_elem
#define K3_R_elem

template <class _T0>
class R_elem {
 public:
  R_elem() {}
  R_elem(_T0 const& _elem): elem(_elem)  {}
  R_elem(_T0&& _elem): elem(std::move(_elem))  {}

  bool operator==(R_elem<_T0> const& __other) const {
    return elem == __other.elem;
  }
  bool operator!=(R_elem<_T0> const& __other) const {
    return std::tie(elem) != std::tie(__other.elem);
  }
  bool operator<(R_elem<_T0> const& __other) const {
    return std::tie(elem) < std::tie(__other.elem);
  }
  bool operator>(R_elem<_T0> const& __other) const {
    return std::tie(elem) > std::tie(__other.elem);
  }
  bool operator<=(R_elem<_T0> const& __other) const {
    return std::tie(elem) <= std::tie(__other.elem);
  }
  bool operator>=(R_elem<_T0> const& __other) const {
    return std::tie(elem) >= std::tie(__other.elem);
  }

  template <class archive>
  void serialize(archive& _archive, const unsigned int) {
    _archive& BOOST_SERIALIZATION_NVP(elem);
  }

  template <class archive>
  void serialize(archive& _archive) {
    _archive& elem;
  }

  template <class T>
  R_elem<_T0>& internalize(T& arg)  {
    arg.internalize(elem);
    return *this;
  }

  template <class T>
  R_elem<_T0>& externalize(T& arg)  {
    arg.externalize(elem);
    return *this;
  }

  _T0 elem;
};
#endif  // K3_R_elem

#ifndef K3_R_i
#define K3_R_i
template <class _T0>
class R_i {
  public:
      R_i(): i()  {}
      R_i(const _T0& _i): i(_i)  {}
      R_i(_T0&& _i): i(std::move(_i))  {}
      bool operator==(const R_i<_T0>& __other) const {
        return i == __other.i;
      }
      bool operator!=(const R_i<_T0>& __other) const {
        return std::tie(i) != std::tie(__other.i);
      }
      bool operator<(const R_i<_T0>& __other) const {
        return std::tie(i) < std::tie(__other.i);
      }
      bool operator>(const R_i<_T0>& __other) const {
        return std::tie(i) > std::tie(__other.i);
      }
      bool operator<=(const R_i<_T0>& __other) const {
        return std::tie(i) <= std::tie(__other.i);
      }
      bool operator>=(const R_i<_T0>& __other) const {
        return std::tie(i) >= std::tie(__other.i);
      }
      template <class archive>
      void serialize(archive& _archive, const unsigned int _version)  {
        _archive & BOOST_SERIALIZATION_NVP(i);
      }
      template <class archive>
      void serialize(archive& _archive)  {
        _archive & i;
      }
      template <class T>
      R_i<_T0>& internalize(T& arg)  {
        arg.internalize(i);
        return *this;
      }
      template <class T>
      R_i<_T0>& externalize(T& arg)  {
        arg.externalize(i);
        return *this;
      }
      _T0 i;
};
#endif // K3_R_i

#ifndef K3_R_key_value
#define K3_R_key_value

template <class _T0, class _T1>
class R_key_value {
 public:
  typedef _T0 KeyType;
  typedef _T1 ValueType;
  R_key_value() : key(), value() {}
  template <class __T0, class __T1>
  R_key_value(__T0&& _key, __T1&& _value)
      : key(std::forward<__T0>(_key)), value(std::forward<__T1>(_value)) {}
  template <class archive>
  void serialize(archive& _archive) {
    _archive& key;
    _archive& value;
  }
  template <class archive>
  void serialize(archive& _archive, const unsigned int) {
    _archive& key;
    _archive& value;
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
  template <class T>
  R_key_value<_T0, _T1>& internalize(T& arg)  {
    arg.internalize(key);
    arg.internalize(value);
    return *this;
  }
  template <class T>
  R_key_value<_T0, _T1>& externalize(T& arg)  {
    arg.externalize(key);
    arg.externalize(value);
    return *this;
  }
  _T0 key;
  _T1 value;
};
#endif  // K3_R_key_value

#ifndef K3_R_key_value_hash
#define K3_R_key_value_hash
namespace std {
template <class _T0, class _T1>
class hash<R_key_value<_T0, _T1>> {
 public:
  std::size_t operator()(const R_key_value<_T0, _T1>& r) const {
    std::size_t seed = 0;
    hash_combine(seed, r.key);
    hash_combine(seed, r.value);
    return seed;
  }
};
}  // namespace std
#endif

#endif
