#ifndef K3_TIMERS
#define K3_TIMERS

#include <boost/asio.hpp>
#include <boost/asio/deadline_timer.hpp>

#include "Common.hpp"
#include "Hash.hpp"

namespace K3 {

enum class TimerType { Delay, DelayOverride, DelayOverrideEdge };

struct TimerKey {
  virtual std::shared_ptr<TimerKey> clone() const { return std::make_shared<TimerKey>(); }
  virtual size_t hash() const { return 0; }
  bool operator==(const TimerKey& other) const { return this->tkCompare(other); }
  virtual bool tkCompare(const TimerKey& other) const { return true; }
  virtual std::string toString() { return "TimerKey"; }
};

struct DelayTimerT : public TimerKey {
  unsigned long id;
  DelayTimerT(unsigned long i) : id(i) {}

  virtual std::shared_ptr<TimerKey> clone() const {
    return std::shared_ptr<TimerKey>(new DelayTimerT(id));
  }

  virtual size_t hash() const {
    std::hash<unsigned long> uh;
    return uh(id);
  }

  virtual bool tkCompare(const TimerKey& other) const {
    bool r = false;
    const DelayTimerT* t = dynamic_cast<const DelayTimerT*>(&other);
    if ( t ) { r = id == t->id; }
    return r;
  }

  virtual std::string toString() { return std::string("DelayTimerT ") + std::to_string(id); }
};

struct DelayOverrideT : public TimerKey {
  Address dst;
  TriggerID trig;
  DelayOverrideT(const Address& d, const TriggerID& t) : dst(d), trig(t) {}

  virtual std::shared_ptr<TimerKey> clone() const {
    return std::shared_ptr<TimerKey>(new DelayOverrideT(dst, trig));
  }

  virtual size_t hash() const {
    std::hash<K3::Address> ah;
    size_t seed = ah(dst);
    hash_combine(seed, trig);
    return seed;
  }

  virtual bool tkCompare(const TimerKey& other) const {
    bool r = false;
    const DelayOverrideT* t = dynamic_cast<const DelayOverrideT*>(&other);
    if ( t ) { r = dst == t->dst && trig == t->trig; }
    return r;
  }

  virtual std::string toString() {
    return std::string("DelayOverrideT ") + dst.toString() + " " + std::to_string(trig);
  }
};

struct DelayOverrideEdgeT : public DelayOverrideT {
  Address src;
  DelayOverrideEdgeT(const Address& s, const Address& d, const TriggerID& t) : DelayOverrideT(d, t), src(s)  {}

  virtual std::shared_ptr<TimerKey> clone() const {
    return std::shared_ptr<TimerKey>(new DelayOverrideEdgeT(src, dst, trig));
  }

  virtual size_t hash() const {
    size_t seed = DelayOverrideT::hash();
    hash_combine(seed, src);
    return seed;
  }

  virtual bool tkCompare(const TimerKey& other) const {
    bool r = false;
    const DelayOverrideEdgeT* t = dynamic_cast<const DelayOverrideEdgeT*>(&other);
    if ( t ) { r = DelayOverrideT::tkCompare(other) && src == t->src; }
    return r;
  }

  virtual std::string toString() {
    return std::string("DelayOverrideEdgeT ")
              + src.toString() + " " + dst.toString() + " " + std::to_string(trig);
  }

};

struct TimerKeyPtrEquality {
  bool operator()(const shared_ptr<TimerKey>& t1, const shared_ptr<TimerKey>& t2) const {
    if ( t1 && t2 ) { return *t1 == *t2; }
    return t1 == t2;
  }
};

struct TimerKeyPtrHash {
  size_t operator()(const shared_ptr<TimerKey>& t) const {
    if ( t ) { return t->hash(); }
    return std::hash<shared_ptr<TimerKey>>()(t);
  }
};

typedef boost::asio::deadline_timer::duration_type Delay;
typedef boost::asio::deadline_timer Timer;
typedef ConcurrentHashMap<std::shared_ptr<TimerKey>, std::unique_ptr<Timer>, TimerKeyPtrHash, TimerKeyPtrEquality> TimerMap;

} // end namespace K3

namespace std {
  // TimerKey hash.
  template<>
  struct hash<K3::TimerKey> {
    size_t operator()(const K3::TimerKey& ty) const {
      return ty.hash();
    }
  };
}

#endif