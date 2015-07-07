#ifndef K3_STANDARDBUILTINS
#define K3_STANDARDBUILTINS

#include <climits>
#include <boost/thread/mutex.hpp>
#include <boost/thread/thread.hpp>

#include "types/BaseString.hpp"
#include "Common.hpp"

namespace K3 {

class Engine;
class StandardBuiltins {
 public:
  StandardBuiltins(Engine& engine);
  unit_t print(string_impl message);
  unit_t sleep(int n);
  unit_t haltEngine(unit_t);
  template <class T>
  T range(int i);
  template <class T>
  T error(unit_t);
  template <class T>
  unit_t ignore(const T& t);
  template <class T>
  int hash(const T& t);
  // Mosaic builtins
  template <class C, class A, class B>
  C max(const R_key_value<A, B>& r) {
    if (r.key > r.value) {
      return r.key;
    } else {
      return r.value;
    }
  }
  
  template <class C>
  C min(const R_key_value<C, C>& r) {
    if (r.key < r.value) {
      return r.key;
    } else {
      return r.value;
    }
  }

  int date_part(const R_key_value<string_impl, int>& r) {
    if (r.key == "day" || r.key == "DAY") {
      return r.value % 100;
    }
    if (r.key == "month" || r.key == "MONTH") {
      return (r.value % 10000) / 100;
    }
    if (r.key == "year" || r.key == "YEAR") {
      return (r.value / 10000);
    }
    throw std::runtime_error("Unrecognized date part key: " + r.key);
  }
  double real_of_int(int n) { return (double)n; }
  int get_max_int(unit_t) { return INT_MAX; }
  int truncate(double n) { return (int)n; }
 protected:
  Engine& __engine_;
  boost::mutex __mutex_;
};

template <class T>
T StandardBuiltins::range(int i) {
  T result;
  for (int j = 0; j < i; j++) {
    result.insert(j);
  }
  return result;
}

template <class T>
T StandardBuiltins::error(unit_t) {
  throw std::runtime_error("K3 Application-Level Error: Terminating");
  return T();
}

template <class T>
unit_t StandardBuiltins::ignore(const T& t) {
  return unit_t();
}

template <class T>
int StandardBuiltins::hash(const T& t) {
  std::hash<T> hasher;
  return hasher(t);
}

}  // namespace K3

#endif
