#ifndef K3_STANDARDBUILTINS
#define K3_STANDARDBUILTINS

#include <climits>
#include <boost/thread/mutex.hpp>
#include <boost/thread/thread.hpp>
#include <random>

#include "types/BaseString.hpp"
#include "Common.hpp"

namespace K3 {

class Engine;
class StandardBuiltins {
 public:
  StandardBuiltins(Engine& engine);

  unit_t print(const string_impl& message);

  unit_t sleep(int n);    // Sleep in milliseconds
  unit_t usleep(int n);   // Sleep in microseconds

  unit_t haltEngine(unit_t);

  template <class T>
  T range(int i);

  template <class T>
  T error(unit_t);

  template <class T>
  unit_t ignore(const T& t);

  template <class T>
  int hash(const T& t);

  double randomFraction(unit_t);
  int randomBinomial(int trials, double p);

 protected:
  Engine& __engine_;
  static boost::mutex __mutex_;
  std::default_random_engine __rand_generator_;
  std::uniform_real_distribution<double> __rand_distribution_;
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

template<>
int StandardBuiltins::hash(const int&);

}  // namespace K3

#endif
