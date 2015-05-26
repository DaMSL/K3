#ifndef K3_STANDARDBUILTINS
#define K3_STANDARDBUILTINS

#include <boost/thread/mutex.hpp>
#include <boost/thread/thread.hpp>

#include "BaseString.hpp"
#include "Common.hpp"

namespace K3 {

typedef base_string string_impl; // Toggle string implementations

class Engine;
class StandardBuiltins {
 public:
  StandardBuiltins(Engine& engine);
  unit_t print(string_impl message);
  unit_t sleep(int n);
  unit_t haltEngine(unit_t);
  template <class T> T range(int i);
  template <class T> T error(unit_t);
  template <class T> unit_t ignore(T t);

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
unit_t StandardBuiltins::ignore(T t) {
  return unit_t();
}

}  // namespace K3

#endif
