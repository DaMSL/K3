#include <thread>
#include <chrono>

#include "Common.hpp"
#include "builtins/StandardBuiltins.hpp"
#include "core/Engine.hpp"

namespace K3 {

StandardBuiltins::StandardBuiltins(Engine& engine)
    : __engine_(engine), __mutex_() {}

unit_t StandardBuiltins::print(string_impl message) {
  boost::lock_guard<boost::mutex> lock(__mutex_);
  std::cout << message << std::endl;
  return unit_t();
}

unit_t StandardBuiltins::sleep(int n) {
  std::this_thread::sleep_for(std::chrono::milliseconds(n));
  return unit_t();
}

unit_t StandardBuiltins::haltEngine(unit_t) {
  throw EndOfProgramException();
  return unit_t();
}

template <>
int StandardBuiltins::hash(const int& b) {
  const unsigned int fnv_prime = 0x811C9DC5;
  unsigned int hash = 0;
  const char* p = (const char*)&b;
  for (std::size_t i = 0; i < sizeof(int); i++) {
    hash *= fnv_prime;
    hash ^= p[i];
  }
  return hash;
}

}  // namespace K3
