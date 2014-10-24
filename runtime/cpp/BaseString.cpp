#include "BaseString.hpp"

// Apparently, strdup isn't a standard function, and isn't in <cstring>
char* strdup(const char *s) throw () {
  if (!s) {
    return nullptr;
  }

  auto n = strlen(s);
  auto d = new char[n + 1];
  return static_cast<char*>(memcpy(d, s, n + 1));
}

namespace K3 {
  template<>
  std::size_t hash_value<K3::base_string>(const K3::base_string& s) {
    std::size_t seed = 0;
    for (auto& i: s) {
      boost::hash_combine(seed, i);
    }
    return seed;
  }
}

