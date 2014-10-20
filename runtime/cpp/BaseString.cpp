#include "BaseString.hpp"
#include "boost/functional/hash.hpp"

// Apparently, strdup isn't a standard function, and isn't in <cstring>
char* strdup(const char *s) throw () {
  auto n = strlen(s);
  auto d = new char[n + 1];
  return static_cast<char*>(memcpy(d, s, n + 1));
}

namespace K3 {
  std::size_t hash_value(const K3::base_string& s) {
    std::size_t seed = 0;
    for (auto& i: s) {
      boost::hash_combine(seed, i);
    }
    return seed;
  }
}
