#include "BaseString.hpp"

// Apparently, strdup isn't a standard function, and isn't in <cstring>
char* strdup(const char *s) throw () {
  auto n = strlen(s);
  auto d = new char[n + 1];
  return static_cast<char*>(memcpy(d, s, n + 1));
}
