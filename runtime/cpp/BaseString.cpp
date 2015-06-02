#include "BaseString.hpp"

char* dupstr(const char *s) throw () {
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
    if (s.length() > 0) {
      for (auto& i: s) {
        boost::hash_combine(seed, i);
      }
    }
    return seed;
  }

  template <>
  void base_string::serialize(csv::parser& a, const unsigned int) {
    std::string tmp;
    a& tmp;

    if (buffer) { delete[] buffer; }
    buffer = dupstr(tmp.c_str());
  }

  // Specializations for CSV parsing/writing, skipping the length field.
  template <>
  void base_string::serialize(csv::writer& a, const unsigned int) {
    std::string tmp = std::string(buffer, length());
    a& tmp;
  }

}

