#include <string>

#include "types/BaseString.hpp"
#include "builtins/StringBuiltins.hpp"

namespace K3 {

StringBuiltins::StringBuiltins() {}

string_impl StringBuiltins::itos(int i) {
  return string_impl(std::to_string(i));
}

string_impl StringBuiltins::rtos(double d) {
  return string_impl(std::to_string(d));
}

string_impl StringBuiltins::atos(Address a) {
  return string_impl(addressAsString(a));
}

int StringBuiltins::tpch_date(const string_impl& s) {
  char delim = '-';
  const char* buf = s.c_str();
  if (!buf) {
    return 0;
  }
  char date[9];
  int i = 0;
  for (; *buf != 0 && i < 8; buf++) {
    if (*buf != delim) {
      date[i] = *buf;
      i++;
    }
  }
  date[i] = 0;
  return std::atoi(date);
}

string_impl StringBuiltins::tpch_date_to_string(const int& date) {
  std::string tmp = std::to_string(date);
  std::string year = tmp.substr(0, 4);
  std::string month = tmp.substr(4, 2);
  std::string day = tmp.substr(6, 2);
  return year + "-" + month + "-" + day;
}

int StringBuiltins::strcomp(const string_impl& s1, const string_impl& s2) {
  return s1.strcomp(s2);
}

int regex_match_int(const R_key_value<string_impl, string_impl>& r) {
  bool b = std::regex_match(r.value.c_str(), std::regex(r.key.c_str()));
  return b ? 1 : 0;
}

}  // namespace K3
