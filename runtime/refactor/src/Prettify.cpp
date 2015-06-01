#include "Prettify.hpp"

namespace K3 {

string_impl prettify_bool(bool b) { return string_impl(std::to_string(b)); }

string_impl prettify_byte(char c) { return string_impl(std::to_string(c)); }

string_impl prettify_int(int i) { return string_impl(std::to_string(i)); }

string_impl prettify_real(double d) { return string_impl(std::to_string(d)); }

string_impl prettify_string(const string_impl& s) { return s; }

string_impl prettify_address(const Address& s) {
  return string_impl(s.toString());
}

}  // namespace K3
