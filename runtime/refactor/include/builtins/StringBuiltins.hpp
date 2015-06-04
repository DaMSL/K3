#ifndef K3_STRINGBUILTINS
#define K3_STRINGBUILTINS

#include "Common.hpp"

namespace K3 {

class StringBuiltins {
 public:
  StringBuiltins();
  string_impl itos(int i);
  string_impl rtos(double d);
  string_impl atos(Address a);
  // Seq<R_elem<string_impl>> splitString(string_impl, const string_impl&);
  int tpch_date(const string_impl& s);
  string_impl tpch_date_to_string(const int& date);
  template <class S>
  S slice_string(const S& s, int x, int y);
  int strcomp(const string_impl& s1, const string_impl& s2);
};
    

template <class S>
S StringBuiltins::slice_string(const S& s, int x, int y) {
  return s.substr(x, y);
}

}  // namespace K3

#endif
