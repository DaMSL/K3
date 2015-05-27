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
};

}  // namespace K3

#endif
