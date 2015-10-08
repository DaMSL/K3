#ifndef K3_STRINGBUILTINS
#define K3_STRINGBUILTINS

#include <regex>

#include "Common.hpp"
#include "serialization/Codec.hpp"
#include "types/Value.hpp"

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
  template <class T>
  T parsePSV(const string_impl& s1) {
    auto bs = BaseStringRefPackedValue(s1, CodecFormat::PSV);
    return *unpack<T>(bs);
  }
  template <class T>
  T parseYAS(const string_impl& s1) {
    auto bs = BaseStringRefPackedValue(s1, CodecFormat::YASBinary);
    T t = *unpack<T>(bs);
    return t;
  }
};

template <class S>
S StringBuiltins::slice_string(const S& s, int x, int y) {
  return s.substr(x, y);
}

}  // namespace K3

#endif
