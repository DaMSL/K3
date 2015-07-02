#ifndef K3_PRETTIFY
#define K3_PRETTIFY

#include <tuple>
#include <sstream>
#include <type_traits>

#include "Common.hpp"
#include "types/BaseString.hpp"
#include "collections/AllCollections.hpp"

namespace K3 {

string_impl prettify_bool(bool b);
string_impl prettify_byte(char c);
string_impl prettify_int(int i);
string_impl prettify_real(double d);
string_impl prettify_string(const string_impl& s);
string_impl prettify_address(const Address& s);
string_impl prettify_tuple(const unit_t&);

template <class T, class F>
string_impl prettify_option(std::shared_ptr<T> o, F f) {
  std::ostringstream oss;
  if (o) {
    oss << "Some " << f(*o);
  } else {
    oss << "None";
  }
  return string_impl(oss.str());
}

template <class T, class F>
string_impl prettify_indirection(std::shared_ptr<T> o, F f) {
  std::ostringstream oss;
  if (o) {
    oss << "Ind " << f(*o);
  } else {
    oss << "nullptr";
  }

  return string_impl(oss.str());
}

template <class R, class F>
string_impl prettify_record(const R& r, F f) {
  return string_impl(f(r));
}

template <class C, class F>
string_impl prettify_collection(const C& c, F f) {
  std::ostringstream oss;
  int i = 0;
  oss << "[";
  auto wrapped = [&f, &i, &oss] (auto& elem) {
    if (i != 0) {
      oss << ",";
    }
    oss << f(elem);
    i++;
    if (i == 10) {
      oss << ",...";
      return;
    }
  };
  c.iterate(wrapped);
  oss << "]";
  return string_impl(oss.str());
}

template <class C, class F>
string_impl prettify_vmap(const C& c, F f) {
  std::ostringstream oss;
  oss << "[";
  for (auto& elem : c) {
    oss << "Version: " << elem.first << std::endl;
    oss << "[";
    int i = 0;
    if (i != 0) {
      oss << ",";
    }
    oss << f(elem.second);
    i++;
    if (i == 10) {
      oss << ",...";
      break;
    }
  };
  oss << "]";
  return string_impl(oss.str());

}

// Template magic for tuples
template <size_t n, typename... T, typename... FS>
typename std::enable_if<(n >= sizeof...(T))>::type print_tuple(
    std::ostream&, const std::tuple<T...>&, FS... funs) {}

template <size_t n, typename... T, typename F, typename... FS>
typename std::enable_if<(n < sizeof...(T))>::type print_tuple(
    std::ostream& os, const std::tuple<T...>& tup, F fun, FS... funs) {
  if (n != 0) os << ", ";
  os << fun(std::get<n>(tup));
  print_tuple<n + 1>(os, tup, funs...);
}

template <typename... T, typename... FS>
string_impl prettify_tuple(const std::tuple<T...>& tup, FS... funs) {
  std::ostringstream os;
  os << "(";
  print_tuple<0>(os, tup, funs...);
  os << ")";
  return os.str();
}


}  // namespace K3

#endif
