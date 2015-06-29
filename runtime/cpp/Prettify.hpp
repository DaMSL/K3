#ifndef K3_RUNTIME_PRETTIFY_H
#define K3_RUNTIME_PRETTIFY_H

#include "Common.hpp"
#include "BaseString.hpp"
#include "dataspace/Dataspace.hpp"

namespace K3 {

  string_impl prettify_bool(bool b) {
    return string_impl(std::to_string(b));
  }

  string_impl prettify_byte(char c) {
    return string_impl(std::to_string(c));
  }

  string_impl prettify_int(int i) {
    return string_impl(std::to_string(i));
  }

  string_impl prettify_real(double d) {
    return string_impl(std::to_string(d));
  }

  string_impl prettify_string(const string_impl& s) {
    return s;
  }

  string_impl prettify_address(const K3::Address& s) {
    return string_impl(K3::addressAsString(s));
  }

  template<class F>
  string_impl prettify_function(F f) {
    return string_impl("<function>");
  }

  template<class T, class F>
  string_impl prettify_option(std::shared_ptr<T> o, F f) {
    std::ostringstream oss;
    if (o) {
      oss << "Some " << f(*o);
    }
    else {
      oss << "None";
    }
    return string_impl(oss.str());
  }

  template<class T, class F>
  string_impl prettify_indirection(std::shared_ptr<T> o, F f) {
    std::ostringstream oss;
    if (o) {
      oss << "Ind " << f(*o);
    }
    else {
      oss << "nullptr";
    }

    return string_impl(oss.str());
  }

  // TODO
  template<class T>
  string_impl prettify_tuple(T o) {
    return string_impl("TODO");
  }

  template<class R, class F>
  string_impl prettify_record(const R& r, F f) {
    return string_impl(f(r));
  }

  template<class C, class F>
  string_impl prettify_collection(const C& c, F f) {
    std::ostringstream oss;
    int i = 0;
    const auto& con = c.getConstContainer();
    oss << "[";
    for (auto elem : con) {
      oss << f(c.elemToRecord(elem));
      i++;
      oss << ",";
      if (i == 10) {
	      oss << "...";
        break;
      }
    }
    oss << "]";
    return string_impl(oss.str());

  }


} //Namespace K3

#endif //K3_RUNTIME_RUN_H
