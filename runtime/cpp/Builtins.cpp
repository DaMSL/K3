#include <functional>
#include <string>

#include "re2/re2.h"
#include "Common.hpp"
#include "Engine.hpp"
#include "BaseTypes.hpp"
#include "dataspace/Dataspace.hpp"
#include "BaseCollections.hpp"
#include "Builtins.hpp"

namespace K3 {
  using std::endl;
  using std::to_string;


  // Standard context implementations
  __standard_context::__standard_context(Engine& __engine)
    : __k3_context(__engine)
  {}

  unit_t __standard_context::openBuiltin(string_impl ch_id, string_impl builtin_ch_id, string_impl fmt) {
    __engine.openBuiltin(ch_id, builtin_ch_id);
    return unit_t();
  }


  unit_t __standard_context::openFile(string_impl ch_id, string_impl path, string_impl fmt, string_impl mode) {
    IOMode iomode = __engine.ioMode(mode);
    __engine.openFile(ch_id, path, iomode);
    return unit_t();
  }

  unit_t __standard_context::openSocket(string_impl ch_id, Address a, string_impl fmt, string_impl mode) {
    throw std::runtime_error("Not implemented: openSocket");
  }

  unit_t __standard_context::close(string_impl chan_id) {
      __engine.close(chan_id);
      return unit_t();
  }

  int __standard_context::random(int n) {
    throw std::runtime_error("Not implemented: random");
  }

  double __standard_context::randomFraction(unit_t) {
    throw std::runtime_error("Not implemented: random");
  }

  int __standard_context::truncate(double n) {
    throw std::runtime_error("Not implemented: truncate");
  }

  double  __standard_context::real_of_int(int n) {
    throw std::runtime_error("Not implemented: real_of_int");
  }

  int  __standard_context::get_max_int(unit_t) {
    throw std::runtime_error("Not implemented: get_max_int");
  }

  unit_t __standard_context::print(string_impl message) {
    std::cout << message << endl;
    return unit_t();
  }

  unit_t __standard_context::haltEngine(unit_t) {
    __engine.forceTerminateEngine();
    return unit_t();
  }

  unit_t __standard_context::drainEngine(unit_t) {
    throw std::runtime_error("Not implemented: drainEngine");
  }

  unit_t sleep(int n) {
    throw std::runtime_error("Not implemented: sleep");
  }

  // TODO fix copies related to base_str / std::sring conversion
  F<Collection<R_elem<string_impl>>(const string_impl &)> __standard_context::regex_matcher(const string_impl& regex) {
    auto pattern = make_shared<RE2>(regex);
    return [pattern] (const string_impl& in_str) {
      re2::StringPiece input(static_cast<std::string>(in_str));
      Collection<R_elem<string_impl>> results;
      std::string s;
      while(RE2::FindAndConsume(&input, *pattern, &s)) {
        results.insert(string_impl(s));
      }
      return results;
    };

  }
  Vector<R_elem<double>> __standard_context::zeroVector(int i) {
    Vector<R_elem<double>> result;
    auto& c = result.getContainer();
    c.resize(i);
    for(int j = 0; j < i; j++) {
     c[j] = R_elem<double>{0.0};
    }
    return result;

  }

  // TODO
  Vector<R_elem<double>> __standard_context::randomVector(int i) {
    Vector<R_elem<double>> result;
    auto& c = result.getContainer();
    c.resize(i);
    for(int j = 0; j < i; j++) {
     c[j] = R_elem<double>{0.0};
    }
    return result;
  }

  // Time:
  __time_context::__time_context() {}

  int __time_context::now_int(unit_t) {
    auto t = std::chrono::system_clock::now();
    auto elapsed =std::chrono::duration_cast<std::chrono::milliseconds>(t.time_since_epoch());
    return elapsed.count();
  }


  // String operations:
  __string_context::__string_context() {}
  string_impl __string_context::itos(int i) {
    return string_impl(to_string(i));
  }

  // TODO: more efficient implementation.
  string_impl __string_context::concat(string_impl s1, string_impl s2) {
    return string_impl(static_cast<std::string>(s1) + static_cast<std::string>(s2));
  }

  string_impl __string_context::rtos(double d) {
    return string_impl(to_string(d));
  }

  // Split a string by substrings
  Seq<R_elem<string_impl>> __string_context::splitString(const string_impl& s, const string_impl& splitter) {
      
      Seq<R_elem<string_impl>> results;
      R_elem<string_impl> rec; 
      char * pch;
      // TODO: avoid this copy. need a char* not a const char * for first arg to strtok
      std::string str = static_cast<std::string>(s);
      pch = strtok (&str[0], splitter.c_str());
      while (pch != NULL)
      {
        rec.elem = string_impl(pch);
        results.insert(rec);
        pch = strtok (NULL, splitter.c_str());
      }

      return results;
  }

} // namespace K3
