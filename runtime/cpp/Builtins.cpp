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
  using std::string;
  using std::endl;
  using std::to_string;


  // Standard context implementations
  __standard_context::__standard_context(Engine& __engine)
    : __k3_context(__engine)
  {}

  unit_t __standard_context::openBuiltin(string ch_id, string builtin_ch_id, string fmt) {
    __engine.openBuiltin(ch_id, builtin_ch_id);
    return unit_t();
  }


  unit_t __standard_context::openFile(string ch_id, string path, string fmt, string mode) {
    IOMode iomode = __engine.ioMode(mode);
    __engine.openFile(ch_id, path, iomode);
    return unit_t();
  }

  unit_t __standard_context::openSocket(string ch_id, Address a, string fmt, string mode) {
    throw std::runtime_error("Not implemented: openSocket");
  }

  unit_t __standard_context::close(string chan_id) {
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

  unit_t __standard_context::print(string message) {
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

  F<Collection<R_elem<string>>(const string &)> __standard_context::regex_matcher(const string& regex) {
    auto pattern = make_shared<RE2>(regex);
    return [pattern] (const string& in_str) {
      re2::StringPiece input(in_str);
      Collection<R_elem<string>> results;
      string s;
      while(RE2::FindAndConsume(&input, *pattern, &s)) {
        results.insert(s);
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
  string __string_context::itos(int i) {
    return to_string(i);
  }

  string __string_context::concat(string s1, string s2) {
    return s1 + s2;
  }

  string __string_context::rtos(double d) {
    return to_string(d);
  }

  // Split a string by substrings
  Seq<R_elem<string>> __string_context::splitString(const string& s, const string& splitter) {
      std::vector<string> words;
      boost::split(words, s, boost::is_any_of(splitter), boost::token_compress_on);

      // Transfer to R_elems
      Seq<R_elem<string>> results;
      auto &c = results.getContainer();
      c.resize(words.size());
      for (auto &elem : words) {
        results.insert(R_elem<string>{std::move(elem)});
      }
      return results;
  }

} // namespace K3
