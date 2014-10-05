#include <functional>
#include <string>

#include "Common.hpp"
#include "Engine.hpp"
#include "BaseTypes.hpp"
#include "dataspace/Dataspace.hpp"
#include "BaseCollections.hpp"
#include "Builtins.hpp"

char *sdup (const char *s) {
    char *d = (char *)malloc (strlen (s) + 1);   // Allocate memory
    if (d != NULL) strcpy (d,s);         // Copy string if okay
    return d;                            // Return new memory
}

namespace K3 {
  using std::string;
  using std::endl;
  using std::to_string;


  // Standard context implementations
  __standard_context::__standard_context(Engine& __engine)
    : __k3_context(__engine)
  {}

  F<F<unit_t(const string&)>(const string&)> __standard_context::openBuiltin(const string& chan_id) {
      return [&] (const string& builtin_chan_id) {
        return [&] (const string& format) {
          __engine.openBuiltin(chan_id, builtin_chan_id);
          return unit_t();
        };
      };
    }

  F<F<F<unit_t(const string&)>(const string&)>(const string&)> __standard_context::openFile(const string& chan_id) {
      return [&] (const string& path) {
          return [&] (const string& fmt) {
              return [&] (const string& mode) {
                  IOMode iomode = __engine.ioMode(mode);
                  __engine.openFile(chan_id, path, iomode);
                  return unit_t();
              };
          };
      };
  }

  unit_t __standard_context::close(string chan_id) {
      __engine.close(chan_id);
      return unit_t();
  }


  unit_t __standard_context::haltEngine(unit_t) {
    __engine.forceTerminateEngine();
    return unit_t();
  }

  unit_t __standard_context::printLine(string message) {
    std::cout << message << endl;
    return unit_t();
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

  // TODO Builtins that require a handle to peers?
  //int index_by_hash(const string& s) {
  //  auto& container = peers.getConstContainer();
  //  size_t h = std::hash<string>()(s);
  //  return h % container.size();
  //}

  //Address& peer_by_index(const int i) {
  //  auto& container = peers.getContainer();
  //  return container[i].addr;
  //}


  // Time:
  __time_context::__time_context() {}

  int __time_context::now(unit_t) {
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


  string __string_context::substring(const string&s, int i, int n) {
    return s.substr(i,n);
  }

  // Split a string by substrings
  F<Seq<R_elem<string> >(const string&)> __string_context::splitString(const string& s) {
    return [&] (const string& splitter) {
      std::vector<string> words;
      boost::split(words, s, boost::is_any_of(splitter), boost::token_compress_on);

      // Transfer to R_elems
      Seq<R_elem<string>> results;
      auto &c = results.getContainer();
      c.resize(words.size());
      for (const auto &elem : words) {
        results.insert(elem);
      }
      return results;
    };
  }

} // namespace K3
