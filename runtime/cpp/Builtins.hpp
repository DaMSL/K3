#ifndef K3_RUNTIME_BUILTINS_H
#define K3_RUNTIME_BUILTINS_H

#include <ctime>
#include <chrono>
#include <climits>
#include <fstream>
#include <sstream>
#include <string>
#include <functional>

namespace K3 {
  //class Builtins: public __k3_context {
  //  public:

  //    Builtins() {
  //      // seed the random number generator
  //      std::srand(std::time(0));
  //    }

  //    int random(int x) { return std::rand(x); }

  //    double randomFraction(unit_t) { return (double)rand() / RAND_MAX; }

  //    <template t>
  //      int hash(t x) { return static_cast<int>std::hash<t>(x); }

  //    int truncate(double x) { return (int)x; }

  //    double real_of_int(int x) { return (double)x; }

  //    int get_max_int(unit_t x) { return INT_MAX; }
  //};


  F<F<F<unit_t(string)>(string)>(string)> openBuiltin =
    [] (string chan_id) {
      return [=] (string builtin_chan_id) {
        return [=] (string format) {
          engine.openBuiltin(chan_id, builtin_chan_id);
          return unit_t();
        };
      };
    };

  F<F<F<F<unit_t(string)>(string)>(string)>(string)> openFile =
    [] (string chan_id) {
      return [=] (string path) {
          return [=] (string fmt) {
              return [=] (string mode) {
                  IOMode iomode = engine.ioMode(mode);
                  engine.openFile(chan_id, path, iomode);
                  return unit_t();
              };
          };
      };
  };

  unit_t close(string chan_id) {
      engine.close(chan_id);
      return unit_t();
  }

  unit_t printLine(string message) {
    std::cout << message << endl;
    return unit_t();
  }

  string itos(int i) {
    return to_string(i);
  }

  string rtos(double d) {
    return to_string(d);
  }

  unit_t haltEngine(unit_t) {
    engine.forceTerminateEngine();
    return unit_t();
  }

  F<F<F<string(int)>(int)>(string)> substring = [] (string s) {
      return [=] (int i) {
            return [=] (int n) {
                return s.substr(i,n);
            };
      };
  };

  // ms
  int now(unit_t) {
    auto t = std::chrono::system_clock::now();
    auto elapsed =std::chrono::duration_cast<std::chrono::milliseconds>(t.time_since_epoch());
    return elapsed.count();
  }

}
#endif /* K3_RUNTIME_BUILTINS_H */
