#ifndef K3_RUNTIME_BUILTINS_H
#define K3_RUNTIME_BUILTINS_H

#include <ctime>
#include <climits>
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

  template <class r> using F = std::function<r>;

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
                  std::cout << "opened file!" << std::endl;
                  return unit_t();
              };
          };
      };
  };

  unit_t close(string chan_id) {
      engine.close(chan_id);
      return unit_t();
  }
}
#endif /* K3_RUNTIME_BUILTINS_H */
