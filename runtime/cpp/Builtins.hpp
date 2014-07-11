#ifndef K3_RUNTIME_BUILTINS_H
#define K3_RUNTIME_BUILTINS_H

#include <ctime>
#include <chrono>
#include <climits>
#include <fstream>
#include <sstream>
#include <string>
#include <functional>

#include "BaseTypes.hpp"

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


  F<F<Collection<R_elem<double>>(const Collection<R_elem<double>>&)>(const Collection<R_elem<double>>&)> vector_add =
    [] (const Collection<R_elem<double>>& c1) {
      return [&] (const Collection<R_elem<double>>& c2) {
        using namespace K3;
        vector<R_elem<double>> v1 = c1.getContainer();
        vector<R_elem<double>> v2 = c2.getContainer();

        Collection<R_elem<double>> result = Collection<R_elem<double>>(nullptr);

        for (auto i = 0; i < v1.size(); ++i) {
          cout << "I is" << i << endl;
          cout << "v1:" << v1[i].elem << endl;
          cout << "v2:" << v2[i].elem << endl;
          double d = v1[i].elem + v2[i].elem;
          cout << "d:" << d << endl;
          R_elem<double> r;
          r.elem = d;
          result.insert(r);
        }

        return result;
      };

  };

  F<F<Collection<R_elem<double>>(const Collection<R_elem<double>>&)>(const Collection<R_elem<double>>&)> vector_sub =
    [] (const Collection<R_elem<double>>& c1) {
      return [&] (const Collection<R_elem<double>>& c2) {
        using namespace K3;
        vector<R_elem<double>> v1 = c1.getContainer();
        vector<R_elem<double>> v2 = c2.getContainer();
        Collection<R_elem<double>> result = Collection<R_elem<double>>(nullptr);
        for (auto i = 0; i < v1.size(); ++i) {
          double d = v1[i].elem + v2[i].elem;
          R_elem<double> r;
          r.elem = d;
          result.insert(r);
        }

        return result;
      };

  };

  F<F<double(const Collection<R_elem<double>>&)>(const Collection<R_elem<double>>&)> dot =
    [] (const Collection<R_elem<double>>& c1) {
      return [&] (const Collection<R_elem<double>>& c2) {
        using namespace K3;
        double ans = 0;
        vector<R_elem<double>> v1 = c1.getContainer();
        vector<R_elem<double>> v2 = c2.getContainer();
        for (auto i = 0; i < v1.size(); ++i) {
          double d = v1[i].elem * v2[i].elem;
          ans += d;
        }

        return ans;
      };

  };

  F<F<double(const Collection<R_elem<double>>)>(const Collection<R_elem<double>>&)> squared_distance =
    [] (const Collection<R_elem<double>>& c1) {
      return [&] (const Collection<R_elem<double>>& c2) {
        using namespace K3;
        double ans = 0;
        vector<R_elem<double>> v1 = c1.getContainer();
        vector<R_elem<double>> v2 = c2.getContainer();
        for (auto i = 0; i < v1.size(); ++i) {
          double d = v1[i].elem - v2[i].elem;
          ans += d * d;
        }

        return ans;
      };

  };

  Collection<R_elem<double>> zero_vector(int n) {
    Collection<R_elem<double>> c = Collection<R_elem<double>>(nullptr);
    for (auto i = 0; i < n; ++i) {
      R_elem<double> rec;
      rec.elem = 0.0;
      c.insert(rec);
    }
    return c;
  }

  F<F<Collection<R_elem<double>>(const Collection<R_elem<double>>&)>(const double&)> scalar_mult =
    [] (const double& d) {
      return [&] (const Collection<R_elem<double>>& c) {
        using namespace K3;
        vector<R_elem<double>> v1 = c.getContainer();
        Collection<R_elem<double>> result = Collection<R_elem<double>>(nullptr);
        for (auto i = 0; i < v1.size(); ++i) {
          cout << "d is :" << d << endl;
          cout << "elem: " << v1[i].elem << endl;
          double d2 = d * v1[i].elem;
          R_elem<double> r;
          r.elem = d2;
          result.insert(r);
        }

        return result;
      };

  };

  // ms
  int now(unit_t) {
    auto t = std::chrono::system_clock::now();
    auto elapsed =std::chrono::duration_cast<std::chrono::milliseconds>(t.time_since_epoch());
    return elapsed.count();
  }

  // Map-specific template function to look up
  template <class E>
  F<shared_ptr<typename E::ValueType>(const typename E::KeyType&)> lookup(const Map<E>& map) {
      return [&] (const typename E::KeyType& key) {
        unordered_map<typename E::KeyType, typename E::ValueType> it =
          map.getContainer().find(key);
        if (it != map.end()) {
          return make_shared<E::ValueType>(it->second);
        } else {
          return nullptr;
        }
      };
  }

} // namespace K3

#endif /* K3_RUNTIME_BUILTINS_H */
