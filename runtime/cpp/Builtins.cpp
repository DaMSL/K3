#include <functional>
#include <string>
#include "Common.hpp"
#include "Collections.hpp"

char *sdup (const char *s) {
    char *d = (char *)malloc (strlen (s) + 1);   // Allocate memory
    if (d != NULL) strcpy (d,s);         // Copy string if okay
    return d;                            // Return new memory
}

#include "Builtins.hpp"

namespace K3 {
  using std::string;

  F<F<unit_t(const string&)>(const string&)> openBuiltin(const string& chan_id) {
      return [&] (const string& builtin_chan_id) {
        return [&] (const string& format) {
          engine.openBuiltin(chan_id, builtin_chan_id);
          return unit_t();
        };
      };
    }

  F<F<F<unit_t(const string&)>(const string&)>(const string&)> openFile(const string& chan_id) {
      return [&] (const string& path) {
          return [&] (const string& fmt) {
              return [&] (const string& mode) {
                  IOMode iomode = engine.ioMode(mode);
                  engine.openFile(chan_id, path, iomode);
                  return unit_t();
              };
          };
      };
  }

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

  F<F<string(const int&)>(const int&)> substring(const string& s) {
      return [&] (const int& i) {
            return [&] (const int& n) {
                return s.substr(i,n);
            };
      };
  }


  F<Collection<R_elem<double>>(const Collection<R_elem<double>>&)> vector_add(const Collection<R_elem<double>>& c1) {
      return [&] (const Collection<R_elem<double>>& c2) {
        using namespace K3;
        const vector<R_elem<double>> &v1 = c1.getConstContainer();
        const vector<R_elem<double>> &v2 = c2.getConstContainer();
        Collection<R_elem<double>> result(nullptr);

        for (auto i = 0; i < v1.size(); ++i) {
          double d = v1[i].elem + v2[i].elem;
          R_elem<double> r(d);
          result.insert(r);
        }

        return result;
      };

  }

  F<Collection<R_elem<double>>(const Collection<R_elem<double>>&)> vector_sub(const Collection<R_elem<double>>& c1) {
      return [&] (const Collection<R_elem<double>>& c2) {
        using namespace K3;
        const auto &v1 = c1.getConstContainer();
        const auto &v2 = c2.getConstContainer();
        Collection<R_elem<double>> result(nullptr);
        for (auto i = 0; i < v1.size(); ++i) {
          double d = v1[i].elem - v2[i].elem;
          R_elem<double> r(d);
          result.insert(r);
        }

        return result;
      };

  }

  F<double(const Collection<R_elem<double>>&)> dot(const Collection<R_elem<double>>& c1) {
      return [&] (const Collection<R_elem<double>>& c2) {
        using namespace K3;
        double ans = 0;
        const auto &v1 = c1.getConstContainer();
        const auto &v2 = c2.getConstContainer();
        for (auto i = 0; i < v1.size(); ++i) {
          double d = v1[i].elem * v2[i].elem;
          ans += d;
        }

        return ans;
      };
  }

  F<double(const Collection<R_elem<double>>&)> squared_distance(const Collection<R_elem<double>>& c1) {
      return [&] (const Collection<R_elem<double>>& c2) {
        using namespace K3;
        double ans = 0;
        const auto &v1 = c1.getConstContainer();
        const auto &v2 = c2.getConstContainer();
        for (auto i = 0; i < v1.size(); ++i) {
          double d = v1[i].elem - v2[i].elem;
          ans += d * d;
        }

        return ans;
      };
  }

  Collection<R_elem<double>> zero_vector(int n) {
    Collection<R_elem<double>> c(nullptr);
    auto &cc(c.getContainer());
    cc.resize(n, R_elem<double> { 0.0 });
    return c;
  }

  F<Collection<R_elem<double>>(const Collection<R_elem<double>>&)> scalar_mult(const double& d) {
      return [&] (const Collection<R_elem<double>>& c) {
        using namespace K3;
        const auto& v1 = c.getConstContainer();
        Collection<R_elem<double>> result(nullptr);
        for (auto i = 0; i < v1.size(); ++i) {
          double d2 = d * v1[i].elem;
          R_elem<double> r(d2);
          result.insert(r);
        }

        return result;
      };
  }

  // ms
  int now(unit_t) {
    auto t = std::chrono::system_clock::now();
    auto elapsed =std::chrono::duration_cast<std::chrono::milliseconds>(t.time_since_epoch());
    return elapsed.count();
  }

  // Split a string by substrings
  F<Seq<R_elem<string> >(const string&)> splitString(const string& s) {
    return [&] (const string& splitter) {
      std::vector<string> words;
      boost::split(words, s, boost::is_any_of(splitter), boost::token_compress_on);

      // Transfer to R_elems
      Seq<R_elem<string>> results(nullptr);
      auto &c = results.getContainer();
      c.resize(words.size());
      for (const auto &elem : words) {
        results.insert(elem);
      }
      return results;
    };
  }

} // namespace K3
