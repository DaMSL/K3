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


  F<F<unit_t(const string&)>(const string&)> openBuiltin(const string& chan_id);

  F<F<F<unit_t(const string&)>(const string&)>(const string&)> openFile(const string& chan_id);

  unit_t close(std::string chan_id);

  unit_t printLine(std::string message);

  std::string itos(int i);

  std::string rtos(double d);

  unit_t haltEngine(unit_t);

  F<F<string(const int&)>(const int&)> substring(const string& s);

  F<Collection<R_elem<double>>(const Collection<R_elem<double>>&)> vector_add(const Collection<R_elem<double>>& c1);

  F<Collection<R_elem<double>>(const Collection<R_elem<double>>&)> vector_sub(const Collection<R_elem<double>>& c1);
  
  F<double(const Collection<R_elem<double>>&)> dot(const Collection<R_elem<double>>& c1);
  
  F<double(const Collection<R_elem<double>>&)> squared_distance(const Collection<R_elem<double>>& c1);

  Collection<R_elem<double>> zero_vector(int n);

  F<Collection<R_elem<double>>(const Collection<R_elem<double>>&)> scalar_mult(const double& d);
  
  // ms
  int now(unit_t);

  // Map-specific template function to look up
  template <class Key, class Value>
  F<shared_ptr<Value>(const Key&)> lookup(Map<R_key_value<Key, Value> >& map) {
    return [&] (const Key& key) {
      const auto &container(map.getContainer());
      const auto it(container.find(key));
      if (it != container.end()) {
        return make_shared<Value>(it->second);
      } else {
        return shared_ptr<Value>();
      }
    };
  }

  template <class E>
  F<F<F<unit_t(F<typename E::ValueType(const typename E::ValueType&)>)>(const typename E::ValueType&)>(const typename E::KeyType&)>
  insert_with(Map<E>& map) {
    return [&] (const typename E::KeyType& key) {
      return [&] (const typename E::ValueType& value) {
        return [&] (std::function<typename E::ValueType(const typename E::ValueType&)> f) {
          auto &c = map.getContainer();
          auto it(c.find(key));
          if (it == map.end()) {
            map.insert(E(key, value));
          } else {
            map.insert(E(key, f(value)));
          }

          return unit_t();
        };
      };
    };
  }

  // Split a std::string by substrings
  F<Seq<R_elem<std::string> >(const std::string&)> splitString(const std::string& s);

} // namespace K3

#endif /* K3_RUNTIME_BUILTINS_H */
