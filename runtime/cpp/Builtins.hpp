#ifndef K3_RUNTIME_BUILTINS_H
#define K3_RUNTIME_BUILTINS_H

#include <ctime>
#include <chrono>
#include <climits>
#include <fstream>
#include <sstream>
#include <string>
#include <functional>



#include "strtk.hpp"
#include "BaseTypes.hpp"
#include "Common.hpp"
#include "dataspace/Dataspace.hpp"


// Hashing:
namespace boost {
  template<>
  struct hash<boost::asio::ip::address> {
    size_t operator()(boost::asio::ip::address const& a) const {
      return hash_value(a.to_string());
    }
  };

}

template <class T>
std::size_t hash_value(T const& t) {
  boost::hash<T> hasher;
  return hasher(t);
}

namespace K3 {

  // Standard context for common builtins that use a handle to the engine (via inheritance)
  class __standard_context : public __k3_context {
    public:
    __standard_context(Engine& engine);

    unit_t openBuiltin(string ch_id, string builtin_ch_id, string fmt);

    unit_t openFile(string ch_id, string path, string fmt, string mode);

    unit_t openSocket(string ch_id, Address a, string fmt, string mode);

    unit_t close(std::string chan_id);

    int random(int n);

    double randomFraction(unit_t);

    template <class T>
    int hash(T x) {
      // We implement hash_value for all of our types.
      // for ordered containers, so we may as well delegate to that.
      return static_cast<int>(hash_value(x));
    }

    template <class T>
    T range(int i) {
      T result;
      for (int j = 0; j < i; j++) {
        result.insert(j);
      }
      return result;
    }

    int truncate(double n);

    double real_of_int(int n);

    int get_max_int(unit_t);

    unit_t print(std::string message);


    // TODO, implement, sharing code with prettify()
    template <class T>
    std::string show(T t) {
      return std::string("TODO: implement show()");
    }

    template <class T>
    T error(unit_t) {
      throw std::runtime_error("Error. Terminating");
      return *((T *) nullptr);
    }

    unit_t haltEngine(unit_t);

    unit_t drainEngine(unit_t);

    unit_t sleep(int n);

    // TODO move to seperate context
    unit_t rkLoaderMap(string file, K3::Map<R_key_value<std::string,int>>& c)  {

           R_key_value<std::string, int> rec;
	   int foo; 
        strtk::for_each_line(file, [&] (const std::string& str)   {
          if (strtk::parse(str, ",", rec.key, rec.value, foo)) {
            c.insert(rec);
          } else {
            std::cout << ("Failed to parse a row!\n");
          }
        });
        return unit_t {};
      }

    F<Collection<R_elem<string>>(const string &)> regex_matcher(const string&);
    Vector<R_elem<double>> zeroVector(int i);
    Vector<R_elem<double>> randomVector(int i);

    template <template<typename S> class C, class V>
    unit_t loadVector(string filepath, C<R_elem<V>>& c) {
      std::string line;
      std::ifstream infile(filepath);
      while (std::getline(infile, line)){
        char * pch;
        pch = strtok(&line[0],",");
        V v;
        while (pch != NULL) {
          R_elem<double> rec;
          rec.elem = std::atof(pch);
          v.insert(rec);
          pch = strtok(NULL,",");
        }
        R_elem<V> rec2 {v};
        c.insert(rec2);
      }
      return unit_t();

    }

  };

  // Utilities:


  // Time:
  class __time_context {
    public:
    __time_context();
    int now_int(unit_t);
  };

  // String operations:

  class __string_context {
    public:
    __string_context();

    std::string concat(string s1, string s2);
    std::string itos(int i);

    std::string rtos(double d);

    template <class S> S slice_string(const S& s, int x, int y) {
      return s.substr(x, y);
    }


    // Split a std::string by substrings
    Seq<R_elem<std::string>> splitString(const std::string&, const std::string&);
  };


  template <class C, class F>
  void read_records(std::istream& in, C& container, F read_record) {

    std::string tmp_buffer;
    while (!in.eof()) {
      container.insert(read_record(in, tmp_buffer));
    }

    return;
  }

} // namespace K3

#endif /* K3_RUNTIME_BUILTINS_H */
