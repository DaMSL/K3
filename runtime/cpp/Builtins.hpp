#ifndef K3_RUNTIME_BUILTINS_H
#define K3_RUNTIME_BUILTINS_H

#ifdef CACHEPROFILE
#include <cpucounters.h>
#endif

#ifdef MEMPROFILE
#include "gperftools/heap-profiler.h"
#endif

#include <ctime>
#include <chrono>
#include <climits>
#include <fstream>
#include <sstream>
#include <string>
#include <climits>
#include <functional>

#include "re2/re2.h"

#include "BaseTypes.hpp"
#include "BaseString.hpp"
#include "Common.hpp"
#include "dataspace/Dataspace.hpp"

// Hashing:
namespace boost {

template<>
struct hash<boost::asio::ip::address> {
  size_t operator()(boost::asio::ip::address const& v) const {
    if (v.is_v4()) {
      return v.to_v4().to_ulong();
    }
    if (v.is_v6()) {
      auto const& range = v.to_v6().to_bytes();
      return hash_range(range.begin(), range.end());
    }
    if (v.is_unspecified()) {
      return 0x4751301174351161ul;
    }
    return hash_value(v.to_string());
  }
};

} // boost

namespace K3 {

  template <class C1, class C, class F>
  void read_records(C1& paths, C& container, F read_record) {

    for (auto rec : paths) {
      std::ifstream in;
      in.open(rec.path);
      std::string tmp_buffer;
      while (!in.eof()) {
        container.insert(read_record(in, tmp_buffer));
        in >> std::ws;
      }
    }

    return;
  }

  class __pcm_context {
    #ifdef CACHEPROFILE
    protected:
      PCM *instance;
      std::shared_ptr<SystemCounterState> initial_state;
    #endif

    public:
      __pcm_context();
      ~__pcm_context();
      unit_t cacheProfilerStart(unit_t);
      unit_t cacheProfilerStop(unit_t);
  };

  class __tcmalloc_context {
    public:
      unit_t heapProfilerStart(const string_impl&);
      unit_t heapProfilerStop(unit_t);
  };

  template <class C1, class C, class F>
  void read_records_with_resize(int size, C1& paths, C& container, F read_record) {

    if (size == 0) {
      return read_records(paths, container, read_record);
    }
    else {
      container.getContainer().resize(size);
    }

    int i = 0;
    for (auto rec : paths) {
      std::ifstream in;
      in.open(rec.path);

      std::string tmp_buffer;
      while (!in.eof()) {
        if (i >= container.size(unit_t {}) ) {
          throw std::runtime_error("Cannot read records, container size is too small");
        }
        container.getContainer()[i++] = read_record(in, tmp_buffer);
        in >> std::ws;
      }
    }

    return;
  }

  // Standard context for common builtins that use a handle to the engine (via inheritance)
  class __standard_context : public __k3_context {
    public:
    __standard_context(Engine&);


    unit_t openBuiltin(string_impl ch_id, string_impl builtin_ch_id, string_impl fmt);
    unit_t openFile(string_impl ch_id, string_impl path, string_impl fmt, string_impl mode);
    unit_t openSocket(string_impl ch_id, Address a, string_impl fmt, string_impl mode);

    bool hasRead(string_impl ch_id);
    template<typename T> T doRead(string_impl ch_id);
    template<typename T> Collection<R_elem<T>> doReadBlock(string_impl ch_id, int block_size);

    bool hasWrite(string_impl ch_id);
    template<typename T> unit_t doWrite(string_impl ch_id, T& val);

    unit_t close(string_impl chan_id);

    int random(int n);

    double randomFraction(unit_t);

    template <class T>
    int hash(const T& x) {
      // We implement hash_value for all of our types.
      // for ordered containers, so we may as well delegate to that.
      return static_cast<int>(hash_value(x));
    }

    template <class T>
    string_impl toJson(const T& in) {
      return K3::serialization::json::encode<T>(in);
    }

    template <class T>
    T range(int i) {
      T result;
      for (int j = 0; j < i; j++) {
        result.insert(j);
      }
      return result;
    }

    int truncate(double n) { return (int)n; }

    double real_of_int(int n) { return (double)n; }

    int get_max_int(unit_t) { return INT_MAX; }

    unit_t print(string_impl message);


    // TODO, implement, sharing code with prettify()
    template <class T>
    string_impl show(T t) {
      return string_impl("TODO: implement show()");
    }

    template <class T>
    T error(unit_t) {
      throw std::runtime_error("Error. Terminating");
      return T();
    }

    template <class T>
    unit_t ignore(T t) {
      return unit_t();
    }

    // TODO add a member to base_string, call that instead
    int strcomp(const string_impl& s1,const string_impl& s2) {
      const char* c1 = s1.c_str();
      const char* c2 = s2.c_str();
      if (c1 && c2) {
        return strcmp(c1,c2);
      }
      else if(c1) {
        return 1;
      }
      else if(c2) {
        return -1;
      }
      else {
        return 0;
      }

    }

    unit_t haltEngine(unit_t);

    unit_t drainEngine(unit_t);

    unit_t sleep(int n);

    template <template <class> class M, template <class> class C,
              template <typename...> class R>
    unit_t loadGraph(string_impl filepath, M<R<int, C<R_elem<int>>>>& c) {
      std::string tmp_buffer;
      std::ifstream in(filepath);

      int source;
      std::size_t position;
      while (!in.eof()) {
        C<R_elem<int>> edge_list;

        std::size_t start = 0;
        std::size_t end = start;
        std::getline(in, tmp_buffer);

        end = tmp_buffer.find(",", start);
        source = std::atoi(tmp_buffer.substr(start, end - start).c_str());

        start = end + 1;

        while (end != std::string::npos) {
          end = tmp_buffer.find(",", start);
          edge_list.insert(R_elem<int>(
              std::atoi(tmp_buffer.substr(start, end - start).c_str())));
          start = end + 1;
        }

        c.insert(R<int, C<R_elem<int>>>{source, std::move(edge_list)});
        in >> std::ws;
      }

      return unit_t{};
    }

    // TODO move to seperate context
    template <class C1>
    unit_t loadRKQ3(const C1& paths, K3::Map<R_key_value<string_impl, int>>& c)
    {
      for (auto r : paths) {
        // Buffers
        std::string tmp_buffer;
        R_key_value<string_impl, int> rec;
        // Infile
        std::ifstream in;
        in.open(r.path);

        // Parse by line
        while (!in.eof()) {
          std::getline(in, tmp_buffer, ',');
          rec.key = tmp_buffer;
          std::getline(in, tmp_buffer, ',');
          rec.value = std::atoi(tmp_buffer.c_str());
          // ignore last value
          std::getline(in, tmp_buffer);
          c.insert(rec);
        }
      }

      return unit_t{};
    }

   int lineCountFile(const string_impl& filepath) {
     std::cout << "LCF: " << filepath << std::endl;
     std::ifstream _in;
     _in.open(filepath);
     std::string tmp_buffer;
     std::getline(_in, tmp_buffer);
     std::cout << "LCF read: " << tmp_buffer << std::endl;
     return std::atoi(tmp_buffer.c_str());
   }

   template <class C1, template <class> class C, template <typename ...> class R>
   unit_t loadQ1(const C1& paths, C<R<int, string_impl>>& c) {
        K3::read_records(paths, c, [] (std::istream& in, std::string& tmp_buffer)   {
          R<int, string_impl> record;
          // Get pageURL
          std::getline(in, tmp_buffer, ',');
          record.pageURL = tmp_buffer;
          // Get pageRank
          std::getline(in, tmp_buffer, ',');
          record.pageRank = std::atoi(tmp_buffer.c_str());
          // Ignore avgDuration
          std::getline(in, tmp_buffer);
          //record.avgDuration = std::atoi(tmp_buffer.c_str());
          return record;
        });
        return unit_t {};
   }


   template <class C1, template<typename S> class C, template <typename ...> class R>
   unit_t loadQ2(const C1& paths, C<R<double, string_impl>>& c) {
        K3::read_records(paths, c, [] (std::istream& in, std::string& tmp_buffer)   {
          R<double, string_impl> record;
          // Get sourceIP
          std::getline(in, tmp_buffer, ',');
          record.sourceIP = tmp_buffer;

          // Ignore until adRevenue
          std::getline(in, tmp_buffer, ',');
          //record.destURL = tmp_buffer;
          std::getline(in, tmp_buffer, ',');
          //record.visitDate = tmp_buffer;

          // Get adRevenue
          std::getline(in, tmp_buffer, ',');
          record.adRevenue = std::atof(tmp_buffer.c_str());

          // Ignore the rest
          std::getline(in, tmp_buffer, ',');
          //record.userAgent = tmp_buffer;
          std::getline(in, tmp_buffer, ',');
          //record.countryCode = tmp_buffer;
          std::getline(in, tmp_buffer, ',');
          //record.languageCode = tmp_buffer;
          std::getline(in, tmp_buffer, ',');
          //record.searchWord = tmp_buffer;
          std::getline(in, tmp_buffer);
          //record.duration = std::atoi(tmp_buffer.c_str());
          return record;
        });
        return unit_t {};
   }

   template <class C1, template<typename S> class C, template <typename ...> class R>
   unit_t loadUVQ3(const C1& paths, C<R<double, string_impl, string_impl, string_impl>>& c) {
        K3::read_records(paths, c, [] (std::istream& in, std::string& tmp_buffer)   {
          R<double, string_impl, string_impl, string_impl> record;
          std::getline(in, tmp_buffer, ',');
          record.sourceIP = tmp_buffer;
          std::getline(in, tmp_buffer, ',');
          record.destURL = tmp_buffer;
          std::getline(in, tmp_buffer, ',');
          record.visitDate = tmp_buffer;
          std::getline(in, tmp_buffer, ',');
          record.adRevenue = std::atof(tmp_buffer.c_str());
          std::getline(in, tmp_buffer, ',');
          //record.userAgent = tmp_buffer;
          std::getline(in, tmp_buffer, ',');
          //record.countryCode = tmp_buffer;
          std::getline(in, tmp_buffer, ',');
          //record.languageCode = tmp_buffer;
          std::getline(in, tmp_buffer, ',');
          //record.searchWord = tmp_buffer;
          std::getline(in, tmp_buffer);
          //record.duration = std::atoi(tmp_buffer.c_str());
          return record;
        });
        return unit_t {};
   }

   template <class C, class F>
   unit_t logHelper(string_impl filepath, const C& c, F f, string_impl sep) {
     std::ofstream outfile;
     outfile.open(filepath);
     auto& container = c.getConstContainer();
     for (auto& elem : container) {
       f(outfile, elem, sep);
       outfile << "\n";
     }
     outfile.close();
     return unit_t();

   }
    Vector<R_elem<double>> zeroVector(int i);
    Vector<R_elem<double>> randomVector(int i);

    template <template <typename S> class C, class R>
    unit_t loadStrings(string_impl filepath, C<R>& c) {
      std::string line;
      std::ifstream infile(filepath);
      while (std::getline(infile, line)) {
        c.insert(R{line});
      }
      return unit_t{};
    }

    template <template<typename S> class C, class V>
    unit_t loadVector(string_impl filepath, C<R_elem<V>>& c) {
      std::string line;
      std::ifstream infile(filepath);
      char *saveptr;

      while (std::getline(infile, line)){
        char * pch;
        pch = strtok_r(&line[0],",", &saveptr);
        V v;
        while (pch) {
          R_elem<double> rec;
          rec.elem = std::atof(pch);
          v.insert(rec);
          pch = strtok_r(NULL,",", &saveptr);
        }
        R_elem<V> rec2 {v};
        c.insert(rec2);
      }
      return unit_t();
    }

    template <template <typename S> class C, template <typename...> class R, class V>
    unit_t loadVectorLabel(int dims, string_impl filepath, C<R<double, V>>& c) {
      // Buffers
      std::string tmp_buffer;
      R<double, V> rec;
      // Infile
      std::ifstream in;
      in.open(filepath);
      char* saveptr;

      // Parse by line
      while (!in.eof()) {
        V v;
        R_elem<double> r;
        for (int j = 0; j < dims; j++) {
          std::getline(in, tmp_buffer, ',');
          r.elem = std::atof(tmp_buffer.c_str());
          v.insert(r);
        }
        std::getline(in, tmp_buffer, ',');
        rec.class_label = std::atof(tmp_buffer.c_str());
        rec.elem = v;
        c.insert(rec);

        in >> std::ws;
      }

      return unit_t{};
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
    shared_ptr<RE2> pattern;
    __string_context();

    string_impl itos(int i);

    string_impl rtos(double d);

    string_impl atos(Address a);

    F<Collection<R_elem<string_impl>>(const string_impl &)> regex_matcher(const string_impl&);
    Collection<R_elem<string_impl>> regex_matcher_q4(const string_impl&);

    template <class S> S slice_string(const S& s, int x, int y) {
      return s.substr(x, y);
    }

    // Split a string by substrings
    Seq<R_elem<string_impl>> splitString(string_impl, const string_impl&);
    string_impl takeUntil(const string_impl& s, const string_impl& splitter);
    int countChar(const string_impl& s, const string_impl& splitter);
    int tpch_date(const string_impl& s);
  };



} // namespace K3

#endif /* K3_RUNTIME_BUILTINS_H */
