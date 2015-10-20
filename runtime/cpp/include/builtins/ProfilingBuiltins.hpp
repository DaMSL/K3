#ifndef K3_PROFILINGBUILTINS
#define K3_PROFILINGBUILTINS

#include <cstdint>
#include <cmath>
#include <string>
#include <fstream>
#include <chrono>
#include <atomic>
#include <random>

#ifdef K3_PCM
#include <cpucounters.h>
#endif

#ifdef K3_TCMALLOC
#include "gperftools/heap-profiler.h"
#endif

#ifdef K3_JEMALLOC
#include "jemalloc/jemalloc.h"
#endif

#include "boost/thread/mutex.hpp"
#include "boost/thread/thread.hpp"

#include "Common.hpp"

namespace K3 {

class __heap_profiler {
 public:
  __heap_profiler() { heap_profiler_done.clear(); }

 protected:
  std::shared_ptr<boost::thread> heap_profiler_thread;
  std::atomic_flag heap_profiler_done;
  int time_milli() {
    auto t = std::chrono::system_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        t.time_since_epoch());
    return elapsed.count();
  }

  template <class I, class B>
  void heap_series_start(I init, B body) {
    heap_profiler_thread = make_shared<boost::thread>([this, &init, &body]() {
      std::string name = init();
      int i = 0;
      std::cout << "Heap profiling thread starting: " << name << " ("
                << time_milli() << ")" << std::endl;
      while (!heap_profiler_done.test_and_set()) {
        heap_profiler_done.clear();
        boost::this_thread::sleep_for(boost::chrono::milliseconds(250));
        body(name, i++);
      }
      std::cout << "Heap profiling thread terminating: " << name << " ("
                << time_milli() << ")" << std::endl;
    });
  }

  void heap_series_stop() {
    heap_profiler_done.test_and_set();
    if (heap_profiler_thread) {
      heap_profiler_thread->interrupt();
    }
  }
};

class ProfilingBuiltins: public __heap_profiler {
 public:
  ProfilingBuiltins();
  // PCM
  unit_t pcmStart(unit_t);
  unit_t pcmStop(unit_t);
  // TCMalloc
  unit_t tcmallocStart(unit_t);
  unit_t tcmallocStop(unit_t);
  // JEMalloc
  unit_t jemallocStart(unit_t);
  unit_t jemallocStop(unit_t);
  unit_t jemallocDump(unit_t);

 protected:
#ifdef K3_PCM
  PCM *pcm_instance_;
  shared_ptr<SystemCounterState> pcm_initial_state_;
#endif
};

  namespace lifetime {
    using lifetime_t = uint32_t;

    class sampler {
     public:
      void push() {
        ++object_counter;
      }

      void dump() {
        std::cout << "Object count: " << object_counter << std::endl;
        return;
      }

      uint64_t object_counter;
    };

    class histogram {
     public:
      void push(const lifetime_t& l, const size_t& sz) { return; }
      void dump() { return; }
    };

    // A reservoir sampling implementation using Algorithm L from:
    // http://dl.acm.org/citation.cfm?id=198435
    using LOSample = std::pair<lifetime_t, size_t>;

    template<size_t n = 1 << 10>
    class reservoir_sampler {
    public:
      reservoir_sampler(const base_string& path)
        : out(static_cast<std::string>(path)), gen(rd), u(), sz(0)
      {
        // Initialize w, S for use in the first instance that the reservoir is full.
        step(true);
      }

      void step(bool initial = false) {
        w = (initial? 1 : w) * exp(log(u(gen))/static_cast<double>(n));
        S = std::floor( log(u(gen)) / log(1-w) );
      }

      void push(const lifetime_t& l, const size_t& o) {
        if ( sz < n ) { x[sz] = std::make_pair(l,o); sz++; }
        else if ( S > 0 ) { S--; }
        else {
          x[std::floor(n*u(gen))] = std::make_pair(l, o);
          step();
        }
      }

      void dump() {
        for (auto i : reservoir) {
          out << i.first << "," << i.second << std::endl;
        }
      }

      std::ofstream out;

      std::random_device rd;
      std::mt19937 gen;
      std::uniform_real_distribution<> u;

      uint64_t S;
      uint32_t sz;
      double w;
      std::array<LOSample, n> x;
    };

    class equi_width_histogram {
     public:
      template<T>
      using HSpec = std::pair<T, T>; // Left as max value, right as bucket width.
      using Frequency = uint64_t;

      histogram(const base_string& path, HSpec<lifetime_t> lspec, HSpec<size_t> ospec)
        : out(static_cast<std::string>(path)),
          lifetime_spec(lspec), objsize_spec(ospec),
          lbuckets(num_buckets(lifetime_spec)), obuckets(num_buckets(objsize_spec))
          hdata(lbuckets * obuckets)
      {}

      void push(const lifetime_t& l, const size_t& o) {
        hdata[bucket_index(l, o)] += 1;
      }

      void dump() {
        for (size_t i = 0; i < lbuckets; ++i) {
          for (size_t j = 0; j < obuckets; ++j) {
            out << i << "," << j << "," << hdata[i*lbuckets+j] << std::endl;
          }
        }
      }

      template<typename T> size_t num_buckets(const HSpec<T>& spec) {
        return static_cast<size_t>(spec.first / spec.second);
      }

      template<typename T> size_t bucket_index(const lifetime_t& l, const size_t& o) {
        auto lidx = (lifetime_spec.first - l) / lifetime_spec.second;
        auto sidx = (objsize_spec.first - o) / objsize_spec.second;
        return lidx * lbuckets + sidx;
      }

      std::ofstream out;

      HSpec<lifetime_t> lifetime_spec;
      HSpec<size_t> objsize_spec;
      size_t lbuckets;
      size_t obuckets;

      vector<Frequency> hdata;
    };

    #ifdef K3_LT_SAMPLE
    extern __thread sampler __active_lt_profiler;
    #endif

    #ifdef K3_LT_HISTOGRAM
    extern __thread histogram __active_lt_profiler;
    #endif

    class sentinel {
     public:
      sentinel() {}
      ~sentinel();
    };
  }
}  // namespace K3

#endif
