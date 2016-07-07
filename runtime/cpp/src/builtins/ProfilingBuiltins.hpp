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

#if defined(K3_JEMALLOC) || defined(K3_JEMALLOC_HEAP_SIZE)
#include "jemalloc/jemalloc.h"
#endif

#ifdef BSL_ALLOC
#ifdef BCOUNT
#include <bsl_iostream.h>
#endif
#endif

#include "boost/thread/mutex.hpp"
#include "boost/thread/thread.hpp"

#include "Common.hpp"

#include "unistd.h"
#include "sys/types.h"

namespace K3 {

class __heap_profiler {
 public:
  __heap_profiler() { heap_profiler_done = false; }
  // set period in ms
  void __set_period(int p) { period_ = p; }
  void heap_series_start(std::function<std::string()> init,
                         std::function<void(std::string&, int)> body,
                         std::function<void(std::string&)> shutdown);
  void heap_series_stop();

 protected:
  int period_ = 250;
  std::unique_ptr<boost::thread> heap_profiler_thread;
  std::atomic_bool heap_profiler_done;

  static int64_t time_milli() {
    auto t = std::chrono::system_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        t.time_since_epoch());
    return elapsed.count();
  }

  // ensures shutdown happens
  struct shutdown_object {
    shutdown_object(std::function<void(std::string&)> s, std::string& name) : s_(s), name_(name) {}
    ~shutdown_object() {
        s_(name_);
        std::cout << "Heap profiling thread terminating: " << name_ << " ("
                  << __heap_profiler::time_milli() << ")" << std::endl;
    }
    std::function<void(std::string&)> s_;
    std::string name_;
  };

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
  void dumpStatsToFile(std::string& name);
  unit_t jemallocTotalSizeStart(unit_t);
  unit_t jemallocTotalSizeStop(unit_t);
  unit_t jemallocDump(unit_t);

  unit_t perfRecordStart(unit_t);
  unit_t perfRecordStop(unit_t);

  unit_t perfStatStart(unit_t);
  unit_t perfStatStop(unit_t);

  // BSL Allocator
  unit_t vmapStart(const Address& addr);
  unit_t vmapStop(unit_t);
  unit_t vmapDump(unit_t);

 protected:
#ifdef K3_JEMALLOC_HEAP_SIZE
  size_t mib_[2] = {0};
  size_t miblen_ = 2;
  std::vector<std::pair<uint64_t, uint64_t>> stats_;
#endif

#ifdef BSL_ALLOC
#ifdef BCOUNT
  bsl::ofstream vmapAllocLog;
#endif
#endif

#ifdef K3_PCM
  PCM *pcm_instance_;
  shared_ptr<SystemCounterState> pcm_initial_state_;
#endif

#if defined(K3_PERF_STAT) || defined(K3_PERF_RECORD)
  pid_t pid;
#endif
};

}  // namespace K3

#endif
