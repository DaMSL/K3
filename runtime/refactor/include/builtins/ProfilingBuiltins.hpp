#ifndef K3_PROFILINGBUILTINS
#define K3_PROFILINGBUILTINS

#include <string>
#include <chrono>

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

class ProfilingBuiltins {
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

}  // namespace K3

#endif
