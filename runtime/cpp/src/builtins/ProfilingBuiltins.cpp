#include <time.h>
#include <string>
#include <chrono>

#include "builtins/ProfilingBuiltins.hpp"

namespace K3 {

ProfilingBuiltins::ProfilingBuiltins() {}

// PCM
unit_t ProfilingBuiltins::pcmStart(unit_t) {
#ifdef K3_PCM
  pcm_instance_ = PCM::getInstance();
  if (pcm_instance_->program() != PCM::Success) {
    std::cout << "PCM startup error!" << std::endl;
  }
  pcm_initial_state_ =
      std::make_shared<SystemCounterState>(getSystemCounterState());
#else
  std::cout << "pcmStart: PCM not set. not starting." << std::endl;
#endif
  return unit_t();
}

unit_t ProfilingBuiltins::pcmStop(unit_t) {
#ifdef K3_PCM
  SystemCounterState after_sstate = getSystemCounterState();
  std::cout << "QPI Incoming: "
            << getAllIncomingQPILinkBytes(*pcm_initial_state_, after_sstate)
            << std::endl;
  std::cout << "QPI Outgoing: "
            << getAllOutgoingQPILinkBytes(*pcm_initial_state_, after_sstate)
            << std::endl;
  std::cout << "L2 cache hit ratio:"
            << getL2CacheHitRatio(*pcm_initial_state_, after_sstate)
            << std::endl;
  std::cout << "L3 cache hit ratio:"
            << getL3CacheHitRatio(*pcm_initial_state_, after_sstate)
            << std::endl;
  std::cout << "Instructions per clock:" << getIPC(*pcm_initial_state_,
                                                   after_sstate) << std::endl;
  pcm_instance_->cleanup();
#endif
  return unit_t();
}

// TCMalloc
unit_t ProfilingBuiltins::tcmallocStart(unit_t) {
#ifdef K3_TCMALLOC
  HeapProfilerStart("K3");
#ifdef K3_HEAP_SERIES
  auto init = []() {
    auto start = time_milli();
    auto start_str = to_string((start - (start % 250)) % 100000);
    return std::string("K3." + start_str + ".");
  };
  auto body = [](std::string& name, int i) {
    std::string heapName = name + to_string(i);
    HeapProfilerDump(heapName.c_str());
  };
  heap_series_start(init, body);
#endif
#else
  std::cout << "tcmallocStart: K3_TCMALLOC is not defined. not starting."
            << std::endl;
#endif
  return unit_t{};
}

unit_t ProfilingBuiltins::tcmallocStop(unit_t) {
#ifdef K3_TCMALLOC
#ifdef K3_HEAP_SERIES
  heap_series_stop();
#endif
  HeapProfilerDump("End of Program");
  HeapProfilerStop();
#endif
  return unit_t{};
}

// JEMalloc
unit_t ProfilingBuiltins::jemallocStart(unit_t) {
#ifdef K3_JEMALLOC
  bool enable = true;
  mallctl("prof.active", NULL, 0, &enable, sizeof(enable));
#ifdef K3_HEAP_SERIES
  auto init = []() {
    const char* hp_prefix;
    size_t hp_sz = sizeof(hp_prefix);
    mallctl("opt.prof_prefix", &hp_prefix, &hp_sz, NULL, 0);
    auto start = time_milli();
    auto start_str = to_string((start - (start % 250)) % 100000);
    return std::string(hp_prefix) + "." + start_str + ".0.t";
  };
  auto body = [](std::string& name, int i) {
    std::string heapName = name + to_string(i) + ".heap";
    const char* hnPtr = heapName.c_str();
    mallctl("prof.dump", NULL, 0, &hnPtr, sizeof(hnPtr));
  };
  heap_series_start(init, body);
#endif
#else
  std::cout << "jemallocStart: JEMALLOC is not defined. not starting."
            << std::endl;
#endif
  return unit_t{};
}

unit_t ProfilingBuiltins::jemallocStop(unit_t) {
#ifdef K3_JEMALLOC
#ifdef K3_HEAP_SERIES
  heap_series_stop();
#endif
  mallctl("prof.dump", NULL, 0, NULL, 0);
  bool enable = false;
  mallctl("prof.active", NULL, 0, &enable, sizeof(enable));
#endif
  return unit_t{};
}

unit_t ProfilingBuiltins::jemallocDump(unit_t) {
#ifdef K3_JEMALLOC
  mallctl("prof.dump", NULL, 0, NULL, 0);
#endif
  return unit_t{};
}

}  // namespace K3
