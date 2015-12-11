#include <time.h>
#include <string>
#include <chrono>

#include "builtins/ProfilingBuiltins.hpp"

#include <sys/wait.h>

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
  auto init = [this]() {
    auto start = time_milli();
    auto start_str = std::to_string((start - (start % 250)) % 100000);
    return std::string("K3." + start_str + ".");
  };
  auto body = [](std::string& name, int i) {
    std::string heapName = name + std::to_string(i);
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
  auto init = [this]() {
    const char* hp_prefix;
    size_t hp_sz = sizeof(hp_prefix);
    mallctl("opt.prof_prefix", &hp_prefix, &hp_sz, NULL, 0);
    auto start = time_milli();
    auto start_str = std::to_string((start - (start % 250)) % 100000);
    return std::string(hp_prefix) + "." + start_str + ".0.t";
  };
  auto body = [](std::string& name, int i) {
    std::string heapName = name + std::to_string(i) + ".heap";
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

  unit_t ProfilingBuiltins::perfStart(unit_t) {
#ifndef K3_PERF
    std::cout << "K3_PERF not set, continuing without perf." << std::endl;
#else
    std::cout << "K3_PERF set, starting perf record..." << std::endl;
    auto pid_stream = std::stringstream {};
    pid_stream << ::getpid();

    std::cout << pid_stream.str() << std::endl;

    pid = fork();

    if (pid == 0) {
      auto fd = open("/dev/null", O_RDWR);
      dup2(fd, 1);
      dup2(fd, 2);

      auto freq = std::getenv("K3_PERF_FREQUENCY");

      exit(execl("/usr/bin/perf", "perf", "record", "-F", (freq ? freq : "10"), "-o", "perf.data", "-p", pid_stream.str().c_str(), nullptr));
    }
#endif

    return unit_t {};
  }

  unit_t ProfilingBuiltins::perfStop(unit_t) {
#ifndef K3_PERF
    std::cout << "K3_PERF not set, ignoring call to stop perf." << std::endl;
#else
    std::cout << "K3_PERF set, stopping perf record..." << std::endl;
    kill(pid, SIGINT);
    waitpid(pid, nullptr, 0);
#endif

    return unit_t {};
  }
}  // namespace K3
