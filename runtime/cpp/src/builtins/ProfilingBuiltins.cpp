#include <time.h>
#include <string>
#include <chrono>
#include <fstream>

#include "builtins/ProfilingBuiltins.hpp"
#include "collections/MultiIndex.hpp"

#include <sys/wait.h>

namespace K3 {
  void __heap_profiler::heap_series_start(std::function<std::string()> init,
                         std::function<void(std::string&, int)> body,
                         std::function<void(std::string&)> shutdown) {
    heap_profiler_thread = make_unique<boost::thread>([this, init, body, shutdown]() {
        // Make sure we can't be interrupted here
        boost::this_thread::disable_interruption di;
        std::string name = init();
        int i = 0; // steps since start
        // firing the interruption exception will still RAII this shutdown
        shutdown_object(shutdown, name);
        std::cout << "Heap profiling thread starting: " << name << " ("
                  << time_milli() << ")" << std::endl;
        // allow interruption out of sleep
        while (!heap_profiler_done) {
          {
            boost::this_thread::restore_interruption ri(di);
            boost::this_thread::sleep_for(boost::chrono::milliseconds(period_));
          }
          body(name, i++);
        }
    });
  }

  void __heap_profiler::heap_series_stop() {
    heap_profiler_done = true;
    if (heap_profiler_thread) {
      heap_profiler_thread->interrupt(); // wake up the thread if needed
    }
    heap_profiler_thread->join();
  }

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
  auto shutdown = [](std::string&) {}
  heap_series_start(init, body, shutdown);
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
  auto shutdown = [](std::string& name) {};
  heap_series_start(init, body, shutdown);
#endif // K3_HEAP_SERIES

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

  void ProfilingBuiltins::dumpStatsToFile (std::string& name) {
#ifdef K3_JEMALLOC_HEAP_SIZE
    std::ofstream file;
    file.open(name, std::ios::out | std::ios::app);
    for (auto p : stats_) {
      file << p.first << "," << p.second << "\n";
    }
    file.close();
    stats_.clear();
#endif
  }

  // gather statistics for the allocated size over time
  // No need for jemalloc profiling (slow)
  unit_t ProfilingBuiltins::jemallocTotalSizeStart(unit_t) {
#ifdef K3_JEMALLOC_HEAP_SIZE
    mallctlnametomib("stats.allocated", mib_, &miblen_);
    std::cout << "Starting JEMALLOC total memory size tracking. Period: " << period_ << "\n";

    auto init = []() {
      std::string name = "heap_size.txt";
      // clear the existing file
      std::ofstream file;
      file.open(name, std::ios::trunc);
      file.close();
      return name;
    };

    // Get time & allocated size and save them
    auto body = [this](std::string& name, int) {
      size_t epoch = 1;
      size_t epoch_sz = sizeof(epoch);
      mallctl("epoch", &epoch, &epoch_sz, &epoch, epoch_sz);
      const int threshold = 10; // how often to save (how many samples)
      size_t alloc_sz = 0;
      size_t alloc_sz_len = sizeof(alloc_sz);
      uint64_t time = time_milli();
      mallctl("stats.allocated", &alloc_sz, &alloc_sz_len, NULL, 0);
      stats_.push_back(std::make_pair(time, (uint64_t)alloc_sz));
      // check if we need to dump
      if (stats_.size() > threshold) {
        dumpStatsToFile(name);
      }
    };

    // Make sure we clear any remaining stats
    auto shutdown = [this](std::string& name) {
      if (!stats_.empty()) {
        dumpStatsToFile(name);
      }
    };

    heap_series_start(init, body, shutdown);
#endif // K3_JEMALLOC_HEAP_SIZE
    return unit_t();
  }

  unit_t ProfilingBuiltins::jemallocTotalSizeStop(unit_t) {
#ifdef K3_JEMALLOC_HEAP_SIZE
    heap_series_stop();
    // wait for a signal from the profiling thread
#endif // K3_JEMALLOC_HEAP_SIZE
    return unit_t();
  }

  unit_t ProfilingBuiltins::perfRecordStart(unit_t) {
#ifndef K3_PERF_RECORD
    std::cout << "K3_PERF_RECORD not set, continuing without perf." << std::endl;
#else
    std::cout << "K3_PERF_RECORD set, starting perf record..." << std::endl;
    auto pid_stream = std::stringstream {};
    pid_stream << ::getpid();

    std::cout << pid_stream.str() << std::endl;

    pid = fork();

    if (pid == 0) {
      auto fd = open("/dev/null", O_RDWR);
      dup2(fd, 1);
      dup2(fd, 2);

      auto freq = std::getenv("K3_PERF_RECORD_FREQUENCY");

      exit(execl("/usr/bin/perf", "perf", "record", "-a", "--call-graph", "dwarf", "-F", (freq ? freq : "10"), "-o", "perf.data", "-p", pid_stream.str().c_str(), nullptr));
    }
#endif

    return unit_t {};
  }

  unit_t ProfilingBuiltins::perfRecordStop(unit_t) {
#ifndef K3_PERF_RECORD
    std::cout << "K3_PERF_RECORD not set, ignoring call to stop perf." << std::endl;
#else
    std::cout << "K3_PERF_RECORD set, stopping perf record..." << std::endl;
    kill(pid, SIGINT);
    waitpid(pid, nullptr, 0);
#endif

    return unit_t {};
  }

  unit_t ProfilingBuiltins::perfStatStart(unit_t) {
#ifndef K3_PERF_STAT
    std::cout << "K3_PERF_STAT not set, continuing without perf." << std::endl;
#else
    std::cout << "K3_PERF_STAT set, starting perf stat..." << std::endl;
    auto pid_stream = std::stringstream {};
    pid_stream << ::getpid();

    pid = fork();

    if (pid == 0) {
      auto fd = open("perf.stat", O_RDWR | O_CREAT);
      dup2(fd, 1);
      dup2(fd, 2);

      auto events = std::getenv("K3_PERF_STAT_EVENTS");

      exit(execl("/usr/bin/perf", "perf", "stat", "-e", (events ? events: "cache-misses,cache-references"),
                 "-p", pid_stream.str().c_str(), nullptr));
    }
#endif

    return unit_t {};
  }

  unit_t ProfilingBuiltins::perfStatStop(unit_t) {
#ifndef K3_PERF_STAT
    std::cout << "K3_PERF_STAT not set, ignoring call to stop perf." << std::endl;
#else
    std::cout << "K3_PERF_STAT set, stopping perf stat..." << std::endl;
    kill(pid, SIGINT);
    waitpid(pid, nullptr, 0);
#endif

    return unit_t {};
  }

unit_t ProfilingBuiltins::vmapStart(const Address& addr) {
#ifdef BSL_ALLOC
#ifdef BCOUNT
    std::string alloc_out_path = std::string("vmapalloc_") + addressAsString(addr);
    vmapAllocLog.open(alloc_out_path.c_str());
#endif
#endif
    return unit_t{};
}

unit_t ProfilingBuiltins::vmapStop(unit_t) {
#ifdef BSL_ALLOC
#ifdef BCOUNT
    vmapAllocLog.flush();
    vmapAllocLog.close();
#endif
#endif
    return unit_t{};
}

unit_t ProfilingBuiltins::vmapDump(unit_t) {
#ifdef BSL_ALLOC
#ifdef BCOUNT
    if ( vmapAllocLog ) {
      mpool->print(vmapAllocLog);
    }
#endif
#endif
    return unit_t{};
}
}  // namespace K3
