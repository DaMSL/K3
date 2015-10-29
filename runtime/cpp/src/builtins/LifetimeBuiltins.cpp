#include "builtins/LifetimeBuiltins.hpp"

namespace K3 {
  namespace lifetime {
#ifdef K3_LT_SAMPLE
    thread_local sampler<> __active_lt_profiler("./sampler.out");
#endif

#ifdef K3_LT_HISTOGRAM
    thread_local histogram __active_lt_profiler;
#endif

    sentinel::~sentinel() {
      auto end = std::chrono::high_resolution_clock::now();
      auto lifetime = end - start;
#if defined(K3_LT_SAMPLE) || defined(K3_LT_HISTOGRAM)
      __active_lt_profiler.push(lifetime.count(), object_size);
#endif
    }

    sentinel& sentinel::operator=(sentinel const& other) {
      auto end = std::chrono::high_resolution_clock::now();
      auto lifetime = end - start;
#if defined(K3_LT_SAMPLE) || defined(K3_LT_HISTOGRAM)
      __active_lt_profiler.push(lifetime.count(), object_size);
#endif
      start = std::chrono::high_resolution_clock::now();
      object_size = other.object_size;

      return *this;
    }

    sentinel& sentinel::operator=(sentinel&& other) {
      auto end = std::chrono::high_resolution_clock::now();
      auto lifetime = end - start;
#if defined(K3_LT_SAMPLE) || defined(K3_LT_HISTOGRAM)
      __active_lt_profiler.push(lifetime.count(), object_size);
#endif

      start = other.start;
      object_size = other.object_size;

      return *this;
    }
  }
}
