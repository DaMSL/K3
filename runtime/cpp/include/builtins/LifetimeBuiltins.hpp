#ifndef K3_LIFETIME
#define K3_LIFETIME

#include <array>
#include <chrono>
#include <fstream>
#include <iostream>
#include <random>
#include <string>
#include <sstream>
#include <vector>
#include <thread>

namespace K3 {
  namespace lifetime {
    using lifetime_t = int64_t;

    const uint32_t RESERVOIR_SIZE = 1000;

    class counter {
     public:
      void push() { ++object_counter; }
      void dump() { std::cout << "Object count: " << object_counter << std::endl; }
      uint64_t object_counter;
    };

    // A reservoir sampling implementation using Algorithm L from:
    // http://dl.acm.org/citation.cfm?id=198435
    using LOSample = std::pair<lifetime_t, size_t>;

    template<size_t n = 1 << 10>
    class sampler {
    public:
      sampler(const std::string& path)
        : gen(std::random_device()()), u(), sz(0)
      {
        auto path_stream = std::ostringstream {};
        path_stream << path << "_" << std::this_thread::get_id();
        out = std::ofstream(path_stream.str());
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
        for (const auto& i : x) {
          out << i.first << "," << i.second << std::endl;
        }
      }

     protected:
      std::ofstream out;

      std::mt19937 gen;
      std::uniform_real_distribution<> u;

      uint64_t S;
      uint32_t sz;
      double w;
      std::array<LOSample, n> x;
    };

    class histogram {
     public:
      template<typename T>
      using HSpec = std::pair<T, T>; // Left as max value, right as bucket width.
      using Frequency = uint64_t;

      histogram(const std::string& path, HSpec<lifetime_t> lspec, HSpec<size_t> ospec)
        : out(path),
          lifetime_spec(lspec), objsize_spec(ospec),
          lbuckets(num_buckets(lifetime_spec)), obuckets(num_buckets(objsize_spec)),
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

      size_t bucket_index(const lifetime_t& l, const size_t& o) {
        auto lidx = (lifetime_spec.first - l) / lifetime_spec.second;
        auto sidx = (objsize_spec.first - o) / objsize_spec.second;
        return lidx * lbuckets + sidx;
      }

     protected:
      std::ofstream out;

      HSpec<lifetime_t> lifetime_spec;
      HSpec<size_t> objsize_spec;
      size_t lbuckets;
      size_t obuckets;

      std::vector<Frequency> hdata;
    };

    class sentinel {
     public:
      sentinel(std::size_t sz = 0): object_size(sz), start(std::chrono::high_resolution_clock::now()) {}
      sentinel(sentinel&&) = default;
      sentinel(sentinel const& other): object_size(other.object_size), start(std::chrono::high_resolution_clock::now()) {}
      ~sentinel();

      sentinel& operator=(sentinel const&);
      sentinel& operator=(sentinel&&);


      std::size_t object_size;
      std::chrono::high_resolution_clock::time_point start;
    };
  }
}
#endif
