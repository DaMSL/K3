#ifndef K3_MOSAICBUILTINS
#define K3_MOSAICBUILTINS

#include <climits>
#include <cmath>
#include <boost/thread/mutex.hpp>
#include <boost/thread/thread.hpp>

#include "types/BaseString.hpp"
#include "Common.hpp"
#include "collections/Collection.hpp"
#include "collections/MapE.hpp"
#include "collections/Set.hpp"
#include "collections/Seq.hpp"

namespace K3 {

class MosaicBuiltins {
 public:
  template <class A, class B>
  auto max(A a, B b) {
    return (a > b ? a : b);
  }

  template <class A, class B>
  auto min(A a, B b) {
    return (a < b ? a : b);
  }

  template <class A>
  auto abs(A a) {
    return std::abs(a);
  }

  int date_part(const string_impl& s, int y) {
    if (s == "day" || s == "DAY") {
      return y % 100;
    }
    if (s == "month" || s == "MONTH") {
      return (y % 10000) / 100;
    }
    if (s == "year" || s == "YEAR") {
      return (y / 10000);
    }
    throw std::runtime_error("Unrecognized date part key: " + s);
  }

  double real_of_int(int n) { return static_cast<double>(n); }
  int get_max_int(unit_t) { return INT_MAX; }
  int truncate(double n) { return static_cast<int>(n); }
  int regex_match_int(const string_impl& s1, const string_impl& s2) {
    bool b = std::regex_match(s2.c_str(), std::regex(s1.c_str()));
    return b ? 1 : 0;
  }

  // For routing
  /*
  void free_buckets_loop(
      Set<R_i<Address>> &results,
      const std::unordered_map<int, R_key_value<int, R_key_value<int, Seq<R_i<int>>>>> &dim_bounds,
      const std::vector<R_i<int>> free_dims,
      std::vector<R_i<int>>::const_iterator free_it,
      std::function<std::function<Address(int)>(int)> get_ring_node,
      int val,
      int max_val) {
    // Assume it exists -- don't check for error
    auto it  = dim_bounds.find((*free_it).i);
    auto dim  = it->second.value.key;
    auto &rng = it->second.value.value;
    // Check if we've reach the end
    if (free_it == free_dims.end()) {
      results.insert(get_ring_node(val)(max_val));
    } else {
      auto free_it2 = free_it + 1;
      for (auto &idx : rng.getConstContainer()) {
        val += idx.i * dim;
        free_buckets_loop(results, dim_bounds, free_dims,
            free_it2, get_ring_node, val, max_val);
      }
    }
  }
  */

  //Set<R_i<Address>> free_buckets_builtin(
  //    const std::unordered_map<int, R_key_value<int, R_key_value<int, Seq<R_i<int>>>>> &dim_bounds,
  //    const Collection<R_i<int>> &free_dims,
  //    std::function<std::function<Address(int)>(int)> get_ring_node,
  //    int bound_bucket,
  //    int max_val) {
  //  Set<R_i<Address>> results;
  //  auto &free_dims_c = free_dims.getConstContainer();
  //  free_buckets_loop(results, dim_bounds, free_dims_c, free_dims_c.begin(),
  //      get_ring_node, bound_bucket, max_val);
  //  return results;
  //}

  }; // class

}  // namespace K3

#endif
