#ifndef K3_MOSAICBUILTINS
#define K3_MOSAICBUILTINS

#include <climits>
#include <boost/thread/mutex.hpp>
#include <boost/thread/thread.hpp>

#include "types/BaseString.hpp"
#include "Common.hpp"

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
  void free_buckets_loop(
      Set<R_i<Address>> &results,
      const std::unordered_map<int, R_key_value<int, Seq<R_i<int>>>> &dim_bounds,
      const std::vector<R_i> free_dims,
      std::vector<R_i>::const_iterator free_it,
      std::function<Address(int)> get_ring_node,
      int val,
      int max_val) {
    // Assume it exists -- don't check for error
    auto it  = dim_bounds.find((*free_it).i);
    auto dim  = it->second().key;
    auto &rng = it->second().value;
    // Check if we've reach the end
    if (free_it == free_dims.end()) {
      results.insert(get_ring_node(val)(max_val));
    } else {
      for (int idx : rng.getConstContainer()) {
        val += idx * dim;
        native_free_buckets_loop(results, dim_bounds_c, free_dims, free_it+1, get_ring_node, val, max_val);
      }
    }

  Set<R_i<Address>> free_buckets_builtin(
      const MapE<R_key_value<int, R_key_value<int, Seq<R_i<int>>>>> &dim_bounds,
      const Collection<R_i<int>> &free_dims,
      std::function<Address(int)> get_ring_node,
      int bound_bucket,
      int max_val) {
    Set<R_i<Address>> results;
    auto &free_dims_c = free_dims.getConstContainer();
    auto &dim_bound_c = dim_bounds.getConstContainer();
    free_buckets_loop(results, dim_bounds_c, free_dims_c, free_dims_c.begin(), get_ring_node, bound_bucket, max_val);
    return results;
  }

  }; // class

}  // namespace K3

#endif
