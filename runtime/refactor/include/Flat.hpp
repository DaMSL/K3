#ifndef K3_FLAT
#define K3_FLAT

#include "Common.hpp"

namespace K3 {
// By default, we assume values are not flat
template <class T>
struct is_flat : std::false_type {};

// We enumerate the set of flat types. All K3 Primitives
// and any composition of flat types
template <>
struct is_flat<int> : std::true_type {};

template <>
struct is_flat<double> : std::true_type {};

template <>
struct is_flat<std::string> : std::true_type {};

template <>
struct is_flat<base_string> : std::true_type {};

template <typename T, typename... TS>
struct is_flat<std::tuple<T, TS...>> {
  static constexpr bool value =
      is_flat<T>::value && ((sizeof...(TS) == 0) ? true : is_flat <std::tuple<TS...>>::value);
};

}  // namespace K3

#endif
