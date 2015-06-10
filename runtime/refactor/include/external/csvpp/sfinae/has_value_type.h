#ifndef CSVPP_SFINAE_HAS_VALUE_TYPE_H
#define CSVPP_SFINAE_HAS_VALUE_TYPE_H

#include <csvpp/sfinae/sfinae.h>

namespace csv {
namespace sfinae {

template <typename T>
class has_value_type {
    template <typename U> static yes &test(typename U::value_type*);
    template <typename> static no &test(...);
public:
    static const bool value = sizeof(test<T>(0)) == sizeof(yes);
};

} // namespace sfinae
} // namespace csv

#endif // CSVPP_SFINAE_HAS_VALUE_TYPE_H
