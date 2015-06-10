#ifndef CSVPP_SFINAE_HAS_FCN_SERIALIZE_H
#define CSVPP_SFINAE_HAS_FCN_SERIALIZE_H

#include <csvpp/sfinae/sfinae.h>

namespace csv {
namespace sfinae {

template <typename T, typename Archive>
class has_fcn_serialize {
    template<typename U, void (U::*f) (Archive&, const unsigned int)> struct match;
    template<typename U> static yes &test(match<U, &U::serialize >*);
    template<typename> static no &test(...);
public:
    static const bool value = sizeof(test<T>(0)) == sizeof(yes);
};

} // namespace sfinae
} // namespace csv

#endif // CSVPP_SFINAE_HAS_FCN_SERIALIZE_H
