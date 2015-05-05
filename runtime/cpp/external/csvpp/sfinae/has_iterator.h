#ifndef CSVPP_SFINAE_HAS_ITERATOR_H
#define CSVPP_SFINAE_HAS_ITERATOR_H

#include <csvpp/sfinae/sfinae.h>

namespace csv {
namespace sfinae {

template <typename T>
class has_iterator {
    template <typename U> static yes &test(typename U::iterator*);
    template <typename> static no &test(...);
public:
    static const bool value = sizeof(test<T>(0)) == sizeof(yes);
};

} // namespace sfinae
} // namespace csv

#endif // CSVPP_SFINAE_HAS_ITERATOR_H
