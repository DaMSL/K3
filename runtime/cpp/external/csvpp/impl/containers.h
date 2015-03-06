#ifndef CSVPP_CONTAINERS_H
#define CSVPP_CONTAINERS_H

// csv++
#include <csvpp/iter.h>

// fwd
namespace csv {
class writer;
class parser;
}

#define CSVPP_STL_CONTAINER_SERIALIZATION(container) \
namespace boost { \
namespace serialization { \
template<class T, class Alloc> \
inline void serialize(csv::writer & wr, container<T, Alloc> & cont, const unsigned int) \
{ \
    csv::iterate(wr, cont.begin(), cont.end()); \
} \
template<class T, class Alloc> \
inline void serialize(csv::parser & par, container<T, Alloc> & cont, const unsigned int) \
{ \
    csv::iterate(par, cont.begin(), cont.end()); \
} \
}}

#endif // CONTAINERS_H
