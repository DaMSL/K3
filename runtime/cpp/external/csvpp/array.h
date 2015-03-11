#ifndef CSVPP_ARRAY_H
#define CSVPP_ARRAY_H

// csv++
#include <csvpp/iter.h>

// boost
#include <boost/array.hpp>

// fwd
namespace csv {
class writer;
class parser;
}

namespace boost {
namespace serialization {
template<class T, std::size_t N>
inline void serialize(csv::writer & wr, boost::array<T, N> & cont, const unsigned int)
{
    csv::iterate(wr, cont.begin(), cont.end());
}
template<class T, std::size_t N>
inline void serialize(csv::parser & par, boost::array<T, N> & cont, const unsigned int)
{
    csv::iterate(par, cont.begin(), cont.end());
}
} // namespace serialization
} // namespace boost

#endif // CSVPP_ARRAY_H
