#ifndef CSVPP_PAIR_H
#define CSVPP_PAIR_H

// stl
#include <list>

// fwd
namespace csv {
class writer;
class parser;
}

namespace boost {
namespace serialization {
template<class T1, class T2>
inline void serialize(csv::writer & wr, std::pair<T1, T2> & pair, const unsigned int)
{
    wr & pair.first;
    wr & pair.second;
}
template<class T1, class T2>
inline void serialize(csv::parser & par, std::pair<T1, T2> & pair, const unsigned int)
{
    par & pair.first;
    par & pair.second;
}
} // namespace serialization
} // namespace boost

#endif // CSVPP_PAIR_H
