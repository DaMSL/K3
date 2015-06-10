#ifndef CSVPP_MAP_H
#define CSVPP_MAP_H

#include <csvpp/iter.h>
#include <csvpp/pair.h>

// stl
#include <map>

// fwd
namespace csv {
class writer;
class parser;
}

namespace boost {
namespace serialization {
template< class Key,        // map::key_type
          class T,          // map::mapped_type
          class Compare,    // map::key_compare
          class Alloc       // map::allocator_type
          >
inline void serialize(csv::writer & wr,
                      std::map<Key, T, Compare, Alloc> & map,
                      const unsigned int)
{
    csv::impl::iterate<csv::writer, std::map<Key, T, Compare, Alloc> >(wr, map.begin(), map.end());
}

template< class Key,        // map::key_type
          class T,          // map::mapped_type
          class Compare,    // map::key_compare
          class Alloc       // map::allocator_type
          >
inline void serialize(csv::parser & par,
                      std::map<Key, T, Compare, Alloc> & map,
                      const unsigned int)
{
    csv::impl::iterate<csv::parser, std::map<Key, T, Compare, Alloc> >(par, map.begin(), map.end());
}
} // namespace serialization
} // namespace boost

#endif // MAP_H
