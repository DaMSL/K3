#ifndef CSVPP_SFINAE_H
#define CSVPP_SFINAE_H

#include <ostream>
#include <istream>

namespace csv {
namespace sfinae {

typedef char no;
typedef char yes[2];

struct any_t {
    template<class T>
    any_t( T const& );
};

no operator<<( std::ostream const&, any_t const& );
no operator>>( std::istream const&, any_t const& );

template <typename T>
class has_insertion_operator {
    static yes& test( std::ostream& );
    static no test( no );
    static std::ostream &s;
    static T const &t;
public:
    static bool const value = sizeof( test(s << t) ) == sizeof( yes );
};

template <typename T>
class has_extraction_operator {
    static yes& test( std::istream& );
    static no test( no );
    static std::istream &s;
    static T const &t;
public:
    static bool const value = sizeof( test(s >> t) ) == sizeof( yes );
};

} // namespace impl
} // namespace csv

#endif // CSVPP_IMPL_SFINAE_H
