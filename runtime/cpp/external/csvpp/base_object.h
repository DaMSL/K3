#ifndef CSVPP_BASE_OBJECT_H
#define CSVPP_BASE_OBJECT_H

// boost
#include <boost/serialization/base_object.hpp>

namespace csv {

#if defined(__BORLANDC__) && __BORLANDC__ < 0x610
template<class Base, class Derived>
const Base &
base_object(const Derived & d)
{
    return boost::serialization::base_object<Base, Derived>(d);
}
#else
template<class Base, class Derived>
BOOST_DEDUCED_TYPENAME boost::serialization::detail::base_cast<Base, Derived>::type &
base_object(Derived &d)
{
    return boost::serialization::base_object<Base, Derived>(d);
}
#endif

} // namespace csv

#endif // CSVPP_BASE_OBJECT_H
