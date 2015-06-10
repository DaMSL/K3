#ifndef CSVPP_WRITER_MANIPULATORS_H
#define CSVPP_WRITER_MANIPULATORS_H

#include <csvpp/writer.h>

namespace csv {

inline writer & endl(writer & wr)
{
    wr._ostream << std::endl;
    wr._first_in_line = true;
    return wr;
}

}

#endif // CSVPP_WRITER_MANIPULATORS_H
