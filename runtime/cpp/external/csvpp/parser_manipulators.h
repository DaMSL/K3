#ifndef CSVPP_PARSER_MANIPULATORS_H
#define CSVPP_PARSER_MANIPULATORS_H

#include <csvpp/parser.h>

namespace csv {

inline parser & endl(parser & par)
{
    par._getline();
    return par;
}

}

#endif // CSVPP_PARSER_MANIPULATORS_H
