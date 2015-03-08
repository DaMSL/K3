#ifndef CSVPP_EXCEPTION_H
#define CSVPP_EXCEPTION_H

#include <stdexcept>
#include <sstream>

namespace csv {
class exception : public std::exception {
public:
    exception(unsigned line, unsigned column) :
        _line(line), _column(column) {}
    unsigned line() const { return _line; }
    unsigned column() const { return _column; }
protected:
    unsigned _line;
    unsigned _column;
};

class bad_cast : public exception {
public:
    bad_cast(unsigned line, unsigned column) :
        exception(line, column) {}
    virtual const char * what() const throw()
    {
        std::stringstream ss;
        ss << "Could not interpret data in line "
           << line()
           << ", column"
           << column();
        return ss.str().c_str();
    }
};

class no_data_available : public exception {
public:
    no_data_available(unsigned line, unsigned column) :
        exception(line, column) {}
    virtual const char * what() const throw()
    {
        std::stringstream ss;
        ss << "There is no data in line "
           << line()
           << ", column"
           << column();
        return ss.str().c_str();
    }
};

}

#endif // CSVPP_EXCEPTION_H
