#ifndef CSVPP_BASE_H
#define CSVPP_BASE_H

namespace csv {
namespace impl {

class base
{
public:
    base()  :
        _separator(','),
        _quote('\"'),
        _escape('\\')
    {}
    base(char separator,
         char quote,
         char escape) :
        _separator(separator),
        _quote(quote),
        _escape(escape)
    {}

    char separator() const { return _separator; }
    char quote_char() const { return _quote; }
    char escape_char() const { return _escape; }
  private:
    const char _separator;
    const char _quote;
    const char _escape;
};
} // namespace impl
} // namespace csv

#endif // CSVPP_BASE_H
