#ifndef CSVPP_HEADERS_H
#define CSVPP_HEADERS_H

#include <csvpp/writer.h>
#include <csvpp/parser.h>
#include <csvpp/impl/smanip.h>

namespace csv {

namespace impl {
template <class T>
class headers_w {
    friend class smanip<headers_w>;
    friend class csmanip<headers_w>;
public:
    typedef parser& parser_ret_type;
    typedef writer& writer_ret_type;
    writer& operator()(writer & wr)
    {
        if(wr._first_in_line) {
            wr._ostream << wr.headers_char() << _wrapped << wr.headers_char();
            wr._first_in_line = false;
            return wr;
        }
        wr._ostream << wr.separator() << wr.headers_char() << _wrapped << wr.headers_char();
        return wr;
    }
    parser& operator()(parser & par)
    {
        parser::streamer::invoke(par, _wrapped);
        return par;
    }

private:
    headers_w(const headers_w&);
    headers_w & operator=(const headers_w &);
    explicit headers_w(T & wrapped) :
        _wrapped(wrapped)
    {}
    explicit headers_w(const T & wrapped) :
        _wrapped(const_cast<T&>(wrapped))
    {}
    T & _wrapped;
};

} // namespace impl

template <class T>
impl::smanip< impl::headers_w<T> > headers(T & wrapped)
{
    return impl::smanip< impl::headers_w<T> >(wrapped);
}

template <class T>
const impl::csmanip< impl::headers_w<T> > headers(const T & wrapped)
{
    return impl::csmanip< impl::headers_w<T> >(wrapped);
}

} // namespace csv

#endif // CSVPP_HEADERS_H
