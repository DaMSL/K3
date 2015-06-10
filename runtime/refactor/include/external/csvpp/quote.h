#ifndef CSVPP_QUOTE_H
#define CSVPP_QUOTE_H

// csv++
#include <csvpp/writer.h>
#include <csvpp/parser.h>
#include <csvpp/impl/smanip.h>

// boost
#include <boost/type_traits/is_const.hpp>
#include <boost/mpl/equal_to.hpp>

namespace csv {


// fwd
namespace impl {
template <class T> class quote_w;
}
template <class T> impl::smanip< impl::quote_w<T> > quote(T &);
template <class T> impl::csmanip< impl::quote_w<T> > quote(const T &);

namespace impl {

template <class T>
class quote_w {
    friend impl::smanip< impl::quote_w<T> > quote<T>(T &);
    friend impl::csmanip< impl::quote_w<T> > quote<T>(const T &);
    friend class smanip<quote_w<T> >;
    friend class csmanip<quote_w<T> >;
public:
    typedef writer & writer_ret_type;
    typedef parser & parser_ret_type;

    parser_ret_type operator()(parser & par)
    {
        //////////////////////////////////////////////////////////////////////////////
        //                                                                          //
        //   HEY PROGRAMMER, READ THIS !!                                           //
        //                                                                          //
        BOOST_STATIC_ASSERT(!boost::is_const<T>::value);                            //
        //                                                                          //
        //   If you see a compilation error here, it means, that you tried to       //
        //   load csv into const object.                                            //
        //                                                                          //
        //   Check your compiler info to find where this error took place.          //
        //                                                                          //
        //////////////////////////////////////////////////////////////////////////////

        parser::load_primitive::invoke(par, _wrapped);
        return par;
    }

    writer_ret_type operator()(writer & wr)
    {
        if(wr._first_in_line) {
            wr._ostream << wr.quote_char() << _wrapped << wr.quote_char();
            wr._first_in_line = false;
            return wr;
        }
        wr._ostream << wr.separator() << wr.quote_char() << _wrapped << wr.quote_char();
        return wr;
    }
private:
    quote_w(const quote_w&);
    quote_w & operator=(const quote_w &);
    explicit quote_w(T & wrapped) :
        _wrapped(wrapped)
    {}
    T & _wrapped;
};

} // namespace impl

template <class T>
impl::smanip< impl::quote_w<T> > quote(T & wrapped)
{
    return impl::smanip< impl::quote_w<T> >(wrapped);
}

template <class T>
impl::csmanip< impl::quote_w<T> > quote(const T & wrapped)
{
    return impl::csmanip< impl::quote_w<T> >(wrapped);
}

} // namespace csv

#endif // CSVPP_QUOTE_H
