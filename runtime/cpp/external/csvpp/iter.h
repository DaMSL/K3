#ifndef CSVPP_ITER_H
#define CSVPP_ITER_H

// boost
#include <boost/static_assert.hpp>

// csv++
#include <csvpp/sfinae/has_const_iterator.h>
#include <csvpp/sfinae/has_iterator.h>
#include <csvpp/writer.h>
#include <csvpp/parser.h>
#include <csvpp/impl/smanip.h>

namespace csv {

// fwd
namespace impl {
template <class T> class const_iter_w;
template <class T> class iter_w;
}
template <class T> impl::smanip< impl::iter_w<T> > iter(T &);
template <class T> impl::csmanip< impl::const_iter_w<T> > iter(const T &);
template <class T> impl::smanip< impl::iter_w<T> > iter(typename T::iterator,
                                                           typename T::iterator);
template <class T> impl::csmanip< impl::const_iter_w<T> > iter(const T &);
template <class T> impl::csmanip< impl::const_iter_w<T> > iter(typename T::const_iterator,
                                                                  typename T::const_iterator);



template <class Archive, class Iterator>
void iterate(Archive & ar,
             Iterator from,
             Iterator to)
{
    while(from != to) {
        ar.operator&(*from);
        ++from;
    }
}

namespace impl {

template <class T>
class const_iter_w {
    friend impl::csmanip< impl::const_iter_w<T> > iter<T>(const T &);
    friend impl::csmanip< impl::const_iter_w<T> > iter<T>(typename T::const_iterator,
                                                          typename T::const_iterator);
    friend class smanip<const_iter_w<T> >;
    friend class csmanip<const_iter_w<T> >;
public:
    typedef writer & writer_ret_type;
    typedef parser & parser_ret_type;
    template<class Archive>
    Archive & operator()(Archive & ar)
    {
        iterate(ar, _from, _to);
        return ar;
    }

private:
    const_iter_w(const const_iter_w&);
    const_iter_w & operator=(const const_iter_w &);
    const_iter_w(const typename T::const_iterator & from,
                 const typename T::const_iterator & to) :
        _from(from), _to(to)
    {}
    typename T::const_iterator _from;
    typename T::const_iterator _to;
};


template <class T>
class iter_w {
    friend impl::smanip< impl::iter_w<T> > iter<T>(T &);
    friend impl::smanip< impl::iter_w<T> > iter<T>(typename T::iterator,
                                                   typename T::iterator);
    friend class smanip<iter_w<T> >;
    friend class csmanip<iter_w<T> >;
public:
    typedef writer & writer_ret_type;
    typedef parser & parser_ret_type;
    template<class Archive>
    Archive & operator()(Archive & ar)
    {
        iterate(ar, _from, _to);
        return ar;
    }

private:
    iter_w(const iter_w&);
    iter_w & operator=(const iter_w &);
    iter_w(const typename T::iterator & from,
           const typename T::iterator & to) :
        _from(from), _to(to)
    {}
    typename T::iterator _from;
    typename T::iterator _to;
};

} // namespace impl

template <class T>
impl::smanip< impl::iter_w<T> > iter(T & wrapped)
{
    typedef sfinae::has_iterator<T> typex;

    //////////////////////////////////////////////////////////////////////////////
    //                                                                          //
    //   HEY PROGRAMMER, READ THIS !!                                           //
    //                                                                          //
    BOOST_STATIC_ASSERT(typex::value);                                          //
    //                                                                          //
    //   If you see a compilation error here, it means, that you used a type,   //
    //   that does not have an iterator.                                        //
    //                                                                          //
    //////////////////////////////////////////////////////////////////////////////

    return impl::smanip< impl::iter_w<T> >(wrapped.begin(), wrapped.end());
}


template <class T>
impl::csmanip< impl::const_iter_w<T> > iter(const T & wrapped)
{
    typedef sfinae::has_const_iterator<T> typex;

    //////////////////////////////////////////////////////////////////////////////
    //                                                                          //
    //   HEY PROGRAMMER, READ THIS !!                                           //
    //                                                                          //
    BOOST_STATIC_ASSERT(typex::value);                                          //
    //                                                                          //
    //   If you see a compilation error here, it means, that you used a type,   //
    //   that does not have a const iterator.                                   //
    //                                                                          //
    //////////////////////////////////////////////////////////////////////////////

    return impl::csmanip< impl::const_iter_w<T> >(wrapped.begin(), wrapped.end());
}


} // namespace csv


#endif // CSVPP_ITER_H
