#ifndef CSVPP_IMPL_MACROS_H
#define CSVPP_IMPL_MACROS_H

#define CSVPP_MANIPULATORS_FWD()  \
    namespace csv { \
    class writer; \
    writer & endl(writer & wr); \
    class parser; \
    parser & endl(parser & wr); \
    namespace impl { \
    template <class T> class quote_w; \
    template <class T> class iter_w; \
    } \
    }

#define CSVPP_MANIPULATORS_FRIENDS() \
    friend writer & csv::endl(writer &); \
    friend parser & csv::endl(parser &); \
    template <class T> friend class csv::impl::quote_w; \
    template <class T> friend class csv::impl::iter_w;

#endif // CSVPP_IMPL_MACROS_H
