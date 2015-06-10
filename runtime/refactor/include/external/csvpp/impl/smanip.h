#ifndef CSVPP_IMPL_SMANIP_H
#define CSVPP_IMPL_SMANIP_H

namespace csv {

// fwd
class writer;
class parser;

namespace impl {

template< class T >
class smanip : public T
{
public:
    template <class A1> explicit smanip(A1 & a1) : T(a1) {}
    template <class A1, class A2> smanip(A1 a1, A2 a2) : T(a1, a2) {}
};

template <class T>
class csmanip : public T
{
public:
    template <class A1> explicit csmanip(A1 & a1) : T(a1) {}
    template <class A1, class A2> csmanip(A1 a1, A2 a2) : T(a1, a2) {}
};

} // namespace impl
} // namespace csv

#endif // CSVPP_IMPL_SMANIP_H
