#ifndef CSVPP_WRITER_H
#define CSVPP_WRITER_H

// STL
#include <ostream>

// boost mpl
#include <boost/mpl/eval_if.hpp>
#include <boost/mpl/equal_to.hpp>
#include <boost/mpl/identity.hpp>
#include <boost/mpl/bool.hpp>

// boost utility
#include <boost/static_assert.hpp>

// boost serialization
#include <boost/serialization/access.hpp>
#include <boost/serialization/level.hpp>
#include <boost/serialization/level_enum.hpp>
#include <boost/serialization/serialization.hpp>

// csv
#include <csvpp/impl/base.h>
#include <csvpp/impl/smanip.h>
#include <csvpp/impl/macros.h>

CSVPP_MANIPULATORS_FWD()

namespace csv {

    class writer : public impl::base {
        CSVPP_MANIPULATORS_FRIENDS();
    public:
        typedef boost::mpl::bool_<false> is_loading;
        typedef boost::mpl::bool_<true> is_saving;

        writer(std::ostream & ostream,
               char separator=',',
               char quote='\"',
               char escape='\\');

        writer& operator<<(writer& (*pf) (writer&)) { return pf(*this); }
        writer& operator&(writer& (*pf) (writer&)) { return pf(*this); }

        template<class T>
        writer &operator<<(const T& val) {
            return operator &(val);
        }

        template<class T>
        typename T::writer_ret_type operator&(const impl::smanip<T>& manip) {
            return const_cast<impl::smanip<T>&>(manip)(*this);
        }

        template<class T>
        typename T::writer_ret_type operator&(const impl::csmanip<T>& manip) {
            return const_cast<impl::csmanip<T>&>(manip)(*this);
        }

        template<class T>
        writer &operator&(const T& val) {
            typedef
            BOOST_DEDUCED_TYPENAME boost::mpl::eval_if<
                    // if its primitive
                    boost::mpl::equal_to<
                    boost::serialization::implementation_level< T >,
                    boost::mpl::int_<boost::serialization::primitive_type>
                    >,
                    boost::mpl::identity<save_primitive>,
                    // else
                    boost::mpl::identity<save_class>
                    >::type typex;

            //////////////////////////////////////////////////////////////////////////////
            //                                                                          //
            //   HEY PROGRAMMER, READ THIS !!                                           //
            //                                                                          //
            BOOST_STATIC_ASSERT(typex::valid);                                          //
            //                                                                          //
            //   If you see a compilation error here, it means, that you used a type,   //
            //   that is not suitable for writing into csv                              //
            //                                                                          //
            //   Provide a proper serialize() or save() method for this to be working.  //
            //                                                                          //
            //////////////////////////////////////////////////////////////////////////////
            typex::invoke(*this, val);
            return *this;
        }

    private:
        writer(const writer&);
        writer& operator=(const writer&);
        std::ostream & _ostream;
        bool _first_in_line;

        struct save_class {
            template<class T>
            static void invoke(writer& wr, const T& val) {
                boost::serialization::serialize_adl(wr, const_cast<T&>(val), 0);
            }
            static const bool valid = true;
        };

        struct save_primitive {
            template<class T>
            static void invoke(writer& wr, const T& val) {

                static const bool primitive =
                        boost::mpl::equal_to<
                        boost::serialization::implementation_level< T >,
                        boost::mpl::int_<boost::serialization::primitive_type>
                        >::value;

                //////////////////////////////////////////////////////////////////////////////
                //                                                                          //
                //   HEY PROGRAMMER, READ THIS !!                                           //
                //                                                                          //
                BOOST_STATIC_ASSERT(primitive);                                             //
                //                                                                          //
                //   If you see a compilation error here, it means, that you tried to       //
                //   save into csv object, which is not a primitive type.                   //
                //   i.e. int or string                                                     //
                //                                                                          //
                //   Check your compiler info to find where this error took place.          //
                //                                                                          //
                //////////////////////////////////////////////////////////////////////////////

                if(wr._first_in_line) {
                    wr._ostream << val;
                    wr._first_in_line = false;
                    return;
                }
                wr._ostream << wr.separator() << val;
            }
            static const bool valid = true;
        };

        struct invalid {
            static const bool valid = false;
            template<class T>
            static void invoke(writer& wr, const T& val);
        };

        friend class save_class;
        friend class save_primitive;
    };

}



#endif // CSVPP_WRITER_H
