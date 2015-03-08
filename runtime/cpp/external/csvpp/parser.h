#ifndef CSVPP_PARSER_H
#define CSVPP_PARSER_H

// STL
#include <istream>
#include <stdexcept>
#include <string>
#include <sstream>

// csv
#include <csvpp/impl/base.h>
#include <csvpp/impl/smanip.h>
#include <csvpp/impl/macros.h>
#include <csvpp/exception.h>

// boost mpl
#include <boost/mpl/eval_if.hpp>
#include <boost/mpl/equal_to.hpp>
#include <boost/mpl/identity.hpp>
#include <boost/mpl/bool.hpp>

// boost utility
#include <boost/tokenizer.hpp>
#include <boost/lexical_cast.hpp>

// boost serialization
#include <boost/serialization/access.hpp>
#include <boost/serialization/level.hpp>
#include <boost/serialization/level_enum.hpp>

CSVPP_MANIPULATORS_FWD()

namespace csv {

    class parser : public impl::base {
        CSVPP_MANIPULATORS_FRIENDS();
    public:
        typedef boost::mpl::bool_<true> is_loading;
        typedef boost::mpl::bool_<false> is_saving;

        parser(std::istream & istream,
               char separator=',',
               char quote='\"',
               char escape='\\');

        template<class T>
        typename T::parser_ret_type operator&(const impl::smanip<T>& manip) {
            return const_cast<impl::smanip<T>&>(manip)(*this);
        }

        template<class T>
        typename T::parser_ret_type operator>>(const impl::smanip<T>& manip) {
            return const_cast<impl::smanip<T>&>(manip)(*this);
        }

        template<class T>
        typename T::parser_ret_type operator&(const impl::csmanip<T>& manip) {
            BOOST_STATIC_ASSERT(sizeof(T)<0);
            //////////////////////////////////////////////////////////////////////////////
            //                                                                          //
            //   HEY PROGRAMMER, READ THIS !!                                           //
            //                                                                          //
            //   If you see a compilation error here, it means, that you tried to       //
            //   load a csv into const object, which is of course not possible.         //
            //                                                                          //
            //   Check your compiler info to find where this error took place.          //
            //                                                                          //
            //////////////////////////////////////////////////////////////////////////////
            return const_cast<impl::csmanip<T>&>(manip)(*this);
        }

        template<class T>
        typename T::parser_ret_type operator>>(const impl::csmanip<T>& manip) {
            return *this & manip;
        }

        template<class T>
        parser &operator>>(T& val) {
            return *this & val;
        }

        template<class T>
        parser &operator>>(const T& val) {
            return *this & val;
        }

        template<class T>
        parser &operator&(T& val) {
            typedef
            BOOST_DEDUCED_TYPENAME boost::mpl::eval_if<
                    // if its primitive
                    boost::mpl::equal_to<
                    boost::serialization::implementation_level< T >,
                    boost::mpl::int_<boost::serialization::primitive_type>
                    >,
                    boost::mpl::identity<load_primitive>,
                    // else
                    boost::mpl::identity<load_class>
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

        template<class T>
        parser &operator&(const T&) {
            BOOST_STATIC_ASSERT(sizeof(T)<0);
            //////////////////////////////////////////////////////////////////////////////
            //                                                                          //
            //   HEY PROGRAMMER, READ THIS !!                                           //
            //                                                                          //
            //   If you see a compilation error here, it means, that you tried to       //
            //   load a csv into const object, which is of course not possible.         //
            //                                                                          //
            //   Check your compiler info to find where this error took place.          //
            //                                                                          //
            //////////////////////////////////////////////////////////////////////////////
            return *this;
        }

        // operatory do manipulatorów
        parser& operator>>(parser& (*pf) (parser&))
        {
            return pf(*this);
        }
        parser& operator&(parser& (*pf) (parser&))
        {
            return pf(*this);
        }

        unsigned line() { return _linenumber; }
        unsigned column() { return _colnumber; }
        bool more_in_line() {
            if(_iterator != _tokenizer->end())
                return true;
            return false;
        }
        bool more_except_line() {
            if(!_empty_istream())
                return true;
            return false;
        }
        bool more() {
            if(_iterator != _tokenizer->end())
                return true;
            if(!_empty_istream())
                return true;
            return false;
        }

    private:
        typedef boost::escaped_list_separator<char> list_separator;
        typedef boost::tokenizer<list_separator> tokenizer;
        parser& operator=(const parser&);
        parser(parser&);
        std::istream & _istream;
        unsigned _linenumber;
        unsigned _colnumber;
        list_separator _list_separator;
        tokenizer * _tokenizer;
        tokenizer::iterator _iterator;

        std::string _line;

        bool _empty_istream()
        {
            return std::istream::traits_type::eq_int_type(
                        _istream.rdbuf()->sgetc(),
                        std::istream::traits_type::eof()
                        );
        }

        void _getline();

        struct load_primitive {
            template<class T>
            static void invoke(parser& par, T& val) {

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
                //   load csv into object, which is not a primitive type.                   //
                //   i.e. int or string                                                     //
                //                                                                          //
                //   Check your compiler info to find where this error took place.          //
                //                                                                          //
                //////////////////////////////////////////////////////////////////////////////

                if(par._iterator == par._tokenizer->end()) {
                    throw no_data_available(par._linenumber, par._colnumber);
                }
                try {
                    val = boost::lexical_cast<T>(*par._iterator);
                } catch (const boost::bad_lexical_cast&) {
                    throw bad_cast(par._linenumber, par._colnumber);
                }

                ++par._iterator;
                ++par._colnumber;
            }
            static const bool valid = true;
        };

        struct load_class {
            template<class T>
            static void invoke(parser& par, T& val) {
                boost::serialization::serialize_adl(par, val, 0);
            }
            static const bool valid = true;
        };

        struct invalid {
            static const bool valid = false;
            template<class T>
            static void invoke(parser& par, const T& val);
        };

        friend struct member_serializer;
        friend struct streamer;

    };

}

#endif // CSVPP_PARSER_H
