
// Copyright (c) 2010-2015 niXman (i dot nixman dog gmail dot com). All
// rights reserved.
//
// This file is part of YAS(https://github.com/niXman/yas) project.
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
//
//
// Boost Software License - Version 1.0 - August 17th, 2003
//
// Permission is hereby granted, free of charge, to any person or organization
// obtaining a copy of the software and accompanying documentation covered by
// this license (the "Software") to use, reproduce, display, distribute,
// execute, and transmit the Software, and to prepare derivative works of the
// Software, and to permit third-parties to whom the Software is furnished to
// do so, all subject to the following:
//
// The copyright notices in the Software and this entire statement, including
// the above license grant, this restriction and the following disclaimer,
// must be included in all copies of the Software, in whole or in part, and
// all derivative works of the Software, unless such copies or derivative
// works are solely in the form of machine-executable object code generated by
// a source language processor.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE, TITLE AND NON-INFRINGEMENT. IN NO EVENT
// SHALL THE COPYRIGHT HOLDERS OR ANYONE DISTRIBUTING THE SOFTWARE BE LIABLE
// FOR ANY DAMAGES OR OTHER LIABILITY, WHETHER IN CONTRACT, TORT OR OTHERWISE,
// ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
// DEALINGS IN THE SOFTWARE.

#ifndef _yas__binary__boost_fusion_vector_serializer_hpp
#define _yas__binary__boost_fusion_vector_serializer_hpp

#include <yas/detail/config/config.hpp>

#if defined(YAS_HAS_BOOST_FUSION)

#include <stdexcept>

#include <yas/detail/type_traits/type_traits.hpp>
#include <yas/detail/type_traits/properties.hpp>
#include <yas/detail/type_traits/selector.hpp>
#include <yas/detail/preprocessor/preprocessor.hpp>

#include <boost/fusion/container/vector.hpp>
#include <boost/fusion/include/vector.hpp>
#include <boost/fusion/container/vector/vector_fwd.hpp>
#include <boost/fusion/include/vector_fwd.hpp>
#include <boost/fusion/sequence/intrinsic/at.hpp>
#include <boost/fusion/include/at.hpp>
#include <boost/fusion/sequence/intrinsic/at_c.hpp>
#include <boost/fusion/include/at_c.hpp>

namespace yas {
namespace detail {

/***************************************************************************/

#define YAS__BINARY__WRITE_BOOST_FUSION_VECTOR_ITEM(unused, idx, type) \
	if ( is_fundamental_and_sizeof_is<YAS_PP_CAT(type, idx), 1>::value ) \
		ar.write(&boost::fusion::at_c<idx>(vector), sizeof(YAS_PP_CAT(type, idx))); \
	else \
		ar & boost::fusion::at_c<idx>(vector);

#define YAS__BINARY__READ_BOOST_FUSION_VECTOR_ITEM(unused, idx, type) \
	if ( is_fundamental_and_sizeof_is<YAS_PP_CAT(type, idx), 1>::value ) \
		ar.read(&boost::fusion::at_c<idx>(vector), sizeof(YAS_PP_CAT(type, idx))); \
	else \
		ar & boost::fusion::at_c<idx>(vector);

#define YAS__BINARY__GENERATE_EMPTY_SAVE_SERIALIZE_BOOST_FUSION_VECTOR_FUNCTION() \
	template<> \
	struct serializer<type_prop::not_a_pod,ser_method::use_internal_serializer, \
		archive_type::binary, direction::out, boost::fusion::vector0<> > \
	{ \
		template<typename Archive> \
		static Archive& apply(Archive& ar, const boost::fusion::vector0<>&) { return ar; } \
	};

#define YAS__BINARY__GENERATE_EMPTY_LOAD_SERIALIZE_BOOST_FUSION_VECTOR_FUNCTION() \
	template<> \
	struct serializer<type_prop::not_a_pod,ser_method::use_internal_serializer, \
		archive_type::binary, direction::in, boost::fusion::vector0<> > \
	{ \
		template<typename Archive> \
		static Archive& apply(Archive& ar, boost::fusion::vector0<>&) { return ar; } \
	};

#define YAS__BINARY__GENERATE_SAVE_SERIALIZE_BOOST_FUSION_VECTOR_FUNCTION(unused, count, text) \
	template<YAS_PP_ENUM_PARAMS(YAS_PP_INC(count), typename T)> \
	struct serializer<type_prop::not_a_pod,ser_method::use_internal_serializer, \
		archive_type::binary, direction::out, \
		YAS_PP_CAT(boost::fusion::vector, YAS_PP_INC(count)) \
			<YAS_PP_ENUM_PARAMS(YAS_PP_INC(count), T)> > \
	{ \
		template<typename Archive> \
		static Archive& apply(\
			Archive& ar, \
			const YAS_PP_CAT(boost::fusion::vector, YAS_PP_INC(count)) \
				<YAS_PP_ENUM_PARAMS(YAS_PP_INC(count), T)>& vector) \
		{ \
			ar.write((std::uint8_t)YAS_PP_INC(count)); \
			YAS_PP_REPEAT( \
				YAS_PP_INC(count), \
				YAS__BINARY__WRITE_BOOST_FUSION_VECTOR_ITEM, \
				T \
			) \
			return ar; \
		} \
	};

#define YAS__BINARY__GENERATE_SAVE_SERIALIZE_BOOST_FUSION_VECTOR_FUNCTIONS(count) \
	YAS__BINARY__GENERATE_EMPTY_SAVE_SERIALIZE_BOOST_FUSION_VECTOR_FUNCTION() \
	YAS_PP_REPEAT( \
		count, \
		YAS__BINARY__GENERATE_SAVE_SERIALIZE_BOOST_FUSION_VECTOR_FUNCTION, \
		~ \
	)

#define YAS__BINARY__GENERATE_LOAD_SERIALIZE_BOOST_FUSION_VECTOR_FUNCTION(unused, count, text) \
	template<YAS_PP_ENUM_PARAMS(YAS_PP_INC(count), typename T)> \
	struct serializer<type_prop::not_a_pod,ser_method::use_internal_serializer, \
		archive_type::binary, direction::in, \
		YAS_PP_CAT(boost::fusion::vector, YAS_PP_INC(count)) \
			<YAS_PP_ENUM_PARAMS(YAS_PP_INC(count), T)> > \
	{ \
		template<typename Archive> \
		static Archive& apply(\
			Archive& ar, \
			YAS_PP_CAT(boost::fusion::vector, YAS_PP_INC(count)) \
				<YAS_PP_ENUM_PARAMS(YAS_PP_INC(count), T)>& vector) \
		{ \
			std::uint8_t size = 0; \
			ar.read(size); \
			if ( size != YAS_PP_INC(count) ) YAS_THROW_BAD_SIZE_ON_DESERIALIZE_FUSION("fusion::vector"); \
			YAS_PP_REPEAT( \
				YAS_PP_INC(count), \
				YAS__BINARY__READ_BOOST_FUSION_VECTOR_ITEM, \
				T \
			) \
			return ar; \
		} \
	};

#define YAS__BINARY__GENERATE_LOAD_SERIALIZE_BOOST_FUSION_VECTOR_FUNCTIONS(count) \
	YAS__BINARY__GENERATE_EMPTY_LOAD_SERIALIZE_BOOST_FUSION_VECTOR_FUNCTION() \
	YAS_PP_REPEAT( \
		count, \
		YAS__BINARY__GENERATE_LOAD_SERIALIZE_BOOST_FUSION_VECTOR_FUNCTION, \
		~ \
	)

/***************************************************************************/

YAS__BINARY__GENERATE_SAVE_SERIALIZE_BOOST_FUSION_VECTOR_FUNCTIONS(FUSION_MAX_VECTOR_SIZE)
YAS__BINARY__GENERATE_LOAD_SERIALIZE_BOOST_FUSION_VECTOR_FUNCTIONS(FUSION_MAX_VECTOR_SIZE)

/***************************************************************************/

#define YAS__BINARY__GENERATE_EMPTY_SAVE_SERIALIZE_BOOST_FUSION_VECTOR_FUNCTION_VARIADIC() \
	template<> \
	struct serializer<type_prop::not_a_pod,ser_method::use_internal_serializer, \
		archive_type::binary, direction::out, boost::fusion::vector<> > \
	{ \
		template<typename Archive> \
		static Archive& apply(Archive& ar, const boost::fusion::vector<>&) { return ar; } \
	};

#define YAS__BINARY__GENERATE_EMPTY_LOAD_SERIALIZE_BOOST_FUSION_VECTOR_FUNCTION_VARIADIC() \
	template<> \
	struct serializer<type_prop::not_a_pod,ser_method::use_internal_serializer, \
		archive_type::binary, direction::in, boost::fusion::vector<> > \
	{ \
		template<typename Archive> \
		static Archive& apply(Archive& ar, boost::fusion::vector<>&) { return ar; } \
	};

#define YAS__BINARY__GENERATE_SAVE_SERIALIZE_BOOST_FUSION_VECTOR_FUNCTION_VARIADIC(unused, count, text) \
	template<YAS_PP_ENUM_PARAMS(YAS_PP_INC(count), typename T)> \
	struct serializer<type_prop::not_a_pod,ser_method::use_internal_serializer, \
		archive_type::binary, direction::out, \
		boost::fusion::vector<YAS_PP_ENUM_PARAMS(YAS_PP_INC(count), T)> > \
	{ \
		template<typename Archive> \
		static Archive& apply(Archive& ar, \
			const boost::fusion::vector<YAS_PP_ENUM_PARAMS(YAS_PP_INC(count), T)>& vector) \
		{ \
			ar.write((std::uint8_t)YAS_PP_INC(count)); \
			YAS_PP_REPEAT( \
				YAS_PP_INC(count), \
				YAS__BINARY__WRITE_BOOST_FUSION_VECTOR_ITEM, \
				T \
			) \
			return ar; \
		} \
	};

#define YAS__BINARY__GENERATE_SAVE_SERIALIZE_BOOST_FUSION_VECTOR_FUNCTIONS_VARIADIC(count) \
	YAS__BINARY__GENERATE_EMPTY_SAVE_SERIALIZE_BOOST_FUSION_VECTOR_FUNCTION_VARIADIC() \
	YAS_PP_REPEAT( \
		count, \
		YAS__BINARY__GENERATE_SAVE_SERIALIZE_BOOST_FUSION_VECTOR_FUNCTION_VARIADIC, \
		~ \
	)

#define YAS__BINARY__GENERATE_LOAD_SERIALIZE_BOOST_FUSION_VECTOR_FUNCTION_VARIADIC(unused, count, text) \
	template<YAS_PP_ENUM_PARAMS(YAS_PP_INC(count), typename T)> \
	struct serializer<type_prop::type_prop::not_a_pod,ser_method::use_internal_serializer, \
		archive_type::binary, direction::in, \
		boost::fusion::vector<YAS_PP_ENUM_PARAMS(YAS_PP_INC(count), T)> > \
	{ \
		template<typename Archive> \
		static Archive& apply(\
			Archive& ar, \
			boost::fusion::vector<YAS_PP_ENUM_PARAMS(YAS_PP_INC(count), T)>& vector) \
		{ \
			std::uint8_t size = 0; \
			ar.read(size); \
			if ( size != YAS_PP_INC(count) ) YAS_THROW_BAD_SIZE_ON_DESERIALIZE_FUSION("fusion::vector"); \
			YAS_PP_REPEAT( \
				YAS_PP_INC(count), \
				YAS__BINARY__READ_BOOST_FUSION_VECTOR_ITEM, \
				T \
			) \
			return ar; \
		} \
	};

#define YAS__BINARY__GENERATE_LOAD_SERIALIZE_BOOST_FUSION_VECTOR_FUNCTIONS_VARIADIC(count) \
	YAS__BINARY__GENERATE_EMPTY_LOAD_SERIALIZE_BOOST_FUSION_VECTOR_FUNCTION_VARIADIC() \
	YAS_PP_REPEAT( \
		count, \
		YAS__BINARY__GENERATE_LOAD_SERIALIZE_BOOST_FUSION_VECTOR_FUNCTION_VARIADIC, \
		~ \
	)

/***************************************************************************/

YAS__BINARY__GENERATE_SAVE_SERIALIZE_BOOST_FUSION_VECTOR_FUNCTIONS_VARIADIC(FUSION_MAX_VECTOR_SIZE)
YAS__BINARY__GENERATE_LOAD_SERIALIZE_BOOST_FUSION_VECTOR_FUNCTIONS_VARIADIC(FUSION_MAX_VECTOR_SIZE)

/***************************************************************************/

} // namespace detail
} // namespace yas

#endif // defined(YAS_SERIALIZE_BOOST_TYPES)

#endif // _yas__binary__boost_fusion_vector_serializer_hpp
