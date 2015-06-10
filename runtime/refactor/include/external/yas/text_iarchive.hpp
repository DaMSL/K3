
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

#ifndef _yas__text_iarchive_hpp
#define _yas__text_iarchive_hpp

#include <istream>

#include <yas/detail/type_traits/properties.hpp>
#include <yas/detail/type_traits/has_method_serialize.hpp>
#include <yas/detail/type_traits/has_function_serialize.hpp>
#include <yas/detail/type_traits/selector.hpp>

#include <yas/detail/io/information.hpp>
#include <yas/detail/io/text_streams.hpp>
#include <yas/defaul_traits.hpp>

#include <yas/detail/base_object.hpp>

#include <yas/serializers/serializer.hpp>
#include <yas/serializers/text/utility/pod_serializers.hpp>
#include <yas/serializers/text/utility/enum_serializer.hpp>
#include <yas/serializers/text/utility/usertype_serializers.hpp>
#include <yas/serializers/text/utility/autoarray_serializers.hpp>
#include <yas/serializers/text/utility/buffer_serializers.hpp>

#include <yas/buffers.hpp>
#include <yas/detail/tools/noncopyable.hpp>

namespace yas {

/***************************************************************************/

template<typename IS, typename Trait = yas::detail::default_traits>
struct text_iarchive
	:detail::text_istream<IS, Trait>
	,detail::archive_information<archive_type::text, direction::in, IS, as_host>
	,private detail::noncopyable
{
	using stream_type = IS;
	using this_type = text_iarchive<IS, Trait>;

	text_iarchive(IS &is, header_flag op = with_header)
		:detail::text_istream<IS, Trait>(is)
		,detail::archive_information<archive_type::text, direction::in, IS, as_host>(is, op)
	{}

	template<typename T>
	this_type& operator& (T& v) {
		using namespace detail;
		return serializer<
			 type_properties<T>::value
			,serialization_method<T, this_type>::value
			,archive_type::text
			,direction::in
			,T
		>::apply(*this, v);
	}

	this_type& serialize() { return *this; }

	template<typename Head, typename... Tail>
	this_type& serialize(Head& head, Tail&... tail) {
		return operator&(head).serialize(tail...);
	}

	template<typename... Args>
	this_type& operator()(Args&... args) {
		return serialize(args...);
	}
};

/***************************************************************************/

} // namespace yas

#endif // _yas__text_iarchive_hpp