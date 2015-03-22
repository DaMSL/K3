
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

#ifndef _yas__file_streams_hpp
#define _yas__file_streams_hpp

#include <yas/detail/config/config.hpp>
#include <yas/detail/tools/noncopyable.hpp>
#include <yas/detail/io/io_exceptions.hpp>

#include <string>
#include <cstdio>

namespace yas {

/***************************************************************************/

enum file_mode { file_none, file_append, file_trunc };

/***************************************************************************/

struct file_ostream: private detail::noncopyable {
	file_ostream(const char *fname, file_mode m = file_mode::file_none)
		:file(0)
	{
		const bool exists = file_exists(fname);
		if ( m == file_mode::file_none && exists )
			YAS_THROW_FILE_ALREADY_EXISTS();
		if ( m == file_mode::file_append && !exists )
			YAS_THROW_FILE_IS_NOT_EXISTS();

		file = std::fopen(fname, file_mode_str(m));
		if ( !file )
			YAS_THROW_ERROR_OPENING_FILE();
	}
	virtual ~file_ostream() {
		std::fclose(file);
	}

	std::size_t write(const void *ptr, std::size_t size) {
		return std::fwrite(ptr, 1, size, file);
	}

	void flush() { std::fflush(file); }
private:
	FILE *file;

private:
	static const char* file_mode_str(file_mode m) {
		switch ( m ) {
			case file_mode::file_none:
			case file_mode::file_trunc:
				return "wb";
			case file_mode::file_append:
				return "ab";
			default:
				YAS_THROW_BAD_FILE_MODE();
		}
	}
	static bool file_exists(const char *fname) {
		FILE* file = std::fopen(fname, "r");
		if ( file ) {
			std::fclose(file);
			return true;
		}
		return false;
	}

}; // struct file_ostream

/***************************************************************************/

struct file_istream: private detail::noncopyable {
	file_istream(const char *fname)
		:file(std::fopen(fname, "rb"))
	{
		if ( !file )
			YAS_THROW_ERROR_OPENING_FILE();
	}
	virtual ~file_istream() {
		std::fclose(file);
	}

	std::size_t read(void *ptr, std::size_t size) {
		return std::fread(ptr, 1, size, file);
	}

	bool eof() const {
		return std::feof(file);
	}

private:
	FILE *file;
}; // struct file_istream

/***************************************************************************/

} // ns yas

#endif // _yas__file_streams_hpp
