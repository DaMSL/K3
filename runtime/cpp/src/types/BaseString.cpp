#include <string>
#include <algorithm>

#include "types/BaseString.hpp"

namespace K3 {
// Constructors/Destructors/Assignment.
base_string::base_string() : buffer_(nullptr) {}

base_string::base_string(const base_string& other)
    : buffer_(dupbuf(other))
{
  set_header(other.has_header());
}

base_string::base_string(base_string&& other) : base_string() {
  set_header(other.has_header());
  swap(*this, other);
}

base_string::base_string(const char* b) : buffer_(dupstr(b)) {}
base_string::base_string(const std::string& s) : buffer_(dupstr(s.c_str())) {}

base_string::base_string(const char* from, std::size_t count) : base_string() {
  if (from && count) {
    buffer_ = new char[count + 1];
    strncpy(buffer_, from, count);
    buffer_[count] = 0;
  }
}

base_string::~base_string() {
  if (bufferp_()) {
    delete[] bufferp_();
  }
  buffer_ = 0;
}

void base_string::steal(char *p) {
  if (bufferp_()) {
    delete[] bufferp_();
  }
  buffer_ = p;
}

base_string& base_string::operator+=(const base_string& other) {
  bool new_header = has_header() || other.has_header();
  size_t len = length() + other.length() + (new_header? header_size : 0);
  auto new_buffer_ = new char[len + 1];
  auto new_c_str_ = new_buffer_ + (new_header? header_size : 0);

  set_header(new_header);
  if ( new_header ) {
    *reinterpret_cast<size_t*>(new_buffer_) = length() + other.length();
  }

  std::strcpy(new_c_str_, (bufferp_() ? c_str() : ""));
  std::strcat(new_c_str_, (other.bufferp_() ? other.c_str() : ""));

  if (bufferp_()) {
    delete[] bufferp_();
  }
  buffer_ = new_buffer_;
  return *this;
}

base_string& base_string::operator+=(const char* other) {
  return * this += base_string(other);
}

base_string& base_string::operator=(const base_string& other) {
  base_string temp(other);
  swap(*this, temp);
  return *this;
}

base_string& base_string::operator=(base_string&& other) {
  swap(*this, other);
  return *this;
}

void swap(base_string& first, base_string& second) {
  using std::swap;
  swap(first.buffer_, second.buffer_);
}

// External string construction and storage tag management.
bool base_string::has_header() const {
  bool b = (as_bits & header_flag) != 0;
  return b;
}

void base_string::set_header(const bool& on) {
  if ( on ) { as_bits |= header_flag; }
  else { as_bits &= ~header_flag; }
}

// Conversions
base_string::operator bool() const {
  return bufferp_();
}

base_string::operator std::string() const {
  return std::string(bufferp_() ? c_str() : "");
}

// Accessors
std::size_t base_string::length() const {
  if (bufferp_()) {
    if (has_header()) {
      return *reinterpret_cast<size_t*>(bufferp_());
    }
    else { return strlen(bufferp_()); }
  }
  return 0;
}

std::size_t base_string::raw_length() const {
  if (bufferp_()) {
    if (has_header()) {
      return *(reinterpret_cast<size_t*>(bufferp_()) + header_size);
    }
    else { return strlen(bufferp_()); }
  }
  return 0;
}

const char* base_string::c_str() const {
  if ( !has_header() ) { return bufferp_(); }
  else { 
    return bufferp_() + header_size; 
  }
}

const char* base_string::data() const {
  return bufferp_();
}


// TODO (optimize comparators)
size_t cmp(const base_string& b1, const base_string& b2) {
  size_t len = b1.length();
  size_t o_len = b2.length();
  size_t min_len = len < o_len ? len : o_len;

  auto val = memcmp(b1.bufferp_() ? b1.c_str() : "", b2.bufferp_() ? b2.c_str() : "", min_len);
  if (val == 0) {
    if (len == o_len) {
      return 0;
    } else if (len < o_len) {
      return -1;
    } else {
      return 1;
    }
  }
  return val;
}

size_t cmp(const base_string& b1, const char* other) {

  size_t len = b1.length();
  size_t o_len = other ? strlen(other) : 0;
  size_t min_len = len < o_len ? len : o_len;

  auto val = memcmp(b1.bufferp_() ? b1.c_str() : "", other ? other : "", min_len);
  if (val == 0) {
    if (len == o_len) {
      return 0;
    } else if (len < o_len) {
      return -1;
    } else {
      return 1;
    }
  }
  return val;
}

// Comparisons
bool base_string::operator==(const base_string& other) const {
  return cmp(*this, other) == 0;
}

bool base_string::operator==(const char* other) const {
  return cmp(*this, other) == 0;
}

bool base_string::operator!=(const base_string& other) const {
  return cmp(*this, other) != 0;
}

bool base_string::operator!=(const char* other) const {
  return cmp(*this, other) != 0;
}

bool base_string::operator<=(const base_string& other) const {
  return cmp(*this, other) <= 0;
}

bool base_string::operator<=(const char* other) const {
  return cmp(*this, other) <= 0;
}

bool base_string::operator<(const base_string& other) const {
  return cmp(*this, other) < 0;
}

bool base_string::operator<(const char* other) const {
  return cmp(*this, other) < 0;
}

bool base_string::operator>=(const base_string& other) const {
  return cmp(*this, other) >= 0;
}

bool base_string::operator>=(const char* other) const {
  return cmp(*this, other) >= 0;
}

bool base_string::operator>(const base_string& other) const {
  return cmp(*this, other) > 0;
}

bool base_string::operator>(const char* other) const {
  return cmp(*this, other) > 0;
}

// Operations
base_string base_string::substr(std::size_t from, std::size_t to) const {
  if (!bufferp_()) {
    return base_string();
  }

  auto n = length();
  if (from > n) { from = n; }
  if (to > n) { to = n; }

  auto new_sz = to - from;
  auto new_len = new_sz + (has_header()? header_size : 0);

  auto new_buffer_ = new char[new_len + 1];
  auto new_c_str_ = new_buffer_ + (has_header()? header_size : 0);
  new_buffer_[new_len] = 0;

  strncpy(new_c_str_, c_str() + from, new_sz);
  base_string result;
  result.steal(new_buffer_);
  result.set_header(has_header());
  return result;
}

int base_string::strcomp(const base_string& other) const {
  const char* c1 = this->c_str();
  const char* c2 = other.c_str();
  if (c1 && c2) {
    return strcmp(c1, c2);
  } else if (c1) {
    return 1;
  } else if (c2) {
    return -1;
  } else {
    return 0;
  }
}

// Modifies this string.
// Seq<R_elem<string_impl>> splitString(const string_impl& splitter) {
//  Seq<R_elem<string_impl>> results;
//  if (!buffer_) {
//    return results;
//  }
//
//  R_elem<string_impl> rec;
//  char * pch;
//  pch = strtok (buffer_, splitter.c_str());
//  while (pch != NULL) {
//    rec.elem = string_impl(pch);
//    results.insert(rec);
//    pch = strtok (NULL, splitter.c_str());
//  }
//
//  return results;
//}

// Stream Operators
std::ostream& operator<<(std::ostream& out, const base_string& s) {
  if (s.buffer_) {
    return out << s.c_str();
  }

  return out;
}

char* base_string::begin() const { return const_cast<char*>(c_str()); }

char* base_string::end() const { return const_cast<char*>(c_str()) + length(); }

// Utilities
char* dupbuf(const base_string& b) throw() {
  if (!b.bufferp_()) { return nullptr; }
  auto n = b.raw_length();
  auto d = new char[n + 1];
  return static_cast<char*>(memcpy(d, b.bufferp_(), n + 1));
}

char* dupstr(const char* s) throw() {
  if (!s) { return nullptr; }
  auto n = strlen(s);
  char* d = new char[n + 1];
  return static_cast<char*>(memcpy(d, s, n + 1));
}

// Specializations for CSV parsing/writing, skipping the length field.
template <>
void base_string::serialize(csv::parser& a, const unsigned int) {
  std::string tmp;
  a& tmp;

  if (bufferp_()) {
    delete[] bufferp_();
  }
  buffer_ = dupstr(tmp.c_str());
}

template <>
void base_string::serialize(csv::writer& a, const unsigned int) {
  std::string tmp = std::string(bufferp_(), raw_length());
  a& tmp;
}

// template<>
// std::size_t hash_value<base_string>(const base_string& s) {
//  std::size_t seed = 0;
//  if (s.length() > 0) {
//    for (auto& i: s) {
//      boost::hash_combine(seed, i);
//    }
//  }
//  return seed;
//}

}  // namespace K3

K3::base_string operator+(K3::base_string s, char const* t) { return s += t; }

K3::base_string operator+(char const* t, K3::base_string const& s) {
  auto new_string = K3::base_string(t);
  return new_string += s;
}
