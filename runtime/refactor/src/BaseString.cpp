#include "BaseString.hpp"

namespace K3 {
// Constructors/Destructors/Assignment.
base_string::base_string(): buffer_(nullptr) {}

base_string::base_string(const base_string& other): buffer_(dupstr(other.buffer_)) {}

base_string::base_string(base_string&& other): base_string() {
  swap(*this, other);
}

base_string::base_string(const char* b): buffer_(dupstr(b)) {}
base_string::base_string(const std::string& s) : buffer_(dupstr(s.c_str())) {}

base_string::base_string(const char* from, std::size_t count): base_string() {
  if (from && count) {
    buffer_ = new char[count + 1];
    strncpy(buffer_, from, count);
    buffer_[count] = 0;
  }
}

base_string::~base_string() {
  if (buffer_) {
    delete [] buffer_;
  }
  buffer_ = 0;
}

base_string& base_string::operator += (const base_string& other) {
  auto new_buffer_ = new char[length() + other.length() + 1];

  std::strcpy(new_buffer_, (buffer_ ? buffer_ : ""));
  std::strcat(new_buffer_, (other.buffer_ ? other.buffer_ : ""));

  if (buffer_) {
    delete [] buffer_;
  }

  buffer_ = new_buffer_;

  return *this;
}

base_string& base_string::operator += (const char* other) {
  return *this += base_string(other);
}

base_string& base_string::operator =(const base_string& other) {
  base_string temp(other);
  swap(*this, temp);
  return *this;
}

base_string& base_string::operator =(base_string&& other) {
  swap(*this, other);
  return *this;
}

void swap(base_string& first, base_string& second) {
  using std::swap;
  swap(first.buffer_, second.buffer_);
}

// Conversions
base_string::operator std::string() const {
  return std::string(buffer_ ? buffer_ : "");
}

// Accessors
std::size_t base_string::length() const {
  if (buffer_) {
    return strlen(buffer_);
  }

  return 0;
}

const char* base_string::c_str() const {
  return buffer_;
}

// Comparisons
bool base_string::operator ==(const base_string& other) const {
  return strcmp(buffer_ ? buffer_ : "", other.buffer_ ? other.buffer_ : "") == 0;
}

bool base_string::operator ==(const char* other) const {
  return strcmp(buffer_ ? buffer_ : "", other ? other : "") == 0;
}

bool base_string::operator !=(const base_string& other) const {
  return strcmp(buffer_ ? buffer_ : "", other.buffer_ ? other.buffer_ : "") != 0;
}

bool base_string::operator !=(const char* other) const {
  return strcmp(buffer_ ? buffer_ : "", other ? other : "") != 0;
}

bool base_string::operator <=(const base_string& other) const {
  return strcmp(buffer_ ? buffer_ : "", other.buffer_ ? other.buffer_ : "") <= 0;
}

bool base_string::operator <=(const char* other) const {
  return strcmp(buffer_ ? buffer_ : "", other ? other : "") <= 0;
}

bool base_string::operator <(const base_string& other) const {
  return strcmp(buffer_ ? buffer_ : "", other.buffer_ ? other.buffer_ : "") < 0;
}

bool base_string::operator <(const char* other) const {
  return strcmp(buffer_ ? buffer_ : "", other ? other : "") < 0;
}

bool base_string::operator >=(const base_string& other) const {
  return strcmp(buffer_ ? buffer_ : "", other.buffer_ ? other.buffer_ : "") >= 0;
}

bool base_string::operator >=(const char* other) const {
  return strcmp(buffer_ ? buffer_ : "", other ? other : "") >= 0;
}

bool base_string::operator >(const base_string& other) const {
  return strcmp(buffer_ ? buffer_ : "", other.buffer_ ? other.buffer_ : "") > 0;
}

bool base_string::operator >(const char* other) const {
  return strcmp(buffer_ ? buffer_ : "", other ? other : "") > 0;
}

// Operations
base_string base_string::substr(std::size_t from, std::size_t to) const {
  if (!buffer_) {
    return base_string();
  }

  auto n = length();

  if (from > n) {
    from = n;
  }

  if (to > n) {
    to = n;
  }

  return base_string(buffer_ + from, to - from);
}

// Modifies this string.
//Seq<R_elem<string_impl>> splitString(const string_impl& splitter) {
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
std::ostream& operator <<(std::ostream& out, const base_string& s) {
  if (s.buffer_) {
    return out << s.c_str();
  }

  return out;
}

char* base_string::begin() const {
  return buffer_;
}

char* base_string::end() const {
  return buffer_ + length();
}

inline base_string operator + (base_string s, base_string const& t) {
  return s += t;
}

inline base_string operator + (base_string s, char const* t) {
  return s += t;
}

inline base_string operator + (char const* t, base_string const& s) {
  auto new_string = base_string(t);
  return new_string += s;
}

// Utilities
char* dupstr(const char *s) throw () {
  if (!s) {
    return nullptr;
  }

  auto n = strlen(s);
  auto d = new char[n + 1];
  return static_cast<char*>(memcpy(d, s, n + 1));
}

//template<>
//std::size_t hash_value<base_string>(const base_string& s) {
//  std::size_t seed = 0;
//  if (s.length() > 0) {
//    for (auto& i: s) {
//      boost::hash_combine(seed, i);
//    }
//  }
//  return seed;
//}
//
//template <>
//void base_string::serialize(csv::parser& a, const unsigned int) {
//  std::string tmp;
//  a& tmp;
//
//  if (buffer_) {
//    delete[] buffer_;
//  }
//  buffer_ = dupstr(tmp.c_str());
//}
//
//// Specializations for CSV parsing/writing, skipping the length field.
//template <>
//void base_string::serialize(csv::writer& a, const unsigned int) {
//  std::string tmp = std::string(buffer_, length());
//  a& tmp;
//}
//
//
}  // namespace K3
