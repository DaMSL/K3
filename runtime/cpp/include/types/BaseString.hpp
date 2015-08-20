#ifndef K3_BASESTRING
#define K3_BASESTRING

#include <cstring>
#include <memory>
#include <vector>

#include <boost/serialization/array.hpp>
#include <boost/serialization/string.hpp>
#include <boost/functional/hash.hpp>

#include <yaml-cpp/yaml.h>
#include <rapidjson/document.h>
#include <csvpp/csv.h>
#include "serialization/Json.hpp"

namespace K3 {

char* dupstr(const char*) throw();
char* dupbuf(const base_string& b) throw();

class base_string {
 public:
  // Constructors/Destructors/Assignment.
  base_string();
  base_string(const base_string& other);
  base_string(base_string&& other);
  base_string(const char* b);
  base_string(const std::string& s);
  base_string(const char* from, std::size_t count);
  ~base_string();

  void steal(char *p);
  base_string& operator+=(const base_string& other);
  base_string& operator+=(const char* other);
  base_string& operator=(const base_string& other);
  base_string& operator=(base_string&& other);
  friend char* dupbuf(const base_string& b) throw();
  friend size_t cmp(const base_string& b1, const base_string& b2);
  friend size_t cmp(const base_string& b1, const char* other);
  friend void swap(base_string& first, base_string& second);

  // Header tag management
  bool has_header() const;
  void set_header(const bool& on);

  // Conversions
  operator std::string() const;

  // Accessors
  std::size_t length() const;
  std::size_t raw_length() const;
  const char* c_str() const;

  // Comparisons
  bool operator==(const base_string& other) const;
  bool operator==(const char* other) const;
  bool operator!=(const base_string& other) const;
  bool operator!=(const char* other) const;
  bool operator<=(const base_string& other) const;
  bool operator<=(const char* other) const;
  bool operator<(const base_string& other) const;
  bool operator<(const char* other) const;
  bool operator>=(const base_string& other) const;
  bool operator>=(const char* other) const;
  bool operator>(const base_string& other) const;
  bool operator>(const char* other) const;

  // Operations
  base_string substr(std::size_t from, std::size_t to) const;
  int strcomp(const base_string& other) const;

  // Seq<R_elem<string_impl>> splitString(const string_impl& splitter); //
  // Modifies this string.

  // Stream Operators
  friend std::ostream& operator<<(std::ostream& out, const base_string& s);
  char* begin() const;
  char* end() const;

  template <class archive>
  void serialize(archive& a, const unsigned int) {
    uint64_t len;
    if (archive::is_saving::value) {
      len = raw_length();
    }
    a& len;
    if (archive::is_loading::value) {
      if (bufferp_()) {
        delete[] bufferp_();
        buffer_ = 0;
      }

      if (len) {
        buffer_ = new char[len + 1];
        buffer_[len] = 0;
      } else {
        buffer_ = 0;
      }
    }
    if (bufferp_()) {
      a& boost::serialization::make_array(bufferp_(), len);
    }
  }

  template <class archive>
  void serialize(archive& a) const {
    uint64_t len = raw_length();
    a& len;
    if (bufferp_()) {
      a.write(bufferp_(), len);
    }
  }

  template <class archive>
  void serialize(archive& a) {
    uint64_t len;
    a& len;
    if (bufferp_()) {
      delete[] bufferp_();
      buffer_ = 0;
    }

    if (len) {
      buffer_ = new char[len + 1];
      buffer_[len] = 0;
    } else {
      buffer_ = 0;
    }
    if (bufferp_()) {
      a.read(bufferp_(), len);
    }
  }

 private:
  union {
    char* buffer_;
    intptr_t as_bits;
  };

  __attribute__((always_inline)) char* bufferp_() const {
    return reinterpret_cast<char*>(as_bits & ~header_mask);
  }

  constexpr static intptr_t header_mask = alignof(char*) - 1;
  constexpr static int header_flag = 0b1;
  constexpr static size_t header_size = sizeof(size_t);
};

inline bool operator==(char const* s, base_string const& b) { return b == s; }

inline bool operator<(char const* s, base_string const& b) { return b >= s; }

inline bool operator<=(char const* s, base_string const& b) { return b > s; }

inline bool operator>(char const* s, base_string const& b) { return b <= s; }

inline bool operator>=(char const* s, base_string const& b) { return b < s; }

inline bool operator!=(char const* s, base_string const& b) { return b != s; }

inline base_string operator+(base_string s, base_string const& t) {
  return s += t;
}


// Specializations for CSV parsing/writing, skipping the length field.
template <>
void base_string::serialize(csv::parser& a, const unsigned int);

template <>
void base_string::serialize(csv::writer& a, const unsigned int);

}  // namespace K3

namespace JSON {
template <>
struct convert<K3::base_string> {
  template <class Allocator>
  static rapidjson::Value encode(const K3::base_string& from, Allocator& al) {
    Value v;
    if (from.c_str()) {
      v.SetString(from.c_str(), al);
    } else {
      v.SetString("", al);
    }
    return v;
  }
};
}

K3::base_string operator+(K3::base_string s, char const* t);
K3::base_string operator+(char const* t, K3::base_string const& s);

namespace YAML {
template <>
struct convert<::K3::base_string> {
  static Node encode(const ::K3::base_string& s) {
    Node node;
    node = std::string(s.c_str());
    return node;
  }

  static bool decode(const Node& node, ::K3::base_string& s) {
    try {
      auto t = node.as<std::string>();
      s = ::K3::base_string(t);
      return true;
    } catch (YAML::TypedBadConversion<std::string> e) {
      return false;
    }
  }
};
}

// Turn off class information tracking in boost serialization for base_strings.
BOOST_CLASS_IMPLEMENTATION(K3::base_string, boost::serialization::object_serializable);

#endif
