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
  base_string() : buffer_(nullptr) {}

  base_string(const base_string& other) : buffer_(dupbuf(other)) {
    set_header(other.has_header());
    set_advance(other.has_advance());
  }

  base_string(base_string&& other) noexcept : base_string() {
    // If the other string has ownership, move, else copy and set our ownership
    if (!other.is_borrowing()) {
      swap(*this, other);
    } else {
      buffer_ = dupbuf(other);
      set_header(other.has_header());
      set_advance(other.has_advance());
    }
  }

  base_string(const char* b) : buffer_(dupstr(b)) {}

  base_string(const std::string& s) : buffer_(dupstr(s.c_str())) { }

  base_string(const char* from, std::size_t count) : base_string() {
    if (from && count) {
      buffer_ = new char[count + 1];
      strncpy(buffer_, from, count);
      buffer_[count] = 0;
    }
  }

  ~base_string() {
    if (!is_borrowing()) {
      delete[] bufferp_();
    }
    buffer_ = nullptr;
  }

  // take a buffer with ownership
  void steal(char *p) {
    if (!is_borrowing()) {
      delete[] bufferp_();
    }
    buffer_ = p;
    set_borrowing(false);
  }

  // take a buffer without taking ownership
  void unowned(char* p) {
    if (!is_borrowing()) { delete[] bufferp_(); }
    buffer_ = p;
    set_borrowing(true);
  }

  base_string& operator+=(const base_string& other);
  base_string& operator+=(const char* other);
  base_string& operator=(const base_string& other);
  base_string& operator=(base_string&& other);
  friend char* dupbuf(const base_string& b) throw();
  friend size_t cmp(const base_string& b1, const base_string& b2);
  friend size_t cmp(const base_string& b1, const char* other);
  friend void swap(base_string& first, base_string& second);

  // Tag getters
  bool has_header() const { return (as_bits & header_flag) != 0; }
  bool has_advance() const { return (as_bits & advance_flag) != 0; }
  bool is_borrowing() const { return (as_bits & borrow_flag) != 0; }

  // tag setters
  void set_header(const bool& on) {
    if (on) { as_bits |= header_flag; }
    else { as_bits &= ~header_flag; }
  }
  void set_advance(const bool& on) {
    if (on) { as_bits |= advance_flag; }
    else { as_bits &= ~advance_flag; }
  }
  void set_borrowing(const bool& on) {
    if (on) { as_bits |= borrow_flag; }
    else { as_bits &= ~borrow_flag; }
  }

  // Conversions
  operator bool() const;
  operator std::string() const;

  // Accessors
  std::size_t length() const;
  std::size_t raw_length() const;
  const char* c_str() const;
  const char* data() const;

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
    bool header = has_header();
    a & header;
    if (archive::is_loading::value) {
      set_header(header);
    }

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
    a& has_header();
    uint64_t len = raw_length();
    a& len;
    if (bufferp_()) {
      a.write(bufferp_(), len);
    }
  }

  template <class archive>
  void serialize(archive& a) {
    bool has_header;
    a& has_header;
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

    set_header(has_header);
  }

  __attribute__((always_inline)) char* bufferp_() const {
    return reinterpret_cast<char*>(as_bits & ~header_mask);
  }

 private:
  union {
    char* buffer_;
    intptr_t as_bits;
  };

  constexpr static size_t header_size = sizeof(size_t);
  constexpr static intptr_t header_mask = alignof(char*) - 1;
  constexpr static int header_flag  = 0b1;
  constexpr static int advance_flag = 0b10;
  // bit signifying the fact that we don't own our own pointer
  constexpr static int borrow_flag  = 0b100;
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
