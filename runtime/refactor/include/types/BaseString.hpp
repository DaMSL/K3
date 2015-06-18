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

  base_string& operator+=(const base_string& other);
  base_string& operator+=(const char* other);
  base_string& operator=(const base_string& other);
  base_string& operator=(base_string&& other);
  friend void swap(base_string& first, base_string& second);

  // Conversions
  operator std::string() const;

  // Accessors
  std::size_t length() const;
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
    std::size_t len;
    if (archive::is_saving::value) {
      len = length();
    }
    a& len;
    if (archive::is_loading::value) {
      if (buffer_) {
        delete[] buffer_;
        buffer_ = 0;
      }

      if (len) {
        buffer_ = new char[len + 1];
        buffer_[len] = 0;
      } else {
        buffer_ = 0;
      }
    }
    if (buffer_) {
      a& boost::serialization::make_array(buffer_, len);
    }
  }

  template <class archive>
  void serialize(archive& a) const {
    uint64_t len = length();
    a& len;
    if (buffer_) {
      a.write(buffer_, len);
    }
  }

  template <class archive>
  void serialize(archive& a) {
    uint64_t len;
    a& len;
    if (buffer_) {
      delete[] buffer_;
      buffer_ = 0;
    }

    if (len) {
      buffer_ = new char[len + 1];
      buffer_[len] = 0;
    } else {
      buffer_ = 0;
    }
    if (buffer_) {
      a.read(buffer_, len);
    }
  }
  

 private:
  char* buffer_;
};
  
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

K3::base_string operator+(K3::base_string s, K3::base_string const& t);
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
