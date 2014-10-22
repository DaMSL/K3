#ifndef K3_RUNTIME_BASESTRING_H
#define K3_RUNTIME_BASESTRING_H

#include <cstring>
#include <memory>
#include <vector>

#include "boost/serialization/array.hpp"
#include "boost/functional/hash.hpp"

#include "Common.hpp"
#include "dataspace/Dataspace.hpp"

char* strdup(const char*);

namespace K3 {
  class base_string {
   public:
    // Constructors/Destructors/Assignment.

    base_string(): buffer(nullptr) {}

    base_string(const base_string& other): buffer(strdup(other.buffer)) {}

    base_string(base_string&& other): base_string() {
      swap(*this, other);
    }

    base_string(const char* b): buffer(strdup(b)) {}
    base_string(const std::string& s) : buffer(strdup(s.c_str())) {}

    base_string(const char* from, std::size_t count) {
      buffer = new char[count + 1];
      strncpy(buffer, from, count);
      buffer[count] = 0;
    }

    ~base_string() {
      delete [] buffer;
    }

    base_string& operator =(const base_string& other) {
      *this = base_string { other };
      return *this;
    }

    base_string& operator =(base_string&& other) {
      swap(*this, other);
      return *this;
    }

    friend void swap(base_string& first, base_string& second) {
      using std::swap;
      swap(first.buffer, second.buffer);
    }

    // Conversions
    operator std::string() const {
      return std::string(buffer);
    }

    // Accessors
    std::size_t length() {
      return strlen(buffer);
    }

    const char* c_str() const {
      return buffer;
    }

    // Comparisons
    bool operator ==(const base_string& other) const {
      return strcmp(buffer, other.buffer) == 0;
    }

    bool operator !=(const base_string& other) const {
      return strcmp(buffer, other.buffer) != 0;
    }

    bool operator <=(const base_string& other) const {
      return strcmp(buffer, other.buffer) <= 0;
    }

    bool operator <(const base_string& other) const {
      return strcmp(buffer, other.buffer) < 0;
    }

    bool operator >=(const base_string& other) const {
      return strcmp(buffer, other.buffer) >= 0;
    }

    bool operator >(const base_string& other) const {
      return strcmp(buffer, other.buffer) > 0;
    }

    // Operations
    base_string substr(std::size_t from, std::size_t to) const {
      auto n = strlen(buffer);

      if (from > n) {
        from = n;
      }

      if (to > n) {
        to = n;
      }

      return base_string(buffer + from, to - from);
    }

    // Modifies this string.
    Seq<R_elem<string_impl>> splitString(const string_impl& splitter) {
      Seq<R_elem<string_impl>> results;
      R_elem<string_impl> rec;
      char * pch;
      pch = strtok (buffer, splitter.c_str());
      while (pch != NULL)
      {
        rec.elem = string_impl(pch);
        results.insert(rec);
        pch = strtok (NULL, splitter.c_str());
      }

      return results;
    }

    // Stream Operators
    friend std::ostream& operator <<(std::ostream& out, const K3::base_string& s) {
      return out << s.c_str();
    }

    char* begin() const {
      return buffer;
    }

    char* end() const {
      return buffer + strlen(buffer);
    }

    template <class archive>
    void serialize(archive& a, const unsigned int) {
      int len;
      if (archive::is_saving::value) {
        len = strlen(buffer);
      }
      a & len;
      if (archive::is_loading::value) {
        buffer = new char[len + 1];
        buffer[len] = 0;
      }
      a & boost::serialization::make_array(buffer, len);
    }

   private:
    char* buffer;
  };

  std::size_t hash_value(const K3::base_string&);
}

#endif
