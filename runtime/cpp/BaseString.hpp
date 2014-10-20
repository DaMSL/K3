#ifndef K3_RUNTIME_BASESTRING_H
#define K3_RUNTIME_BASESTRING_H

#include <cstring>
#include <memory>

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

    template <class archive>
    void serialize(archive& a, const unsigned int) {
      a & boost::serialization::make_array(buffer, strlen(buffer));
    }
   private:
    char* buffer;
  };
}

#endif
