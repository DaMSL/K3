#ifndef K3_BOXING
#define K3_BOXING

#include <memory>
#include "serialization/Yaml.hpp"
#include "rapidjson/document.h"
#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"

namespace K3 {
  template <class T>
  class Box {
   public:
    Box(): contents(std::make_unique<T>()) {}

    Box(Box<T> const& other): contents(std::make_unique<T>(*other)) {}
    Box(Box<T>&& other): contents(std::make_unique<T>(std::move(*other))) {}

    Box(T* other): contents(other) {}

    Box<T>& operator=(Box<T> const& other) {
      contents = std::make_unique<T>(*other);
    }

    Box<T>& operator=(Box<T>&& other) {
      contents = std::make_unique<T>(std::move(*other));
    }

    T& operator*() {
      return *contents;
    }

    T const& operator*() const {
      return *contents;
    }

    std::unique_ptr<T>& operator->() {
      return contents;
    }

    std::unique_ptr<T> const& operator->() const {
      return contents;
    }

    bool operator==(Box<T> const& other) const {
      return *contents == *other;
    }

    bool operator!=(Box<T> const& other) const {
      return *contents != *other;
    }

    bool operator<(Box<T> const& other) const {
      return *contents < *other;
    }

    bool operator<=(Box<T> const& other) const {
      return *contents <= *other;
    }

    bool operator>(Box<T> const& other) const {
      return *contents > *other;
    }

    bool operator>=(Box<T> const& other) const {
      return *contents >= *other;
    }

    template <class archive>
    void serialize(archive& _archive, unsigned int const _version) {
      return contents->serialize(_archive, _version);
    }

    template <class archive>
    void serialize(archive& _archive) {
      return contents->serialize(_archive);
    }

    std::unique_ptr<T> contents;
  };

  template <class T, class... Args>
  Box<T> make_box(Args&&... args) {
    return Box<T>(new T {std::forward<Args>(args)...});
  }
}

namespace YAML {
  template <class T>
  class convert<K3::Box<T>> {
   public:
    static Node encode(K3::Box<T> const& b) {
      return YAML::convert<T>::encode(*b);
    }

    static bool decode(Node const& node, K3::Box<T>& b) {
      return YAML::convert<T>::decode(node, *b);
    }
  };
}

namespace std {
  template <class T>
  struct hash<K3::Box<T>> {
    size_t operator()(K3::Box<T> const& b) const {
      return hash<T>()(*b);
    }
  };
}

namespace JSON {
  using namespace rapidjson;

  template <class T>
  struct convert<K3::Box<T>> {
    template <class Allocator>
    static Value encode(K3::Box<T> const& from, Allocator& al) {
      return convert<T>::encode(*from, al);
    }
  };
}
#endif
