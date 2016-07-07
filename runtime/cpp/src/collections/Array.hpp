#ifndef K3_ARRAY
#define K3_ARRAY

#include <array>
#include <string>


#include <boost/serialization/array.hpp>
#include <boost/serialization/string.hpp>
#include <boost/functional/hash.hpp>

#include <yaml-cpp/yaml.h>
#include <rapidjson/document.h>
#include <csvpp/csv.h>
#include "serialization/Json.hpp"

#include "Common.hpp"

namespace K3 {

template <class Elem, size_t N>
class Array {
	using Container = std::array<Elem, N>;

public:
  Array() : container() {}
  Array(const Container& c) : container(c) {}
  Array(Container&& c) : container(std::move(c)) {}

  static const int array_size = N;

  int size(const unit_t&) const {
    return container.size();
  }

  template <class F, class G>
  auto peek(F f, G g) const {
    auto it = container.begin();
    if (it == container.end()) {
      return f(unit_t{});
    } else {
      return g(*it);
    }
  }

  Elem at(int i) const { return container[i]; }

  template<class F, class G>
  auto safe_at(int i, F f, G g) const {
    if ( i < container.size() ) {
      return g(container[i]);
    } else {
      return f(unit_t {});
    }
  }

  unit_t set(int i, Elem f) {
    container[i] = f;
    return unit_t();
  }

  Elem swap(int i, Elem n) {
    auto old = container[i];
    container[i] = n;
    return old;
  }

  unit_t fill(Elem e) {
    fill(e);
    return unit_t {};
  }

  template<class F>
  auto fill_by(F f) {
    for (size_t i = 0; i < N; ++i) {
      container[i] = fill_by(i);
    }
    return unit_t {};
  }

  template<typename Fun>
  unit_t iterate(Fun f) const {
    for (const Elem& e : *this) {
      f(e);
    }
    return unit_t();
  }

  template<typename Fun>
  auto map(Fun f) const -> Array<R_elem<RT<Fun, Elem>>, N>  const {
    Array<R_elem<RT<Fun, Elem>>, N> result;
    for (size_t i = 0; i < N; ++i) {
      result.set(i, R_elem<RT<Fun, Elem>> { f(container[i]) });
    }
    return result;
  }


  template<typename Fun, typename Acc>
  Acc fold(Fun f, Acc acc) const {
    for (const Elem &e : *this) {
      acc = f(std::move(acc), e);
    }
    return acc;
  }

  template<typename Fun, class OtherElem>
  auto zip(Fun f, const Array<R_elem<OtherElem>, N>& other) const {
    Array<RT<Fun, Elem>, N> result;
    for (size_t i = 0; i < N; ++i) {
      result[i] = f(container[i], other[i]);
    }
    return result;
  }

  // Iterators
  using iterator = typename Container::iterator;
  using const_iterator = typename Container::const_iterator;
  //using reverse_iterator = typename Container::reverse_iterator;

  iterator begin() { return container.begin(); }
  iterator end() { return container.end(); }

  const_iterator begin() const { return container.cbegin(); }
  const_iterator end() const { return container.cend(); }

  const_iterator cbegin() const { return container.cbegin(); }
  const_iterator cend() const { return container.cend(); }


  bool operator==(const Array& other) const {
    return container == other.container;
  }

  bool operator!=(const Array& other) const {
    return container != other.container;
  }

  bool operator<(const Array& other) const {
    return container < other.container;
  }

  bool operator>(const Array& other) const {
    return container < other.container;
  }

  template<class Archive>
  void serialize(Archive &ar) {
    ar & container;
  }

  template<class Archive>
  void serialize(Archive &ar, const unsigned int) {
    ar & container;
  }

 protected:
  Container container;
};

}; // namespace K3

namespace YAML {
template <class E, size_t N>
struct convert<K3::Array<E,N>> {
  static Node encode(const K3::Array<E,N>& c) {
    Node node;
    if (c.size(K3::unit_t{}) > 0) {
      for (auto i : c) {
        node.push_back(convert<E>::encode(i));
      }
    } else {
      node = YAML::Load("[]");
    }
    return node;
  }

  static bool decode(const Node& node, K3::Array<E,N>& c) {
    size_t j = 0;
    for (auto& i : node) {
      c.set(j, i.as<E>());
      ++j;
    }
    return true;
  }
};
}  // namespace YAML

namespace JSON {
using namespace rapidjson;
template <class E, size_t N>
struct convert<K3::Array<E, N>> {
  template <class Allocator>
  static Value encode(const K3::Array<E,N>& c, Allocator& al) {
    Value v;
    v.SetObject();
    v.AddMember("type", Value("Array"), al);
    Value inner;
    inner.SetArray();
    for (const auto& e : c) {
      inner.PushBack(convert<E>::encode(e, al), al);
    }
    v.AddMember("value", inner.Move(), al);
    return v;
  }
};
}  // namespace JSON

#endif