#ifndef K3_UNIQPOLYBUF
#define K3_UNIQPOLYBUF

#include <unordered_set>

#if HAS_LIBDYNAMIC

#include <boost/serialization/array.hpp>
#include <boost/serialization/string.hpp>
#include <boost/functional/hash.hpp>

#include <yaml-cpp/yaml.h>
#include <rapidjson/document.h>
#include <csvpp/csv.h>
#include "serialization/Json.hpp"

#include "Common.hpp"
#include "collections/STLDataspace.hpp"
#include "collections/Collection.hpp"
#include "FlatPolyBuffer.hpp"

namespace K3 {
namespace Libdynamic {

//////////////////////////////////////////
//
// Buffer functors.

template<typename Tag>
using UPBKey = std::pair<Tag, void*>;

template<class T, typename Tag>
struct UPBEqual : std::binary_function<UPBKey<Tag>, UPBKey<Tag>, bool> {
  bool operator()(const UPBKey<Tag>& left, const UPBKey<Tag>& right) const {
    return T::equalelem(left.first, left.second, right.first, right.second);
  }
};

template<typename T, typename Tag>
struct UPBHash : std::unary_function<UPBKey<Tag>, std::size_t> {
  std::size_t operator()(const UPBKey<Tag>& k) const {
    std::hash<Tag> hash;
    size_t h1 = hash(k.first);
    boost::hash_combine(h1, T::hashelem(k.first, k.second));
    return h1;
  }
};


//////////////////////////////////////////
//
// UniquePolyBuffer

template<class Ignore, class Derived>
class UniquePolyBuffer : public FlatPolyBuffer<Ignore> {
public:
  using Super = FlatPolyBuffer<Ignore>;
  using Tag = typename Super::Tag;

  using FContainer = typename Super::FContainer;
  using VContainer = typename Super::VContainer;
  using TContainer = typename Super::TContainer;

  UniquePolyBuffer() {}

  UniquePolyBuffer(const UniquePolyBuffer& other) : Super(other) {
    keys = other.keys;
  }

  UniquePolyBuffer(UniquePolyBuffer&& other) : Super(other) {
    keys.swap(other.keys);
  }

  UniquePolyBuffer& operator=(const UniquePolyBuffer& other) {
    Super::operator=(other);
    keys = other.keys;
  }

  void rebuildKeys() {
    size_t foffset = 0, sz = Super::size(unit_t{});
    for (size_t i = 0; i < sz; ++i) {
      Tag tg = Super::tag_at(i);
      keys.insert(std::make_pair(tg, reinterpret_cast<void*>(buffer_data(Super::fixed()) + foffset)));
      foffset += Super::elemsize(tg);
    }
  }

  //////////////////////////////////
  //
  // Tag-specific accessors.

  template<typename T>
  unit_t append(Tag tg, const T& t) {
    if ( keys.find(std::make_pair(tg, reinterpret_cast<void*>(const_cast<T*>(&t)))) == keys.end() ) {
      FContainer* ncf = const_cast<FContainer*>(Super::fixedc());
      size_t offset = buffer_size(ncf);
      Super::append(tg, t);
      keys.insert(std::make_pair(tg, buffer_data(ncf)+offset));
    }
    return unit_t{};
  }

  // Clears a container, deleting any backing buffer.
  unit_t clear(unit_t) {
    keys.clear();
    return Super::clear(unit_t{});
  }

  unit_t load(const base_string& str) {
    return load(base_string(str));
  }

  // Restores a flat poly buffer from a string.
  unit_t load(base_string&& str) {
    Super::load(std::forward<base_string>(str));
    rebuildKeys();
    return unit_t{};
  }

  template <class archive>
  void save(archive& a, const unsigned int) const {
    a << boost::serialization::base_object<const Super>(*this);
  }

  template <class archive>
  void load(archive& a, const unsigned int) {
    a >> boost::serialization::base_object<Super>(*this);
    rebuildKeys();
  }

  template <class archive>
  void serialize(archive& a) const {
    a << yas::base_object<const Super>(*this);
  }

  template <class archive>
  void serialize(archive& a) {
    a >> yas::base_object<Super>(*this);
    rebuildKeys();
  }

  BOOST_SERIALIZATION_SPLIT_MEMBER()

private:
  std::unordered_set<UPBKey<Tag>, UPBHash<Derived, Tag>, UPBEqual<Derived, Tag>> keys;
};

}; // end namespace Libdynamic

template<class Ignored, class Derived>
using UniquePolyBuffer = Libdynamic::UniquePolyBuffer<Ignored, Derived>;

}; // end namespace K3


namespace YAML {
template <class E, class Derived>
struct convert<K3::UniquePolyBuffer<E, Derived>> {
  using Tag = typename K3::UniquePolyBuffer<E, Derived>::Tag;
  static Node encode(const K3::UniquePolyBuffer<E, Derived>& c) {
    Node node;
    bool flag = true;
    c.iterate([&c, &node, &flag](Tag tg, size_t idx, size_t offset){
      if (flag) { flag = false; }
      node.push_back(c.yamlencode(tg, idx, offset));
    });
    if (flag) {
      node = YAML::Load("[]");
    }
    return node;
  }

  static bool decode(const Node& node, K3::UniquePolyBuffer<E, Derived>& c) {
    for (auto i : node) {
      c.yamldecode(i);
    }
    return true;
  }
};
}  // namespace YAML

namespace JSON {
using namespace rapidjson;
template <class E, class Derived>
struct convert<K3::UniquePolyBuffer<E, Derived>> {
  using Tag = typename K3::UniquePolyBuffer<E, Derived>::Tag;
  template <class Allocator>
  static Value encode(const K3::UniquePolyBuffer<E, Derived>& c, Allocator& al) {
    Value v;
    v.SetObject();
    v.AddMember("type", Value("UniquePolyBuffer"), al);
    Value inner;
    inner.SetArray();
    c.iterate([&c, &inner, &al](Tag tg, size_t idx, size_t offset){
      inner.PushBack(c.jsonencode(tg, idx, offset, al), al);
    });
    v.AddMember("value", inner.Move(), al);
    return v;
  }
};
}  // namespace JSON

#endif // HAS_LIBDYNAMIC

#endif
