#ifndef K3_INTSET
#define K3_INTSET

#include <tuple>
#include <functional>
#include <stdexcept>

#if HAS_LIBDYNAMIC
extern "C" {
#include <dynamic/mapi.h>
}

#include "boost/serialization/array.hpp"
#include "boost/serialization/binary_object.hpp"
#include "boost/serialization/string.hpp"
#include "csvpp/csv.h"

#include "Common.hpp"
#include "serialization/Json.hpp"
#include "serialization/Yaml.hpp"

namespace K3 {
namespace Libdynamic {

template <class Elem>
class IntSet {
  using CloneFn = void (*)(void*, void*, size_t);
  using Container = shared_ptr<mapi>;

 public:
  IntSet() : container() {}

  IntSet(const Container& con) {
    mapi* m = con.get();
    if (m) {
      init_mapi(true, sizeof(Elem));
      mapi* n = get_mapi();
      for (auto o = mapi_begin(m); o < mapi_end(m); o = mapi_next(m, o)) {
        mapi_insert(n, o);
      }
    }
  }

  IntSet(Container&& con) : container(std::move(con)) {}

  template <typename Iterator>
  IntSet(Iterator begin, Iterator end) {
    init_mapi(true, sizeof(Elem));
    mapi* m = get_mapi();
    for (auto it = begin; it != end; ++it) {
      mapi_insert(m, static_cast<void*>(&(*it)));
    }
  }

  template <class I>
  class set_iterator : public std::iterator<std::forward_iterator_tag, Elem> {
    using reference = typename std::iterator<std::forward_iterator_tag, Elem>::reference;

   public:
    template <class _I>
    set_iterator(mapi* _m, _I&& _i) : m(_m), i(static_cast<I>(std::forward<_I>(_i))) {}

    set_iterator& operator++() {
      i = static_cast<I>(mapi_next(m, const_cast<void*>(static_cast<const void*>(i))));
      return *this;
    }

    set_iterator operator++(int) {
      set_iterator t = *this;
      *this++;
      return t;
    }

    auto operator -> () const { return i; }
    auto& operator*() const { return *i; }

    bool operator==(const set_iterator& other) const { return i == other.i; }
    bool operator!=(const set_iterator& other) const { return i != other.i; }

   private:
    mapi* m;
    I i;
  };

  using iterator = set_iterator<Elem*>;
  using const_iterator = set_iterator<const Elem*>;

  iterator begin() {
    mapi* m = get_mapi();
    return iterator(m, mapi_begin(m));
  }

  iterator end() {
    mapi* m = get_mapi();
    return iterator(m, mapi_end(m));
  }

  const_iterator begin() const {
    mapi* m = get_mapi();
    return const_iterator(m, mapi_begin(m));
  }

  const_iterator end() const {
    mapi* m = get_mapi();
    return const_iterator(m, mapi_end(m));
  }

  // Functionality
  int size(unit_t) const { return mapi_size(get_mapi()); }

  template<typename F, typename G>
  auto peek(F f, G g) const {
    auto it = mapi_begin(get_mapi());
    if (it < mapi_end(get_mapi()) ) {
      return g(*static_cast<Elem*>(it));
    } else {
      return f(unit_t {});
    }
  }


  // Set-specific functions.

  bool member(const Elem& r) const {
    mapi* m = get_mapi();
    return m->size == 0 ? false : (mapi_find(m, *reinterpret_cast<uint32_t*>(&r)) != nullptr);
  }

  // TODO: is_subset_of, intersect, difference
  bool is_subset_of(const IntSet<Elem>& other) const {
    mapi* m = get_mapi();
    for (auto o = mapi_begin(m); o < mapi_end(m); o = mapi_next(m, o)) {
      if (!other.member(*o)) {
        return false;
      }
    }
    return true;
  }

  IntSet<Elem> intersect(const IntSet<Elem>& other) const {
    IntSet<Elem> result;
    mapi* m = get_mapi();
    for (auto o = mapi_begin(m); o < mapi_end(m); o = mapi_next(m, o)) {
      if (other.member(*o)) {
        result.insert(*o);
      }
    }
    return result;
  }

  IntSet<Elem> difference(const IntSet<Elem>& other) const {
    IntSet<Elem> result;
    mapi* m = get_mapi();
    for (auto o = mapi_begin(m); o < mapi_end(m); o = mapi_next(m, o)) {
      if (!other.member(*o)) {
        result.insert(*o);
      }
    }
    return result;
  }

  // Modifiers.

  // TODO(yanif): Fix insert semantics to replace value if key exists.
  unit_t insert(const Elem& q) {
    mapi_insert(get_mapi(), const_cast<void*>(static_cast<const void*>(&q)));
    return unit_t();
  }

  unit_t insert(Elem&& q) {
    mapi_clone(get_mapi(), (CloneFn)&IntSet<Elem>::moveElem);
    mapi_insert(get_mapi(), static_cast<void*>(&q));
    mapi_clone(get_mapi(), (CloneFn)&IntSet<Elem>::cloneElem);
    return unit_t();
  }

  unit_t update(const Elem& rec1, Elem& rec2) {
    mapi* m = get_mapi();
    if (m->size > 0) {
      auto existing = mapi_find(m, *reinterpret_cast<uint32_t*>(&rec1));
      if (existing != nullptr) {
        mapi_erase(m, *reinterpret_cast<uint32_t*>(&rec1));
        mapi_insert(m, &rec2);
      }
    }
    return unit_t();
  }

  unit_t erase(const Elem& rec) {
    mapi* m = get_mapi();
    if (m->size > 0) {
      auto existing = mapi_find(m, *reinterpret_cast<uint32_t*>(&rec));
      if (existing != nullptr) {
        mapi_erase(m, *reinterpret_cast<uint32_t*>(&rec));
      }
    }
    return unit_t();
  }


  //////////////////////////////////////////////////////////////
  // Bulk transformations.

  IntSet combine(const IntSet& other) const {
    // copy this DS
    IntSet result = IntSet(*this);
    // copy other DS
    mapi* m = other.get_mapi();
    for (auto o = mapi_begin(m); o < mapi_end(m); o = mapi_next(m, o)) {
      result.insert(*o);
    }
    return result;
  }

  tuple<IntSet, IntSet> split(const unit_t&) const {
    mapi* m = get_mapi();

    // Find midpoint
    const size_t size = mapi_size(m);
    const size_t half = size / 2;

    // Setup iterators
    auto mid = mapi_begin(m);
    for (size_t i = 0; i < half; ++i) {
      mid = mapi_next(m, mid);
    }

    // Construct DS from iterators
    return std::make_tuple(IntSet(begin(), iterator(m, mid)),
                           IntSet(iterator(m, mid), end()));
  }

  template <typename Fun>
  unit_t iterate(Fun f) const {
    mapi* m = get_mapi();
    for (auto o = mapi_begin(m); o < mapi_end(m); o = mapi_next(m, o)) {
      f(*static_cast<Elem*>(o));
    }
    return unit_t();
  }

  template <typename Fun>
  auto map(Fun f) const -> IntSet<RT<Fun, Elem>> {
    mapi* m = get_mapi();
    IntSet<RT<Fun, Elem>> result;
    for (auto o = mapi_begin(m); o < mapi_end(m); o = mapi_next(m, o)) {
      result.insert(f(*o));
    }
    return result;
  }

  template <typename Fun>
  auto map_generic(Fun f) const -> Set<RT<Fun, Elem>> {
    mapi* m = get_mapi();
    Set<RT<Fun, Elem>> result;
    for (auto o = mapi_begin(m); o < mapi_end(m); o = mapi_next(m, o)) {
      result.insert(R_elem<RT<Fun, Elem>> { f(*o) });
    }
    return result;
  }

  template <typename Fun>
  IntSet<Elem> filter(Fun predicate) const {
    mapi* m = get_mapi();
    IntSet<Elem> result;
    for (auto o = mapi_begin(m); o < mapi_end(m); o = mapi_next(m, o)) {
      if (predicate(*o)) {
        result.insert(*o);
      }
    }
    return result;
  }

  template <typename Fun, typename Acc>
  Acc fold(Fun f, Acc acc) const {
    mapi* m = get_mapi();
    for (auto o = mapi_begin(m); o < mapi_end(m); o = mapi_next(m, o)) {
      acc = f(std::move(acc), *o);
    }
    return acc;
  }

  template <typename F1, typename F2, typename Z>
  Map<R_key_value<RT<F1, Elem>, Z>> group_by_generic(F1 grouper, F2 folder, const Z& init) const
  {
    // Create a std::unordered_map to hold partial results
    typedef RT<F1, Elem> K;
    std::unordered_map<K, Z> accs;

    mapi* m = get_mapi();
    for (auto o = mapi_begin(m); o < mapi_end(m); o = mapi_next(m, o)) {
      Elem* elem = static_cast<Elem*>(o);
      K key = grouper(*elem);
      if (accs.find(key) == accs.end()) {
        accs[key] = init;
      }
      accs[key] = folder(std::move(accs[key]), *elem);
    }

    Map<R_key_value<K, Z>> result;
    for (auto&& it : accs) {
      result.insert(std::move(
          R_key_value<K, Z>{std::move(it.first), std::move(it.second)}));
    }
    return result;
  }

  template <class Fun>
  auto ext(Fun expand) const -> IntSet<typename RT<Fun, Elem>::ElemType> {
    typedef typename RT<Fun, Elem>::ElemType T;
    IntSet<T> result;
    mapi* m = get_mapi();
    auto end = mapi_end(m);
    for (auto it = mapi_begin(m); it < end; it = mapi_next(m, it)) {
      mapi* n = expand(*it);
      auto end2 = mapi_end(n);
      for (auto it2 = mapi_begin(n); it2 < end2; it2 = mapi_next(n, it2)) {
        result.insert(*it2);
      }
    }
    return result;
  }

  template <class Fun>
  auto ext_generic(Fun expand) const -> Set<typename RT<Fun, Elem>::ElemType> {
    typedef typename RT<Fun, Elem>::ElemType T;
    Set<T> result;
    mapi* m = get_mapi();
    auto end = mapi_end(m);
    for (auto it = mapi_begin(m); it < end; it = mapi_next(m, it)) {
      for (T&& elem : expand(*it).container) {
        result.insert(std::move(elem));
      }
    }
    return result;
  }

  bool operator==(const IntSet& other) const {
    return get_mapi() == other.get_mapi() ||
           (size() == other.size() &&
            std::is_permutation(begin(), end(), other.begin(), other.end()));
  }

  bool operator!=(const IntSet& other) const {
    return !(this->operator==(other));
  }

  bool operator<(const IntSet& other) const {
    return std::lexicographical_compare(begin(), end(), other.begin(),
                                        other.end());
  }

  bool operator>(const IntSet& other) const {
    return !(this->operator<(other) || this->operator==(other));
  }

  IntSet& getContainer() { return *const_cast<IntSet*>(this); }

  // Return a constant reference to the container
  const IntSet& getConstContainer() const { return *this; }

  template <class archive>
  void serialize(archive& ar) const {
    mapi* m = get_mapi();

    ar.write(reinterpret_cast<const char*>(&m->object_size), sizeof(m->object_size));
    ar & m->empty_key;
    ar.write(reinterpret_cast<const char*>(&m->size), sizeof(m->size));
    ar.write(reinterpret_cast<const char*>(&m->capacity), sizeof(m->capacity));

    for (auto o = mapi_begin(m); o < mapi_end(m); o = mapi_next(m, o)) {
      ar & *static_cast<Elem*>(o);
    }
  }

  template <class archive>
  void serialize(archive& ar) {
    size_t object_size;
    uint32_t empty_key;
    size_t container_size;
    size_t capacity;

    ar.read(reinterpret_cast<char*>(&object_size), sizeof(object_size));
    ar & empty_key;
    ar.read(reinterpret_cast<char*>(&container_size), sizeof(container_size));
    ar.read(reinterpret_cast<char*>(&capacity), sizeof(capacity));

    if (container) {
      mapi_clear(get_mapi());
      container.reset();
    }
    init_mapi(true, object_size);

    if (container) {
      mapi* n = get_mapi();
      mapi_empty_key(n, empty_key);
      mapi_rehash(n, capacity);
      mapi_clone(get_mapi(), (CloneFn)&IntSet<Elem>::moveElem);

      for (auto i = 0; i < container_size; ++i) {
        Elem r;
        ar & r;
        mapi_insert(n, &r);
      }

      mapi_clone(get_mapi(), (CloneFn)&IntSet<Elem>::cloneElem);
    } else {
      throw std::runtime_error(
          "Failed to instantiate IntSet for deserialization");
    }
  }

  BOOST_SERIALIZATION_SPLIT_MEMBER()

  static inline void cloneElem(void* dest, void* src, size_t sz) {
    new (dest) Elem(*static_cast<Elem*>(src));
  }

  static inline void moveElem(void* dest, void* src, size_t sz) {
    new (dest) Elem(std::move(*static_cast<Elem*>(src)));
  }

 protected:
  shared_ptr<mapi> container;

 private:
  friend class boost::serialization::access;

  mapi* get_mapi() const {
    if (!container) {
      init_mapi(true, sizeof(Elem));
    }
    return container.get();
  }

  void init_mapi(bool alloc, size_t sz) {
    if (alloc) {
      container = Container(mapi_new(sz), [](mapi* m) { mapi_free(m); });
    }
    mapi_clone(get_mapi(), (CloneFn)&IntSet<Elem>::cloneElem);
  }

  void init_mapi(bool alloc, size_t sz) const {
    if (alloc) {
      const_cast<shared_ptr<mapi>&>(container) =
          Container(mapi_new(sz), [](mapi* m) { mapi_free(m); });
    }
    mapi_clone(get_mapi(), (CloneFn)&IntSet<Elem>::cloneElem);
  }

  template <class archive>
  void save(archive& ar, const unsigned int) const {
    ar << boost::serialization::make_nvp("object_size", container->object_size);
    ar << boost::serialization::make_nvp("empty_key", container->empty_key);
    ar << boost::serialization::make_nvp("size", container->size);
    ar << boost::serialization::make_nvp("capacity", container->capacity);

    mapi* m = get_mapi();
    for (auto o = mapi_begin(m); o < mapi_end(m); o = mapi_next(m, o)) {
      ar << boost::serialization::make_nvp("item", *static_cast<Elem*>(o));
    }
  }

  template <class archive>
  void load(archive& ar, const unsigned int) {
    size_t object_size;
    uint32_t empty_key;
    size_t container_size;
    size_t capacity;

    ar >> boost::serialization::make_nvp("object_size", object_size);
    ar >> boost::serialization::make_nvp("empty_key", empty_key);
    ar >> boost::serialization::make_nvp("size", container_size);
    ar >> boost::serialization::make_nvp("capacity", capacity);

    if (container) {
      mapi_clear(get_mapi());
      container.reset();
    }
    init_mapi(true, object_size);

    if (container) {
      mapi* n = get_mapi();
      mapi_empty_key(n, empty_key);
      mapi_rehash(n, capacity);

      for (auto i = 0; i < container_size; ++i) {
        Elem r;
        ar >> boost::serialization::make_nvp("item", r);
        mapi_insert(n, &r);
      }
    } else {
      throw std::runtime_error(
          "Failed to instantiate IntSet for deserialization");
    }
  }
};

}  // namespace Libdynamic
}  // namespace K3

namespace YAML {
template <class Elem>
struct convert<K3::Libdynamic::IntSet<Elem>> {
  static Node encode(const K3::Libdynamic::IntSet<Elem>& c) {
    Node node;
    if (c.size(K3::unit_t{}) > 0) {
      for (auto& i : c) {
        node.push_back(convert<Elem>::encode(i));
      }
    } else {
      node = YAML::Load("[]");
    }
    return node;
  }

  static bool decode(const Node& node, K3::Libdynamic::IntSet<Elem>& c) {
    for (auto& i : node) {
      c.insert(i.as<Elem>());
    }
    return true;
  }
};
}  // namespace YAML

namespace JSON {

using namespace rapidjson;
template <class E>
struct convert<K3::Libdynamic::IntSet<E>> {
  template <class Allocator>
  static Value encode(const K3::Libdynamic::IntSet<E>& c, Allocator& al) {
    Value v;
    v.SetObject();
    v.AddMember("type", Value("IntSet"), al);
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

// Map selection.
#if USE_CUSTOM_HASHMAPS && HAS_LIBDYNAMIC

namespace K3 {
template <class Elem>
using IntSet = Libdynamic::IntSet<Elem>;
}

#else

namespace K3 {
template <class Elem>
using IntSet = Set<Elem>;
}

#endif
#endif
