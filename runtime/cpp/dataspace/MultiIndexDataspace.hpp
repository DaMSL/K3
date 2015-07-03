#ifndef __K3_RUNTIME_MULTIINDEX_DATASPACE__
#define __K3_RUNTIME_MULTIINDEX_DATASPACE__

#include <math.h>
#include <random>

#include <boost/serialization/base_object.hpp>
#include <boost/multi_index_container.hpp>
#include <boost/multi_index/sequenced_index.hpp>
#include <boost/lambda/lambda.hpp>

#include <Common.hpp>
#include <BaseTypes.hpp>
#include <dataspace/Dataspace.hpp>

#include <serialization/json.hpp>
#include <rapidjson/document.h>
#include <rapidjson/writer.h>
#include <rapidjson/stringbuffer.h>
#include <yaml-cpp/yaml.h>

#include <yas/mem_streams.hpp>
#include <yas/binary_iarchive.hpp>
#include <yas/binary_oarchive.hpp>
#include <yas/serializers/std_types_serializers.hpp>
#include <yas/serializers/boost_types_serializers.hpp>

namespace K3 {

// Utility to give the return type of a Function F expecting an Element E as an argument:
template <class F, class E> using RT = decltype(std::declval<F>()(std::declval<E>()));

template <template <typename, typename...> class Derived, typename Elem, typename BaseIndex, typename... Indexes>
class MultiIndexDS {
  public:

  typedef boost::multi_index_container<
    Elem,
    boost::multi_index::indexed_by<BaseIndex, Indexes...>
  > Container;

  Container container;

  // Default
  MultiIndexDS(): container() {}
  MultiIndexDS(const Container& con): container(con) {}
  MultiIndexDS(Container&& con): container(std::move(con)) {}

  // Construct from (container) iterators
  template<typename Iterator>
  MultiIndexDS(Iterator begin, Iterator end): container(begin,end) {}

  Elem elemToRecord(const Elem& e) const { return e; }

  // Maybe return the first element in the ds
  shared_ptr<Elem> peek(unit_t) const {
    shared_ptr<Elem> res(nullptr);
    auto it = container.begin();
    if (it != container.end()) {
      res = std::make_shared<Elem>(*it);
    }
    return res;
  }

   // Insert by move
  unit_t insert(Elem &&e) {
    container.insert(container.end(), std::move(e));
    return unit_t();
  }

  // Insert by copy
  unit_t insert(const Elem& e) {
    // Create a copy, then delegate to a insert-by-move
    return insert(Elem(e));
  }

  // If v is found in the container, proxy a call to erase on the container.
  // Behavior depends on the container's erase implementation
  unit_t erase(const Elem& v) {
    auto it = std::find(container.begin(), container.end(), v);
    if (it != container.end()) {
      container.erase(it);
    }
    return unit_t();
  }

  // Update by move
  // Find v in the container. Insert (by move) v2 in its position. Erase v.
  unit_t update(const Elem& v, Elem&& v2) {
    auto it = std::find(container.begin(), container.end(), v);
    if (it != container.end()) {
      *it = std::forward<Elem>(v2);
    }
    return unit_t();
  }

  // Return the number of elements in this ds
  int size(const unit_t&) const {
    return container.size();
  }

  // Return a new DS with data from this and other
  Derived<Elem, Indexes...> combine(const MultiIndexDS& other) const {
    // copy this DS
    Derived<Elem, Indexes...> result;
    result = MultiIndexDS(*this);
    // copy other DS
    for (const Elem &e : other.container) {
      result.insert(e);
    }
    return result;
  }

  // Split the ds at its midpoint
  tuple<Derived<Elem, Indexes...>, Derived<Elem, Indexes...>> split(const unit_t&) const {
    // Find midpoint
    const size_t size = container.size();
    const size_t half = size / 2;
    // Setup iterators
    auto beg = container.begin();
    auto mid = container.begin();
    std::advance(mid, half);
    auto end = container.end();
    // Construct from iterators
    return std::make_tuple(Derived<Elem, Indexes...>(MultiIndexDS(beg,mid)),
                           Derived<Elem, Indexes...>(MultiIndexDS(mid,end)));
  }

  // Apply a function to each element of this ds
  template<typename Fun>
  unit_t iterate(Fun f) const {
    for (const Elem& e : container) {
      f(e);
    }
    return unit_t();
  }

  // Produce a new ds by mapping a function over this ds
  template<typename Fun>
  auto map(Fun f) const -> Derived<R_elem<RT<Fun, Elem>>> const {
    Derived<R_elem<RT<Fun, Elem>>> result;
    for (const Elem &e : container) {
      result.insert( R_elem<RT<Fun, Elem>>{ f(e) } ); // Copies e (f is pass by value), then move inserts
    }
    return result;
  }

  // Create a new DS consisting of elements from this ds that satisfy the predicate
  template<typename Fun>
  Derived<Elem, Indexes...> filter(Fun predicate) const {
    Derived<Elem, Indexes...> result;
    for (const Elem &e : container) {
      if (predicate(e)) {
        result.insert(e);
      }
    }
    return result;
  }

  // Fold a function over this ds
  template<typename Fun, typename Acc>
  Acc fold(Fun f, const Acc& init_acc) const {
    Acc acc = init_acc;
    for (const Elem &e : container) { acc = f(std::move(acc))(e); }
    return acc;
  }

  // Group By
  template<typename F1, typename F2, typename Z>
  Derived<R_key_value<RT<F1, Elem>,Z>> groupBy(F1 grouper, F2 folder, const Z& init) const {
    // Create a map to hold partial results
    typedef RT<F1, Elem> K;
    unordered_map<K, Z> accs;

    for (const auto& elem : container) {
       K key = grouper(elem);
       if (accs.find(key) == accs.end()) {
         accs[key] = init;
       }
       accs[key] = folder(std::move(accs[key]))(elem);
    }

    // Build the R_key_value records and insert them into resul
    Derived<R_key_value<RT<F1, Elem>,Z>> result;
    for(const auto& it : accs) {
      result.insert(R_key_value<K, Z>{std::move(it.first), std::move(it.second)});
    }

    return result;
  }

  template <class Fun>
  auto ext(Fun expand) -> Derived<typename RT<Fun, Elem>::ElemType> const {
    typedef typename RT<Fun, Elem>::ElemType T;
    Derived<T> result;
    for (const Elem& elem : container) {
      for (T& elem2 : expand(elem).container) {
        result.insert(std::move(elem2));
      }
    }

    return result;
  }

  // Accumulate over a sampled ds.
  // This number of items accessed depends on the iterator implementation, via std::advance.
  template<typename Fun, typename Acc>
  Acc sample(Fun f, Acc acc, size_t sampleSize) const {
    auto it = container.begin();
    DSSampler seqSampler(container.size(), sampleSize);

    tuple<bool, size_t> next_skip = seqSampler.next();
    while ( it != container.end() && std::get<0>(next_skip) ) {
      std::advance(it, std::get<1>(next_skip));
      if ( it != container.end() ) {
        acc = f(std::move(acc))(*it);
        next_skip = seqSampler.next();
      }
    }

    return acc;
  }

  bool operator==(const MultiIndexDS& other) const {
    return container == other.container;
  }

  bool operator!=(const MultiIndexDS& other) const {
    return container != other.container;
  }

  bool operator<(const MultiIndexDS& other) const {
    return container < other.container;
  }

  bool operator>(const MultiIndexDS& other) const {
    return container > other.container;
  }

  template <class Index, class Key>
  shared_ptr<Elem> lookup_with_index(const Index& index, Key key) const {
    const auto& it = index.find(key);
    shared_ptr<Elem> result;
    if (it != index.end()) {
      result = make_shared<Elem>(*it);
    }
    return result;
  }

  template <class Index, class Key>
  Derived<Elem, Indexes...> slice_with_index(const Index& index, Key key) const {
    Derived<Elem, Indexes...> result;
    std::pair<typename Index::iterator, typename Index::iterator> p = index.equal_range(key);
    for (typename Index::iterator it = p.first; it != p.second; it++) {
      result.insert(*it);
    }
    return result;
  }

  template <class Index, class Key>
  Derived<Elem, Indexes...> range_with_index(const Index& index, Key a, Key b) const {
    Derived<Elem, Indexes...> result;
    std::pair<typename Index::iterator, typename Index::iterator> p =
      index.range(a <= boost::lambda::_1, b >= boost::lambda::_1);
    for (typename Index::iterator it = p.first; it != p.second; it++) {
      result.insert(*it);
    }
    return result;
  }

  Container& getContainer() { return container; }

  // Return a constant reference to the container
  const Container& getConstContainer() const {return container;}

  template<class Archive>
  void serialize(Archive &ar) const {
    ar.write(container.size());
    for (const auto& it : container) {
      ar & it;
    }
  }

  template<class Archive>
  void serialize(Archive &ar) {
    size_t sz = 0;
    ar.read(sz);
    while ( sz > 0 ) {
      Elem e;
      ar & e;
      insert(std::move(e));
      sz--;
    }
  }

  template<class Archive>
  void serialize(Archive &ar, const unsigned int) {
    ar & boost::serialization::make_nvp("__K3MultiIndexDS", container);
  }

 private:
  friend class boost::serialization::access;

};


template <typename Elem, typename... Indexes>
class MultiIndexBag : public MultiIndexDS<K3::MultiIndexBag, Elem, boost::multi_index::sequenced<>, Indexes...> {
  using Super = MultiIndexDS<K3::MultiIndexBag, Elem, boost::multi_index::sequenced<>, Indexes...>;

  public:
  MultiIndexBag(): Super() {}
  MultiIndexBag(const Super& c): Super(c) { }
  MultiIndexBag(Super&& c): Super(std::move(c)) { }

  template<class Archive>
  void serialize(Archive &ar) {
    ar & yas::base_object<Super>(*this);
  }

  template<class Archive>
  void serialize(Archive &ar, const unsigned int) {
    ar &  boost::serialization::make_nvp("__K3MultiIndexBag",
            boost::serialization::base_object<Super>(*this));
  }

 private:
  friend class boost::serialization::access;
};


template<typename R, typename Key>
using HashUniqueIndex = boost::multi_index::hashed_unique<boost::multi_index::member<R,Key,&R::key>>;

template <typename R, typename... Indexes>
class MultiIndexMap : public MultiIndexDS<K3::MultiIndexMap, R, HashUniqueIndex<R, typename R::KeyType>, Indexes...> {
  using Key   = typename R::KeyType;
  using Super = MultiIndexDS<K3::MultiIndexMap, R, HashUniqueIndex<R, Key>, Indexes...>;

  public:
  MultiIndexMap(): Super() {}
  MultiIndexMap(const Super& c): Super(c) { }
  MultiIndexMap(Super&& c): Super(std::move(c)) { }

  // Override MultiIndexDS::insert to omit the end insertion hint.
  unit_t insert(R &&rec) {
    auto& c = Super::getContainer();
    auto inserted = c.emplace(std::move(rec));
    if (!inserted.second) {
      c.replace(inserted.first, std::move(rec));
    }
    return unit_t();
  }

  // Insert by copy
  unit_t insert(const R& rec) {
    // Create a copy, then delegate to a insert-by-move
    return insert(R(rec));
  }

  template <class F>
  unit_t insert_with(const R& rec, F f) {
    auto& c = Super::getContainer();
    auto existing = c.find(rec.key);
    if (existing == std::end(c)) {
      c.insert(rec);
    } else {
      c.replace(existing, f(std::move(existing->second))(rec));
    }
    return unit_t {};
  }

  template <class F, class G>
  unit_t upsert_with(const R& rec, F f, G g) {
    auto& c = Super::getContainer();
    auto existing = c.find(rec.key);
    if (existing == std::end(c)) {
      c.insert(f(unit_t {}));
    } else {
      c.replace(existing, g(std::move(existing->second)));
    }
    return unit_t {};
  }

  // Override MultiIndexDS::erase to use the base hash index.
  unit_t erase(const R& rec) {
    auto& c = Super::getContainer();
    auto it = c.find(rec.key);
    if (it != c.end()) {
      c.erase(it);
    }
    return unit_t();
  }

  // Override MultiIndexDS::update to use the base hash index.
  unit_t update(const R& rec1, const R& rec2) {
    auto& c = Super::getContainer();
    auto it = c.find(rec1.key);
    if (it != c.end()) {
      c.replace(it, rec2);
    }
    return unit_t();
  }

  bool member(const R& r) const {
    auto& c = Super::getConstContainer();
    return c.find(r.key) != c.end();
  }

  shared_ptr<R> lookup(const R& r) const {
    auto& c = Super::getConstContainer();
    auto it = c.find(r.key);
    if (it != c.end()) {
      return std::make_shared<R>(it->second);
    } else {
      return nullptr;
    }
  }

  template <class F>
  unit_t lookup_with(R const& r, F f) const {
    auto& c = Super::getConstContainer();
    auto it = c.find(r.key);
    if (it != c.end()) {
      return f(it->second);
    }
    return unit_t {};
  }

  template <class F, class G>
  auto lookup_with2(R const& r, F f, G g) const {
    auto& c = Super::getConstContainer();
    auto it = c.find(r.key);
    if (it == c.end()) {
      return f(unit_t {});
    } else {
      return g(it->second);
    }
  }

  template <class F>
  auto lookup_with3(R const& r, F f) const {
    auto& c = Super::getConstContainer();
    auto it = c.find(r.key);
    if (it != c.end()) {
      return f(it->second);
    }
    throw std::runtime_error("No match on Map.lookup_with3");
  }

  template<class Archive>
  void serialize(Archive &ar) {
    ar & yas::base_object<Super>(*this);
  }

  template<class Archive>
  void serialize(Archive &ar, const unsigned int) {
    ar &  boost::serialization::make_nvp("__K3MultiIndexMap",
            boost::serialization::base_object<Super>(*this));
  }

 private:
  friend class boost::serialization::access;
};

} // Namespace K3

namespace boost {
  namespace serialization {
    template <class _T0, class... _T1>
    class implementation_level<K3::MultiIndexBag<_T0, _T1...>> {
      public:
        typedef  mpl::integral_c_tag tag;
        typedef  mpl::int_<object_serializable> type;
        BOOST_STATIC_CONSTANT(int, value = implementation_level::type::value);
    };

    template <class _T0, class... _T1>
    class implementation_level<K3::MultiIndexMap<_T0, _T1...>> {
      public:
        typedef  mpl::integral_c_tag tag;
        typedef  mpl::int_<object_serializable> type;
        BOOST_STATIC_CONSTANT(int, value = implementation_level::type::value);
    };
  }
}

template <typename... Args>
std::size_t hash_value(K3::MultiIndexBag<Args...> const& b) {
  return hash_collection(b);
}

template <typename... Args>
std::size_t hash_value(K3::MultiIndexMap<Args...> const& b) {
  return hash_collection(b);
}

namespace JSON {
  using namespace rapidjson;

  template <class E, typename... Indexes>
  struct convert<K3::MultiIndexBag<E, Indexes...>> {
    template <class Allocator>
    static Value encode(const K3::MultiIndexBag<E, Indexes...>& c, Allocator& al) {
     Value v;
     v.SetObject();
     v.AddMember("type", Value("MultiIndexBag"), al);
     Value inner;
     inner.SetArray();
     for (const auto& e : c.getConstContainer()) {
       inner.PushBack(convert<E>::encode(e, al), al);
     }
     v.AddMember("value", inner.Move(), al);
     return v;
    }
  };

  template <class E, typename... Indexes>
  struct convert<K3::MultiIndexMap<E, Indexes...>> {
    template <class Allocator>
    static Value encode(const K3::MultiIndexMap<E, Indexes...>& c, Allocator& al) {
     Value v;
     v.SetObject();
     v.AddMember("type", Value("MultiIndexMap"), al);
     Value inner;
     inner.SetArray();
     for (const auto& e : c.getConstContainer()) {
       inner.PushBack(convert<E>::encode(e, al), al);
     }
     v.AddMember("value", inner.Move(), al);
     return v;
    }
  };

}


namespace YAML {
  template <class E, typename... Indexes>
  struct convert<K3::MultiIndexBag<E, Indexes...>> {
    static Node encode(const K3::MultiIndexBag<E, Indexes...>& c) {
      Node node;
      for (auto i: c.getConstContainer()) {
        node.push_back(convert<E>::encode(i));
      }
      return node;
    }

    static bool decode(const Node& node, K3::MultiIndexBag<E, Indexes...>& c) {
      for (auto& i: node) {
        c.insert(i.as<E>());
      }
      return true;
    }
  };

  template <class E, typename... Indexes>
  struct convert<K3::MultiIndexMap<E, Indexes...>> {
    static Node encode(const K3::MultiIndexMap<E, Indexes...>& c) {
      Node node;
      auto container = c.getConstContainer();
      if (container.size() > 0) {
        for (auto i: container) {
          node.push_back(convert<E>::encode(i));
        }
      } else {
        node = YAML::Load("[]");
      }
      return node;
    }

    static bool decode(const Node& node, K3::MultiIndexMap<E, Indexes...>& c) {
      for (auto& i: node) {
        c.insert(i.as<E>());
      }
      return true;
    }
  };

}

#endif
