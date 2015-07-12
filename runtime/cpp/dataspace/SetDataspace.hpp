// Custom map implementations for K3
#ifndef __K3_RUNTIME_SET_DATASPACE__
#define __K3_RUNTIME_SET_DATASPACE__

#include <iterator>
#include <set>

#include <functional>
#include <stdexcept>

#include <boost/serialization/array.hpp>
#include <boost/serialization/binary_object.hpp>
#include <boost/serialization/string.hpp>
#include <csvpp/csv.h>

namespace K3 {

template <class Elem>
class SortedSet {
  // Iterator Types:
  typedef std::set<Elem> Container;
  typedef typename Container::const_iterator const_iterator_type;
  typedef typename Container::iterator iterator_type;

  public:
  // Expose the element type with a public typedef  ( needed in ext() ):
  typedef Elem ElemType;

  // Constructors:
  // Default Constructor
  SortedSet(): container() {}

  // Copy Constructor from container
  SortedSet(const Container& con): container(con) {}

  // Move Constructor from container
  SortedSet(Container&& con): container(std::move(con)) {}

  // Construct from (container) iterators
  template<typename Iterator>
  SortedSet(Iterator begin, Iterator end): container(begin,end) {}

  Elem elemToRecord(const Elem& e) const { return e; }

  using iterator = typename Container::iterator;
  using const_iterator = typename Container::const_iterator;

  iterator begin() {
    return iterator(container.begin());
  }

  iterator end() {
    return iterator(container.end());
  }

  const_iterator begin() const {
    return const_iterator(container.cbegin());
  }

  const_iterator end() const {
    return const_iterator(container.cend());
  }

  shared_ptr<Elem> peek(unit_t) const {
    shared_ptr<Elem> res(nullptr);
    const_iterator_type it = container.begin();
    if (it != container.end()) {
      res = std::make_shared<Elem>(*it);
    }
    return res;
  }

  template<typename F, typename G>
  auto peek_with(F f, G g) const {
    auto it = container.begin();
    if (it == container.end()) {
      return f(unit_t {});
    } else {
      return g(*it);
    }
  }

  template <class T>
  unit_t insert(T &&e) {
    container.insert(std::forward<T>(e));
    return unit_t();
  }

  unit_t erase(const Elem& v) {
    container.erase(v);
    return unit_t();
  }

  template <class T>
  unit_t update(const Elem& v, T&& v2) {
    container.erase(v);
    container.insert(std::forward<T>(v2));
    return unit_t();
  }

  // Return the number of elements
  int size(const unit_t&) const {
    return container.size();
  }

  SortedSet<Elem> combine(Set<Elem> other) const {
    // copy this DS
    SortedSet<Elem> result(*this);
    // move insert each element of the other.
    // this is okay since we are working with a copy
    for (auto&& e : other.container) {
      result.insert(std::move(e));
    }
    return result;
  }

  // Split the set at its midpoint
  tuple<SortedSet<Elem>, SortedSet<Elem>> split(const unit_t&) const {
    // Find midpoint
    const size_t size = container.size();
    const size_t half = size / 2;
    // Setup iterators
    const_iterator_type beg = container.begin();
    const_iterator_type mid = container.begin();
    std::advance(mid, half);
    const_iterator_type end = container.end();
    // Construct from iterators
    return std::make_tuple(SortedSet(beg,mid), SortedSet(mid,end));
  }

  // Apply a function to each element of this ds
  template<typename Fun>
  unit_t iterate(Fun f) const {
    for (const Elem& e : container) {
      f(e);
    }
    return unit_t();
  }

  // Produce a new set by mapping a function over this set
  template<typename Fun>
  auto map(Fun f) const -> SortedSet<R_elem<RT<Fun, Elem>>>  {
    SortedSet<Elem> result;
    for (const Elem &e : container) {
      result.insert(R_elem<RT<Fun, Elem>>{f(e)}); // Copies e (f is pass by value), then move inserts
    }
    return result;
  }

  // Create a new set consisting of elements from this set that satisfy the predicate
  template<typename Fun>
  SortedSet<Elem> filter(Fun predicate) const {
    SortedSet<Elem> result;
    for (const Elem &e : container) {
      if (predicate(e)) {
        result.insert(e);
      }
    }
    return result;
  }

  // Fold a function over this ds
  template<typename Fun, typename Acc>
  Acc fold(Fun f, Acc acc) const {
    for (const Elem &e : container) { acc = f(std::move(acc))(e); }
    return acc;
  }

  // Group By
  template<typename F1, typename F2, typename Z>
  SortedSet<R_key_value<RT<F1, Elem>,Z>> groupBy(F1 grouper, F2 folder, const Z& init) const {
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

    // Build the R_key_value records and insert them into result
    SortedSet<R_key_value<RT<F1, Elem>,Z>> result;
    for(auto& it : accs) {
      // move out of the map as we iterate
      result.insert(R_key_value<K, Z>{std::move(it.first), std::move(it.second)});
    }
    return result;
  }

  template <class G, class F, class Z>
  SortedSet<R_key_value<RT<G, Elem>, Z>> groupByContiguous(G grouper, F folder, const Z& zero, const int& size) const {
    auto table = std::vector<Z>(size, zero);
    for (const auto& elem: container) {
      auto key = grouper(elem);
      table[key] = folder(std::move(table[key]))(elem);
    }
    // Build the R_key_value records and insert them into result
    SortedSet<R_key_value<RT<G, Elem>,Z>> result;
    for (auto i = 0; i < table.size(); ++i) {
      // move out of the map as we iterate
      result.insert(R_key_value<int, Z>{i, std::move(table[i])});
    }
    return result;
  }

  template <class Fun>
  auto ext(Fun expand) const -> SortedSet<typename RT<Fun, Elem>::ElemType> {
    typedef typename RT<Fun, Elem>::ElemType T;
    SortedSet<T> result;
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

  bool operator==(const Set<Elem>& other) const {
    return container == other.container;
  }

  bool operator!=(const Set<Elem>& other) const {
    return container != other.container;
  }

  bool operator<(const Set<Elem>& other) const {
    return container < other.container;
  }

  bool operator>(const Set<Elem>& other) const {
    return container < other.container;
  }

  // Set specific functions
  bool member(const Elem& e) const {
    return container.find(e) != container.end();
  }

  bool isSubsetOf(const SortedSet<Elem>& other) const {
    for (const auto &x : getConstContainer()) {
      if (!other.member(x)) { return false; }
    }
    return true;
  }

  // union is a reserved word
  SortedSet<Elem> union1(const SortedSet<Elem>& other) const {
    SortedSet<Elem> result;
    for (const auto &x : getConstContainer())       { result.insert(x); }
    for (const auto &x : other.getConstContainer()) { result.insert(x); }
    return result;
  }

  SortedSet<Elem> intersect(const SortedSet<Elem>& other) const {
    SortedSet<Elem> result;
    for (const auto &x : getConstContainer()) {
      if(other.member(x)) { result.insert(x); }
    }
    return result;
  }

  SortedSet<Elem> difference(const SortedSet<Elem>& other) const {
    SortedSet<Elem> result;
    for (const auto &x : getConstContainer()) {
      if(!other.member(x)) { result.insert(x); }
    }
    return result;
  }

  // Sort-based retrieval.
  std::shared_ptr<Elem> min() const {
     // begin is the smallest element
     const auto& x = getConstContainer();
     auto it = x.begin();
     std::shared_ptr<Elem> result(nullptr);
     if (it != x.end()) {
       result = std::make_shared<Elem>(*it);
     }
     return result;
  }

  std::shared_ptr<Elem> max() const {
     const auto& x = getConstContainer();
     auto it = x.rbegin();
     std::shared_ptr<Elem> result(nullptr);
     if (it != x.rend()) {
      result = std::make_shared<Elem>(*it);
     }
     return result;
  }

  template <class F>
  unit_t min_with(F f) const {
    auto it = container.begin();
    if (it != container.end()) {
      return f(*it);
    }

    return unit_t {};
  }

  template <class F>
  unit_t max_with(F f) const {
    auto it = container.rbegin();
    if (it != container.rend()) {
      return f(*it);
    }

    return unit_t {};
  }

  std::shared_ptr<Elem> lower_bound(const Elem& e) const {
    const auto& x = getConstContainer();
    auto it = x.lower_bound(e);
    std::shared_ptr<Elem> result(nullptr);
    if (it != x.end()) {
      result = std::make_shared<Elem>(*it);
    }
    return result;
  }

  std::shared_ptr<Elem> upper_bound(const Elem& e) const {
    const auto& x = getConstContainer();
    auto it = x.upper_bound(e);
    std::shared_ptr<Elem> result(nullptr);
    if (it != x.end()) {
      result = std::make_shared<Elem>(*it);
    }
    return result;
  }

  SortedSet<Elem> filter_lt(const Elem& bound) const {
    const auto& x = getConstContainer();
    SortedSet<Elem> result;
    auto it = x.lower_bound(bound);
    return SortedSet<Elem>(x.begin(), it);
  }

  SortedSet<Elem> filter_gt(const Elem& bound) const {
    const auto& x = getConstContainer();
    auto it = x.upper_bound(bound);
    return SortedSet<Elem>(it, x.end());
  }

  SortedSet<Elem> between(const Elem& a, const Elem& b) const {
    const auto& x = getConstContainer();
    auto it = x.lower_bound(a);
    auto end = x.upper_bound(b);
    if ( it != x.end() ){
      return SortedSet<Elem>(it, end);
    } else {
      return SortedSet<Elem>();
    }
  }

  // Range-based modification, exclusive of the given element if it exists in the set.

  unit_t erase_prefix(const Elem& bound) {
    auto it = container.lower_bound(bound);
    if (it != container.end()) {
      container.erase(container.cbegin(), it);
    }
    return unit_t();
  }

  unit_t erase_suffix(const Elem& bound) {
    auto it = container.upper_bound(bound);
    if (it != container.end()) {
      container.erase(it, container.cend());
    }
    return unit_t();
  }

  Container& getContainer() { return container; }

  // Return a constant reference to the container
  const Container& getConstContainer() const { return container; }

  Container container;

  template<class Archive>
  void serialize(Archive &ar) { ar & container; }

  template<class Archive>
  void serialize(Archive &ar, const unsigned int) {
    ar & boost::serialization::make_nvp("__K3SortedSet", container);
  }

 private:
  friend class boost::serialization::access;
}; // class Set

} // Namespace K3

namespace boost {
  namespace serialization {
    template <class _T0>
    class implementation_level<K3::SortedSet<_T0>> {
      public:
        typedef  mpl::integral_c_tag tag;
        typedef  mpl::int_<object_serializable> type;
        BOOST_STATIC_CONSTANT(int, value = implementation_level::type::value);
    };
  }
}

template <class Elem>
std::size_t hash_value(K3::SortedSet<Elem> const& b) {
  return hash_collection(b);
}

namespace JSON {
  using namespace rapidjson;
  template <class E>
  struct convert<K3::SortedSet<E>> {
    template <class Allocator>
    static Value encode(const K3::SortedSet<E>& c, Allocator& al) {
     Value v;
     v.SetObject();
     v.AddMember("type", Value("SortedSet"), al);
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
  template <class E>
  struct convert<K3::SortedSet<E>> {
    static Node encode(const K3::SortedSet<E>& c) {
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

    static bool decode(const Node& node, K3::SortedSet<E>& c) {
      for (auto& i: node) { c.insert(i.as<E>()); }
      return true;
    }
  };
}


#endif