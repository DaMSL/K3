#ifndef __K3_RUNTIME_DATASPACE__
#define __K3_RUNTIME_DATASPACE__

#include <list>
#include <vector>
#include <random>
#include <math.h>
#include <random>

#include <boost/tr1/unordered_set.hpp>
#include <boost/tr1/unordered_map.hpp>
#include "boost/serialization/vector.hpp"
#include "boost/serialization/set.hpp"
#include "boost/serialization/list.hpp"
#include "boost_ext/unordered_map.hpp"
#include <boost/serialization/base_object.hpp>
#include <boost/multi_index_container.hpp>
#include <boost/multi_index/sequenced_index.hpp>
#include <boost/lambda/lambda.hpp>

#include "Common.hpp"
#include "BaseTypes.hpp"
#include "yaml-cpp/yaml.h"

#include "serialization/json.hpp"
#include "rapidjson/document.h"
#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"


namespace K3 {

// Utility to give the return type of a Function F expecting an Element E as an argument:
template <class F, class E> using RT = decltype(std::declval<F>()(std::declval<E>()));

// Utility Typedefs
template<class K, class V>
using unordered_map = std::tr1::unordered_map<K,V>;

template<class K>
using unordered_set = std::tr1::unordered_set<K>;

// Sequential sampling with Vitter's Algorithm D:
// Jeffrey Scott Vitter: Faster Methods for Random Sampling. Commun. ACM 27(7): 703-718 (1984)
//
// Our implementation is based on:
// https://github.com/WebDrake/GrSL/blob/master/sampling/vitter.c
class DSSampler {
public:
  DSSampler(size_t populationSize, size_t sampleSize) : eng(rd()) {
    reset(populationSize, sampleSize);
  }

  void reset(size_t populationSize, size_t sampleSize) {
    N = populationSize;
    n = sampleSize;
    if ( !(use_a = (alpha_inv * n > N)) ) {
      vprime = vprimeF(n);
      q1 = N - n + 1;
      q2 = static_cast<double>(q1) / N;
      threshold = alpha_inv * n;
    }
  }

  tuple<bool, size_t> next()
  {
    tuple<bool, size_t> result = make_tuple(false, 0);

    if ( use_a ) {
      result = std::move(next_A());
    }
    else if ( n > 1 && threshold < N ) {
      size_t S, top, t, limit;
      double X, y, bottom;

      while ( true ) {
        /* Step D2 */
        for(X = N * (1 - vprime), S = std::trunc(X); S >= q1; ) {
          vprime = vprimeF(n);
          X = N * (1 - vprime); S = std::trunc(X);
        }

        y = std::generate_canonical<double,64>(eng) / q2;
        vprime = std::pow(y, 1.0/(n-1)) * ((-X/N) + 1.0) * (q1/static_cast<double>(q1 - S));

        /* Step D3 */
        if ( vprime > 1.0 ) {
          if ( n - 1 > S ) {
            bottom = N - n;
            limit = N - S;
          } else {
            bottom = N - S - 1;
            limit = q1;
          }

          for ( top = N - 1; top >= limit; --top, --bottom) {
            y *= top/bottom;
          }

          /* Step D4 */
          if( N / (N - X) < std::pow(y, 1.0/(n - 1)) ) {
            vprime = vprimeF(n);
          } else {
            vprime = vprimeF(n - 1);
            result = make_tuple(true, S);
            break;
          }
        } else {
          result = make_tuple(true, S);
          break;
        }
      } // End inner while

      /* Step D5 */
      N = N - S - 1;
      --n;
      q1 = q1 - S;
      q2 = static_cast<double>(q1) / N;
      threshold -= alpha_inv;
    }
    else if ( n > 1 ) {
      use_a = true;
      result = std::move(next_A());
    }
    else if ( n == 1 ) {
      result = make_tuple(true, std::trunc(N * vprime));
      --n;
    }

    return result;
  }

private:
  size_t N;
  size_t n;

  // Seed with a real random value, if available
  std::random_device rd;
  std::default_random_engine eng;

  bool use_a;

  // Variables for Algorithm D
  const short alpha_inv = 13;
  double vprime;
  size_t q1;
  double q2;
  double threshold;

  double vprimeF(size_t n) { return std::pow(std::generate_canonical<double,64>(eng), 1.0/n); }

  // Algorithm A helper.
  tuple<bool, size_t> next_A() {
    tuple<bool, size_t> result = make_tuple(false, 0);
    if ( n == 0 ) { return result; }

    size_t S = 0;
    double V, quot, top;

    if (n == 1) {
      std::uniform_int_distribution<size_t> unif(0,N);
      S = unif(eng);
    } else if ( n >= 2 ) {
      top = N - n;
      quot = top/N;
      V = std::generate_canonical<double, 64>(eng);

      while (quot > V) {
        ++S;
        quot *= (top - S) / (N - S);
      }
    }

    if ( S > 0 ) {
      result = make_tuple(true, S);
      N = N - S - 1;
      --n;
    }

    return result;
  }
};


// StlDS provides the basic Collection transformers via generic implementations
// that should work with any STL container.
template <template <typename> class Derived, template<typename...> class StlContainer, class Elem>
class StlDS {
  // The underlying STL container type backing this dataspace:
  typedef StlContainer<Elem> Container;
  // Iterator Types:
  typedef typename Container::const_iterator const_iterator_type;
  typedef typename Container::iterator iterator_type;

  public:
  // Expose the element type with a public typedef  ( needed in ext() ):
  typedef Elem ElemType;

  // Constructors:
  // Default Constructor
  StlDS(): container() {}

  // Copy Constructor from container
  StlDS(const Container& con): container(con) {}

  // Move Constructor from container
  StlDS(Container&& con): container(std::move(con)) {}

  // Construct from (container) iterators
  template<typename Iterator>
  StlDS(Iterator begin, Iterator end): container(begin,end) {}

  Elem elemToRecord(const Elem& e) const { return e; }

  // Maybe return the first element in the ds
  shared_ptr<Elem> peek(unit_t) const {
    shared_ptr<Elem> res(nullptr);
    const_iterator_type it = container.begin();
    if (it != container.end()) {
      res = std::make_shared<Elem>(*it);
    }
    return res;
  }

  template <class T>
  unit_t insert(T &&e) {
    container.insert(container.end(), std::forward<T>(e));
    return unit_t();
  }

  // If v is found in the container, proxy a call to erase on the container.
  // Behavior depends on the container's erase implementation
  unit_t erase(const Elem& v) {
    iterator_type it;
    it = std::find(container.begin(), container.end(), v);
    if (it != container.end()) {
      container.erase(it);
    }
    return unit_t();
  }

  template <class T>
  unit_t update(const Elem& v, T&& v2) {
    iterator_type it;
    it = std::find(container.begin(), container.end(), v);
    if (it != container.end()) {
      *it = std::forward<T>(v2);
    }
    return unit_t();
  }

  // Return the number of elements in this ds
  int size(const unit_t&) const { return container.size(); }

  using iterator = typename Container::iterator;
  using const_iterator = typename Container::const_iterator;
  using reverse_iterator = typename Container::reverse_iterator;

  // Iterators
  iterator begin() {
    return container.begin();
  }

  iterator end() {
    return container.end();
  }

  const_iterator begin() const {
    return container.cbegin();
  }

  const_iterator end() const {
    return container.cend();
  }

  const_iterator cbegin() const {
    return container.cbegin();
  }

  const_iterator cend() const {
    return container.cend();
  }

  reverse_iterator rbegin() {
    return container.rbegin();
  }

  reverse_iterator rend() {
    return container.rend();
  }

  // Return a new DS with data from this and other
  Derived<Elem> combine(StlDS other) const {
    // copy this DS
    Derived<Elem> result;
    result = StlDS(*this);
    // move insert each element of the other.
    // this is okay since we are working with a copy
    for (Elem &e : other.container) {
      result.insert(std::move(e));
    }
    return result;
  }

  // Split the ds at its midpoint
  tuple<Derived<Elem>, Derived<Elem>> split(const unit_t&) const {
    // Find midpoint
    const size_t size = container.size();
    const size_t half = size / 2;
    // Setup iterators
    const_iterator_type beg = container.begin();
    const_iterator_type mid = container.begin();
    std::advance(mid, half);
    const_iterator_type end = container.end();
    // Construct from iterators
    return std::make_tuple(Derived<Elem>(StlDS(beg,mid)), Derived<Elem>(StlDS(mid,end)));
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
  auto map(Fun f) const -> Derived<R_elem<RT<Fun, Elem>>>  const {
    Derived<R_elem<RT<Fun, Elem>>> result;
    for (const Elem &e : container) {
      result.insert( R_elem<RT<Fun, Elem>>{ f(e) } ); // Copies e (f is pass by value), then move inserts
    }
    return result;
  }

  // Create a new DS consisting of elements from this ds that satisfy the predicate
  template<typename Fun>
  Derived<Elem> filter(Fun predicate) const {
    Derived<Elem> result;
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

      // Build the R_key_value records and insert them into result
      Derived<R_key_value<RT<F1, Elem>,Z>> result;
      for(auto& it : accs) {
        // move out of the map as we iterate
        result.insert(R_key_value<K, Z>{std::move(it.first), std::move(it.second)});
      }
      return result;
  }

  template <class G, class F, class Z>
  Derived<R_key_value<RT<G, Elem>, Z>> groupByContiguous(G grouper, F folder, const Z& zero, const int& size) const {
    auto table = std::vector<Z>(size, zero);
    for (const auto& elem: container) {
      auto key = grouper(elem);
      table[key] = folder(std::move(table[key]))(elem);
    }

    // Build the R_key_value records and insert them into result
    Derived<R_key_value<RT<G, Elem>,Z>> result;
    for (auto i = 0; i < table.size(); ++i) {
      // move out of the map as we iterate
      result.insert(R_key_value<int, Z>{i, std::move(table[i])});
    }
    return result;
  }

  template <class Fun>
  auto ext(Fun expand) const -> Derived<typename RT<Fun, Elem>::ElemType> {
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
    auto it = begin();
    DSSampler seqSampler(container.size(), sampleSize);

    tuple<bool, size_t> next_skip = seqSampler.next();
    while ( it != end() && std::get<0>(next_skip) ) {
      std::advance(it, std::get<1>(next_skip));
      if ( it != end() ) {
        acc = f(std::move(acc))(*it);
        next_skip = seqSampler.next();
      }
    }

    return acc;
  }

  bool operator==(const StlDS& other) const {
    return container == other.container;
  }

  bool operator!=(const StlDS& other) const {
    return container != other.container;
  }

  bool operator<(const StlDS& other) const {
    return container < other.container;
  }

  bool operator>(const StlDS& other) const {
    return container < other.container;
  }

  Container& getContainer() { return container; }

  // Return a constant reference to the container
  const Container& getConstContainer() const {return container;}

  Container container;

 private:
  friend class boost::serialization::access;
  template<class Archive>
  void serialize(Archive &ar, const unsigned int) {
    ar & container;
  }
};

// Various DS's achieved through typedefs
template <template <typename> class Derived, class Elem>
using ListDS = StlDS<Derived, std::list, Elem>;

template <template <class> class Derived, class Elem>
using VectorDS = StlDS<Derived, std::vector, Elem>;


// The Collection variants inherit functionality from a dataspace.
// Each variant may also add extra functionality.
template <class Elem>
class Collection: public VectorDS<K3::Collection, Elem> {
  using Super = VectorDS<K3::Collection, Elem>;

 public:
  Collection(): Super() {}
  Collection(const Super& c): Super(c) { }
  Collection(Super&& c): Super(std::move(c)) { }

  shared_ptr<Elem> at(int i) const {
    auto& c = Super::getConstContainer();
    if (i < c.size()) {
      return std::make_shared<Elem>(c[i]);
    } else {
      return nullptr;
    }
  }

};

// StlDS provides the basic Collection transformers via generic implementations
// that should work with any STL container.
template <class Elem>
class Set {
  // Iterator Types:
  typedef unordered_set<Elem> Container;
  typedef typename Container::const_iterator const_iterator_type;
  typedef typename Container::iterator iterator_type;

  public:
  // Expose the element type with a public typedef  ( needed in ext() ):
  typedef Elem ElemType;

  // Constructors:
  // Default Constructor
  Set(): container() {}

  // Copy Constructor from container
  Set(const Container& con): container(con) {}

  // Move Constructor from container
  Set(Container&& con): container(std::move(con)) {}

  // Construct from (container) iterators
  template<typename Iterator>
  Set(Iterator begin, Iterator end): container(begin,end) {}

  Elem elemToRecord(const Elem& e) const { return e; }

  shared_ptr<Elem> peek(unit_t) const {
    shared_ptr<Elem> res(nullptr);
    const_iterator_type it = container.begin();
    if (it != container.end()) {
      res = std::make_shared<Elem>(*it);
    }
    return res;
  }

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

  Set<Elem> combine(const Set<Elem> other) const {
    // copy this DS
    Set<Elem> result(*this);
    // move insert each element of the other.
    // this is okay since we are working with a copy
    for (Elem &e : other.container) {
      result.insert(std::move(e));
    }
    return result;
  }

  // Split the set at its midpoint
  tuple<Set<Elem>, Set<Elem>> split(const unit_t&) const {
    // Find midpoint
    const size_t size = container.size();
    const size_t half = size / 2;
    // Setup iterators
    const_iterator_type beg = container.begin();
    const_iterator_type mid = container.begin();
    std::advance(mid, half);
    const_iterator_type end = container.end();
    // Construct from iterators
    return std::make_tuple(Set(beg,mid), Set(mid,end));
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
  auto map(Fun f) const -> Set<R_elem<RT<Fun, Elem>>>  {
    Set<Elem> result;
    for (const Elem &e : container) {
      result.insert(R_elem<RT<Fun, Elem>>{f(e)}); // Copies e (f is pass by value), then move inserts
    }
    return result;
  }

  // Create a new set consisting of elements from this set that satisfy the predicate
  template<typename Fun>
  Set<Elem> filter(Fun predicate) const {
    Set<Elem> result;
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
  Set<R_key_value<RT<F1, Elem>,Z>> groupBy(F1 grouper, F2 folder, const Z& init) const {
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
      Set<R_key_value<RT<F1, Elem>,Z>> result;
      for(auto& it : accs) {
        // move out of the map as we iterate
        result.insert(R_key_value<K, Z>{std::move(it.first), std::move(it.second)});
      }
      return result;
  }

  template <class G, class F, class Z>
  Set<R_key_value<RT<G, Elem>, Z>> groupByContiguous(G grouper, F folder, const Z& zero, const int& size) const {
    auto table = std::vector<Z>(size, zero);
    for (const auto& elem: container) {
      auto key = grouper(elem);
      table[key] = folder(std::move(table[key]))(elem);
    }
    // Build the R_key_value records and insert them into result
    Set<R_key_value<RT<G, Elem>,Z>> result;
    for (auto i = 0; i < table.size(); ++i) {
      // move out of the map as we iterate
      result.insert(R_key_value<int, Z>{i, std::move(table[i])});
    }
    return result;
  }

  template <class Fun>
  auto ext(Fun expand) const -> Set<typename RT<Fun, Elem>::ElemType> {
    typedef typename RT<Fun, Elem>::ElemType T;
    Set<T> result;
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
    auto it = std::find(getConstContainer().begin(), getConstContainer().end(), e);
    return (it != getConstContainer().end());
  }

  bool isSubsetOf(const Set<Elem>& other) const {
    for (const auto &x : getConstContainer()) {
      if (!other.member(x)) { return false; }
    }
    return true;
  }

  // union is a reserved word
  Set<Elem> union1(const Set<Elem>& other) const {
    Set<Elem> result;
    for (const auto &x : getConstContainer())       { result.insert(x); }
    for (const auto &x : other.getConstContainer()) { result.insert(x); }
    return result;
  }

  Set<Elem> intersect(const Set<Elem>& other) const {
    Set<Elem> result;
    for (const auto &x : getConstContainer()) {
      if(other.member(x)) { result.insert(x); }
    }
    return result;
  }

  Set<Elem> difference(const Set<Elem>& other) const {
    Set<Elem> result;
    for (const auto &x : getConstContainer()) {
      if(!other.member(x)) { result.insert(x); }
    }
    return result;
  }

  Container& getContainer() { return container; }

  // Return a constant reference to the container
  const Container& getConstContainer() const {return container;}

  Container container;

 private:
  friend class boost::serialization::access;
  template<class Archive>
  void serialize(Archive &ar, const unsigned int) {
    ar & container;
  }
}; // class Set

template <class Elem>
class Seq : public ListDS<K3::Seq, Elem> {
  using Super = ListDS<K3::Seq, Elem>;
 public:
  Seq(): Super() { }
  Seq(const Super& c): Super(c) { }
  Seq(Super&& c): Super(std::move(c)) { }

  // Seq specific functions
  Seq sort(F<F<int(Elem)>(Elem)> comp) {
    auto& l(Super::getContainer());
    auto f = [&] (Elem& a, Elem& b) mutable {
      return comp(a)(b) < 0;
    };
    l.sort(f);
    // Force Move into ListDS class.
    // Dont reference l again
    return Seq(Super(std::move(l)));
  }

  Elem at(int i) const {
    auto& l = Super::getConstContainer();
    auto it = l.begin();
    std::advance(it, i);
    return *it;
  }

};

// StlDS provides the basic Collection transformers via generic implementations
// that should work with any STL container.
template <class Elem>
class Sorted {
  // Iterator Types:
  typedef std::multiset<Elem> Container;
  using const_iterator_type = typename Container::const_iterator;
  using iterator_type       = typename Container::iterator;

  public:
  // Expose the element type with a public typedef  ( needed in ext() ):
  typedef Elem ElemType;

  // Constructors:
  // Default Constructor
  Sorted(): container() {}

  // Copy Constructor from container
  Sorted(const Container& con): container(con) {}

  // Move Constructor from container
  Sorted(Container&& con): container(std::move(con)) {}

  // Construct from (container) iterators
  template<typename Iterator>
  Sorted(Iterator begin, Iterator end): container(begin,end) {}

  Elem elemToRecord(const Elem& e) const { return e; }

  // Maybe return the first element in the ds
  shared_ptr<Elem> peek(unit_t) const {
    shared_ptr<Elem> res(nullptr);
    const_iterator_type it = container.begin();
    if (it != container.end()) {
      res = std::make_shared<Elem>(*it);
    }
    return res;
  }

   // Insert by move
  template <class T>
  unit_t insert(T &&e) {
    container.insert(std::forward<T>(e));
    return unit_t();
  }

  // If v is found in the container, proxy a call to erase on the container.
  // Behavior depends on the container's erase implementation
  unit_t erase(const Elem& v) {
    container.erase(v);
    return unit_t();
  }

  // Update by move
  template <class T>
  unit_t update(const Elem& v, T&& v2) {
    container.erase(v);
    container.insert(std::forward<T>(v2));
    return unit_t();
  }

  // Return the number of elements in this ds
  int size(const unit_t&) const { return container.size(); }

  Sorted<Elem> combine(Sorted<Elem> other) const {
    // copy this DS
    Sorted<Elem> result;
    result = Sorted<Elem>(*this);
    // move insert each element of the other.
    // this is okay since we are working with a copy
    for (Elem &e : other.container) {
      result.insert(std::move(e));
    }
    return result;
  }

  // Split the ds at its midpoint
  tuple<Sorted<Elem>, Sorted<Elem>> split(const unit_t&) const {
    const size_t midpoint = container.size() / 2;
    const_iterator_type beg = container.begin();
    const_iterator_type mid = container.begin();
    std::advance(mid, midpoint);
    const_iterator_type end = container.end();
    return std::make_tuple(Sorted<Elem>(beg, mid), Sorted<Elem>(mid, end));
  }

  // Apply a function to each element of this ds
  template<typename Fun>
  unit_t iterate(Fun f) const {
    for (const Elem& e : container) { f(e); }
    return unit_t();
  }


  // Produce a new ds by mapping a function over this ds
  template<typename Fun>
  auto map(Fun f) const -> Sorted<R_elem<RT<Fun, Elem>>>  {
    Sorted<Elem> result;
    for (const Elem &e : container) {
      result.insert(R_elem<RT<Fun, Elem>>{f(e)}); // Copies e (f is pass by value), then move inserts
    }
    return result;
  }

  // Create a new DS consisting of elements from this ds that satisfy the predicate
  template<typename Fun>
  Sorted<Elem> filter(Fun predicate) const {
    Sorted<Elem> result;
    for (const Elem &e : container) {
      if (predicate(e)) { result.insert(e); }
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
  Sorted<R_key_value<RT<F1, Elem>,Z>> groupBy(F1 grouper, F2 folder, const Z& init) const {
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
      Sorted<R_key_value<RT<F1, Elem>,Z>> result;
      for(auto& it : accs) {
        // move out of the map as we iterate
        result.insert(R_key_value<K, Z>{std::move(it.first), std::move(it.second)});
      }
      return result;
  }

  template <class G, class F, class Z>
  Sorted<R_key_value<RT<G, Elem>, Z>> groupByContiguous(G grouper, F folder, const Z& zero, const int& size) const {
    auto table = std::vector<Z>(size, zero);
    for (const auto& elem: container) {
      auto key = grouper(elem);
      table[key] = folder(std::move(table[key]))(elem);
    }

    // Build the R_key_value records and insert them into result
    Sorted<R_key_value<RT<G, Elem>,Z>> result;
    for (auto i = 0; i < table.size(); ++i) {
      // move out of the map as we iterate
      result.insert(R_key_value<int, Z>{i, std::move(table[i])});
    }
    return result;
  }

  template <class Fun>
  auto ext(Fun expand) const -> Sorted<typename RT<Fun, Elem>::ElemType> {
    typedef typename RT<Fun, Elem>::ElemType T;
    Sorted<T> result;
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

  bool operator==(const Sorted<Elem>& other) const {
    return container == other.container;
  }

  bool operator!=(const Sorted<Elem>& other) const {
    return container != other.container;
  }

  bool operator<(const Sorted<Elem>& other) const {
    return container < other.container;
  }

  bool operator>(const Sorted<Elem>& other) const {
    return container < other.container;
  }

  // Sorted specific functions
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

  std::shared_ptr<Elem> lowerBound(const Elem& e) const {
    const auto& x = getConstContainer();
    auto it = x.lower_bound(e);
    std::shared_ptr<Elem> result(nullptr);
    if (it != x.end()) {
      result = std::make_shared<Elem>(*it);
    }
    return result;
  }

  std::shared_ptr<Elem> upperBound(const Elem& e) const {
    const auto& x = getConstContainer();
    auto it = x.upper_bound(e);
    std::shared_ptr<Elem> result(nullptr);
    if (it != x.end()) {
      result = std::make_shared<Elem>(*it);
    }
    return result;
  }

  Sorted<Elem> slice(const Elem& a, const Elem& b) const {
    const auto& x = getConstContainer();
    Sorted<Elem> result;
    for (Elem e : x) {
      if (e >= a && e <= b) {
        result.insert(e);
      }
      if (e > b) {
        break;
      }
    }
    return result;
  }

  std::multiset<Elem>& getContainer() { return container; }

  // Return a constant reference to the container
  const std::multiset<Elem>& getConstContainer() const {return container;}

  std::multiset<Elem> container;

 private:
  friend class boost::serialization::access;
  template<class Archive>
  void serialize(Archive &ar, const unsigned int) {
    ar & container;
  }
}; // Class Sorted

// TODO reorder functions to match the others
template<class R>
class Map {
  using Key = typename R::KeyType;

 public:
  // Default Constructor
  Map(): container() {}
  Map(const unordered_map<Key,R>& con): container(con) {}
  Map(unordered_map<Key, R>&& con): container(std::move(con)) {}

  // Construct from (container) iterators
  template<typename Iterator>
  Map(Iterator begin, Iterator end): container(begin,end) {}

  template <class Pair>
  R elemToRecord(const Pair& e) const { return e.second; }

  // DS Operations:
  // Maybe return the first element in the DS
  shared_ptr<R> peek(const unit_t&) const {
    shared_ptr<R> res(nullptr);
    auto it = container.begin();
    if (it != container.end()) {
      res = std::make_shared<R>(it->second);
    }
    return res;
  }

  template <class I>
  class map_iterator: public std::iterator<std::forward_iterator_tag, R> {
    using container = unordered_map<Key, R>;
    using reference = typename std::iterator<std::forward_iterator_tag, R>::reference;
   public:
    template <class _I>
    map_iterator(_I&& _i): i(std::forward<_I>(_i)) {}

    map_iterator& operator ++() {
      ++i;
      return *this;
    }


    map_iterator operator ++(int) {
      map_iterator t = *this;
      *this++;
      return t;
    }

    auto& operator*() const {
      return i->second;
    }


    bool operator ==(const map_iterator& other) const {
      return i == other.i;
    }

    bool operator !=(const map_iterator& other) const {
      return i != other.i;
    }

   private:
    I i;
  };

  using iterator = map_iterator<typename unordered_map<Key, R>::iterator>;
  using const_iterator = map_iterator<typename unordered_map<Key, R>::const_iterator>;

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

  template <class Q>
  unit_t insert(Q&& q) {
    container[q.key] = std::forward<Q>(q);
    return unit_t();
  }

  template <class F>
  unit_t insert_with(const R& rec, F f) {
    auto existing = container.find(rec.key);
    if (existing == std::end(container)) {
      container[rec.key] = rec;
    } else {
      container[rec.key] = f(std::move(existing->second))(rec);
    }

    return unit_t {};
  }

  template <class F, class G>
  unit_t upsert_with(const R& rec, F f, G g) {
    auto existing = container.find(rec.key);
    if (existing == std::end(container)) {
      container[rec.key] = f(unit_t {});
    } else {
      container[rec.key] = g(std::move(existing->second));
    }

    return unit_t {};
  }

  unit_t erase(const R& rec) {
    auto it = container.find(rec.key);
    if (it != container.end()) {
        container.erase(it);
    }
    return unit_t();
  }

  unit_t update(const R& rec1, const R& rec2) {
    auto it = container.find(rec1.key);
    if (it != container.end()) {
        container.erase(it);
        container[rec2.key] = rec2;
    }
    return unit_t();
  }

  template<typename Fun, typename Acc>
  Acc fold(Fun f, Acc acc) const {
    for (const auto& p : container) {
      acc = f(std::move(acc))(p.second);
    }
    return acc;
  }

  template<typename Fun>
  auto map(Fun f) const -> Map< RT<Fun, R> > {
    Map< RT<Fun,R> > result;
    for (const auto& p : container) {
      result.insert( f(p.second) );
    }
    return result;
  }

  template <typename Fun>
  unit_t iterate(Fun f) const {
    for (const auto& p : container) {
      f(p.second);
    }
    return unit_t();
  }

  template <typename Fun>
  Map<R> filter(Fun predicate) const {
    Map<R> result;
    for (const auto& p : container) {
      if (predicate(p.second)) {
        result.insert(p.second);
      }
    }
    return result;
  }

  tuple<Map, Map> split(const unit_t&) const {
    // Find midpoint
    const size_t size = container.size();
    const size_t half = size / 2;
    // Setup iterators
    auto beg = container.begin();
    auto mid = container.begin();
    std::advance(mid, half);
    auto end = container.end();
    // Construct DS from iterators
    return std::make_tuple(Map(beg, mid),Map(mid, end));
  }

  Map combine(const Map& other) const {
    // copy this DS
    Map result = Map(*this);
    // copy other DS
    for (const auto& p: other.container) {
      result.container[p.first] = p.second;
    }
    return result;
  }

  template<typename F1, typename F2, typename Z>
  Map< R_key_value< RT<F1, R>,Z >> groupBy(F1 grouper, F2 folder, const Z& init) const {
    // Create a map to hold partial results
    typedef RT<F1, R> K;
    unordered_map<K, Z> accs;

    for (const auto& it : container) {
      K key = grouper(it.second);
      if (accs.find(key) == accs.end()) {
        accs[key] = init;
      }
      accs[key] = folder(std::move(accs[key]))(it.second);
    }

    // TODO more efficient implementation?
    Map<R_key_value<K,Z>> result;
    for (auto&& it : accs) {
      result.insert(std::move(R_key_value<K, Z> {std::move(it.first), std::move(it.second)}));
    }
    return result;
  }

  template <class Fun>
  auto ext(Fun expand) const -> Map < typename RT<Fun, R>::ElemType >  {
    typedef typename RT<Fun, R>::ElemType T;
    Map<T> result;
    for (const auto& it : container) {
      for (auto& it2 : expand(it.second).container) {
        result.insert(it2.second);
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

  int size(unit_t) const { return container.size(); }

  // lookup ignores the value of the argument
  shared_ptr<R> lookup(const R& r) const {
      auto it = container.find(r.key);
      if (it != container.end()) {
        return std::make_shared<R>(it->second);
      } else {
        return nullptr;
      }
  }

  bool member(const R& r) const {
    return container.find(r.key) != container.end();
  }

  template <class F>
  unit_t lookup_with(R const& r, F f) const {
    auto it = container.find(r.key);
    if (it != container.end()) {
      return f(it->second);
    }

    return unit_t {};
  }


  template <class F, class G>
  auto lookup_with2(R const& r, F f, G g) const {
    auto it = container.find(r.key);
    if (it == container.end()) {
      return f(unit_t {});
    } else {
      return g(it->second);
    }
  }

  bool operator==(const Map& other) const {
    return container == other.container;
  }

  bool operator!=(const Map& other) const {
    return container != other.container;
  }

  bool operator<(const Map& other) const {
    return container < other.container;
  }

  bool operator>(const Map& other) const {
    return container > other.container;
  }

  unordered_map<Key, R>& getContainer() { return container; }

  const unordered_map<Key, R>& getConstContainer() const { return container; }

 protected:
  unordered_map<Key,R> container;

  private:
  friend class boost::serialization::access;
  template<class Archive>
  void serialize(Archive &ar, const unsigned int) {
    ar & container;
  }

}; // class Map

template <class Elem>
class Vector: public VectorDS<K3::Vector, Elem> {
  using Super = VectorDS<K3::Vector, Elem>;

 public:
  Vector(): Super() {}
  Vector(const Super& c): Super(c) {}
  Vector(Super&& c): Super(std::move(c)) {}

  // TODO bounds checking
  Elem at(int i) const {
    auto& vec = Super::getConstContainer();
    return vec[i];
  }

  // TODO bounds checking
  unit_t set(int i, Elem f) {
    auto& vec = Super::getContainer();
    vec[i] = f;
    return unit_t();
  }

  unit_t inPlaceAdd(const Vector<Elem>& other) {
    auto& vec = Super::getContainer();
    auto& other_vec = other.getConstContainer();
    if (vec.size() != other_vec.size()) {
      throw std::runtime_error("Vector inPlaceAdd size mismatch");
    }

    #pragma clang loop vectorize(enable) interleave(enable)
    for (int i =0; i < vec.size(); i++) {
      vec[i].elem += other_vec[i].elem;
    }

    return unit_t {};
  }

  Vector<Elem> add(const Vector<Elem>& other) const {
    auto copy = Vector<Elem>(*this);
    copy.inPlaceAdd(other);
    return copy;
  }

  Vector<Elem> add(Vector<Elem>&& other) const {
    other.inPlaceAdd(*this);
    return other;
  }

  unit_t inPlaceSub(const Vector<Elem>& other) {
    auto& vec = Super::getContainer();
    auto& other_vec = other.getConstContainer();
    if (vec.size() != other_vec.size()) {
      throw std::runtime_error("Vector inPlaceSub size mismatch");
    }

    #pragma clang loop vectorize(enable) interleave(enable)
    for (int i =0; i < vec.size(); i++) {
      vec[i].elem -= other_vec[i].elem;
    }

    return unit_t {};
  }

  unit_t inPlaceSubFrom(const Vector<Elem>& other) {
    auto& vec = Super::getContainer();
    auto& other_vec = other.getConstContainer();
    if (vec.size() != other_vec.size()) {
      throw std::runtime_error("Vector inPlaceSub size mismatch");
    }

    #pragma clang loop vectorize(enable) interleave(enable)
    for (int i =0; i < vec.size(); i++) {
      vec[i].elem = other_vec[i].elem - vec[i].elem;
    }

    return unit_t {};
  }

  Vector<Elem> sub(const Vector<Elem>& other) const {
    auto copy = Vector<Elem>(*this);
    copy.inPlaceSub(other);
    return copy;
  }

  Vector<Elem> sub(Vector<Elem>&& other) const {
    other.inPlaceSubFrom(*this);
    return other;
  }

  double dot(const Vector<Elem>& other) const {
    auto& vec = Super::getConstContainer();
    auto& other_vec = other.getConstContainer();
    if (vec.size() != other_vec.size()) {
      throw std::runtime_error("Vector dot size mismatch");
    }

    double d = 0;

    #pragma clang loop vectorize(enable) interleave(enable)
    for(int i = 0; i < vec.size(); i++) {
      d += vec[i].elem * other_vec[i].elem;
    }
    return d;
  }

  double distance(const Vector<Elem>& other) const {
    double d = 0;
    auto& vec = Super::getConstContainer();
    auto& other_vec = other.getConstContainer();
    if (vec.size() != other_vec.size()) {
      throw std::runtime_error("Vector squareDistance size mismatch");
    }

    #pragma clang loop vectorize(enable) interleave(enable)
    for (int i = 0; i < vec.size(); i++) {
      d += pow(vec[i].elem - other_vec[i].elem, 2);
    }

    return sqrt(d);
  }

  string toString(unit_t) const {
    std::ostringstream oss;
    auto& c = Super::getConstContainer();
    int i =0;
    oss << "[";
    for (auto& relem : c) {
      oss << relem.elem;
      i++;
      if (i < c.size()) {
        oss << ",";
      }
    }
    oss << "]";
    return oss.str();
  }

  // PERF: Vectorize.
  unit_t inPlaceScalarMult(double c) {
    #pragma clang loop vectorize(enable) interleave(enable)
    for (auto& i: this->getContainer()) {
      i.elem *= c;
    }
  }

  Vector<Elem> scalarMult(double c) const {
    auto result = Vector<Elem>(*this);
    auto& vec = result.getContainer();

    #pragma clang loop vectorize(enable) interleave(enable)
    for (auto i = 0; i < vec.size(); ++i) {
      vec[i].elem *= c;
    }

    return result;
  }
};

template <typename Elem, typename... Indexes>
class MultiIndex {
  public:

  typedef boost::multi_index_container<
    Elem,
    boost::multi_index::indexed_by<
      boost::multi_index::sequenced<>,
      Indexes...
    >
  > Container;

  Container container;

  // Default
  MultiIndex(): container() {}
  MultiIndex(const Container& con): container(con) {}
  MultiIndex(Container&& con): container(std::move(con)) {}

  // Construct from (container) iterators
  template<typename Iterator>
  MultiIndex(Iterator begin, Iterator end): container(begin,end) {}

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
  MultiIndex<Elem, Indexes...> combine(const MultiIndex& other) const {
    // copy this DS
    MultiIndex<Elem, Indexes...> result;
    result = MultiIndex(*this);
    // copy other DS
    for (const Elem &e : other.container) {
      result.insert(e);
    }
    return result;
  }

  // Split the ds at its midpoint
  tuple<MultiIndex<Elem, Indexes...>, MultiIndex<Elem, Indexes...>> split(const unit_t&) const {
    // Find midpoint
    const size_t size = container.size();
    const size_t half = size / 2;
    // Setup iterators
    auto  beg = container.begin();
    auto mid = container.begin();
    std::advance(mid, half);
    auto end = container.end();
    // Construct from iterators
    return std::make_tuple(MultiIndex(beg,mid), MultiIndex(mid,end));
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
  auto map(Fun f) -> MultiIndex<R_elem<RT<Fun, Elem>>> const {
    MultiIndex<R_elem<RT<Fun, Elem>>> result;
    for (const Elem &e : container) {
      result.insert( R_elem<RT<Fun, Elem>>{ f(e) } ); // Copies e (f is pass by value), then move inserts
    }
    return result;
  }

  // Create a new DS consisting of elements from this ds that satisfy the predicate
  template<typename Fun>
  MultiIndex<Elem, Indexes...> filter(Fun predicate) const {
    MultiIndex<Elem, Indexes...> result;
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
  MultiIndex<R_key_value<RT<F1, Elem>,Z>> groupBy(F1 grouper, F2 folder, const Z& init) const {
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
      MultiIndex<R_key_value<RT<F1, Elem>,Z>> result;
      for(const auto& it : accs) {
        result.insert(R_key_value<K, Z>{std::move(it.first), std::move(it.second)});

      }

      return result;
  }

  template <class Fun>
  auto ext(Fun expand) -> MultiIndex<typename RT<Fun, Elem>::ElemType> const {
    typedef typename RT<Fun, Elem>::ElemType T;
    MultiIndex<T> result;
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

  bool operator==(const MultiIndex& other) const {
    return container == other.container;
  }

  bool operator!=(const MultiIndex& other) const {
    return container != other.container;
  }

  bool operator<(const MultiIndex& other) const {
    return container < other.container;
  }

  bool operator>(const MultiIndex& other) const {
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
  MultiIndex<Elem, Indexes...> slice_with_index(const Index& index, Key a, Key b) const {
    MultiIndex<Elem, Indexes...> result;
    std::pair<typename Index::iterator, typename Index::iterator> p = index.range(a <= boost::lambda::_1, b >= boost::lambda::_1);
    for (typename Index::iterator it = p.first; it != p.second; it++) {
      result.insert(*it);

    }

    return result;
  }

  Container& getContainer() { return container; }

  // Return a constant reference to the container
  const Container& getConstContainer() const {return container;}

 private:
  friend class boost::serialization::access;
  template<class Archive>
  void serialize(Archive &ar, const unsigned int) {
    ar & container;
  }

};

} // Namespace K3

template <class K3Collection>
std::size_t hash_collection(K3Collection const& b) {
  const auto& c = b.getConstContainer();
  return boost::hash_range(c.begin(), c.end());

}

template <template<typename> class Derived, template<typename...> class Container, class Elem>
std::size_t hash_value(K3::StlDS<Derived, Container,Elem> const& b) {
  return hash_collection(b);
}

template <class Elem>
std::size_t hash_value(K3::Map<Elem> const& b) {
  return hash_collection(b);
}

template <class Elem>
std::size_t hash_value(K3::Set<Elem> const& b) {
  return hash_collection(b);
}

template <class Elem>
std::size_t hash_value(K3::Sorted<Elem> const& b) {
  return hash_collection(b);
}

template <class Elem>
std::size_t hash_value(K3::Vector<Elem> const& b) {
  return hash_collection(b);
}

template <typename... Args>
std::size_t hash_value(K3::MultiIndex<Args...> const& b) {
  return hash_collection(b);
}


namespace JSON {

  using namespace rapidjson;
  template <class E>
  struct convert<K3::Collection<E>> {
    template <class Allocator>
    static Value encode(const K3::Collection<E>& c, Allocator& al) {
     Value v;
     v.SetObject();
     v.AddMember("type", Value("Collection"), al);
     Value inner;
     inner.SetArray();
     for (const auto& e : c.getConstContainer()) {
       inner.PushBack(convert<E>::encode(e, al), al);
     }
     v.AddMember("value", inner.Move(), al);
     return v;
    }

  };

  template <class E>
  struct convert<K3::Seq<E>> {
    template <class Allocator>
    static Value encode(const K3::Seq<E>& c, Allocator& al) {
     Value v;
     v.SetObject();
     v.AddMember("type", Value("Seq"), al);
     Value inner;
     inner.SetArray();
     for (const auto& e : c.getConstContainer()) {
       inner.PushBack(convert<E>::encode(e, al), al);
     }
     v.AddMember("value", inner.Move(), al);
     return v;
    }

  };

  template <class E>
  struct convert<K3::Set<E>> {
    template <class Allocator>
    static Value encode(const K3::Set<E>& c, Allocator& al) {
     Value v;
     v.SetObject();
     v.AddMember("type", Value("Set"), al);
     Value inner;
     inner.SetArray();
     for (const auto& e : c.getConstContainer()) {
       inner.PushBack(convert<E>::encode(e, al), al);
     }
     v.AddMember("value", inner.Move(), al);
     return v;
    }

  };

  template <class E>
  struct convert<K3::Sorted<E>> {
    template <class Allocator>
    static Value encode(const K3::Sorted<E>& c, Allocator& al) {
     Value v;
     v.SetObject();
     v.AddMember("type", Value("Sorted"), al);
     Value inner;
     inner.SetArray();
     for (const auto& e : c.getConstContainer()) {
       inner.PushBack(convert<E>::encode(e, al), al);
     }
     v.AddMember("value", inner.Move(), al);
     return v;
    }

  };

  template <class E>
  struct convert<K3::Vector<E>> {
    template <class Allocator>
    static Value encode(const K3::Vector<E>& c, Allocator& al) {
     Value v;
     v.SetObject();
     v.AddMember("type", Value("Vector"), al);
     Value inner;
     inner.SetArray();
     for (const auto& e : c.getConstContainer()) {
       inner.PushBack(convert<E>::encode(e, al), al);
     }
     v.AddMember("value", inner.Move(), al);
     return v;
    }

  };

  template <class E>
  struct convert<K3::Map<E>> {
    template <class Allocator>
    static Value encode(const K3::Map<E>& c, Allocator& al) {
     Value v;
     v.SetObject();
     v.AddMember("type", Value("Map"), al);
     Value inner;
     inner.SetArray();
     for (const auto& e : c.getConstContainer()) {
       inner.PushBack(convert<E>::encode(e.second, al), al);
     }
     v.AddMember("value", inner.Move(), al);
     return v;
    }

  };

  template <class E, typename... Indexes>
  struct convert<K3::MultiIndex<E, Indexes...>> {
    template <class Allocator>
    static Value encode(const K3::MultiIndex<E, Indexes...>& c, Allocator& al) {
     Value v;
     v.SetObject();
     v.AddMember("type", Value("MultiIndex"), al);
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
  struct convert<K3::Collection<E>> {
    static Node encode(const K3::Collection<E>& c) {
      Node node;
      auto container = c.getConstContainer();
      if (container.size() > 0 ) {
        for (auto i: container) {
          node.push_back(convert<E>::encode(i));
        }
      }
      else {
        node = YAML::Load("[]");
      }
      return node;
    }

    static bool decode(const Node& node, K3::Collection<E>& c) {
      for (auto& i: node) {
        c.insert(i.as<E>());
      }

      return true;
    }
  };

  template <class E>
  struct convert<K3::Vector<E>> {
    static Node encode(const K3::Vector<E>& c) {
      Node node;
      auto container = c.getConstContainer();
      if (container.size() > 0) {
        for (auto i: container) {
          node.push_back(convert<E>::encode(i));
        }
      }
      else {
        node = YAML::Load("[]");
      }
      return node;
    }

    static bool decode(const Node& node, K3::Vector<E>& c) {
      for (auto& i: node) {
        c.insert(i.as<E>());
      }

      return true;
    }
  };

  template <class E>
  struct convert<K3::Seq<E>> {
    static Node encode(const K3::Seq<E>& c) {
      Node node;
      auto container = c.getConstContainer();
      if (container.size() > 0) {
        for (auto i: container) {
          node.push_back(convert<E>::encode(i));
        }
      }
      else {
        node = YAML::Load("[]");
      }

      return node;
    }

    static bool decode(const Node& node, K3::Seq<E>& c) {
      for (auto& i: node) {
        c.insert(i.as<E>());
      }

      return true;
    }
  };

  template <class R>
  struct convert<K3::Map<R>> {
    static Node encode(const K3::Map<R>& c) {
      Node node;
      auto container = c.getConstContainer();
      if (container.size() > 0) {
        for (auto i: container) {
          node.push_back(convert<R>::encode(i.second));
        }
      }
      else {
        node = YAML::Load("[]");
      }

      return node;
    }

    static bool decode(const Node& node, K3::Map<R>& c) {
      for (auto& i: node) {
        c.insert(i.as<R>());
      }

      return true;
    }
  };

  template <class E>
  struct convert<K3::Set<E>> {
    static Node encode(const K3::Set<E>& c) {
      Node node;
      for (auto i: c.getConstContainer()) {
        node.push_back(convert<E>::encode(i));
      }

      return node;
    }

    static bool decode(const Node& node, K3::Set<E>& c) {
      for (auto& i: node) {
        c.insert(i.as<E>());
      }

      return true;
    }
  };

  template <class E, typename... Indexes>
  struct convert<K3::MultiIndex<E, Indexes...>> {
    static Node encode(const K3::MultiIndex<E, Indexes...>& c) {
      Node node;
      for (auto i: c.getConstContainer()) {
        node.push_back(convert<E>::encode(i));
      }

      return node;
    }

    static bool decode(const Node& node, K3::MultiIndex<E, Indexes...>& c) {
      for (auto& i: node) {
        c.insert(i.as<E>());
      }

      return true;
    }
  };
}

#endif
