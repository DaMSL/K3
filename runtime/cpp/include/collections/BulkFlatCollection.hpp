#ifndef K3_EXTERNAL_COLLECTION
#define K3_EXTERNAL_COLLECTION

#include <limits>
#include <tuple>

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
#include "Page.hpp"

namespace K3 {

template<size_t PageSize>
struct PageCollection
{
  using Container = std::vector<Page<PageSize>>;

  void internalize(bool on) { internalized_ = !container.empty() && on; }
  bool internalized() { return internalized_; }

  void rewind() { pos = 0; }
  Page<PageSize>& operator*() { return at(pos); }
  PageCollection& operator++() { pos++; return *this; }

  Page<PageSize>& at(size_t i) {
    size_t j;
    if ( i >= (j = container.size()) ) {
      container.resize(i+1);
      for (int k = j; k < i+1; k++) { container[k].reset(); }
    }
    return container.at(i);
  }

  size_t size() const { return container.size(); }
  size_t capacity() const { return container.capacity(); }
  void resize(size_t count) { container.resize(count); }

  Container container;
  size_t pos;
  bool internalized_;
};

using IteratorProxy = std::tuple<bool, uint32_t>;

template<size_t PageSize>
class Externalizer
{
  using VContainer = PageCollection<PageSize>;

public:
  Externalizer(VContainer& v) : vcon(v) {}

  template<typename T> struct type{};

  template<class T> void write_string(T& t) {
    write_string(t, type<T>{});
  }

  template<class T> void write_string(T&, type<T>) {}

  void write_string(base_string& str, type<base_string>) {
    Page<PageSize>& pg = *vcon;
    bool advanced = false;
    if ( !pg.has_insert(str.length()) ) {
      ++vcon;
      advanced = true;
    }

    auto slot_id = pg.insert(str.data(), str.raw_length()+1);
    if ( slot_id == Page<PageSize>::null_slot ) {
      throw std::runtime_error("Externalization failed on variable length field");
    }

    auto buf = pg.get(slot_id);
    intptr_t* p = reinterpret_cast<intptr_t*>(&str);
    *p = static_cast<intptr_t>(slot_id & slot_mask);
    if ( advanced ) { *p |= proxy_mask; } else { *p &= ~proxy_mask; }
  }

private:
  VContainer& vcon;
  constexpr static intptr_t proxy_mask = std::numeric_limits<uint32_t>::max() + 1;
  constexpr static intptr_t slot_mask = std::numeric_limits<uint32_t>::max();
};

template<size_t PageSize>
class Internalizer
{
  using VContainer = PageCollection<PageSize>;

public:
  Internalizer(VContainer& v) : vcon(v) {}
  ~Internalizer() { vcon.internalize(true); }

  template<typename T> struct type{};

  template<class T> void read_string(T& t) {
    read_string(t, type<T>{});
  }

  template<class T> void read_string(T&, type<T>) {}

  void read_string(base_string& str) {
    if ( !vcon.internalized() ) {
      intptr_t* p = reinterpret_cast<intptr_t*>(&str);
      uint32_t slot_id = static_cast<uint32_t>(*p & slot_mask);
      bool advance = (*p & advance_mask) != 0;

      if ( advance ) { ++vcon; }
      Page<PageSize>& pg = *vcon;
      char* buf = pg.get(slot_id);
      if ( buf != nullptr ) { str.unowned(buf); }
      else {
        throw std::runtime_error("Internalization failed on variable length field");
      }
    }
  }

private:
  VContainer& vcon;

  constexpr static intptr_t advance_mask = std::numeric_limits<uint32_t>::max() + 1;
  constexpr static intptr_t slot_mask = std::numeric_limits<uint32_t>::max();
};

template<class Elem, size_t PageSize = 2 << 21>
class BulkFlatCollection {
public:
  using FContainer = Collection<Elem>;
  using VContainer = PageCollection<PageSize>;
  using Container  = std::tuple<FContainer, VContainer>;
  using Externalizer = Externalizer<PageSize>;
  using Internalizer = Internalizer<PageSize>;

  BulkFlatCollection()
  {
    cvariable().internalize(false);
    cvariable().rewind();
  }

  BulkFlatCollection(const Container& con) : container(con) {}
  BulkFlatCollection(Container&& con) : container(std::move(con)) {}

  // Sizing utilities.
  size_t fixseg_size()     const { return cfixed().getConstContainer().size(); }
  size_t fixseg_capacity() const { return cfixed().getConstContainer().capacity(); }
  void resize_fixed(size_t sz)   { cfixed().getContainer().resize(sz); }
  void reserve_fixed(size_t sz)  { cfixed().getContainer().reserve(sz); }

  size_t varseg_size()     const { return cvariable().size(); }
  size_t varseg_capacity() const { return cvariable().capacity(); }

  size_t byte_size()     const { return fixseg_size() * sizeof(Elem) + varseg_size() * PageSize; }
  size_t byte_capacity() const { return fixseg_capacity() * sizeof(Elem) + varseg_capacity() * PageSize; }


  ///////////////////////////////////////////
  // Bulk construction and externalization.

  unit_t clone(const Collection<R_elem<Elem>>& other) {
    if ( !cvariable().internalized() ) {
      auto os = other.size(unit_t{});
      if ( fixseg_size() < os ) { reserve_fixed(os); }

      Externalizer etl(cvariable());
      for (auto& e : other) {
        cfixed().insert(e.elem);
        cfixed().getContainer().back().externalize(etl);
      }
    }
    return unit_t {};
  }


  ///////////////////////////////////////////////////
  // Accessors.

  int size(const unit_t&) const { return cfixed().size(unit_t{}); }

  ///////////////////////////////////////////////////
  // Bulk transformations.

  template<class Fun>
  unit_t iterate(Fun f) const {
    Internalizer itl(cvariable());
    for (auto& e : cfixed()) {
      f(e.internalize(itl));
    }
    return unit_t{};
  }

  // Produce a new collection by mapping a function over this external collection.
  template <typename Fun>
  auto map(Fun f) const -> BulkFlatCollection<R_elem<RT<Fun, Elem>>> {
    BulkFlatCollection<R_elem<RT<Fun, Elem>>> result;
    Internalizer itl(cvariable());
    for (auto& e : cfixed()) {
      result.insert(R_elem<RT<Fun, Elem>>{f(e.internalize(itl))});
    }
    return result;
  }

  // Create a new collection consisting of elements from this set that satisfy the predicate.
  template <typename Fun>
  BulkFlatCollection<Elem> filter(Fun predicate) const {
    BulkFlatCollection<Elem> result;
    Internalizer itl(cvariable());
    for (auto& e : cfixed()) {
      if (predicate(e.internalize(itl))) {
        result.insert(e);
      }
    }
    return result;
  }

  // Fold a function over this collection.
  template <typename Fun, typename Acc>
  Acc fold(Fun f, Acc acc) const {
    Internalizer itl(cvariable());
    for (auto& e : cfixed()) {
      acc = f(std::move(acc), e.internalize(itl));
    }
    return acc;
  }

  Container& getContainer() { return container; }
  const Container& getConstContainer() const { return container; }

  template <class archive>
  void serialize(archive& a, const unsigned int) {
    uint64_t fixed_count;
    uint64_t page_count;
    if (archive::is_saving::value) {
      fixed_count = fixseg_size();
      page_count = varseg_size();
    }
    a & fixed_count;
    a & page_count;
    if (archive::is_loading::value) {
      resize_fixed(fixed_count);
      cvariable().container.resize(page_count);
    }
    if (fixed_count > 0) {
      a & boost::serialization::make_array(cfixed().getContainer().data(), fixed_count);
    }
    if (page_count > 0) {
      a & boost::serialization::make_array(cvariable().container.data(), page_count);
    }
  }

  template <class archive>
  void serialize(archive& a) const {
    uint64_t fixed_count = fixseg_size();
    uint64_t page_count = varseg_size();
    a & fixed_count;
    a & page_count;
    if (fixed_count > 0) {
      a.write(cfixed().getContainer().data(), fixed_count);
    }
    if (page_count > 0) {
      a.write(cvariable().container.data(), page_count);
    }
  }

  template <class archive>
  void serialize(archive& a) {
    uint64_t fixed_count;
    uint64_t page_count;
    a & fixed_count;
    a & page_count;
    if (fixed_count > 0) {
      resize_fixed(fixed_count);
      a.read(cfixed().getContainer().data(), fixed_count);
    }
    if (page_count > 0) {
      cvariable().container.resize(page_count);
      a.read(cvariable.container.data(), page_count);
    }
  }

private:
  Container   container;
  FContainer& cfixed() { return std::get<0>(container); }
  VContainer& cvariable() { return std::get<1>(container); }
};

}; // end namespace K3

#endif