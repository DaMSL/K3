#ifndef K3_BULK_FLAT_COLLECTION
#define K3_BULK_FLAT_COLLECTION

#include <limits>
#include <tuple>

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
#include "collections/BulkFlatCollection.hpp"
#include "storage/Page.hpp"

namespace LibdynamicVector {
extern "C" {
#include <dynamic/buffer.h>
#include <dynamic/vector.h>
}
};

namespace K3 {
namespace Libdynamic {

using namespace LibdynamicVector;

template<size_t PageSize>
struct PageCollection
{
  using Container = LibdynamicVector::vector;

  PageCollection() : container(vector_new(PageSize)) {}

  PageCollection(const PageCollection& c) {
    container = vector_new(PageSize);
    auto c_size = vector_size(c.container);
    vector_reserve(container, c_size);
    for (int i = 0; i < c_size; ++i) {
      vector_push_back(container, vector_at(c.container, i));
    }
  }

  PageCollection(PageCollection&& v) { std::swap(container, v.container); }

  ~PageCollection() { vector_free(container); }

  // Returns whether fixed segment associated with these pages uses absolute pointers.
  bool internalized() { return internalized_; }
  void internalize(bool on) { internalized_ = !vector_empty(container) && on; }

  void rewind() { pos = 0; }
  Page<PageSize>& operator*() { return at(pos); }
  PageCollection& operator++() { pos++; return *this; }

  Page<PageSize>& at(size_t i) {
    if ( i >= vector_size(container) ) { resize(i+1); }
    return *reinterpret_cast<Page<PageSize>*>(vector_at(container, i));
  }

  size_t size() const { return vector_size(container); }
  size_t capacity() const { return vector_capacity(container); }
  void* data() { return vector_data(container); }

  void reserve(size_t count) {
    vector_reserve(container, count);
  }

  void resize(size_t count) {
    size_t j = vector_size(container);
    reserve(count);
    container->buffer.size = count * PageSize;
    for (int k = j; k < count; k++) {
      reinterpret_cast<Page<PageSize>*>(vector_at(container, k))->reset();
    }
  }

  Container* container;
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

  template<class T> void externalize(T& t) {
    externalize(t, type<T>{});
  }

  template<class T> void externalize(T&, type<T>) {}

  void externalize(base_string& str, type<base_string>) {
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

  template<class T> void internalize(T& t) {
    internalize(t, type<T>{});
  }

  template<class T> void internalize(T&, type<T>) {}

  void internalize(base_string& str) {
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
  using FContainer = LibdynamicVector::vector;
  using VContainer = PageCollection<PageSize>;

  typedef struct {
    FContainer cfixed;
    VContainer cvariable;
  } Container;

  using Externalizer = Externalizer<PageSize>;
  using Internalizer = Internalizer<PageSize>;

  BulkFlatCollection()
  {
    vector_init(fixed(), sizeof(Elem));
    variable()->internalize(false);
    variable()->rewind();
  }

  BulkFlatCollection(const Container& con) : container(con) {}
  BulkFlatCollection(Container&& con) : container(std::move(con)) {}

  // Sizing utilities.
  size_t fixseg_size()     const { return vector_size(const_cast<FContainer*>(fixedc())); }
  size_t fixseg_capacity() const { return vector_capacity(const_cast<FContainer*>(fixedc())); }
  void reserve_fixed(size_t sz)  { vector_reserve(fixed(), sz); }

  size_t varseg_size()     const { return variablec()->size(); }
  size_t varseg_capacity() const { return variablec()->capacity(); }

  size_t byte_size()     const { return fixseg_size() * sizeof(Elem) + varseg_size() * PageSize; }
  size_t byte_capacity() const { return fixseg_capacity() * sizeof(Elem) + varseg_capacity() * PageSize; }


  ///////////////////////////////////////////
  // Bulk construction and externalization.

  unit_t append(const Collection<R_elem<Elem>>& other) {
    if ( !buffer && !variable()->internalized() ) {
      auto os = other.size(unit_t{});
      if ( fixseg_size() < os ) { reserve_fixed(os); }

      Externalizer etl(*variable());
      for (auto& e : other) {
        vector_push_back(fixed(), const_cast<Elem*>(&(e.elem)));
        reinterpret_cast<Elem*>(vector_back(fixed()))->externalize(etl);
      }
    } else {
      throw std::runtime_error("Invalid append on a BulkFlatCollection");
    }
    return unit_t {};
  }

  base_string save(unit_t) const {
    FContainer* ncf = const_cast<FContainer*>(fixedc());
    VContainer* ncv = const_cast<VContainer*>(variablec());

    /*
    if ( variable()->internalized() ) {
      Externalizer etl(*ncv);
      auto sz = vector_size(ncf);
      for (size_t i = 0; i < sz; ++i) {
        reinterpret_cast<Elem*>(vector_at(ncf, i))->externalize(etl);
      }
    }
    */

    uint64_t fixed_count = fixseg_size();
    uint64_t page_count = varseg_size();

    auto len = sizeof(size_t) + 2 * sizeof(uint64_t) * byte_size();
    auto buffer_ = new char[len+1];
    buffer_[len] = 0;

    size_t offset = sizeof(size_t);
    memcpy(buffer_ + offset, &fixed_count, sizeof(fixed_count));
    offset += sizeof(uint64_t);

    memcpy(buffer_ + offset, &page_count, sizeof(page_count));
    offset += sizeof(uint64_t);

    if (fixed_count > 0) {
      memcpy(buffer_ + offset, vector_data(ncf), fixseg_size() * sizeof(Elem));
      offset += fixseg_size() * sizeof(Elem);
    }
    if (page_count > 0) {
      memcpy(buffer_ + offset, ncv->data(), varseg_size() * PageSize);
    }

    base_string str;
    len -= sizeof(size_t);
    memcpy(buffer_, &len, sizeof(size_t));
    str.steal(buffer_);
    str.set_header(true);
    return str;
  }

  unit_t load(const base_string& str) {
    return load(base_string(str));
  }

  unit_t load(base_string&& str) {
    assert( vector_empty(fixed()) );
    buffer = std::move(str);
    size_t offset = 0;

    uint64_t fixed_count = *reinterpret_cast<uint64_t*>(buffer.begin());
    offset += sizeof(uint64_t);

    uint64_t page_count = *reinterpret_cast<uint64_t*>(buffer.begin() + offset);
    offset += sizeof(uint64_t);

    if (fixed_count > 0) {
      fixed()->buffer.data = buffer.begin() + offset;
      fixed()->buffer.size = fixed_count * sizeof(Elem);
      fixed()->buffer.capacity = fixed()->buffer.size;
      fixed()->object_size = sizeof(Elem);
      offset += fixed_count * sizeof(Elem);
    }
    if (page_count > 0) {
      variable()->container->buffer.data = buffer.begin() + offset;
      variable()->container->buffer.size = page_count * PageSize;
      variable()->container->buffer.capacity = variable()->container->buffer.size;
      variable()->container->object_size = PageSize;
    }
    return unit_t {};
  }

  ///////////////////////////////////////////////////
  // Accessors.

  int size(const unit_t&) const { return vector_size(const_cast<FContainer*>(fixedc())); }

  ///////////////////////////////////////////////////
  // Bulk transformations.

  template<class Fun>
  unit_t iterate(Fun f) const {
    FContainer* ncf = const_cast<FContainer*>(fixedc());
    VContainer* ncv = const_cast<VContainer*>(variablec());
    Internalizer itl(*ncv);
    auto sz = fixseg_size();
    for (size_t i = 0; i < sz; ++i) {
      auto& e = reinterpret_cast<Elem*>(vector_at(ncf, i))->internalize(itl);
      f(e);
    }
    return unit_t{};
  }

  // Produce a new collection by mapping a function over this external collection.
  template <typename Fun>
  auto map_generic(Fun f) const -> Collection<R_elem<RT<Fun, Elem>>> {
    FContainer* ncf = const_cast<FContainer*>(fixedc());
    VContainer* ncv = const_cast<VContainer*>(variablec());
    Collection<R_elem<RT<Fun, Elem>>> result;
    Internalizer itl(*ncv);
    auto sz = fixseg_size();
    for (size_t i = 0; i < sz; ++i) {
      auto& e = reinterpret_cast<Elem*>(vector_at(ncf, i))->internalize(itl);
      result.insert(R_elem<RT<Fun, Elem>>{ f(e) });
    }
    return result;
  }

  // Create a new collection consisting of elements from this set that satisfy the predicate.
  template <typename Fun>
  Collection<R_elem<Elem>> filter_generic(Fun predicate) const {
    FContainer* ncf = const_cast<FContainer*>(fixedc());
    VContainer* ncv = const_cast<VContainer*>(variablec());
    Collection<R_elem<Elem>> result;
    Internalizer itl(*ncv);
    auto sz = fixseg_size();
    for (size_t i = 0; i < sz; ++i) {
      auto& e = reinterpret_cast<Elem*>(vector_at(ncf, i))->internalize(itl);
      if (predicate(e)) {
        result.insert(R_elem<Elem>{ e });
      }
    }
    return result;
  }

  // Fold a function over this collection.
  template <typename Fun, typename Acc>
  Acc fold(Fun f, Acc acc) const {
    FContainer* ncf = const_cast<FContainer*>(fixedc());
    VContainer* ncv = const_cast<VContainer*>(variablec());
    Internalizer itl(*ncv);
    auto sz = fixseg_size();
    for (size_t i = 0; i < sz; ++i) {
      auto& e = reinterpret_cast<Elem*>(vector_at(ncf, i))->internalize(itl);
      acc = f(std::move(acc), e);
    }
    return acc;
  }

  Container& getContainer() { return container; }
  const Container& getConstContainer() const { return container; }

  template <class archive>
  void serialize(archive& a) const {
    uint64_t fixed_count = fixseg_size();
    uint64_t page_count = varseg_size();
    a.write(reinterpret_cast<const char*>(&fixed_count), sizeof(fixed_count));
    a.write(reinterpret_cast<const char*>(&page_count), sizeof(page_count));
    if (fixed_count > 0) {
      a.write(vector_data(fixed()), fixseg_size() * sizeof(Elem));
    }
    if (page_count > 0) {
      a.write(variable()->data(), varseg_size() * PageSize);
    }
  }

  template <class archive>
  void serialize(archive& a) {
    uint64_t fixed_count;
    uint64_t page_count;
    a.read(reinterpret_cast<const char*>(&fixed_count), sizeof(fixed_count));
    a.read(reinterpret_cast<const char*>(&page_count), sizeof(page_count));
    if (fixed_count > 0) {
      reserve_fixed(fixed_count);
      fixed()->buffer.size = fixed_count * sizeof(Elem);
      a.read(vector_data(fixed()), fixed_count * sizeof(Elem));
    }
    if (page_count > 0) {
      variable()->resize(page_count);
      a.read(variable()->data(), page_count * PageSize);
    }
  }

  BOOST_SERIALIZATION_SPLIT_MEMBER()

private:
  Container container;
  FContainer* fixed()    { return &(container.cfixed); }
  VContainer* variable() { return &(container.cvariable); }

  const FContainer* fixedc()    const { return &(container.cfixed); }
  const VContainer* variablec() const { return &(container.cvariable); }

  base_string buffer;
};

}; // end namespace Libdynamic

template<class Elem>
using BulkFlatCollection = Libdynamic::BulkFlatCollection<Elem>;

}; // end namespace K3

#endif // HAS_LIBDYNAMIC

#endif