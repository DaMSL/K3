#ifndef K3_PAGE_COLLECTION
#define K3_PAGE_COLLECTION

/*
 * An auxiliary class implementing a page collection as a contiguous vector of pages.
 * This file should be included directly by any classes that need a PageCollection
 * (e.g., BulkFlatCollection, PolyQueue)
 */

#include <limits>
#include <tuple>

#if HAS_LIBDYNAMIC

#include "Common.hpp"
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

//////////////////////////////////////////////////////////
// PageCollection: a directly addressable vector of pages.
//
template<size_t PageSize>
struct PageCollection
{
  using Container = LibdynamicVector::vector;

  PageCollection() : container(vector_new(PageSize)) {
    external_buffer = false;
  }

  PageCollection(const PageCollection& c) {
    container = vector_new(PageSize);
    auto c_size = vector_size(c.container);
    vector_reserve(container, c_size);
    for (int i = 0; i < c_size; ++i) {
      vector_push_back(container, vector_at(c.container, i));
    }
    external_buffer = false;
  }

  ~PageCollection() {
    if (!external_buffer) {
      vector_free(container);
    } else {
      free(container);
    }
  }

  // Returns whether fixed segment associated with these pages uses absolute pointers.
  bool internalized() { return internalized_; }
  void internalize(bool on) { internalized_ = !vector_empty(container) && on; }

  void rewind() { pos = 0; }
  Page<PageSize>& operator*() { return *(at(pos)); }
  Page<PageSize>* operator->() { return at(pos); }
  PageCollection& operator++() { pos++; return *this; }
  void externalBuffer(bool isExternal) {
    if (isExternal && !external_buffer) {
      buffer_clear(&container->buffer);
    }
    external_buffer = isExternal;
  }

  Page<PageSize>* at(size_t i) {
    if ( i >= vector_size(container) ) { resize(i+1); }
    return reinterpret_cast<Page<PageSize>*>(vector_at(container, i));
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

  /////////////////////////////////////////////
  // Iterators
  template <class I>
  class page_iterator : public std::iterator<std::forward_iterator_tag, Page<PageSize>> {
    using reference = typename std::iterator<std::forward_iterator_tag, Page<PageSize>>::reference;

   public:
    template <class _I>
    page_iterator(Container* _m, _I&& _i)
        : m(_m), i(std::forward<_I>(_i)) {}

    page_iterator& operator++() {
      ++i;
      return *this;
    }

    page_iterator operator++(int) {
      page_iterator t = *this;
      *this++;
      return t;
    }

    auto operator -> () const { return static_cast<Page<PageSize>*>(vector_at(m, i)); }
    auto& operator*() const { return *static_cast<Page<PageSize>*>(vector_at(m, i)); }

    bool operator==(const page_iterator& other) const { return i == other.i; }
    bool operator!=(const page_iterator& other) const { return i != other.i; }

   private:
    Container* m;
    I i;
  };

  using iterator       = page_iterator<size_t>;
  using const_iterator = page_iterator<size_t>;

  iterator begin() { return iterator(container, 0); }
  iterator end() { return iterator(container, vector_size(container)); }

  const_iterator begin() const { return const_iterator(container, 0); }
  const_iterator end() const { return const_iterator(container, vector_size(container)); }

  Container* container;
  bool external_buffer;
  size_t pos;
  bool internalized_;
};

////////////////////////////////////////////////////
// Externalizer and Internalizer.
// These are support classes that hold externalization
// and internalizaton metadata while traversing through
// data structures to perform pointer switching.

using IteratorProxy = std::tuple<bool, uint32_t>;

template<size_t PageSize>
class Externalizer
{
  using VContainer = PageCollection<PageSize>;

public:
  enum class ExternalizeOp { Create, Reuse };

  Externalizer(VContainer& v, ExternalizeOp o) : vcon(v), vtraversal(vcon.begin()), op(o)
  {
    switch (op) {
      case ExternalizeOp::Reuse:
        reuse_slot_id = 0;
      default:
        break;
    }
  }

  template<typename T> struct type{};

  template<class T> void externalize(T& t) { externalize(t, type<T>{}); }
  template<class T> void externalize(T& t, type<T>) {
    t.externalize(*this);
  }

  void externalize(int&, type<int>) { }
  void externalize(double&, type<double>) { }

  void externalize(base_string& str, type<base_string>)
  {
    bool advanced = false;
    if ( op == ExternalizeOp::Create ) {
      if ( !(vcon->has_insert(str.raw_length()+1)) ) {
        ++vcon;
        advanced = true;
      }
      Page<PageSize>& pg = *vcon;

      auto slot_id = pg.insert(str.data(), str.raw_length()+1);
      if ( slot_id == Page<PageSize>::null_slot ) {
        throw std::runtime_error("Externalization failed on variable length field");
      }

      auto buf = pg.get(slot_id);
      intptr_t* p = reinterpret_cast<intptr_t*>(&str);
      *p = static_cast<intptr_t>(slot_id & slot_mask);
      if ( advanced ) { *p |= advance_mask; } else { *p &= ~advance_mask; }
    }
    else if ( op == ExternalizeOp::Reuse ) {
      if ( str.has_advance() ) {
        reuse_slot_id = 0;
        advanced = true;
      }

      intptr_t* p = reinterpret_cast<intptr_t*>(&str);
      *p = static_cast<intptr_t>(reuse_slot_id & slot_mask);
      if ( advanced ) { *p |= advance_mask; } else { *p &= ~advance_mask; }
      reuse_slot_id++;
    }
    else {
      throw std::runtime_error("Invalid externalization mode");
    }
  }

private:
  VContainer& vcon;
  typename VContainer::iterator vtraversal;

  ExternalizeOp op;

  // Externalization mode metadata.
  union {
    size_t reuse_slot_id;
  };

  constexpr static intptr_t advance_mask = static_cast<intptr_t>(std::numeric_limits<uint32_t>::max()) + 1;
  constexpr static intptr_t slot_mask = std::numeric_limits<uint32_t>::max();
};

template<size_t PageSize>
class Internalizer
{
  using VContainer = PageCollection<PageSize>;

public:
  Internalizer(VContainer& v) : vcon(v), vtraversal(vcon.begin()) {}

  ~Internalizer() { vcon.internalize(true); }

  template<typename T> struct type{};

  template<class T> void internalize(T& t) {
    internalize(t, type<T>{});
  }

  template<class T> void internalize(T& t, type<T>) {
    t.internalize(*this);
  }

  void internalize(int&, type<int>) { }
  void internalize(double&, type<double>) { }

  void internalize(base_string& str) {
    if ( !vcon.internalized() ) {
      intptr_t* p = reinterpret_cast<intptr_t*>(&str);
      uint32_t slot_id = static_cast<uint32_t>(*p & slot_mask);
      bool advance = (*p & advance_mask) != 0;

      if ( advance ) { ++vtraversal; }
      Page<PageSize>& pg = *vtraversal;
      char* buf = pg.get(slot_id);
      if ( buf != nullptr ) {
        str.unowned(buf);
        str.set_advance(advance);
      }
      else {
        throw std::runtime_error("Internalization failed on variable length field");
      }
    }
  }

private:
  VContainer& vcon;
  typename VContainer::iterator vtraversal;

  constexpr static intptr_t advance_mask = static_cast<intptr_t>(std::numeric_limits<uint32_t>::max()) + 1;
  constexpr static intptr_t slot_mask = std::numeric_limits<uint32_t>::max();
};


}; // end namespace Libdynamic

}; // end namespace K3

#endif // HAS_LIBDYNAMIC

#endif