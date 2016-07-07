#ifndef K3_PAGE
#define K3_PAGE

#include <array>
#include <cstdint>
#include <cstring>
#include <limits>
#include <iostream>

namespace K3 {

////////////////////////////////////////////////
// Page layout:
//   page header
//   slot array
//   free space
//   data segment, filled backwards.

template<size_t PageSize>
struct PageHeader
{
  using slot_id_t     = uint32_t;  // Invalid when equal to numeric_limits::max().
  using page_offset_t = uint32_t;
  using slot_length_t = uint32_t;  // Invalid when 0.

  slot_id_t current_slot;
  slot_id_t num_slots;
  size_t free_space_offset;

  void reset() {
    current_slot = 0;
    num_slots = default_slots();
    free_space_offset = PageSize;
  }

  constexpr size_t slot_size() {
    return sizeof(page_offset_t) + sizeof(slot_length_t);
  }

  constexpr slot_id_t default_slots() {
    // Assume an average of 16 byte values.
    return (PageSize - sizeof(PageHeader)) / (slot_size() + 16);
  }
};

// Helper class to avoid reinterpret_casts for the header.
template<size_t PageSize>
struct BasePage {
  union {
    std::array<char, PageSize> page_;
    PageHeader<PageSize> header_;
  };
};

// TODO: iterator
template<size_t PageSize>
struct Page : public BasePage<PageSize>
{
  using Super = BasePage<PageSize>;

  using slot_id_t     = uint32_t;
  using page_offset_t = uint32_t;
  using slot_length_t = uint32_t;  // Invalid when 0.

  ///////////////////////////////////
  // Initialization.

  void reset() {
    Super::header_.reset();
    reset_slots();
  }

  void reset_slots() {
    for (slot_id_t i = 0; i < Super::header_.num_slots; ++i) {
      set_slot(i, 0, 0);
    }
  }

  ///////////////////////////////////
  // Space usage.

  size_t overhead() {
    return sizeof(PageHeader<PageSize>)
             + Super::header_.num_slots * Super::header_.slot_size();
  }

  size_t available() { return Super::header_.free_space_offset - overhead(); }

  size_t used() { return PageSize - available(); }

  ///////////////////////////////////
  // Data access.

  char* get(const slot_id_t& s) {
    if (s != null_slot && slot_length(s) > 0) {
      auto o = slot_offset(s);
      return Super::page_.data() + o;
    }
    return nullptr;
  }

  bool put(const slot_id_t& s, const char* data, size_t len) {
    auto p = get(s);
    auto sl = slot_length_ptr(s);
    if (p != nullptr && len <= *sl) {
      memcpy(p, data, len);
      *sl = len;
      return true;
    }
    return false;
  }

  bool has_insert(size_t len) {
    bool slots_avail = Super::header_.current_slot < Super::header_.num_slots;
    size_t requested = len + (!slots_avail? Super::header_.slot_size() : 0);
    return available() > align_sz(requested);
  }

  slot_id_t insert(const char* data, size_t len) {
    char* p = nullptr;
    auto s = next_slot(len);
    if ( s != null_slot && (p = get(s)) != nullptr ) {
      auto provisioned = align_sz(slot_length(s));
      memcpy(p, data, len);
      if ( provisioned > len ) {
        memset(p+len, 0, provisioned - len);
      }
    }
    return s;
  }

  void erase(const slot_id_t& s, bool zero = false) {
    if ( zero ) { clear(s); }
    set_slot(s, 0, 0);
  }

  void clear(const slot_id_t& s) {
    auto p = get(s);
    auto sl = slot_length(s);
    if ( p != nullptr ) { memset(p, 0, sl); }
  }

  ///////////////////////////////////
  // Slot management.

  char* slot0_data() { return Super::page_.data() + sizeof(PageHeader<PageSize>); }

  page_offset_t* slot_offset_ptr(const slot_id_t& s) {
    return reinterpret_cast<page_offset_t*>(slot0_data() + s * Super::header_.slot_size());
  }

  slot_length_t* slot_length_ptr(const slot_id_t& s) {
    return reinterpret_cast<slot_length_t*>(
            slot0_data() + s * Super::header_.slot_size() + sizeof(page_offset_t));
  }

  page_offset_t slot_offset(const slot_id_t& s) { return *slot_offset_ptr(s); }
  slot_length_t slot_length(const slot_id_t& s) { return *slot_length_ptr(s); }

  void set_slot(const slot_id_t& s, page_offset_t o, slot_length_t l) {
    auto so = slot_offset_ptr(s);
    auto sl = slot_length_ptr(s);
    *so = o;
    *sl = l;
  }

  slot_id_t next_slot(size_t len) {
    bool slots_avail = Super::header_.current_slot < Super::header_.num_slots;
    size_t requested = len + (!slots_avail? Super::header_.slot_size() : 0);
    /*
    if ( available() > requested ) {
      if ( !slots_avail ) { Super::header_.num_slots++; }
      Super::header_.current_slot++;
      auto s = Super::header_.current_slot - 1;
      Super::header_.free_space_offset -= len;
      set_slot(s, Super::header_.free_space_offset, len);
      return s;
    }
    */
    size_t aligned = align_sz(requested);
    if ( available() > aligned ) {
      if ( !slots_avail ) { Super::header_.num_slots++; }
      Super::header_.current_slot++;
      auto s = Super::header_.current_slot - 1;
      Super::header_.free_space_offset -= aligned;
      set_slot(s, Super::header_.free_space_offset, len);
      return s;
    }

    return null_slot;
  }

  size_t align_sz(size_t sz) {
    return sz + (sizeof(intptr_t) - (sz % sizeof(intptr_t)));
  }

  template<class Archive>
  void serialize(Archive &ar) {
    ar & Super::page_;
  }

  template<class Archive>
  void serialize(Archive &ar, const unsigned int) {
    ar & Super::page_;
  }

  constexpr static slot_id_t null_slot = std::numeric_limits<uint32_t>::max();
};

// Common typedefs.
using Page4K  = Page<2<<12>;
using Page8K  = Page<2<<13>;
using Page16K = Page<2<<14>;
using Page32K = Page<2<<15>;
using Page64K = Page<2<<16>;
using Page1M  = Page<2<<20>;
using Page2M  = Page<2<<21>;

} // Namespace K3
#endif