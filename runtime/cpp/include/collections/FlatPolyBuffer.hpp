#ifndef K3_POLYQUEUE
#define K3_POLYQUEUE

/*
 * A page-based heterogeneous queue for contiguously storing different types of elements.
 */

#include <limits>
#include <tuple>

#if HAS_LIBDYNAMIC

namespace LibdynamicVector {
extern "C" {
#include <dynamic/buffer.h>
#include <dynamic/vector.h>
}
};

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

namespace K3 {
namespace Libdynamic {


////////////////////////////////////////////////////
// Externalizer and Internalizer.
// These are support classes that hold externalization
// and internalizaton metadata while traversing through
// data structures to perform pointer switching.

class BufferExternalizer
{
  using VContainer = LibdynamicVector::buffer;

public:
  enum class ExternalizeOp { Create, Reuse };

  BufferExternalizer(VContainer* v, ExternalizeOp o) : vcon(v), op(o) {}

  template<typename T> struct type{};

  template<class T> void externalize(T& t) { externalize(t, type<T>{}); }
  template<class T> void externalize(T& t, type<T>) { t.externalize(*this); }

  void externalize(unit_t&, type<unit_t>) {}
  void externalize(bool&, type<bool>) {}
  void externalize(int&, type<int>) {}
  void externalize(double&, type<double>) {}
  void externalize(unsigned long&, type<unsigned long>) {}
  void externalize(unsigned short&, type<unsigned short>) {}

  void externalize(base_string& str, type<base_string>)
  {
    if ( op == ExternalizeOp::Create ) {
      size_t offset = buffer_size(vcon);
      size_t len = str.raw_length() + 1;
      size_t gap = 8 - ((offset + len) % 8);
      char filler[gap] = { 0 };

      int status = buffer_insert(vcon, offset, const_cast<char*>(str.data()), len);
      if ( status == 0 ) {
        status = buffer_insert(vcon, offset+len, &(filler[0]), gap);
      } else {
        throw std::runtime_error("Invalid externalization operation1");
      }
      if ( status == 0 ) {
        // The base_str we made inside the buffer does not own the external buffer. That will be freed
        // by the real base_str that exists out in the world
        str.set_borrowing(true);
        str.unowned((char *)offset);
      }
      else {
        throw std::runtime_error("Invalid externalization operation2");
      }
    }
    else if ( op == ExternalizeOp::Reuse ) {
      size_t offset = str.data() - buffer_data(vcon);
      str.unowned((char *)offset);
    }
  }

private:
  VContainer* vcon;
  ExternalizeOp op;
};


class BufferInternalizer
{
  using VContainer = LibdynamicVector::buffer;

public:
  BufferInternalizer(VContainer* v) : vcon(v) {}
  ~BufferInternalizer() {}

  template<typename T> struct type{};

  template<class T> void internalize(T& t) {
    internalize(t, type<T>{});
  }

  template<class T> void internalize(T& t, type<T>) {
    t.internalize(*this);
  }

  void internalize(unit_t&, type<unit_t>) {}
  void internalize(bool&, type<bool>) {}
  void internalize(int&, type<int>) {}
  void internalize(double&, type<double>) {}
  void internalize(unsigned long&, type<unsigned long>) {}
  void internalize(unsigned short&, type<unsigned short>) {}

  void internalize(base_string& str) {
    char *p = str.bufferp_();
    size_t offset = (size_t)(p);
    char* buf = buffer_data(vcon) + offset;
    if ( buf != nullptr ) {
      str.unowned(buf);
    }
    else {
      throw std::runtime_error("Internalization failed on variable length field");
    }
  }

private:
  VContainer* vcon;
};



//////////////////////////////////////////
//
// FlatPolyBuffer
template<class Ignore>
class FlatPolyBuffer {
public:
  using Buf = LibdynamicVector::buffer;
  using Vec = LibdynamicVector::vector;
  using Tag = uint16_t;

  using FContainer = Buf;
  using VContainer = Buf;
  using TContainer = Vec;

  typedef struct {
    FContainer cfixed;
    VContainer cvariable;
    TContainer ctags;
  } Container;

  using ExternalizerT = BufferExternalizer;
  using InternalizerT = BufferInternalizer;

  FlatPolyBuffer() : container(), buffer(), internalized(false) {
    initContainer();
  }

  FlatPolyBuffer(const FlatPolyBuffer& other) : FlatPolyBuffer() {
    copyPolyBuffer(other);
    // Leave it to the child classes to internalize, if necessary
  }

  FlatPolyBuffer(FlatPolyBuffer&& other) : FlatPolyBuffer() {
    swapPolyBuffer(std::move(other));
  }

  FlatPolyBuffer& operator=(const FlatPolyBuffer& other) {
    clear(unit_t{});
    copyPolyBuffer(other);
    if (other.internalized) {
      unpack(unit_t{});
    }
  }

  FlatPolyBuffer& operator=(FlatPolyBuffer&& other) {
    swapPolyBuffer(std::move(other));
    return *this;
  }

  ~FlatPolyBuffer() {
    freeContainer();
  }

  void initContainer() {
    buffer_init(fixed());
    buffer_init(variable());
    vector_init(tags(), sizeof(Tag));
  }

  void freeContainer() {
    if (!buffer.data()) {
      // buffer clear just deallocates. vector clear zeroes out the vector, which we don't need
      buffer_clear(fixed());
      buffer_clear(variable());
      buffer_clear(&(tags()->buffer));
    }
  }

  void freeBackingBuffer() {
    buffer = std::move(base_string {});
  }

  void copyPolyBuffer(const FlatPolyBuffer& other) {
    bool otherModified = false;
    auto p = const_cast<FlatPolyBuffer*>(&other);

    // Ensure the other container is externalized so that pointers are stored as offsets.
    if ( other.internalized ) {
      otherModified = true;
      p->repack(unit_t{});
    }

    // Copy the container.
    copyBuffer(fixed(), p->fixed());
    copyBuffer(variable(), p->variable());
    copyVector(tags(), p->tags());

    // This container is left externalized.
    // We expect the callsite to internalize if necessary
    internalized = false;

    // Return other container to original state
    if ( otherModified ) { p->unpack(unit_t{}); }
  }

  void swapPolyBuffer(FlatPolyBuffer&& other) {
    if ( other.buffer.data() != nullptr ) { swap(buffer, other.buffer); }

    container.cfixed.size     = other.container.cfixed.size;
    container.cfixed.capacity = other.container.cfixed.capacity;
    std::swap(container.cfixed.data, other.container.cfixed.data);

    container.cvariable.size     = other.container.cvariable.size;
    container.cvariable.capacity = other.container.cvariable.capacity;
    std::swap(container.cvariable.data, other.container.cvariable.data);

    container.ctags.buffer.size     = other.container.ctags.buffer.size;
    container.ctags.buffer.capacity = other.container.ctags.buffer.capacity;
    container.ctags.object_size     = other.container.ctags.object_size;
    container.ctags.release         = other.container.ctags.release;
    std::swap(container.ctags.buffer.data, other.container.ctags.buffer.data);

    std::swap(internalized, other.internalized);
  }

  void copyBuffer(Buf* a, Buf* b) {
    size_t bsize = buffer_size(b);
    buffer_reserve(a, bsize);
    memcpy(a->data, buffer_data(b), bsize);
    a->size = bsize;
  }

  void copyVector(Vec* a, Vec* b) {
    if ( a->object_size != b->object_size ) {
      vector_init(a, b->object_size);
    }
    size_t bsize = vector_size(b);
    vector_reserve(a, bsize);
    copyBuffer(&(a->buffer), &(b->buffer));
    a->release = b->release;
  }

  // Sizing utilities.
  size_t fixseg_size()     const { return buffer_size(const_cast<FContainer*>(fixedc())); }
  size_t fixseg_capacity() const { return buffer_capacity(const_cast<FContainer*>(fixedc())); }

  size_t varseg_size()     const { return buffer_size(const_cast<VContainer*>(variablec())); }
  size_t varseg_capacity() const { return buffer_capacity(const_cast<VContainer*>(variablec())); }

  size_t tags_size()     const { return vector_size(const_cast<TContainer*>(tagsc())); }
  size_t tags_capacity() const { return vector_capacity(const_cast<TContainer*>(tagsc())); }

  size_t byte_size()     const { return fixseg_size() + varseg_size() + tags_size() * sizeof(Tag); }
  size_t byte_capacity() const { return fixseg_capacity() + varseg_capacity() + tags_capacity() * sizeof(Tag); }

  void reserve_fixed(size_t sz) { buffer_reserve(fixed(), sz); }
  void reserve_var(size_t sz)   { buffer_reserve(variable(), sz); }
  void reserve_tags(size_t sz)  { vector_reserve(tags(), sz); }


  // Virtual methods to be implemented by instances.
  virtual size_t elemsize(Tag t) const = 0;
  virtual void externalize(ExternalizerT& e, Tag t, char* data) = 0;
  virtual void internalize(InternalizerT& i, Tag t, char* data) = 0;

  virtual YAML::Node yamlencode(Tag t, int idx, size_t offset) const = 0;
  virtual void yamldecode(YAML::Node& n) = 0;
  virtual rapidjson::Value jsonencode(Tag t, int idx, size_t offset, rapidjson::Value::AllocatorType& al) const = 0;

  /////////////////////////////
  //
  // Accessors.
  // These must work on an internalized collection, and we leave
  // it to the application to ensure internalization has happened.
  bool isInternalized() const {
    return internalized;
  }

  int size(const unit_t&) const {
    auto p = const_cast<FlatPolyBuffer*>(this);
    return static_cast<int>(vector_size(p->tags()));
  }

  Tag tag_at(int i) const {
    auto p = const_cast<FlatPolyBuffer*>(this);
    return *static_cast<Tag*>(vector_at(p->tags(), static_cast<size_t>(i)));
  }

  template<typename T>
  T at(int i, int offset) const {
    if (!internalized) { throw std::runtime_error ("Invalid at on externalized poly buffer"); }
    if ( i < size(unit_t{}) ) {
      auto p = const_cast<FlatPolyBuffer*>(this);
      return *reinterpret_cast<T*>(buffer_data(p->fixed()) + static_cast<size_t>(offset));
    } else {
      throw std::runtime_error("Invalid element access");
    }
  }

  template<typename T, typename F, typename G>
  auto safe_at(int i, int offset, F f, G g) const {
    if (!internalized) { throw std::runtime_error ("Invalid safe_at on externalized poly buffer"); }
    if ( i >= size(unit_t{}) ) {
      return f(unit_t {});
    } else {
      auto p = const_cast<FlatPolyBuffer*>(this);
      return g(*reinterpret_cast<T*>(buffer_data(p->fixed()) + static_cast<size_t>(offset)));
    }
  }

  // Apply a function on the tag and offset.
  template<typename Fun>
  unit_t iterate(Fun f) const {
    if (!internalized) { throw std::runtime_error ("Invalid iterate on externalized poly buffer"); }
    size_t foffset = 0, sz = size(unit_t{});
    for (size_t i = 0; i < sz; ++i) {
      Tag tg = tag_at(static_cast<int>(i));
      f(tg, i, static_cast<int>(foffset));
      foffset += elemsize(tg);
    }
    return unit_t {};
  }

  // Accumulate with the tag and offset.
  template <typename Fun, typename Acc>
  Acc foldl(Fun f, Acc acc) const {
    if (!internalized) { throw std::runtime_error ("Invalid foldl on externalized poly buffer"); }
    size_t foffset = 0, sz = size(unit_t{});
    for (size_t i = 0; i < sz; ++i) {
      Tag tg = tag_at(static_cast<int>(i));
      acc = f(std::move(acc), tg, i, static_cast<int>(foffset));
      foffset += elemsize(tg);
    }
    return acc;
  }

  template<typename Fun>
  unit_t traverse(Fun f) const {
    if (!internalized) { throw std::runtime_error ("Invalid traverse on externalized poly buffer"); }
    size_t foffset = 0, sz = size(unit_t{});
    size_t i = 0;
    // warning: could loop infinitely
    while (i < sz) {
      Tag tg = tag_at(static_cast<int>(i));
      R_key_value<int, int> next = f(tg, i, static_cast<int>(foffset));
      i = static_cast<size_t>(next.key);
      foffset = static_cast<size_t>(next.value);
    }
    return unit_t {};
  }

  template<typename Fun>
  auto traverse2(int idx, int offset, Fun f) const {
    if (!internalized) { throw std::runtime_error ("Invalid traverse on externalized poly buffer"); }
    size_t sz = size(unit_t{});
    // warning: could loop infinitely
    while (static_cast<size_t>idx < sz) {
      Tag tg = tag_at(idx);
      R_key_value<int, int> next = f(tg, idx, offset);
      // Check for end condition
      if (next.key == idx) break;
      idx = next.key;
      offset = next.value;
    }
    return R_key_value<int, int>{idx, offset};
  }

  //////////////////////////////////
  //
  // Tag-specific accessors.

  template<typename T, typename Fun>
  unit_t iterate_tag(Tag t, int i, int offset, Fun f) const {
    if (!internalized) { throw std::runtime_error ("Invalid iterate_tag on externalized poly buffer"); }
    auto p = buffer_data(const_cast<FlatPolyBuffer*>(this)->fixed());
    size_t foffset = static_cast<size_t>(offset);
    size_t sz = size(unit_t{});
    for (size_t j = i; j < sz && t == tag_at(static_cast<int>(j)); ++j) {
      f(static_cast<int>(j), static_cast<int>(foffset), *reinterpret_cast<T*>(p + foffset));
      foffset += elemsize(t);
    }
    return unit_t {};
  }

  template <typename T, typename Fun, typename Acc>
  Acc foldl_tag(Tag t, int i, int offset, Fun f, Acc acc) const {
    if (!internalized) { throw std::runtime_error ("Invalid foldl_tag on externalized poly buffer"); }
    auto p = buffer_data(const_cast<FlatPolyBuffer*>(this)->fixed());
    size_t foffset = static_cast<size_t>(offset);
    size_t sz = size(unit_t{});
    for (size_t j = i; j < sz && t == tag_at(static_cast<int>(j)); ++j) {
      acc = f(std::move(acc), static_cast<int>(j), static_cast<int>(foffset), *reinterpret_cast<T*>(p + foffset));
      foffset += elemsize(t);
    }
    return acc;
  }

  template<typename T>
  unit_t append(Tag tg, const T& t) {
    if (buffer.data()) {
      throw std::runtime_error("Invalid append on a FPB: backed by a base_string");
    }
    if (internalized) { throw std::runtime_error ("Invalid append on internalized poly buffer"); }

    FContainer* ncf = const_cast<FContainer*>(fixedc());
    VContainer* ncv = const_cast<VContainer*>(variablec());

    ExternalizerT etl(ncv, ExternalizerT::ExternalizeOp::Create);

    size_t offset = buffer_size(ncf);
    int status = buffer_insert(ncf, offset, reinterpret_cast<char*>(const_cast<T*>(&t)), sizeof(t));
    if ( status == 0 ) {
      reinterpret_cast<T*>(buffer_data(ncf)+offset)->externalize(etl);
      vector_push_back(tags(), const_cast<Tag*>(&tg));
    } else {
      throw std::runtime_error("Append failed on a FPB");
    }
    return unit_t {};
  }

  R_key_value<int, int> skip_tag(Tag t, int i, int offset) const {
    size_t sz = size(unit_t{});
    if ( i < sz && tag_at(i) == t ) {
      return R_key_value<int, int> { i+1, static_cast<int>(static_cast<size_t>(offset) + elemsize(t)) };
    }
    return R_key_value<int, int>{ i, offset };
  }

  R_key_value<int, int> skip_all_tag(Tag t, int i, int offset) const {
    size_t j = static_cast<size_t>(i);
    size_t foffset = static_cast<size_t>(offset);
    size_t sz = size(unit_t{});
    for (; j < sz && t == tag_at(static_cast<int>(j)); ++j) {
      foffset += elemsize(t);
    }
    return R_key_value<int, int> { j, static_cast<int>(foffset) };
  }

  // Clears a container
  unit_t clear(unit_t) {
    if (buffer.data()) { freeBackingBuffer(); }
    else {
      // keep the inner containers ie. no deallocation
      fixed()->size = 0;
      variable()->size = 0;
      tags()->buffer.size = 0;
      internalized=false;
    }
    return unit_t{};
  }

  // Reserve a certain number of elements and fixed size
  unit_t reserve(int num_elems, int fixed_size, int var_size) {
    if (!buffer.data()) {
      vector_reserve(tags(), num_elems);
      buffer_reserve(fixed(), fixed_size);
      buffer_reserve(variable(), var_size);
    }
  }

  // Externalizes an existing buffer, reusing the variable-length segment.
  unit_t repack(unit_t) {
    if ( !internalized ) {
      throw std::runtime_error("FlatPolyBuffer: Attempt to repack failed: already externalized");
    }

    size_t foffset = 0;
    FContainer* ncf = const_cast<FContainer*>(fixedc());
    VContainer* ncv = const_cast<VContainer*>(variablec());
    ExternalizerT etl(ncv, ExternalizerT::ExternalizeOp::Reuse);

    size_t sz = size(unit_t{});
    for (size_t i = 0; i < sz; ++i) {
      Tag tg = tag_at(static_cast<int>(i));
      externalize(etl, tg, buffer_data(ncf) + foffset);
      foffset += elemsize(tg);
    }
    internalized = false;
    return unit_t{};
  }

  // Internalizes an existing buffer if it is not already internalized.
  unit_t unpack(unit_t) {
    if ( internalized ) {
      throw std::runtime_error("FlatPolyBuffer: Attempt to unpack failed: already internalized");
    }

    size_t foffset = 0;
    FContainer* ncf = const_cast<FContainer*>(fixedc());
    VContainer* ncv = const_cast<VContainer*>(variablec());
    InternalizerT itl(ncv);

    size_t sz = size(unit_t{});
    for (size_t i = 0; i < sz; ++i) {
      Tag tg = tag_at(static_cast<int>(i));
      internalize(itl, tg, buffer_data(ncf) + foffset);
      foffset += elemsize(tg);
    }
    internalized = true;
    return unit_t{};
  }

  // Writes a flat poly buffer to a string.
  base_string save(unit_t) {
    FContainer* ncf = fixed();
    VContainer* ncv = variable();
    TContainer* nct = tags();

    // Reset element pointers to slot ids as necessary.
    bool internal = internalized;
    bool modified = false;
    if ( internalized ) {
      repack(unit_t{});
      modified = true;
    }

    size_t fixed_sz = fixseg_size();
    size_t var_sz   = varseg_size();
    size_t tags_sz  = tags_size();

    // 4 lengths + a bool (with gap to preserve alignment, see below)
    auto len = 5 * sizeof(size_t) + byte_size();
    auto buffer_ = new char[len+1];
    buffer_[len] = 0;

    size_t offsets[4] = { len - sizeof(size_t), fixed_sz, var_sz, tags_sz };
    memcpy(buffer_, &(offsets[0]), 4*sizeof(size_t));
    size_t offset = 4 * sizeof(size_t);

    // Stashing a bool (internal) messes up the 8-byte alignment of the pointers. Be sure to align next size_t at 8 byte offset.
    memcpy(buffer_ + offset, &internal, sizeof(bool));
    offset += sizeof(size_t); // We don't need to explicitly write 0's, we can leave the 'junk' as is.`

    if (fixed_sz > 0) {
      memcpy(buffer_ + offset, buffer_data(ncf), fixed_sz);
      offset += fixed_sz;
    }
    if (var_sz > 0) {
      memcpy(buffer_ + offset, buffer_data(ncv), var_sz);
      offset += var_sz;
    }
    if (tags_sz > 0) {
      memcpy(buffer_ + offset, vector_data(nct), tags_sz * sizeof(Tag));
    }

    base_string str;
    str.steal(buffer_);
    str.set_header(true);

    if (modified) {
      unpack(unit_t{});
    }
    return str;
  }

  unit_t load(const base_string& str) {
    return load(base_string(str));
  }

  // Restores a flat poly buffer from a string.
  unit_t load(base_string&& str) {
    assert( buffer_size(fixed()) == 0 );
    freeContainer();

    buffer = std::move(str);
    size_t offset = 0;

    size_t fixed_sz = *reinterpret_cast<size_t*>(buffer.begin() + offset);
    offset += sizeof(fixed_sz);

    size_t var_sz = *reinterpret_cast<size_t*>(buffer.begin() + offset);
    offset += sizeof(var_sz);

    size_t tags_sz = *reinterpret_cast<size_t*>(buffer.begin() + offset);
    offset += sizeof(tags_sz);

    bool make_internal = *reinterpret_cast<bool*>(buffer.begin() + offset);
    offset += sizeof(size_t); // Padding to preserve 8-byte alignment. See save().

    if (fixed_sz > 0) {
      fixed()->data     = buffer.begin() + offset;
      fixed()->size     = fixed_sz;
      fixed()->capacity = fixed_sz;
      offset += fixed_sz;
    }
    if (var_sz > 0) {
      variable()->data     = buffer.begin() + offset;
      variable()->size     = var_sz;
      variable()->capacity = var_sz;
      offset += var_sz;
    }
    if (tags_sz > 0) {
      tags()->buffer.data     = buffer.begin() + offset;
      tags()->buffer.size     = tags_sz * sizeof(Tag);
      tags()->buffer.capacity = tags()->buffer.size;
      tags()->object_size     = sizeof(Tag);
    }

    if (make_internal) {
      unpack(unit_t {});
    }
    return unit_t{};
  }

  Container& getContainer() { return container; }
  const Container& getConstContainer() const { return container; }

  template <class archive>
  void save(archive& a, const unsigned int) const {
    FContainer* ncf = fixed();
    VContainer* ncv = variable();
    TContainer* nct = tags();

    a.save_binary(&internalized, sizeof(internalized));

    // Reset element pointers to slot ids as necessary.
    auto p = const_cast<FlatPolyBuffer*>(this);
    bool modified = false;
    if ( p->internalized ) {
      p->repack(unit_t{});
      modified = true;
    }

    size_t fixed_sz = fixseg_size();
    size_t var_sz   = varseg_size();
    size_t tags_sz  = tags_size();

    a.save_binary(&fixed_sz, sizeof(fixed_sz));
    a.save_binary(&var_sz,   sizeof(var_sz));
    a.save_binary(&tags_sz,  sizeof(tags_sz));

    if (fixed_sz > 0) {
      a.save_binary(buffer_data(ncf), fixed_sz);
    }
    if (var_sz > 0) {
      a.save_binary(buffer_data(ncv), var_sz);
    }
    if (tags_sz > 0) {
      a.save_binary(vector_data(nct), tags_sz * sizeof(Tag));
    }

    if (modified) {
      p->unpack(unit_t{});
    }
  }

  template <class archive>
  void load(archive& a, const unsigned int) {
    size_t fixed_sz;
    size_t var_sz;
    size_t tags_sz;
    bool should_internalize;

    a.load_binary(&should_internalize, sizeof(should_internalize));
    a.load_binary(&fixed_sz, sizeof(fixed_sz));
    a.load_binary(&var_sz,   sizeof(var_sz));
    a.load_binary(&tags_sz,  sizeof(tags_sz));

    if (fixed_sz > 0) {
      reserve_fixed(fixed_sz);
      fixed()->size = fixed_sz;
      a.load_binary(buffer_data(fixed()), fixed_sz);
    }
    if (var_sz > 0) {
      reserve_var(var_sz);
      variable()->size = var_sz;
      a.load_binary(buffer_data(variable()), var_sz);
    }
    if (tags_sz > 0) {
      reserve_tags(tags_sz);
      tags()->buffer.size = tags_sz * sizeof(Tag);
      a.load_binary(vector_data(tags()), tags_sz * sizeof(Tag));
    }

    if (should_internalize) {
      unpack(unit_t{});
    }
  }

  template <class archive>
  void serialize(archive& a) const {
    auto p = const_cast<FlatPolyBuffer*>(this);
    bool modified = false;

    a.write(&internalized, sizeof(internalized));

    if ( p->internalized ) {
      p->repack(unit_t {});
      modified = true;
    }

    FContainer* ncf = fixed();
    VContainer* ncv = variable();
    TContainer* nct = tags();

    size_t fixed_sz = fixseg_size();
    size_t var_sz   = varseg_size();
    size_t tags_sz  = tags_size();

    a.write(&fixed_sz, sizeof(fixed_sz));
    a.write(&var_sz, sizeof(var_sz));
    a.write(&tags_sz, sizeof(tags_sz));

    if (fixed_sz > 0) {
      a.write(buffer_data(ncf), fixed_sz);
    }
    if (var_sz > 0) {
      a.write(buffer_data(ncv), var_sz);
    }
    if (tags_sz > 0) {
      a.write(vector_data(nct), tags_sz * sizeof(Tag));
    }

    if (modified) {
      p->unpack(unit_t{});
    }
  }

  template <class archive>
  void serialize(archive& a) {
    size_t fixed_sz;
    size_t var_sz;
    size_t tags_sz;
    bool should_internalize;

    a.read(&should_internalize, sizeof(should_internalize));
    a.read(&fixed_sz, sizeof(fixed_sz));
    a.read(&var_sz, sizeof(var_sz));
    a.read(&tags_sz, sizeof(tags_sz));

    if (fixed_sz > 0) {
      reserve_fixed(fixed_sz);
      fixed()->size = fixed_sz;
      a.read(buffer_data(fixed()), fixed_sz);
    }
    if (var_sz > 0) {
      reserve_var(var_sz);
      variable()->size = var_sz;
      a.read(buffer_data(variable()), var_sz);
    }
    if (tags_sz > 0) {
      reserve_tags(tags_sz);
      tags()->buffer.size = tags_sz * sizeof(Tag);
      a.read(vector_data(tags()), tags_sz * sizeof(Tag));
    }

    if (should_internalize) {
      unpack(unit_t{});
    }
  }

  BOOST_SERIALIZATION_SPLIT_MEMBER()

protected:
  // FlatPolyBuffer is backed by either a base_string or a Container
  Container container;
  FContainer* fixed()    const { return const_cast<FContainer*>(&(container.cfixed)); }
  VContainer* variable() const { return const_cast<VContainer*>(&(container.cvariable)); }
  TContainer* tags()     const { return const_cast<TContainer*>(&(container.ctags)); }

  const FContainer* fixedc()    const { return &(container.cfixed); }
  const VContainer* variablec() const { return &(container.cvariable); }
  const TContainer* tagsc()     const { return &(container.ctags); }

  base_string buffer;
  bool internalized;
};

}; // end namespace Libdynamic

template<class Ignored>
using FlatPolyBuffer = Libdynamic::FlatPolyBuffer<Ignored>;

}; // end namespace K3


namespace YAML {
template <class E>
struct convert<K3::FlatPolyBuffer<E>> {
  using Tag = typename K3::FlatPolyBuffer<E>::Tag;
  static Node encode(const K3::FlatPolyBuffer<E>& c) {
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

  static bool decode(const Node& node, K3::FlatPolyBuffer<E>& c) {
    for (auto i : node) {
      c.yamldecode(i);
    }
    return true;
  }
};
}  // namespace YAML

namespace JSON {
using namespace rapidjson;
template <class E>
struct convert<K3::FlatPolyBuffer<E>> {
  using Tag = typename K3::FlatPolyBuffer<E>::Tag;
  template <class Allocator>
  static Value encode(const K3::FlatPolyBuffer<E>& c, Allocator& al) {
    bool modified = false;
    K3::FlatPolyBuffer<E>& non_const = const_cast<K3::FlatPolyBuffer<E>&>(c);
    if (!c.isInternalized()) {
      non_const.unpack(K3::unit_t {});
      modified = true;
    }

    Value v;
    v.SetObject();
    v.AddMember("type", Value("FlatPolyBuffer"), al);
    Value inner;
    inner.SetArray();
    c.iterate([&c, &inner, &al](Tag tg, size_t idx, size_t offset){
      inner.PushBack(c.jsonencode(tg, idx, offset, al), al);
    });
    v.AddMember("value", inner.Move(), al);

    if (modified) {
      non_const.repack(K3::unit_t {});
    }
    return v;
  }
};
}  // namespace JSON

#endif // HAS_LIBDYNAMIC

#endif
