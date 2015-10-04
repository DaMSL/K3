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

  void externalize(int&, type<int>) { }
  void externalize(double&, type<double>) { }

  void externalize(base_string& str, type<base_string>)
  {
    if ( op == ExternalizeOp::Create ) {
      size_t offset = buffer_size(vcon);
      int status = buffer_insert(vcon, offset, str.data(), str.raw_length()+1);
      if ( status == 0 ) {
        intptr_t* p = reinterpret_cast<intptr_t*>(&str);
        *p = offset;
      }
      else {
        throw std::runtime_error("Invalid externalization operation");
      }
    }
    else if ( op == ExternalizeOp::Reuse ) {
      size_t offset = str.data() - buffer_data(vcon);
      intptr_t* p = reinterpret_cast<intptr_t*>(&str);
      *p = offset;
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

  void internalize(int&, type<int>) { }
  void internalize(double&, type<double>) { }

  void internalize(base_string& str) {
    intptr_t* p = reinterpret_cast<intptr_t*>(&str);
    size_t offset = static_cast<size_t>(*p);
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

  FlatPolyBuffer() : container(), internalized(false) {
    initContainer();
  }

  FlatPolyBuffer(const FlatPolyBuffer& other) : container(), buffer() {
    internalized = other.internalized;
    initContainer();
    copyContainer(other.container);
  }

  FlatPolyBuffer(FlatPolyBuffer&& other) : container(), buffer() {
    swapContainer(other);
    if ( other.buffer.data ) { buffer.swap(buffer, other.buffer); }
  }

  FlatPolyBuffer& operator=(const FlatPolyBuffer& other) {
    freeContainer();
    internalized = other.internalized;
    initContainer();
    copyContainer(other.container);
  }

  ~FlatPolyBuffer() {
    freeContainer();
  }

  void initContainer() {
    buffer_init(fixed());
    buffer_init(variable());
    vector_init(tags(), sizeof(Tag))
  }

  void freeContainer() {
    if (!buffer.data()) {
      buffer_clear(fixed());
      buffer_clear(variable());
      vector_clear(tags());
    }
  }

  void copyContainer(const Container& other) {
    bool otherModified = false;

    // Ensure the other container is externalized so that pointers are stored as offsets.
    if ( other.internalized ) {
      otherModified = true;
      other.repack(unit_t{});
    }

    // Copy the container.
    copyBuffer(fixed(), other.fixed());
    copyBuffer(variable(), other.variable());
    copyVector(tags(), other.tags());

    // Internalize this container to ensure its pointers are self-contained.
    unpack(unit_t{});
    if ( otherModified ) { other.unpack(unit_t{}); }
  }

  void swapContainer(Container&& other) {
    container.cfixed.size     = other.cfixed.size;
    container.cfixed.capacity = other.cfixed.capacity;
    std::swap(container.cfixed.data, other.cfixed.data);

    container.cvariable.size     = other.cvariable.size;
    container.cvariable.capacity = other.cvariable.capacity;
    std::swap(container.cvariable.data, other.cvariable.data);

    container.ctags.buffer.size     = other.ctags.buffer.size;
    container.ctags.buffer.capacity = other.ctags.buffer.capacity;
    container.ctags.object_size     = other.ctags.object_size;
    container.ctags.release         = other.ctags.release;
    std::swap(container.ctags.buffer.data, other.ctags.buffer.data);
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

  size_t byte_size()     const { return fixseg_size() + varseg_size() + tags_size * sizeof(Tag); }
  size_t byte_capacity() const { return fixseg_capacity() + varseg_capacity() + tags_capacity() * sizeof(Tag); }


  // Virtual methods to be implemented by instances.
  virtual size_t elemsize(Tag t) = 0;
  virtual void externalize(ExternalizerT& e, Tag t, char* data) = 0;
  virtual void internalize(InternalizerT& i, Tag t, char* data) = 0;


  // Accessors.
  int size(const unit_t&) const { return vector_size(tagsc()); }

  Tag tag_at(int i) const { return *static_cast<Tag*>(vector_at(tagsc(), i)); }

  template<typename T>
  T at(int i, size_t offset) {
    if ( i < size() ) {
      return *static_cast<T*>(buffer_data() + offset);
    } else {
      throw std::runtime_error("Invalid element access");
    }
  }

  template<typename T, typename F, typename G>
  auto safe_at(int i, size_t offset, F f, G f) {
    if ( i < size() ) {
      return f(*static_cast<T*>(buffer_data() + offset));
    } else {
      return g(unit_t {});
    }
  }

  // Apply a function with the element index and offset.
  template<typename Fun>
  unit_t iterate(Fun f) {
    size_f foffset = 0;
    for (int i = 0; i < size(); ++i) {
      Tag tg = tag_at(i);
      f(tg, foffset);
      foffset += elemsize(tg);
    }
    return unit_t {};
  }

  template<typename T>
  void append(Tag tg, T& t) {
    if (buffer.data()) {
      throw std::runtime_error("Invalid append on a FPB: backed by a base_string");
    }

    FContainer* ncf = const_cast<FContainer*>(fixedc());
    VContainer* ncv = const_cast<VContainer*>(variablec());

    ExternalizerT etl(ncv, ExternalizerT::ExternalizeOp::Create);
    InternalizerT itl(ncv);

    size_t offset = buffer_size(ncf);
    int status = buffer_insert(ncf, offset, const_cast<T*>(&t), sizeof(t));
    if ( status == 0 ) {
      reinterpret_cast<T*>(buffer_data(ncf)+offset)->externalize(etl).internalize(itl);
      vector_push_back(tags(), const_cast<Tag*>(&tg));
    } else {
      throw std::runtime_error("Append failed on a FPB");
    }
  }

  // Clears a container provided it is not backed by a string buffer.
  void clear() { freeContainer() }

  // Externalizes an existing buffer, reusing the variable-length segment.
  unit_t repack(unit_t) {
    size_f foffset = 0;
    FContainer* ncf = const_cast<FContainer*>(fixedc());
    VContainer* ncv = const_cast<VContainer*>(variablec());
    ExternalizerT etl(ncv, ExternalizerT::ExternalizeOp::Reuse);

    for (int i = 0; i < size(); ++i) {
      Tag tg = tag_at(i);
      externalize(etl, tg, buffer_data(ncf) + foffset);
      foffset += elemsize(tg);
    }
    internalized = false;
    return unit_t{};
  }

  // Internalizes an existing buffer if it is not already internalized.
  unit_t unpack(unit_t) {
    if ( !internalized ) {
      size_f foffset = 0;
      FContainer* ncf = const_cast<FContainer*>(fixedc());
      VContainer* ncv = const_cast<VContainer*>(variablec());
      InternalizerT itl(ncv);

      for (int i = 0; i < size(); ++i) {
        Tag tg = tag_at(i);
        internalize(itl, tg, buffer_data(ncf) + foffset);
        foffset += elemsize(tg);
      }
      internalized = true;
    }
    return unit_t{};
  }

  // Writes a flat poly buffer to a string.
  base_string save(unit_t) {
    FContainer* ncf = fixed();
    VContainer* ncv = variable();
    TContainer* nct = tags();

    // Reset element pointers to slot ids as necessary.
    repack(unit_t{});

    size_t fixed_sz = fixseg_size();
    size_t var_sz   = varseg_size();
    size_t tags_sz  = tags_size();

    auto len = 4 * sizeof(size_t) + byte_size();
    auto buffer_ = new char[len+1];
    buffer_[len] = 0;

    size_t offsets[4] = { len - sizeof(size_t), fixed_sz, var_sz, tags_sz };
    memcpy(buffer_, &(offsets[0]), 4*sizeof(size_t));

    size_t offset = 4 * sizeof(size_t);
    if (fixed_sz > 0) {
      memcpy(buffer_ + offset, buffer_data(ncf), fixed_sz);
      offset += fixed_sz;
    }
    if (var_sz > 0) {
      memcpy(buffer_ + offset, buffer_data(ncv), var_sz);
      offset += var_sz;
    }
    if (tag_sz > 0) {
      memcpy(buffer_ + offset, vector_data(nct), tags_sz * sizeof(Tag));
    }

    base_string str;
    str.steal(buffer_);
    str.set_header(true);

    unpack(unit_t{});
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

    size_t fixed_sz = *reinterpret_cast<size_t*>(buffer.begin());
    offset += sizeof(size_t);

    size_t var_sz = *reinterpret_cast<size_t*>(buffer.begin() + offset);
    offset += sizeof(size_t);

    size_t tags_sz = *reinterpret_cast<size_t*>(buffer.begin() + offset);
    offset += sizeof(size_t);

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
    if (tag_sz > 0) {
      tags()->buffer.data     = buffer.begin() + offset;
      tags()->buffer.size     = tag_sz * sizeof(Tag);
      tags()->buffer.capacity = tags()->buffer.size;
      tags()->object_size     = sizeof(Tag);
    }

    unpack(unit_t{});
    return unit_t{};
  }

private:
  // FlatPolyBuffer is backed by either a base_string or a Container
  Container container;
  FContainer* fixed()    { return &(container.cfixed); }
  VContainer* variable() { return &(container.cvariable); }
  TContainer* tags()     { return &(container.ctags); }

  const FContainer* fixedc()    const { return &(container.cfixed); }
  const VContainer* variablec() const { return &(container.cvariable); }
  const TContainer* tagsc()     const { return &(container.ctags); }

  base_string buffer;
  bool internalized;
};

}; // end namespace Libdynamic
}; // end namespace K3

#endif // HAS_LIBDYNAMIC

#endif