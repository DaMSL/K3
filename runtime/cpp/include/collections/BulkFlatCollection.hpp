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

//////////////////////////////////////////////////////
// BulkFlatCollection:
// a contiguous bulk-oriented collection class, that
// supports storage of flat (i.e., non-nested) elements.
//
template<class Elem, size_t PageSize = 2 << 21>
class BulkFlatCollection {
public:
  using FContainer = LibdynamicVector::vector;
  using VContainer = PageCollection<PageSize>;

  typedef struct {
    FContainer cfixed;
    VContainer cvariable;
  } Container;

  using ExternalizerT = Externalizer<PageSize>;
  using InternalizerT = Internalizer<PageSize>;

  BulkFlatCollection() : container() {
    vector_init(fixed(), sizeof(Elem));
    variable()->internalize(false);
    variable()->rewind();
  }

  BulkFlatCollection(const BulkFlatCollection& other) : container(), buffer() {
    vector_init(fixed(), sizeof(Elem));
    variable()->internalize(false);
    variable()->rewind();

    FContainer* me_fixed = fixed();
    FContainer* other_fixed = const_cast<FContainer*>(other.fixedc());

    auto os = vector_size(other_fixed);
    if ( fixseg_size() < os ) { reserve_fixed(os); }

    ExternalizerT etl(*variable(), ExternalizerT::ExternalizeOp::Create);
    InternalizerT itl(*variable());
    for (const auto& e : other) {
      vector_push_back(fixed(), const_cast<Elem*>(&e));
      reinterpret_cast<Elem*>(vector_back(fixed()))->externalize(etl).internalize(itl);
    }

    variable()->internalize(true);
  }

  BulkFlatCollection& operator=(const BulkFlatCollection& other) {
    freeContainer();

    vector_init(fixed(), sizeof(Elem));
    variable()->internalize(false);
    variable()->rewind();

    FContainer* me_fixed = fixed();
    FContainer* other_fixed = const_cast<FContainer*>(other.fixedc());

    auto os = vector_size(other_fixed);
    if ( fixseg_size() < os ) { reserve_fixed(os); }

    ExternalizerT etl(*variable(), ExternalizerT::ExternalizeOp::Create);
    InternalizerT itl(*variable());
    for (const auto& e : other) {
      vector_push_back(fixed(), const_cast<Elem*>(&e));
      reinterpret_cast<Elem*>(vector_back(fixed()))->externalize(etl).internalize(itl);
    }

    variable()->internalize(true);
    return *this;
  }

  // TODO(jbw) Move constructors for BFC and PageCollection

  ~BulkFlatCollection() {
    freeContainer();
  }

  void insert(const Elem& elem) {
    if (buffer.data()) {
      throw std::runtime_error("Invalid insert on a BFC: backed by a base_string");
    }
    variable()->internalize(false);
    ExternalizerT etl(*variable(), ExternalizerT::ExternalizeOp::Create);
    InternalizerT itl(*variable());
    vector_push_back(fixed(), const_cast<Elem*>(&elem));
    reinterpret_cast<Elem*>(vector_back(fixed()))->externalize(etl).internalize(itl);
    variable()->internalize(true);
  }

  void freeContainer() {
    if (!buffer.data()) {
      vector_clear(fixed());
    }
  }

  template <class V, class I>
  class bfc_iterator : public std::iterator<std::forward_iterator_tag, V> {
    using reference = typename std::iterator<std::forward_iterator_tag, V>::reference;

   public:
    template <class _I>
    bfc_iterator(FContainer* _m, _I&& _i)
        : m(_m), i(std::forward<_I>(_i)) {}

    bfc_iterator& operator++() {
      ++i;
      return *this;
    }

    bfc_iterator operator++(int) {
      bfc_iterator t = *this;
      *this++;
      return t;
    }

    auto operator -> () const { return static_cast<V*>(vector_at(m, i)); }
    auto& operator*() const { return *static_cast<V*>(vector_at(m, i)); }

    bool operator==(const bfc_iterator& other) const { return i == other.i; }
    bool operator!=(const bfc_iterator& other) const { return i != other.i; }

   private:
    FContainer* m;
    I i;
  };

  using iterator = bfc_iterator<Elem, size_t>;
  using const_iterator = bfc_iterator<const Elem, size_t>;

  iterator begin() { return iterator(fixed(), 0); }
  iterator end() { return iterator(fixed(), vector_size(fixed())); }

  const_iterator begin() const { return const_iterator(const_cast<FContainer*>(fixedc()), 0); }
  const_iterator end() const { return const_iterator(const_cast<FContainer*>(fixedc()), vector_size(const_cast<FContainer*>(fixedc()))); }

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

      ExternalizerT etl(*variable(), ExternalizerT::ExternalizeOp::Create);
      InternalizerT itl(*variable());
      for (auto& e : other) {
        vector_push_back(fixed(), const_cast<Elem*>(&(e.elem)));
        reinterpret_cast<Elem*>(vector_back(fixed()))->externalize(etl).internalize(itl);
      }
    } else {
      throw std::runtime_error("Invalid append on a BulkFlatCollection");
    }
    variable()->internalize(true);
    return unit_t {};
  }

  // Externalizes an existing collection, reusing the variable-length segment.
  unit_t repack(unit_t) {
    FContainer* ncf = const_cast<FContainer*>(fixedc());
    VContainer* ncv = const_cast<VContainer*>(variablec());

    if ( ncv->internalized() ) {
      ExternalizerT etl(*ncv, ExternalizerT::ExternalizeOp::Reuse);
      auto sz = vector_size(ncf);
      for (size_t i = 0; i < sz; ++i) {
        reinterpret_cast<Elem*>(vector_at(ncf, i))->externalize(etl);
      }
      ncv->internalize(false);
    }

    return unit_t{};
  }

  unit_t unpack(unit_t) {
    FContainer* ncf = const_cast<FContainer*>(fixedc());
    VContainer* ncv = const_cast<VContainer*>(variablec());

    if ( !ncv->internalized() ) {
      InternalizerT itl(*ncv);
      auto sz = vector_size(ncf);
      for (size_t i = 0; i < sz; ++i) {
        reinterpret_cast<Elem*>(vector_at(ncf, i))->internalize(itl);
      }
      ncv->internalize(true);
    }

    return unit_t{};
  }

  base_string save(unit_t) {
    FContainer* ncf = fixed();
    VContainer* ncv = variable();

    // Reset element pointers to slot ids as necessary.
    repack(unit_t{});

    uint64_t fixed_count = fixseg_size();
    uint64_t page_count = varseg_size();

    auto len = sizeof(size_t) + 2 * sizeof(uint64_t) + byte_size();
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

    unpack(unit_t{});
    return str;
  }

  unit_t load(const base_string& str) {
    return load(base_string(str));
  }

  unit_t load(base_string&& str) {
    assert( vector_empty(fixed()) );
    freeContainer();

    buffer = std::move(str);
    size_t offset = 0;

    uint64_t fixed_count = *reinterpret_cast<uint64_t*>(buffer.begin());
    offset += sizeof(uint64_t);

    uint64_t page_count = *reinterpret_cast<uint64_t*>(buffer.begin() + offset);
    offset += sizeof(uint64_t);

    variable()->externalBuffer(true);

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

    unpack(unit_t{});
    return unit_t{};
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
    auto sz = fixseg_size();
    for (size_t i = 0; i < sz; ++i) {
      auto& e = reinterpret_cast<Elem*>(vector_at(ncf, i));
      f(e);
    }
    return unit_t{};
  }

  // Produce a new collection by mapping a function over this external collection.
  template <typename Fun>
  auto map(Fun f) const -> BulkFlatCollection<R_elem<RT<Fun, Elem>>> {
    FContainer* ncf = const_cast<FContainer*>(fixedc());
    VContainer* ncv = const_cast<VContainer*>(variablec());
    BulkFlatCollection<R_elem<RT<Fun, Elem>>> result;
    auto sz = fixseg_size();
    for (size_t i = 0; i < sz; ++i) {
      auto& e = reinterpret_cast<Elem*>(vector_at(ncf, i));
      result.insert(R_elem<RT<Fun, Elem>>{ f(e) });
    }
    return result;
  }

  // Create a new collection consisting of elements from this set that satisfy the predicate.
  template <typename Fun>
  BulkFlatCollection<R_elem<Elem>> filter(Fun predicate) const {
    FContainer* ncf = const_cast<FContainer*>(fixedc());
    VContainer* ncv = const_cast<VContainer*>(variablec());
    BulkFlatCollection<R_elem<Elem>> result;
    auto sz = fixseg_size();
    for (size_t i = 0; i < sz; ++i) {
      auto& e = reinterpret_cast<Elem*>(vector_at(ncf, i));
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
    auto sz = fixseg_size();
    for (size_t i = 0; i < sz; ++i) {
      auto& e = reinterpret_cast<Elem*>(vector_at(ncf, i));
      acc = f(std::move(acc), e);
    }
    return acc;
  }

  // TODO(jbw) group_by

  Container& getContainer() { return container; }
  const Container& getConstContainer() const { return container; }

  template <class archive>
  void save(archive& a, const unsigned int) const {
    auto p = const_cast<BulkFlatCollection*>(this);
    p->repack(unit_t {});

    uint64_t fixed_count = fixseg_size();
    uint64_t page_count = varseg_size();
    a.save_binary(&fixed_count, sizeof(fixed_count));
    a.save_binary(&page_count, sizeof(page_count));
    if (fixed_count > 0) {
      a.save_binary(vector_data(const_cast<FContainer*>(fixedc())), fixseg_size() * sizeof(Elem));
    }
    if (page_count > 0) {
      a.save_binary(const_cast<VContainer*>(variablec())->data(), varseg_size() * PageSize);
    }

    p->unpack(unit_t{});
  }

  template <class archive>
  void load(archive& a, const unsigned int) {
    uint64_t fixed_count;
    uint64_t page_count;
    a.load_binary(&fixed_count, sizeof(fixed_count));
    a.load_binary(&page_count, sizeof(page_count));
    if (fixed_count > 0) {
      reserve_fixed(fixed_count);
      fixed()->buffer.size = fixed_count * sizeof(Elem);
      a.load_binary(vector_data(fixed()), fixed_count * sizeof(Elem));
    }
    if (page_count > 0) {
      variable()->resize(page_count);
      a.load_binary(variable()->data(), page_count * PageSize);
    }

    unpack(unit_t{});
  }

  template <class archive>
  void serialize(archive& a) const {
    auto p = const_cast<BulkFlatCollection*>(this);
    p->repack(unit_t {});

    uint64_t fixed_count = fixseg_size();
    uint64_t page_count = varseg_size();
    a.write(&fixed_count, sizeof(fixed_count));
    a.write(&page_count, sizeof(page_count));
    if (fixed_count > 0) {
      a.write(vector_data(const_cast<FContainer*>(fixedc())), fixseg_size() * sizeof(Elem));
    }
    if (page_count > 0) {
      a.write(const_cast<VContainer*>(variablec())->data(), varseg_size() * PageSize);
    }

    p->unpack(unit_t{});
  }

  template <class archive>
  void serialize(archive& a) {
    uint64_t fixed_count;
    uint64_t page_count;
    a.read(&fixed_count, sizeof(fixed_count));
    a.read(&page_count, sizeof(page_count));
    if (fixed_count > 0) {
      reserve_fixed(fixed_count);
      fixed()->buffer.size = fixed_count * sizeof(Elem);
      a.read(vector_data(fixed()), fixed_count * sizeof(Elem));
    }
    if (page_count > 0) {
      variable()->resize(page_count);
      a.read(variable()->data(), page_count * PageSize);
    }

    unpack(unit_t{});
  }

  BOOST_SERIALIZATION_SPLIT_MEMBER()

private:
  // BulkFlatCollection is backed by either a base_string or a Container
  Container container;
  FContainer* fixed()    { return &(container.cfixed); }
  VContainer* variable() { return &(container.cvariable); }

  const FContainer* fixedc()    const { return &(container.cfixed); }
  const VContainer* variablec() const { return &(container.cvariable); }

  base_string buffer;
};

}; // end namespace Libdynamic

template<class Elem, size_t PageSize = 2 << 21>
using BulkFlatCollection = Libdynamic::BulkFlatCollection<Elem, PageSize>;

}; // end namespace K3


namespace YAML {
template <class E>
struct convert<K3::BulkFlatCollection<E>> {
  static Node encode(const K3::BulkFlatCollection<E>& c) {
    Node node;
    bool flag = true;
    for (const auto& i : c) {
      if (flag) { flag = false; }
      node.push_back(convert<E>::encode(i));
    }
    if (flag) {
      node = YAML::Load("[]");
    }
    return node;
  }

  static bool decode(const Node& node, K3::BulkFlatCollection<E>& c) {
    K3::Collection<R_elem<E>> tmp;
    for (auto& i : node) {
      tmp.insert(i.as<E>());
    }
    c.append(tmp);
    return true;
  }
};
}  // namespace YAML

namespace JSON {
using namespace rapidjson;
template <class E>
struct convert<K3::BulkFlatCollection<E>> {
  template <class Allocator>
  static Value encode(const K3::BulkFlatCollection<E>& c, Allocator& al) {
    Value v;
    v.SetObject();
    v.AddMember("type", Value("BulkFlatCollection"), al);
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


#endif // HAS_LIBDYNAMIC

#endif
