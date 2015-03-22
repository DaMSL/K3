#ifndef K3_RUNTIME_SERIALIZATION_H
#define K3_RUNTIME_SERIALIZATION_H

#include <sstream>

#include <boost/archive/binary_iarchive.hpp>
#include <boost/archive/binary_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/archive/xml_iarchive.hpp>
#include <boost/archive/xml_oarchive.hpp>
#include <boost/serialization/nvp.hpp>
#include <yas/mem_streams.hpp>
#include <yas/binary_iarchive.hpp>
#include <yas/binary_oarchive.hpp>
#include <yas/text_iarchive.hpp>
#include <yas/text_oarchive.hpp>
#include <csvpp/csv.h>
#include <csvpp/string.h>

#include <Common.hpp>

using std::istringstream;
using std::ostringstream;

namespace K3 {

class Engine;

namespace K3Serializer {

/////////////////////////
//
// Boost archive serdes.

template <typename V, typename Archive, typename... ArchiveArgs>
inline string pack_archive(const V& v, ArchiveArgs... args) {
  ostringstream out_sstream;
  Archive out_archive(out_sstream, args...);
  out_archive << boost::serialization::make_nvp(typeid(v).name(), v);
  return out_sstream.str();
}

template <typename V, typename Archive, typename... ArchiveArgs>
inline shared_ptr<V> unpack_archive(const string& s, ArchiveArgs... args) {
  istringstream in_sstream(s);
  Archive in_archive(in_sstream, args...);

  V p;
  in_archive >> boost::serialization::make_nvp(typeid(p).name(), p);
  return std::make_shared<V>(std::move(p));
}

template <typename V, typename Archive, typename... ArchiveArgs>
inline shared_ptr<V> unpack_archive_with_engine(const string& s, Engine* eng, ArchiveArgs... args) {
  istringstream in_sstream(s);
  Archive in_archive(in_sstream, args...);

  V p(eng);
  in_archive >> boost::serialization::make_nvp(typeid(p).name(), p);
  return make_shared<V>(std::move(p));
}

// Binary packing.
template <typename V>
inline string pack(const V& v) {
  return pack_archive<V, boost::archive::binary_oarchive, boost::archive::archive_flags>(
            v, boost::archive::no_header);
}

template <typename V>
inline shared_ptr<V> unpack(const string& s) {
  return unpack_archive<V, boost::archive::binary_iarchive, boost::archive::archive_flags>(
            s, boost::archive::no_header);
}

template <typename V>
inline shared_ptr<V> unpack_with_engine(const string& s, Engine * eng) {
  return unpack_archive_with_engine<V, boost::archive::binary_iarchive, boost::archive::archive_flags>(
            s, eng, boost::archive::no_header);
}

// Text packing.
template <typename V>
inline string pack_text(const V& v) {
  return pack_archive<V, boost::archive::text_oarchive, boost::archive::archive_flags>(
            v, boost::archive::no_header);
}

template <typename V>
inline shared_ptr<V> unpack_text(const string& s) {
  return unpack_archive<V, boost::archive::text_iarchive, boost::archive::archive_flags>(
            s, boost::archive::no_header);
}

template <typename V>
inline shared_ptr<V> unpack_text_with_engine(const string& s, Engine * eng) {
  return unpack_archive_with_engine<V, boost::archive::text_iarchive, boost::archive::archive_flags>(
            s, eng, boost::archive::no_header);
}

// Csv packing.
template <typename V>
inline string pack_csv(const V& v, char sep=',') {
  return pack_archive<V,csv::writer>(v, sep);
}

template <typename V>
inline shared_ptr<V> unpack_csv(const string& s, char sep=',') {
  return unpack_archive<V,csv::parser>(s, sep);
}

template <typename V>
inline shared_ptr<V> unpack_csv_with_engine(const string& s, Engine * eng) {
  return unpack_archive_with_engine<V,csv::parser>(s, eng);
}

// Xml packing.
template <typename V>
inline string pack_xml(const V& v) {
  return pack_archive<V, boost::archive::xml_oarchive, boost::archive::archive_flags>(
            v, boost::archive::no_header);
}

template <typename V>
inline shared_ptr<V> unpack_xml(const string& s) {
  return unpack_archive<V, boost::archive::xml_iarchive, boost::archive::archive_flags>(
            s, boost::archive::no_header);
}

template <typename V>
inline shared_ptr<V> unpack_xml_with_engine(const string& s, Engine * eng) {
  return unpack_archive_with_engine<V, boost::archive::xml_iarchive, boost::archive::archive_flags>(
            s, eng, boost::archive::no_header);
}


/////////////////////////
//
// Yas archive serdes.

template<typename OS, typename yarchive, typename barchive>
struct YBOArchive : public yarchive
{
  using stream_type = OS;
  using this_type = YBOArchive<OS,yarchive,barchive>;

  YBOArchive(OS& _os)
    : yarchive(_os, yas::no_header), os(_os), bss(), b(bss, boost::archive::no_header)
  {}

  string str() {
    const yas::intrusive_buffer buf = os.get_intrusive_buffer();
    std::string r = bss.str();
    size_t bsz = r.size();
    r.reserve(bsz+buf.size+sizeof(size_t));
    r.append(buf.data, buf.size);
    r.append(reinterpret_cast<char*>(&bsz), sizeof(size_t));
    return r;
  }

  template<class T>
  this_type& operator&(const T& v) {
    using namespace yas::detail;
    return serializer<
       type_properties<T>::value
      ,serialization_method<T, this_type>::value
      ,yas::archive_type::binary
      ,yas::direction::out
      ,T
    >::apply(*this, v);
  }

  template<class T>
  this_type& operator&(const std::shared_ptr<T>& sp) {
    T* p = sp.get();
    b & p;
  }

  this_type& serialize() { return *this; }

  template<typename Head, typename... Tail>
  this_type& serialize(const Head& head, const Tail&... tail) {
    return operator&(head).serialize(tail...);
  }

  template<typename... Args>
  this_type& operator()(const Args&... args) {
    return serialize(args...);
  }

private:
  OS& os;
  ostringstream bss;
  barchive b;
};

template<typename IS, typename yarchive, typename barchive>
struct YBIArchive : public yarchive
{
  using stream_type = IS;
  using this_type = YBIArchive<IS,yarchive,barchive>;

  YBIArchive(const string& s, IS& _is)
    : yarchive(_is, yas::no_header), is(_is), bss(s), b(bss, boost::archive::no_header)
  {}

  template<typename T>
  this_type& operator& (T& v) {
    using namespace yas::detail;
    return serializer<
       type_properties<T>::value
      ,serialization_method<T, this_type>::value
      ,yas::archive_type::binary
      ,yas::direction::in
      ,T
    >::apply(*this, v);
  }

  template<class T>
  this_type& operator&(std::shared_ptr<T>& sp) {
    T* p;
    b & p;
    sp = std::shared_ptr<T>(p);
  }

  this_type& serialize() { return *this; }

  template<typename Head, typename... Tail>
  this_type& serialize(Head& head, Tail&... tail) {
    return operator&(head).serialize(tail...);
  }

  template<typename... Args>
  this_type& operator()(Args&... args) {
    return serialize(args...);
  }

  static yas::intrusive_buffer make_buffer(const string& s) {
    size_t bsz = *reinterpret_cast<const size_t*>(s.data()+(s.size() - sizeof(size_t)));
    return yas::intrusive_buffer(s.data()+bsz, (s.size() - (sizeof(size_t) + bsz)));
  }

private:
  IS& is;
  istringstream bss;
  boost::archive::binary_iarchive b;
};

using K3YBOArchive  = YBOArchive<yas::mem_ostream, yas::binary_oarchive<yas::mem_ostream>, boost::archive::binary_oarchive>;
using K3YBIArchive  = YBIArchive<yas::mem_istream, yas::binary_iarchive<yas::mem_istream>, boost::archive::binary_iarchive>;
using K3YBTOArchive = YBOArchive<yas::mem_ostream, yas::text_oarchive<yas::mem_ostream>, boost::archive::text_oarchive>;
using K3YBTIArchive = YBIArchive<yas::mem_istream, yas::text_iarchive<yas::mem_istream>, boost::archive::text_iarchive>;

// Binary yas packing.
template <typename V>
inline string pack_yas(const V& v) {
  yas::mem_ostream os;
  K3YBOArchive out_archive(os);
  out_archive & v;
  return out_archive.str();
}

template <typename V>
inline shared_ptr<V> unpack_yas(const string& s) {
  yas::intrusive_buffer sbuf = K3YBIArchive::make_buffer(s);
  yas::mem_istream is(sbuf);
  K3YBIArchive in_archive(s, is);

  V p;
  in_archive & p;
  return std::make_shared<V>(std::move(p));
}

template <typename V>
inline shared_ptr<V> unpack_yas_with_engine(const string& s, Engine* eng) {
  yas::intrusive_buffer sbuf = K3YBIArchive::make_buffer(s);
  yas::mem_istream is(sbuf);
  K3YBIArchive in_archive(s, is);

  V p(eng);
  in_archive & p;
  return std::make_shared<V>(std::move(p));
}

// Text yas packing.
template <typename V>
inline string pack_yas_text(const V& v) {
  yas::mem_ostream os;
  K3YBTOArchive out_archive(os);
  out_archive & v;
  return out_archive.str();
}

template <typename V>
inline shared_ptr<V> unpack_yas_text(const string& s) {
  yas::intrusive_buffer sbuf = K3YBTIArchive::make_buffer(s);
  yas::mem_istream is(sbuf);
  K3YBTIArchive in_archive(s, is);

  V p;
  in_archive & p;
  return std::make_shared<V>(std::move(p));
}

template <typename V>
inline shared_ptr<V> unpack_yas_text_with_engine(const string& s, Engine* eng) {
  yas::intrusive_buffer sbuf = K3YBTIArchive::make_buffer(s);
  yas::mem_istream is(sbuf);
  K3YBTIArchive in_archive(s, is);

  V p(eng);
  in_archive & p;
  return std::make_shared<V>(std::move(p));
}


} } // K3Serializer

namespace boost { namespace serialization {

template <unsigned int N>
struct tuple_serializer {
    template <class archive, class ... args>
    static void serialize(archive& a, std::tuple<args ...>& t, const unsigned int version) {
        tuple_serializer<N - 1>::serialize(a, t, version);
        a & make_nvp(typeid(std::get<N - 1>(t)).name(), std::get<N - 1>(t));
    }
};

template <>
struct tuple_serializer<0> {
    template <class archive, class ... args>
    static void serialize(archive&, std::tuple<args ...>&, const unsigned int) {}
};

template <class archive, class ... args>
void serialize(archive& a, std::tuple<args ...>& t, const unsigned int version) {
    tuple_serializer<sizeof ... (args)>::serialize(a, t, version);
}

// Elide object class information for std::pair and std::tuple serialization
template <class _T0, class _T1>
class implementation_level<std::pair<_T0, _T1>> {
  public:
    typedef  mpl::integral_c_tag tag;
    typedef  mpl::int_<object_serializable> type;
    BOOST_STATIC_CONSTANT(int, value = implementation_level::type::value);
};

template <class... _T0>
class implementation_level<std::tuple<_T0...>> {
  public:
    typedef  mpl::integral_c_tag tag;
    typedef  mpl::int_<object_serializable> type;
    BOOST_STATIC_CONSTANT(int, value = implementation_level::type::value);
};

// Elide object class information for standard collection types.
template <class _T0>
class implementation_level<std::vector<_T0>> {
  public:
    typedef  mpl::integral_c_tag tag;
    typedef  mpl::int_<object_serializable> type;
    BOOST_STATIC_CONSTANT(int, value = implementation_level::type::value);
};

template <class _T0>
class implementation_level<std::list<_T0>> {
  public:
    typedef  mpl::integral_c_tag tag;
    typedef  mpl::int_<object_serializable> type;
    BOOST_STATIC_CONSTANT(int, value = implementation_level::type::value);
};

template <class _T0>
class implementation_level<std::tr1::unordered_set<_T0>> {
  public:
    typedef  mpl::integral_c_tag tag;
    typedef  mpl::int_<object_serializable> type;
    BOOST_STATIC_CONSTANT(int, value = implementation_level::type::value);
};

template <class _T0>
class implementation_level<std::multiset<_T0>> {
  public:
    typedef  mpl::integral_c_tag tag;
    typedef  mpl::int_<object_serializable> type;
    BOOST_STATIC_CONSTANT(int, value = implementation_level::type::value);
};

template <class _T0, class _T1>
class implementation_level<std::tr1::unordered_map<_T0, _T1>> {
  public:
    typedef  mpl::integral_c_tag tag;
    typedef  mpl::int_<object_serializable> type;
    BOOST_STATIC_CONSTANT(int, value = implementation_level::type::value);
};


} } // boost::serialization

#include <boost/serialization/split_free.hpp>

namespace boost { namespace serialization {

// For asio ip address we don't have access to the member, so we'll do a separate save and load
template<class archive>
void save(archive& ar, const asio::ip::address& ip, unsigned int) {
  std::string s = ip.to_string();
  ar << s;
}

template<class archive>
void load(archive& ar, asio::ip::address& ip, unsigned int) {
  std::string s;
  ar >> s;
  ip = asio::ip::address::from_string(s);
}

// For std::shared_ptr
template<class archive, class T>
void save(archive& ar, const std::shared_ptr<T>& sp, unsigned int) {
  T* p = sp.get();
  ar << p;
}

template<class archive, class T>
void load(archive& ar, std::shared_ptr<T>& sp, unsigned int) {
  T* p;
  ar >> p;
  sp = std::shared_ptr<T>(p);
}

}} // boost::serialization

// For address, this does the trick
BOOST_SERIALIZATION_SPLIT_FREE(boost::asio::ip::address);

namespace boost { namespace serialization {
// For std::shared_ptr, we need to be more explicit
template<class Archive, class T>
inline void serialize(Archive & ar, std::shared_ptr<T>& t, const unsigned int file_version)
{
    split_free(ar, t, file_version);
}
}} // boost::serialization


////////////////////////////////////
//
// Yas serialization for K3 types.
//
// TODO: shared pointers
namespace yas { namespace detail {

template<typename archive_type>
void serialize(archive_type& ar, const boost::asio::ip::address& ip) {
  std::string s = ip.to_string();
  ar & s;
}

template<typename archive_type>
void serialize(archive_type& ar, boost::asio::ip::address& ip) {
  std::string s;
  ar & s;
  ip = boost::asio::ip::address::from_string(s);
}

template<typename archive_type, typename T>
void serialize(archive_type& ar, const boost::serialization::nvp<T>& nvp) {
  ar & nvp.const_value();
}

template<typename archive_type, typename T>
void serialize(archive_type& ar, boost::serialization::nvp<T>& nvp) {
  ar & nvp.value();
}

} // namespace detail
} // namespace yas


#endif // K3_RUNTIME_SERIALIZATION_H
