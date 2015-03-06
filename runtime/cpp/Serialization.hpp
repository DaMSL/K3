#ifndef K3_RUNTIME_SERIALIZATION_H
#define K3_RUNTIME_SERIALIZATION_H

#include <sstream>

#include <boost/archive/binary_iarchive.hpp>
#include <boost/archive/binary_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <csvpp/csv.h>
#include <csvpp/string.h>

#include <Common.hpp>

using std::istringstream;
using std::ostringstream;

namespace K3 {

class Engine;

namespace BoostSerializer {

template <typename V, typename Archive>
string pack_archive(const V& v) {
  ostringstream out_sstream;
  Archive out_archive(out_sstream);
  out_archive << v;
  return out_sstream.str();
}

template <typename V, typename Archive>
shared_ptr<V> unpack_archive(const string& s) {
  istringstream in_sstream(s);
  Archive in_archive(in_sstream);

  V p;
  in_archive >> p;
  return std::make_shared<V>(std::move(p));
}

template <typename V, typename Archive>
shared_ptr<V> unpack_archive_with_engine(const string& s, Engine * eng) {
  istringstream in_sstream(s);
  Archive in_archive(in_sstream);

  V p(eng);
  in_archive >> p;
  return make_shared<V>(p);
}

// Binary packing.
template <typename V> string pack(const V& v) {
  return pack_archive<V,boost::archive::binary_oarchive>(v);
}

template <typename V> shared_ptr<V> unpack(const string& s) {
  return unpack_archive<V,boost::archive::binary_iarchive>(s);
}

template <typename V>
shared_ptr<V> unpack_with_engine(const string& s, Engine * eng) {
  return unpack_archive_with_engine<V,boost::archive::binary_iarchive>(s, eng);
}

// Text packing.
template <typename V> string pack_text(const V& v) {
  return pack_archive<V,boost::archive::text_oarchive>(v);
}

template <typename V> shared_ptr<V> unpack_text(const string& s) {
  return unpack_archive<V,boost::archive::text_iarchive>(s);
}

template <typename V>
shared_ptr<V> unpack_text_with_engine(const string& s, Engine * eng) {
  return unpack_archive_with_engine<V,boost::archive::text_iarchive>(s, eng);
}

// Csv packing.
template <typename V> string pack_csv(const V& v) {
  return pack_archive<V,csv::writer>(v);
}

template <typename V> shared_ptr<V> unpack_csv(const string& s) {
  return unpack_archive<V,csv::parser>(s);
}

template <typename V>
shared_ptr<V> unpack_csv_with_engine(const string& s, Engine * eng) {
  return unpack_archive_with_engine<V,csv::parser>(s, eng);
}

} } // BoostSerializer

namespace boost { namespace serialization {

template <unsigned int N>
struct tuple_serializer {
    template <class archive, class ... args>
    static void serialize(archive& a, std::tuple<args ...>& t, const unsigned int version) {
        tuple_serializer<N - 1>::serialize(a, t, version);
        a & std::get<N - 1>(t);
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


#endif // K3_RUNTIME_SERIALIZATION_H
