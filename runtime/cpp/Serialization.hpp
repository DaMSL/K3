#ifndef K3_RUNTIME_SERIALIZATION_H
#define K3_RUNTIME_SERIALIZATION_H

#include <sstream>

#include <boost/archive/binary_iarchive.hpp>
#include <boost/archive/binary_oarchive.hpp>

#include "Common.hpp"

using std::istringstream;
using std::ostringstream;

namespace K3 {

class Engine;

namespace BoostSerializer {

template <typename V>
string pack(const V& v) {
  ostringstream out_sstream;
  boost::archive::binary_oarchive out_archive(out_sstream);
  out_archive << v;
  return out_sstream.str();
}

template <typename V>
shared_ptr<V> unpack(const string& s) {
  istringstream in_sstream(s);
  boost::archive::binary_iarchive in_archive(in_sstream);

  V p;
  in_archive >> p;
  return make_shared<V>(p);
}

template <typename V>
shared_ptr<V> unpack_with_engine(const string& s, Engine * eng) {
  istringstream in_sstream(s);
  boost::archive::binary_iarchive in_archive(in_sstream);

  V p(eng);
  in_archive >> p;
  return make_shared<V>(p);
}

} } // namespaces

namespace boost { namespace serialization {

template <uint N>
struct tuple_serializer {
    template <class archive, class ... args>
    static void serialize(archive& a, std::tuple<args ...>& t, const unsigned int version) {
        a & std::get<N - 1>(t);
        tuple_serializer<N - 1>::serialize(a, t, version);
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

} } // namespaces

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
  ip.from_string(s);
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

}} // namespace boost/serialization

// For address, this does the trick
BOOST_SERIALIZATION_SPLIT_FREE(boost::asio::ip::address);

namespace boost { namespace serialization {
// For std::shared_ptr, we need to be more explicit
template<class Archive, class T>
inline void serialize(
    Archive & ar,
    std::shared_ptr<T>& t,
    const unsigned int file_version
){
    split_free(ar, t, file_version);
}
}} // namespaces

#endif // K3_RUNTIME_SERIALIZATION_H
