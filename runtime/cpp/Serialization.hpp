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

#endif // K3_RUNTIME_SERIALIZATION_H
