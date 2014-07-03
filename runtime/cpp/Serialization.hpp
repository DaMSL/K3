#ifndef K3_RUNTIME_SERIALIZATION_H
#define K3_RUNTIME_SERIALIZATION_H

#include <memory>
#include <sstream>
#include <string>

#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>

namespace K3 {

  using std::istringstream;
  using std::make_shared;
  using std::ostringstream;
  using std::shared_ptr;
  using std::string;

  namespace BoostSerializer {
      template <typename V>
      string& pack(const V& v) {
        ostringstream out_sstream;
        boost::archive::text_oarchive out_archive(out_sstream);
        out_archive << v;
        return out_sstream.str();
      }

      template <typename V>
      shared_ptr<V> unpack(const string& s) {
        istringstream in_sstream(s);
        boost::archive::text_iarchive in_archive(in_sstream);

        auto p = make_shared<V>();
        in_archive >> p;
        return p;
      }

      template <typename V>
      shared_ptr<V> unpack_with_engine(const string& s, Engine * eng) {
        istringstream in_sstream(s);
        boost::archive::text_iarchive in_archive(in_sstream);

        auto p = make_shared<V>(eng);
        in_archive >> p;
        return p;
      }
  }
}

namespace boost {
    namespace serialization {
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
    }
}

#endif // K3_RUNTIME_SERIALIZATION_H
