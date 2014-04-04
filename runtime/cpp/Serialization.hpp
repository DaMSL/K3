#ifndef K3_RUNTIME_SERIALIZATION_H
#define K3_RUNTIME_SERIALIZATION_H

#include <memory>
#include <sstream>
#include <string>

#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>

namespace K3 {

  using std::istringstream;
  using std::ostringstream;
  using std::shared_ptr;
  using std::string;

  class BoostSerializer {
    public:
      template <typename V>
      string pack(const V& v) {
        ostringstream out_sstream;
        boost::archive::text_oarchive out_archive(out_sstream);
        out_archive << v;
        return out_sstream.str();
      }

      template <typename V>
      shared_ptr<V> unpack(const string& s) {
        istringstream in_sstream(s);
        boost::archive::text_iarchive in_archive(in_sstream);

        shared_ptr<V> p;
        in_archive >> *p;
        return p;
      }
  };

}

#endif // K3_RUNTIME_SERIALIZATION_H
