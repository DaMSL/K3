#ifndef K3_YAS
#define K3_YAS

#include <yas/mem_streams.hpp>
#include <yas/binary_iarchive.hpp>
#include <yas/binary_oarchive.hpp>
#include <yas/text_iarchive.hpp>
#include <yas/text_oarchive.hpp>
#include <yas/serializers/std_types_serializers.hpp>
#include <yas/serializers/boost_types_serializers.hpp>

namespace yas {
namespace detail {

template <typename archive_type, typename T>
void serialize(archive_type& ar, const std::shared_ptr<T>& sp) {
  bool b = sp ? 1 : 0;
  ar& b;
  if (b) {
    ar&* sp;
  }
}

template <typename archive_type, typename T>
void serialize(archive_type& ar, std::shared_ptr<T>& sp) {
  bool b;
  ar& b;
  if (b) {
    T t;
    ar& t;
    sp = std::make_shared<T>(std::move(t));
  }
}

template <typename archive_type, typename T>
void serialize(archive_type& ar, const boost::serialization::nvp<T>& nvp) {
  ar& nvp.const_value();
}

template <typename archive_type, typename T>
void serialize(archive_type& ar, boost::serialization::nvp<T>& nvp) {
  ar& nvp.value();
}

}  // namespace detail
}  // namespace yas

#endif
