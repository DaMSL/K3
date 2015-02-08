#ifndef K3_SERIALIZATION_JSON
#define K3_SERIALIZATION_JSON

#include "rapidjson/document.h"
#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"

#include <boost/asio.hpp>
#include <stdexcept>
#include <tuple>

namespace JSON {
using namespace rapidjson;
  template <class T>
  bool tCheck() { return false; }

  // Default
  template <class T>
  struct convert {
    template <class Allocator>
    static Value encode(const T& from, Allocator&) {
      // Force an error if no specialization is hit.
      static_assert(tCheck<T>(), "No Specialization found for type.");
    }
  };

  // Bool
  template <> struct convert<bool> {
    template <class Allocator>
    static Value encode(const bool& b, Allocator& al) {
      return Value(b);
    }
  };

  // Unit
  template <> struct convert<unit_t> {
    template <class Allocator>
    static Value encode(const unit_t&, Allocator& al) {
      return Value("()");
    }
  };
  // Int
  template <> struct convert<int> {
    template <class Allocator>
    static Value encode(const int& from, Allocator&) {
      return Value(from);
    }
  };

  // Unsigned Short
  template <> struct convert<unsigned short> {
    template <class Allocator>
    static Value encode(const unsigned short& from, Allocator&) {
      return Value(from);
    }
  };

  // Double
  template <> struct convert<double> {
    template <class Allocator>
    static Value encode(const double& from, Allocator&) {
      return Value(from);
    }
  };


  // Address
  template <> struct convert<boost::asio::ip::address> {
    template <class Allocator>
    static Value encode(const boost::asio::ip::address& from, Allocator& al) {
      Value v2;
      v2.SetString(from.to_string().c_str(), al);
      return v2;
    }

  };

  template <> struct convert<K3::Address> {
    template <class Allocator>
    static Value encode(const K3::Address& from, Allocator& al) {
      Value v;
      v.SetObject();
      v.AddMember("type", Value("address"), al);
      Value v2;
      v2.SetArray();
      v2.PushBack(convert<boost::asio::ip::address>::encode(std::get<0>(from), al), al);
      v2.PushBack(convert<unsigned short>::encode(std::get<1>(from), al), al);
      v.AddMember("value", v2.Move(), al);
      return v;
    }
  };


  // TODO: Recursive calls to encode rely on C++ overloading, which cannot distinguish between
  // our option and indirection types (since they are both shared_ptr). The easiest fix here is to
  // create two distrinct wrappers.
  // Option and Indirection
  template <class T> struct convert<std::shared_ptr<T>> {
    template <class Allocator>
    static Value encode(const std::shared_ptr<T>& from, Allocator& al) {
      Value v;
      v.SetObject();
      v.AddMember("type", Value("option_or_ind"), al);

      Value v2;
      if (from) {
        v2 = convert<T>::encode(*from, al);
      }
      v.AddMember("value", v2.Move(), al);
      return v;
    }
  };

  // Tuple
  template <class T, std::size_t I, std::size_t N> struct tuple_convert;

  template <class T, std::size_t I> struct tuple_convert<T, I, I> {
    template <class Allocator>
    static void encode(Value&, const T&, Allocator& ) {}
  };

  template <class T, std::size_t I, std::size_t N> struct tuple_convert {
    template <class Allocator>
    static void encode(Value& node, const T& t, Allocator& al) {
      auto v = std::get<I>(t);
      node.PushBack(convert<typename std::tuple_element<I, T>::type>::encode(v, al).Move(), al);
      tuple_convert<T, I + 1, N>::encode(node, t, al);
      return;
    }
  };

  template <class ... Ts> struct convert<std::tuple<Ts...>> {
    template <class Allocator>
    static Value encode(const std::tuple<Ts...>& t, Allocator& al) {
      Value inner;
      inner.SetArray();
      tuple_convert<std::tuple<Ts...>, 0, sizeof...(Ts)>::encode(inner, t, al);

      Value v;
      v.SetObject();
      v.AddMember("type", Value("tuple"), al);
      v.AddMember("value", inner.Move(), al);
      return v;
    }
  };
} // namespace JSON

namespace K3 {
  namespace serialization {

    using namespace rapidjson;
    struct json {
      template <class T>
      static std::string encode(const T& t) {
        StringBuffer buffer;
        Writer<StringBuffer> writer(buffer);
        Document d;
        rapidjson::Value v = JSON::convert<T>::encode(t, d.GetAllocator());
        v.Accept(writer);
        return std::string(buffer.GetString());
      }
    };

  }
}
#endif
