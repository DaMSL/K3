#ifndef K3_SERIALIZATION_YAML
#define K3_SERIALIZATION_YAML

// Yaml encoding/decoding for K3 Values

#include <memory>
#include <string>
#include <tuple>

#include "boost/asio.hpp"

#include "yaml-cpp/yaml.h"

#include "Common.hpp"

namespace YAML {

template <class T, std::size_t I, std::size_t N>
struct tuple_convert;

template <class T, std::size_t I>
struct tuple_convert<T, I, I> {
  static void encode(Node&, const T&) {}

  // We do not require that the YAML sequence representing a tuple be exactly
  // the same length as
  // the tuple. Any list of at least the necessary length will do, as long as
  // the individual
  // elements are type-correct.
  static bool decode(const Node&, T&) { return true; }
};

template <class T, std::size_t I, std::size_t N>
struct tuple_convert {
  static void encode(Node& node, const T& t) {
    node.push_back(convert<typename std::tuple_element<I, T>::type>::encode(
        std::get<I>(t)));
    tuple_convert<T, I + 1, N>::encode(node, t);
    return;
  }

  static bool decode(const Node& node, T& t) {
    std::get<I>(t) = node[I].as<typename std::tuple_element<I, T>::type>();
    return tuple_convert<T, I + 1, N>::decode(node, t);
  }
};

template <class... Ts>
struct convert<std::tuple<Ts...>> {
  static Node encode(const std::tuple<Ts...>& t) {
    Node node;
    tuple_convert<std::tuple<Ts...>, 0, sizeof...(Ts)>::encode(node, t);
    return node;
  }

  static bool decode(const Node& node, std::tuple<Ts...>& t) {
    return tuple_convert<std::tuple<Ts...>, 0, sizeof...(Ts)>::decode(node, t);
  }
};

template <class T>
struct convert<std::shared_ptr<T>> {
  static Node encode(const std::shared_ptr<T>& p) {
    Node node;

    if (p) {
      node["ptr"] = convert<T>::encode(*p);
    } else {
      node = "none";
    }

    return node;
  }

  static bool decode(const Node& node, std::shared_ptr<T>& p) {
    if (!node.IsMap()) {
      p = nullptr;
      return true;
    }

    if (node.size() != 1 || !node["ptr"]) {
      return false;
    }

    p = std::make_shared<T>(node["ptr"].as<T>());
    return true;
  }
};

template <>
struct convert<::K3::Address> {
  static Node encode(const ::K3::Address& addr) {
    Node node;
    auto str = boost::asio::ip::address_v4(addr.ip).to_string();
    node.push_back(convert<std::string>::encode(str));
    node.push_back(convert<unsigned short>::encode(addr.port));
    return node;
  }

  static bool decode(const Node& node, K3::Address& addr) {
    if (!node.IsSequence()) {
      return false;
    }

    if (node.size() != 2) {
      return false;
    }

    auto str = node[0].as<std::string>();
    addr.ip = boost::asio::ip::address_v4::from_string(str).to_ulong();
    addr.port = node[1].as<unsigned short>();
    return true;
  }
};

template <>
struct convert<::K3::unit_t> {
  static Node encode(const ::K3::unit_t&) {
    Node node;
    return node;
  }

  static bool decode(const Node& node, K3::unit_t& addr) {
    return true;
  }
};

}  // namespace YAML

namespace K3 {
namespace serialization {

struct yaml {
  template <class T>
  static std::string encode(const T& t) {
    YAML::Node node = YAML::convert<T>::encode(t);

    YAML::Emitter out;
    out << YAML::Flow << node;

    return std::string(out.c_str());
  }

  template <class T>
  static T decode(const std::string& s) {
    YAML::Node node = YAML::Load(s);
    return node.as<T>();
  }
};

}  // namespace serialization
}  // namespace K3

#endif
