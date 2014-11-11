// yaml.hpp: Yaml serialization for K3 objects.

#ifndef K3_SERIALIZATION_YAML
#define K3_SERIALIZATION_YAML

#include <memory>
#include <string>

#include "yaml-cpp/yaml.h"

#include <boost/asio.hpp>

namespace YAML {
  template <class T, std::size_t I, std::size_t N> struct tuple_convert;

  template <class T, std::size_t I> struct tuple_convert<T, I, I> {
    static void encode(Node&, const T&) {}

    // We do not require that the YAML sequence representing a tuple be exactly the same length as
    // the tuple. Any list of at least the necessary length will do, as long as the individual
    // elements are type-correct.
    static bool decode(const Node&, T&) {
      return true;
    }
  };

  template <class T, std::size_t I, std::size_t N> struct tuple_convert {
    static void encode(Node& node, const T& t) {
      node.push_back(convert<typename std::tuple_element<I, T>::type>::encode(std::get<I>(t)));
      tuple_convert<T, I + 1, N>::encode(node, t);
      return;
    }

    static bool decode(const Node& node, T& t) {
      std::get<I>(t) = node[I].as<typename std::tuple_element<I, T>::type>();
      return tuple_convert<T, I + 1, N>::decode(node, t);
    }
  };

  template <class ... Ts> struct convert<std::tuple<Ts...>> {
    static Node encode(const std::tuple<Ts...>& t) {
      Node node;
      tuple_convert<std::tuple<Ts...>, 0, sizeof...(Ts)>::encode(node, t);
      return node;
    }

    static bool decode(const Node& node, std::tuple<Ts...>& t) {
      return tuple_convert<std::tuple<Ts...>, 0, sizeof...(Ts)>::decode(node, t);
    }
  };

  template <> struct convert<boost::asio::ip::address> {
    static Node encode(const boost::asio::ip::address& a) {
      Node node;
      node = a.to_string();
      return node;
    }

    static bool decode(const Node& node, boost::asio::ip::address& a) {
      try {
        a.from_string(node.as<std::string>());
        return true;
      } catch (YAML::TypedBadConversion<std::string> e) {
        return false;
      }
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
}

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

  }
}

#endif
