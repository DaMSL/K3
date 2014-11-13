#ifndef K3_RUNTIME_BASETYPES_H
#define K3_RUNTIME_BASETYPES_H

#include "boost/functional/hash.hpp"
#include "serialization/yaml.hpp"
// Basic types needed by our builtin libraries

class unit_t {
  public:
  template <class archive>
  void serialize(archive&, const unsigned int) {}
  bool operator==(const unit_t& r) const { return true; }
  bool operator!=(const unit_t& r) const { return false; }
  bool operator<(const unit_t& r) const { return false; }
  bool operator>(const unit_t& r) const { return false; }
};

namespace YAML {
  template <>
  class convert<unit_t> {
    public:
        static Node encode(const unit_t& r)  {
          Node node;
          return node;
        }
        static bool decode(const Node& node, unit_t& r)  {
          return true;
        }
  };
}


#ifndef K3_R_addr
#define K3_R_addr
template <class _T0>
class R_addr {
    public:
        R_addr() {}
        R_addr(_T0 _addr): addr(_addr) {}
        R_addr(const R_addr<_T0>& _r): addr(_r.addr) {}
        bool operator==(const R_addr& _r) const {
            if (addr == _r.addr)
                return true;
            return false;
        }
        bool operator!=(const R_addr& _r) const {
            return !(*this == _r);
        }
        bool operator<(const R_addr& _r) const {
          return addr < _r.addr;
        }
        template <class archive>
        void serialize(archive& _archive,const unsigned int) {
            _archive & addr;

        }
        _T0 addr;
};

#endif // K3_R_addr

#ifndef K3_R_addr_hash_value
#define K3_R_addr_hash_value
template <class T>
  std::size_t hash_value(R_addr<T> const& b) {
    boost::hash<T> hasher;
    return hasher(b.addr);
}
#endif // K3_R_addr_hash_value


#ifndef K3_R_elem
#define K3_R_elem

template <class _T0>
class R_elem {
    public:
        R_elem() {}
        R_elem(_T0 _elem): elem(_elem) {}
        R_elem(const R_elem<_T0>& _r): elem(_r.elem) {}
        bool operator==(const R_elem& _r) const {
            if (elem == _r.elem)
                return true;
            return false;
        }
        // TODO, beter implementation?
        bool operator!=(const R_elem& _r) const {
            return !(*this == _r);
        }
        bool operator<(const R_elem& _r) const {
            return elem < _r.elem;
        }
        template <class archive>
        void serialize(archive& _archive,const unsigned int) {
            _archive & elem;
        }
        _T0 elem;
};
#endif // K3_R_elem


#ifndef K3_R_elem_hash_value
#define K3_R_elem_hash_value
template <class T>
  std::size_t hash_value(R_elem<T> const& b) {
    boost::hash<T> hasher;
    return hasher(b.elem);
}
#endif // K3_R_elem_hash_value

#ifndef K3_R_key_value
#define K3_R_key_value

template <class _T0, class _T1>
class R_key_value {
  public:
      typedef _T0 KeyType;
      typedef _T1 ValueType;
      R_key_value(): key(), value()  {}
      template <class __T0, class __T1>
      R_key_value(__T0&& _key, __T1&& _value): key(std::forward<__T0>(_key)),
      value(std::forward<__T1>(_value))  {}
      R_key_value(const R_key_value<_T0, _T1>& __other): key(__other.key), value(__other.value)  {}
      R_key_value(R_key_value<_T0, _T1>&& __other): key(std::move(__other.key)),
      value(std::move(__other.value))  {}
      template <class archive>
      void serialize(archive& _archive)  {
        _archive & key;
        _archive & value;
      }
      R_key_value<_T0, _T1>& operator=(const R_key_value<_T0, _T1>& __other)  {
        key = (__other.key);
        value = (__other.value);
        return *(this);
      }
      R_key_value<_T0, _T1>& operator=(R_key_value<_T0, _T1>&& __other)  {
        key = std::move(__other.key);
        value = std::move(__other.value);
        return *(this);
      }
      bool operator==(const R_key_value<_T0, _T1>& __other) const {
        return key == (__other.key) && value == (__other.value);
      }
      bool operator!=(const R_key_value<_T0, _T1>& __other) const {
        return std::tie(key, value) != std::tie(__other.key, __other.value);
      }
      bool operator<(const R_key_value<_T0, _T1>& __other) const {
        return std::tie(key, value) < std::tie(__other.key, __other.value);
      }
      bool operator>(const R_key_value<_T0, _T1>& __other) const {
        return std::tie(key, value) > std::tie(__other.key, __other.value);
      }
      _T0 key;
      _T1 value;
};

#endif // K3_R_key_value

#ifndef K3_R_key_value_hash_value
#define K3_R_key_value_hash_value
template <class K,class V>
  std::size_t hash_value(R_key_value<K,V> const& b) {
    boost::hash<std::tuple<K,V>> hasher;
    return hasher(std::tie(b.key, b.value));
}
#endif // K3_R_key_value_hash_value


#endif // K3_RUNTIME_BASETYPES_H
