#ifndef K3_RUNTIME_BASETYPES_H
#define K3_RUNTIME_BASETYPES_H

// Basic types needed by our builtin libraries

char *sdup (const char *s);

class Str {
  public:
  Str() : _buf(nullptr) {}

  // Move constructor
  Str(Str &&other) : Str() {
    swap(*this, other);
  }

  // Copy constructor
  Str(const Str &other) : _buf(sdup(other._buf)) {}

  Str& operator=(Str other) {
    // rely on copy for by-value
    swap(*this, other);
    return *this;
  }

  ~Str() { if (_buf) delete(_buf); }

  Str(const char *b) : _buf(sdup(b)) {}
  Str(const std::string &s) : _buf(sdup(s.c_str())) {}

  char *c_str() { return _buf; }

  friend void swap(Str& first, Str& second) {
    using std::swap;

    swap(first._buf, second._buf);
  }

  char *_buf;
};

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
        bool operator<(const R_addr& _r) const {
          return addr < _r.addr;
        }
        template <class archive>
        void serialize(archive& _archive,const unsigned int) {
            _archive & addr;

        }
        _T0 addr;
};
template <class T>
  std::size_t hash_value(R_addr<T> const& b) {
    boost::hash<T> hasher;
    return hasher(b.addr);
}

#endif // K3_R_addr

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
        bool operator<(const R_elem& _r) const {
            return elem < _r.elem;
        }
        template <class archive>
        void serialize(archive& _archive,const unsigned int) {
            _archive & elem;
        }
        _T0 elem;
};
template <class T>
  std::size_t hash_value(R_elem<T> const& b) {
    boost::hash<T> hasher;
    return hasher(b.elem);
}

#endif // K3_R_elem

#ifndef K3_R_key_value
#define K3_R_key_value
template <class _T0,class _T1>
class R_key_value {
    public:
        R_key_value() {}
        R_key_value(_T0 _key,_T1 _value): key(_key), value(_value) {}
        R_key_value(const R_key_value<_T0, _T1>& _r): key(_r.key), value(_r.value) {}
        bool operator==(const R_key_value& _r) const {
            if (key == _r.key&& value == _r.value)
                return true;
            return false;
        }
        bool operator<(const R_key_value& _r) const {
          return std::tie(key, value) < std::tie(_r.key, _r.value);
        }
        template <class archive>
        void serialize(archive& _archive,const unsigned int) {
            _archive & key;
            _archive & value;

        }
        _T0 key;
        _T1 value;

        typedef _T0 KeyType;
        typedef _T1 ValueType;
};

template <class K,class V>
  std::size_t hash_value(R_key_value<K,V> const& b) {
    boost::hash<std::tuple<K,V>> hasher;
    return hasher(std::tie(b.key, b.value));
}

#endif


#endif // K3_RUNTIME_BASETYPES_H
