#ifndef K3_RUNTIME_BASETYPES_H
#define K3_RUNTIME_BASETYPES_H

// Basic types needed by our builtin libraries

#ifndef K3_R_elem
#define K3_R_elem

char *sdup (const char *s);

class Str {
  public:
  Str() : _buf(nullptr) {}
  Str(char *b) : _buf(sdup(b)) {}
  Str(const std::string &s) : _buf(sdup(s.c_str())) {}
  Str(Str &&other) : _buf(other._buf) {other._buf=nullptr;}
  Str(const Str &other) : _buf(sdup(other._buf)) {}
  Str& operator=(const Str& other) { _buf = (sdup(other._buf)); return *this;}
  char *c_str() { return _buf; }
  ~Str() { if (_buf) free(_buf); }

  char *_buf;
};


template <class _T0>
class R_elem {
    public:
        R_elem() {}
        R_elem(_T0 _elem): elem(_elem) {}
        R_elem(const R_elem<_T0>& _r): elem(_r.elem) {}
        bool operator==(R_elem _r) {
            if (elem == _r.elem)
                return true;
            return false;
        }
        template <class archive>
        void serialize(archive& _archive,const unsigned int) {
            _archive & elem;
        }
        _T0 elem;
};

#endif // K3_R_elem

#ifndef K3_R_key_value
#define K3_R_key_value
template <class _T0,class _T1>
class R_key_value {
    public:
        R_key_value() {}
        R_key_value(_T0 _key,_T1 _value): key(_key), value(_value) {}
        R_key_value(const R_key_value<_T0, _T1>& _r): key(_r.key), value(_r.value) {}
        bool operator==(R_key_value _r) {
            if (key == _r.key&& value == _r.value)
                return true;
            return false;
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
#endif

#endif // K3_RUNTIME_BASETYPES_H
