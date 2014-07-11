#ifndef K3_RUNTIME_BASETYPES_H
#define K3_RUNTIME_BASETYPES_H

// Basic types needed by our builtin libraries

namespace K3 {

#ifndef R_elem
#define R_elem

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

#endif // R_elem

} // namespace K3

#endif // K3_RUNTIME_BASETYPES_H
