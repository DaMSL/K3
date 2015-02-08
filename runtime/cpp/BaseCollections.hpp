#ifndef K3_RUNTIME_BASECOLLECTIONS_H
#define K3_RUNTIME_BASECOLLECTIONS_H

// Basic collection types needed by our builtin libraries

#ifndef K3__Collection
#define K3__Collection
template <class CONTENT>
class _Collection: public K3::Collection<CONTENT> {
 public:
  _Collection(): K3::Collection<CONTENT>() {}

  _Collection(const _Collection& c): K3::Collection<CONTENT>(c) {}

  _Collection(_Collection&& c): K3::Collection<CONTENT>(std::move(c)) {}

  _Collection(const K3::Collection<CONTENT>& c): K3::Collection<CONTENT>(c) {}

  _Collection(K3::Collection<CONTENT>&& c): K3::Collection<CONTENT>(std::move(c)) {}

  _Collection& operator=(const _Collection& other){
    K3::Collection<CONTENT>::operator=(other);
    return *this;
  }

  _Collection& operator=(const _Collection&& other){
    K3::Collection<CONTENT>::operator=(std::move(other));
    return *this;
  }

  template <class archive>
  void serialize(archive& _archive,const unsigned int) {

    _archive & boost::serialization::base_object<K3::Collection<CONTENT>>(*this);
  }

};
#endif // K3__Collection

#endif // K3_RUNTIME_BASECOLLECTIONS_H
