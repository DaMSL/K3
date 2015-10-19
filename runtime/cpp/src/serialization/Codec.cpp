#include <string>

#include "serialization/Codec.hpp"

namespace K3 {

namespace Codec {

CodecFormat getFormat(const string& s) {
  if (s == "csv") {
    return CodecFormat::CSV;
  } else if (s == "psv") {
    return CodecFormat::PSV;
  } else if (s == "k3") {
    return CodecFormat::YASBinary;
  }  else if (s == "raw") {
    return CodecFormat::Raw;
  }
  else {
    throw std::runtime_error("Unrecognized codec format: " + s);
  }
}
}

template<>
Pool::unique_ptr<PackedValue> pack(const base_string& t, CodecFormat format) {
  switch (format) {
    case CodecFormat::YASBinary:
      return yas_ser::pack<base_string>(t, format);
    case CodecFormat::BoostBinary:
      return boost_ser::pack<base_string>(t, format);
    case CodecFormat::CSV:
      return csvpp_ser::pack<base_string, ','>(t, format);
    case CodecFormat::PSV:
      return csvpp_ser::pack<base_string, '|'>(t, format);
    case CodecFormat::Raw:
      return Pool::getInstance().make_unique_subclass<PackedValue, BaseStringPackedValue>(base_string(t), format); 
    default:
      throw std::runtime_error("Unrecognized codec format");
  }
}

template <>
Pool::unique_ptr<base_string> unpack(Pool::unique_ptr<PackedValue> t) {
  switch (t->format()) {
    case CodecFormat::YASBinary:
      return yas_ser::unpack<base_string>(*t);
    case CodecFormat::BoostBinary:
      return boost_ser::unpack<base_string>(*t);
    case CodecFormat::CSV:
      return csvpp_ser::unpack<base_string, ','>(*t);
    case CodecFormat::PSV:
      return csvpp_ser::unpack<base_string, '|'>(*t);
    case CodecFormat::Raw:
      return steal_base_string(t.get()); 
    default:
      throw std::runtime_error("Unrecognized codec format");
  }
}

Pool::unique_ptr<base_string> steal_base_string(PackedValue* t) {
  BaseStringPackedValue* s1 = dynamic_cast<BaseStringPackedValue*>(t); 
  return Pool::getInstance().make_unique<base_string>(s1->steal());
}

}  // namespace K3
