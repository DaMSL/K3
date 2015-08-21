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

template <>
unique_ptr<base_string> unpack(unique_ptr<PackedValue> t) {
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

unique_ptr<base_string> steal_base_string(PackedValue* t) {
  BaseStringPackedValue* s1 = dynamic_cast<BaseStringPackedValue*>(t); 
  return make_unique<base_string>(s1->steal());
}

}  // namespace K3
