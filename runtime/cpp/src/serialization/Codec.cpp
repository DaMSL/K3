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
  } else {
    throw std::runtime_error("Unrecognized codec format: " + s);
  }
}
}

template <>
unique_ptr<string> unpack(unique_ptr<PackedValue> t) {
  switch (t->format()) {
    case CodecFormat::YASBinary:
      return yas_ser::unpack<string>(*t);
    case CodecFormat::BoostBinary:
      return boost_ser::unpack<string>(*t);
    case CodecFormat::CSV:
      return steal(std::move(t));
    case CodecFormat::PSV:
      return steal(std::move(t));
    default:
      throw std::runtime_error("Unrecognized codec format");
  }
}

unique_ptr<string> steal(unique_ptr<PackedValue> t) {
  StringPackedValue& s1 = dynamic_cast<StringPackedValue&>(*t); 
  return make_unique<string>(s1.steal());
}

}  // namespace K3
