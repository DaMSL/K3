#include <string>

#include "serialization/Codec.hpp"

namespace K3 {

CodecFormat Codec::getFormat(const string& s) {
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

}  // namespace K3
