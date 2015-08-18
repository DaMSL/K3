#include "storage/FileHandle.hpp"
#include "serialization/Codec.hpp"

namespace K3 {

using std::ios_base;

//  SOURCE FILE HANDLE (binary file access)
SourceFileHandle::SourceFileHandle (std::string path, CodecFormat fmt) : FileHandle(fmt) {

    file_.exceptions (ios_base::failbit | std::ios_base::badbit );
    try {
      file_.open (path, std::ios::in | std::ios::binary); 
    }
    catch (ios_base::failure e) {
      throw e;
    }
}

bool SourceFileHandle::hasRead() {
  return file_.good() && (file_.peek() != EOF);
}

// TODO: Optimize using low-level c file I/O (if needed)
unique_ptr<PackedValue> SourceFileHandle::doRead() {
  // Get value size
  uint32_t len;

  file_ >> len;
  // file_.read( (char *) len, 4);

  // Allocate new value buffer
  vector <char> buf = std::vector<char> (len);

  // Read data from file to buffer
  file_.read((char *) buf.data(), len);

  return make_unique<BufferPackedValue>(std::move(buf), fmt_);
}

SourceTextHandle::SourceTextHandle (std::string path, CodecFormat fmt) {
    file_.exceptions (std::ifstream::failbit | std::ifstream::badbit );
    try {
      file_.open (path, std::ios::in); 
    }
    catch (std::ifstream::failure e) {
      throw e;
    }
  fmt_ = fmt;
}

unique_ptr<PackedValue> SourceTextHandle::doRead()  {
  // allocate buffer
  std::string buf;
  auto p = make_unique<StringPackedValue>(std::move(buf), fmt_);
  //Read next line
  std::getline (file_, p->string_);
  return std::move(p);
}

//  SINK FILE HANDLE (binary file access)
SinkFileHandle::SinkFileHandle (std::string path, CodecFormat fmt) : FileHandle(fmt) {
    file_.exceptions (std::ofstream::failbit | std::ofstream::badbit );
    try {
      file_.open (path);
    }
    catch (std::ofstream::failure e) {
      throw e;
    }
}

bool SinkFileHandle::hasWrite() {
  return (file_.good());  
}

// Note: value size limits are 32-bit ints (4GB per value)
void SinkFileHandle::doWriteHelper(const PackedValue& val) {
  file_ << static_cast<uint32_t>(val.length());
  file_.write (val.buf(), val.length());
}

void SinkTextHandle::doWriteHelper(const PackedValue& val) {
  file_.write (val.buf(), val.length());
  file_ << std::endl;
}

}
