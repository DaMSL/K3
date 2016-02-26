#include "storage/FileHandle.hpp"
#include "serialization/Codec.hpp"

namespace K3 {

using std::ios_base;

//  SOURCE FILE HANDLE (binary file access)
SourceFileHandle::SourceFileHandle (std::string path, CodecFormat fmt) : FileHandle(fmt)
{
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
  size_t len;

  file_.read((char *) &len, sizeof(len));
  // Allocate new value buffer
  char* buf = new char[sizeof(len) + len + 1];
  memcpy(buf, &len, sizeof(len));
  file_.read(buf + sizeof(len), len);
  buf[len + sizeof(len)] = '\0';
  base_string b;
  b.steal(buf);
  b.set_header(true);
  auto p  = make_unique<BaseStringPackedValue>(std::move(b), fmt_);
  return std::move(p);
}

SourceTextHandle::SourceTextHandle (std::string path, CodecFormat fmt)
{
  file_.exceptions (std::ifstream::failbit | std::ifstream::badbit );
  try {
    file_.open (path, std::ios::in);
  }
  catch (std::ifstream::failure e) {
    throw e;
  }
  fmt_ = fmt;
}

unique_ptr<PackedValue> SourceTextHandle::doRead()
{
  // allocate buffer
  std::string buf;
  auto p = make_unique<StringPackedValue>(std::move(buf), fmt_);
  //Read next line
  std::getline (file_, p->string_);
  return std::move(p);
}

//  SINK FILE HANDLE (binary file access)
SinkFileHandle::SinkFileHandle (std::string path, CodecFormat fmt) : FileHandle(fmt)
{
  file_.exceptions (std::ofstream::failbit | std::ofstream::badbit );
  try {
    file_.open (path, std::ios::out | std::ios::binary );
  }
  catch (std::ofstream::failure e) {
    throw e;
  }
}

bool SinkFileHandle::hasWrite() {
  return (file_.good());
}

void SinkFileHandle::doWriteHelper(const PackedValue& val) {
  size_t len = val.length();
  
  file_.write((char *) &len, sizeof(len));
  file_.write (val.buf(), len);
}

SinkTextHandle::SinkTextHandle (std::string path, CodecFormat fmt)
{
  file_.exceptions (std::ofstream::failbit | std::ofstream::badbit );
  try {
    file_.open (path, std::ios::out);
  }
  catch (std::ofstream::failure e) {
    throw e;
  }
  fmt_ = fmt;
}

void SinkTextHandle::doWriteHelper(const PackedValue& val) {
  file_.write (val.buf(), val.length());
  file_ << std::endl;
}

}
