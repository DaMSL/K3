#include "FileHandle.hpp"


//  SOURCE FILE HANDLE (binary file access)
bool SourceFileHandle::hasRead() {
  return file_.good();  
}


shared_ptr<PackedValue> SourceFileHandle::doRead() {
  // Get value size
  uint32_t len;

  file_ >> len;
  // file_.read( (char *) len, 4);

  // Allocate new value buffer
  vector <char> buf = std::vector<char> (len);

  // Read data from file to buffer
  file_.read((char *) buf.data(), len);

  shared_ptr<PackedValue> result = make_shared<PackedValue>(std::move(buf), fmt_);
  return result;  
}




//  SINK FILE HANDLE (binary file access)
bool SinkFileHandle::hasWrite() {
  return (file_.good());  
}

// Note: value size limits are 32-bit ints (4GB per value)
void SinkFileHandle::doWrite(shared_ptr<PackedValue> val) {
  // file_.write ((char *) static_cast<uint32_t>(val->length()), 4);
  file_ << static_cast<uint32_t>(val->length());
  file_.write (val->buf(), val->length());
}

