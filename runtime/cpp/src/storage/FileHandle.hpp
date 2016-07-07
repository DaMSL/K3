#ifndef K3_FILE_HANDLE
#define K3_FILE_HANDLE

// FileHandle is an is an interface for reading/writing files
#include <iostream>
#include <fstream>

#include "Common.hpp"
#include "types/Value.hpp"
#include "serialization/Codec.hpp"

namespace K3 {


// FileHandle Interface
//    Abstract class for both sources & sinks
class FileHandle {
  public:
    FileHandle() { }
    FileHandle(CodecFormat fmt) : fmt_(fmt) { }
    virtual ~FileHandle() { }
    virtual bool hasRead()   {return false;}
    virtual bool hasWrite()  {return false;}

    virtual unique_ptr<PackedValue> doRead() = 0;
    template<class T>
    void doWrite(const T& val) {
      auto pv = pack<T>(val, fmt_);
      return this->doWriteHelper(*pv);
    }
    virtual void doWriteHelper(const PackedValue& val) = 0;
    virtual void close() = 0;
    CodecFormat format() { return fmt_; }

  protected:
    CodecFormat fmt_;
};

//  SourceFileHande Class
//    Binary File Handle  (explicit sized delimited values)
class SourceFileHandle : public FileHandle  {
public:
  SourceFileHandle() {}
  SourceFileHandle (std::string path, CodecFormat codec);

  virtual bool hasRead();
  virtual unique_ptr<PackedValue> doRead();

  virtual void doWriteHelper(const PackedValue& val) {
    throw std::ios_base::failure ("ERROR trying to write to source.");
  }

  virtual void close() {
    file_.close();
  }

protected:
  std::ifstream file_;
};


//  Source File Hande (for binary data)
class SourceTextHandle : public SourceFileHandle  {
public:
  SourceTextHandle (std::string path, CodecFormat codec);
  virtual unique_ptr<PackedValue> doRead();
};


//  Sink File Hande
class SinkFileHandle : public FileHandle  {
public:
  SinkFileHandle() {}
  SinkFileHandle (std::string path, CodecFormat fmt);

  virtual bool hasWrite();
  virtual void doWriteHelper(const PackedValue& val);

  virtual unique_ptr<PackedValue> doRead() {
    throw std::ios_base::failure ("ERROR trying to read from sink.");
  }

  virtual void close()  {
    file_.close();
  }

protected:
  std::ofstream file_;
};

class SinkTextHandle : public SinkFileHandle  {
public:
  SinkTextHandle (std::string path, CodecFormat fmt);
  virtual void doWriteHelper(const PackedValue& val);
};

}  // namespace K3
#endif
