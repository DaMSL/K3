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

    virtual shared_ptr<PackedValue> doRead() = 0;
    // TODO(jbw) top level could take const T& instead
    template<class T>
    void doWrite(const NativeValue& val) {
      auto cdec = Codec::getCodec<T>(fmt_);
      return this->doWriteHelper(val, cdec);
    }
    virtual void doWriteHelper(const NativeValue& val, shared_ptr<Codec> cdec) = 0;
    virtual void close() = 0;

  protected:
    CodecFormat fmt_;
};

//  SourceFileHande Class
//    Binary File Handle  (explicit sized delimited values)
class SourceFileHandle : public FileHandle  {
public:
  SourceFileHandle() { }
  SourceFileHandle (std::string path, CodecFormat codec);

  virtual bool hasRead();
  virtual shared_ptr<PackedValue> doRead();

  virtual void doWriteHelper(const NativeValue& val, shared_ptr<Codec> cdec) {
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
  virtual shared_ptr<PackedValue> doRead();
};


//  Sink File Hande
class SinkFileHandle : public FileHandle  {
public:
  SinkFileHandle (std::string path, CodecFormat fmt);

  virtual bool hasWrite();
  virtual void doWriteHelper(const NativeValue& val, shared_ptr<Codec> cdec);


  virtual shared_ptr<PackedValue> doRead() {
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
  SinkTextHandle (std::string path, CodecFormat fmt) : SinkFileHandle (path, fmt) {}
  virtual void doWriteHelper(const NativeValue& val, shared_ptr<Codec> cdec);
};


}  // namespace K3
#endif
