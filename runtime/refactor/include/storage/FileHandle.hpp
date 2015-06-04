#ifndef K3_FILE_HANDLE
#define K3_FILE_HANDLE

// FileHandle is an is an interface for reading/writing files
#include <iostream>
#include <fstream>

#include "Common.hpp"
#include "types/Value.hpp"

namespace K3 {


// FileHandle Interface
//    Abstract class for both sources & sinks
class FileHandle {
  public:
    virtual bool hasRead()   {return false;}
    virtual bool hasWrite()  {return false;}

    virtual shared_ptr<PackedValue> doRead()  = 0;
    virtual void doWrite(shared_ptr<PackedValue> val)  = 0;
    virtual void close()      = 0;
};



//  SourceFileHande Class
//    Binary File Handle  (explicit sized delimited values)
class SourceFileHandle : public FileHandle  {
public:

  SourceFileHandle () {};
  SourceFileHandle (std::string path, CodecFormat codec);

  virtual bool hasRead();
  virtual shared_ptr<PackedValue> doRead();

  virtual void doWrite(shared_ptr<PackedValue> val) {
    throw std::ios_base::failure ("ERROR trying to write to source.");
  }

  virtual void close() {
    file_.close();
  }

protected:
  std::ifstream file_;
  CodecFormat fmt_;
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

  SinkFileHandle () {};
  SinkFileHandle (std::string path);

  virtual bool hasWrite();
  virtual void doWrite(shared_ptr<PackedValue> val);


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
  SinkTextHandle (std::string path) : SinkFileHandle (path) {}
  virtual void doWrite(shared_ptr<PackedValue> val);
};


}  // namespace K3
#endif
