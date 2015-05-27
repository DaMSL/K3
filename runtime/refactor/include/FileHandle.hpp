#ifndef K3_FILE_HANDLE
#define K3_FILE_HANDLE

// FileHandle is an is an interface for reading/writing files
#include <iostream>
#include <fstream>

#include "Common.hpp"
#include "Value.hpp"
#include "Serialization.hpp"

using std::make_shared;



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

  SourceFileHandle (std::string path, CodecFormat codec) {
    file_.open (path, std::ios::in | std::ios::binary); 
    fmt_ = codec;
  }

  virtual bool hasRead();
  virtual shared_ptr<PackedValue> doRead();

  virtual void doWrite(shared_ptr<PackedValue> val) {
    // TODO (ben): throw error
    return;
  }

  virtual void close() {
    file_.close();
  }

private:
  std::ifstream file_;
  CodecFormat fmt_;
};


//  Source File Hande (for binary data)
class SourceTextHandle : public SourceFileHandle  {

  SourceTextHandle (std::string path, CodecFormat codec) {
    file_.open (path, std::ios::in); 
    fmt_ = codec;    
  }

  virtual shared_ptr<PackedValue> doRead();

}


//  Sink File Hande
class SinkFileHandle : public FileHandle  {
public:

  SinkFileHandle (std::string path)  {
    file_.open (path);
  }

  virtual shared_ptr<PackedValue> doRead() {
    return nullptr;
  }

  virtual bool hasWrite();
  virtual void doWrite(shared_ptr<PackedValue> val);

  virtual void close()  {
    file_.close();
  }

private:
  std::ofstream file_;
};




class SinkTextHandle : public SinkFileHandle  {

  SourceTextHandle (std::string path)  {
    file_.open (path); 
  }

  virtual void doWrite(shared_ptr<PackedValue> val);

}

// // ??? Add additional check ? or leave hasRead check to caller
// Value SourceTextHandle::doRead() {


//   std::string buf;
//   std::getLine (file_, buf);
  
//   //Unpack raw string to Value???
//   return buf;  
// }







// ----- SINK TEXT HANDLE
// SinkTextHandle::SinkTextHandle (std::string path)  {
//   file_.open (path); 
// }

// // ??? Add additional check ? or leave hasWrite check to caller
// void SinkTextHandle::doWrite(NativeValue val) {
//   file_ << val << endl;
// }


#endif