#ifndef K3_RUNTIME_IOHANDLE_HPP
#define K3_RUNTIME_IOHANDLE_HPP

#include <memory>
#include <boost/iostreams/device/file.hpp>
#include <boost/iostreams/filtering_stream.hpp>
#include <boost/iostreams/filter/line.hpp>

#include <Common.hpp>
#include <Network.hpp>

namespace K3
{
  using namespace std;
  using namespace boost::iostreams;

  //--------------------------
  // IO handles

  class IOHandle : public virtual LogMT
  {
  public:
    typedef tuple<shared_ptr<Codec>, shared_ptr<Net::NEndpoint> > SourceDetails;
    typedef tuple<shared_ptr<Codec>, shared_ptr<Net::NConnection> > SinkDetails;

    IOHandle(shared_ptr<Codec> cdec) : LogMT("IOHandle"), codec(cdec) {}

    virtual bool hasRead() = 0;
    virtual shared_ptr<Value> doRead() = 0;

    virtual bool hasWrite() = 0;
    virtual void doWrite(Value& v) = 0;

    virtual void close() = 0;

    virtual bool builtin() = 0;
    virtual bool file() = 0;

    virtual SourceDetails networkSource() = 0;
    virtual SinkDetails networkSink() = 0;

  protected:
    shared_ptr<Codec> codec;
  };

  class IStreamHandle : public virtual LogMT
  {
  public:
    template<typename Source>
    IStreamHandle(shared_ptr<Codec> cdec, Source& src) 
      : LogMT("IStreamHandle"), codec(cdec) 
    {
      input = shared_ptr<filtering_istream>(new filtering_istream());
      input->push(src);      
    }

    bool hasRead() { 
      return input? 
        ((input->good() && codec->good()) || codec->decode_ready())
        : false;
    }
    
    shared_ptr<string> doRead() {
      shared_ptr<string> result;
      while ( !result && input->good() ) {
        char * buffer = new char[1024];
        input->read(buffer,sizeof(buffer));
        result = codec->decode(string(buffer));
        delete[] buffer;
      } 
      return result;
    }

    bool hasWrite() {
      BOOST_LOG(*this) << "Invalid write operation on input handle";
      return false;
    }
    
    void doWrite(string& data) {
      BOOST_LOG(*this) << "Invalid write operation on input handle";
    }

    // Invoke the destructor on the filtering_istream, which in 
    // turn closes all associated iostream filters and devices.
    void close() { if ( input ) { input.reset(); } }

  protected:
    shared_ptr<filtering_istream> input;
    shared_ptr<Codec> codec;
  };

  class OStreamHandle : public virtual LogMT
  {
  public:
    template<typename Sink>
    OStreamHandle(shared_ptr<Codec> cdec, Sink& sink) 
      : LogMT("OStreamHandle"), codec(cdec)
    {
      output = shared_ptr<filtering_ostream>(new filtering_ostream());
      output->push(sink);
    }

    bool hasRead() {
      BOOST_LOG(*this) << "Invalid read operation on output handle";
      return false;
    }

    shared_ptr<string> doRead() {
      BOOST_LOG(*this) << "Invalid read operation on output handle";
      return shared_ptr<string>();
    }

    bool hasWrite() { return output? output->good() : false; }
    
    void doWrite(string& data) { if ( output ) { (*output) << data << std::endl; } }
  
    void close() { if ( output ) { output.reset(); } }

  protected:
    shared_ptr<filtering_ostream> output;
    shared_ptr<Codec> codec;
  };

  class StreamHandle : public IOHandle
  {
  public:
    struct Input  {};
    struct Output {};

    template<typename Source>
    StreamHandle(shared_ptr<Codec> cdec, Input i, Source& src)
      : LogMT("StreamHandle"), IOHandle(cdec)
    {
      inImpl = shared_ptr<IStreamHandle>(new IStreamHandle(cdec, src));
    }
    
    template<typename Sink>
    StreamHandle(shared_ptr<Codec>  cdec, Output o, Sink& sink)
      : LogMT("StreamHandle"), IOHandle(cdec)
    {
      outImpl = shared_ptr<OStreamHandle>(new OStreamHandle(cdec, sink));
    }

    bool hasRead()  { 
      bool r = false;
      if ( inImpl ) { r = inImpl->hasRead(); }
      else { BOOST_LOG(*this) << "Invalid hasRead on LineBasedHandle"; }
      return r;
    }
    
    bool hasWrite() {
      bool r = false;
      if ( outImpl ) { r = outImpl->hasWrite(); }
      else { BOOST_LOG(*this) << "Invalid hasWrite on LineBasedHandle"; }
      return r;
    }

    shared_ptr<Value> doRead() {
      shared_ptr<Value> data;
      if ( inImpl ) {
        data = inImpl->doRead();
      }
      else { BOOST_LOG(*this) << "Invalid doRead on LineBasedHandle"; }
      return data;      
    }

    void doWrite(Value& v) {
      if ( outImpl) {
        outImpl->doWrite(v);
      }
      else { BOOST_LOG(*this) << "Invalid doWrite on LineBasedHandle"; }
    }

    void close() {
      if ( inImpl ) { inImpl->close(); }
      else if ( outImpl ) { outImpl->close(); }
    }

  protected:
    shared_ptr<IStreamHandle> inImpl;
    shared_ptr<OStreamHandle> outImpl;
  };


  class BuiltinHandle : public StreamHandle
  {
  public:
    struct Stdin  {};
    struct Stdout {};
    struct Stderr {};

    BuiltinHandle(shared_ptr<Codec> cdec, Stdin s)
      : LogMT("BuiltinHandle"), StreamHandle(cdec, StreamHandle::Input(), cin)
    {}
    
    BuiltinHandle(shared_ptr<Codec> cdec, Stdout s)
      : LogMT("BuiltinHandle"), StreamHandle(cdec, StreamHandle::Output(), cout)
    {}
    
    BuiltinHandle(shared_ptr<Codec> cdec, Stderr s)
      : LogMT("BuiltinHandle"), StreamHandle(cdec, StreamHandle::Output(), cerr)
    {}

    bool builtin () { return true; }
    bool file() { return false; }

    IOHandle::SourceDetails networkSource()
    {
      return make_tuple(shared_ptr<Codec>(), shared_ptr<Net::NEndpoint>());
    }

    IOHandle::SinkDetails networkSink()
    {
      return make_tuple(shared_ptr<Codec>(), shared_ptr<Net::NConnection>());
    }
  };

  class FileHandle : public StreamHandle 
  {
  public:
    FileHandle(shared_ptr<Codec> cdec, shared_ptr<file_source> fs, StreamHandle::Input i) 
      :  StreamHandle(cdec, i, *fs), LogMT("FileHandle")
    {}

    FileHandle(shared_ptr<Codec> cdec, shared_ptr<file_sink> fs, StreamHandle::Output o)
      :  LogMT("FileHandle"), StreamHandle(cdec, o, *fs)
    {}

    bool builtin () { return false; }
    bool file() { return true; }

    IOHandle::SourceDetails networkSource()
    {
      return make_tuple(shared_ptr<Codec>(), shared_ptr<Net::NEndpoint>());
    }

    IOHandle::SinkDetails networkSink()
    {
      return make_tuple(shared_ptr<Codec>(), shared_ptr<Net::NConnection>());
    }

  private:
    shared_ptr<file_source> fileSrc;
    shared_ptr<file_sink>   fileSink;
  };

  class NetworkHandle : public IOHandle
  {
  public:
    NetworkHandle(shared_ptr<Codec> cdec, shared_ptr<Net::NConnection> c) 
      : LogMT("NetworkHandle"), connection(c), IOHandle(cdec)
    {}
    
    NetworkHandle(shared_ptr<Codec> cdec, shared_ptr<Net::NEndpoint> e)
      : LogMT("NetworkHandle"), endpoint(e), IOHandle(cdec)
    {}

    bool hasRead()  { 
      BOOST_LOG(*this) << "Invalid hasRead on NetworkHandle";
      return false;
    }
    
    bool hasWrite() {
      bool r = false;
      if ( connection ) { r = connection->connected(); }
      else { BOOST_LOG(*this) << "Invalid hasWrite on NetworkHandle"; }
      return r;
    }

    shared_ptr<Value> doRead() {
      BOOST_LOG(*this) << "Invalid doRead on NetworkHandle";
      return shared_ptr<Value>();
    }

    void doWrite(Value& v) {
      if ( connection && this->codec ) {
        string data = this->codec->encode(v);
        connection->write(data);
      }
      else { BOOST_LOG(*this) << "Invalid doWrite on NetworkHandle"; }
    }

    void close() {
      if ( connection ) { connection->close(); }
      else if ( endpoint ) { endpoint->close(); }
    }

    bool builtin () { return false; }
    bool file() { return false; }

    IOHandle::SourceDetails networkSource()
    {
      shared_ptr<Codec> cdec = endpoint? this->codec : shared_ptr<Codec>();
      return make_tuple(cdec, endpoint);
    }

    IOHandle::SinkDetails networkSink()
    {
      shared_ptr<Codec> cdec = connection? this->codec : shared_ptr<Codec>();
      return make_tuple(cdec, connection);
    }

  protected:
    shared_ptr<Net::NConnection> connection;
    shared_ptr<Net::NEndpoint> endpoint;
  };
}

#endif
