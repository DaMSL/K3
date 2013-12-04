#ifndef K3_RUNTIME_IOHANDLE_HPP
#define K3_RUNTIME_IOHANDLE_HPP

#include <memory>
#include <boost/iostreams/device/file.hpp>
#include <boost/iostreams/filtering_stream.hpp>
#include <boost/iostreams/filter/line.hpp>
#include <k3/runtime/cpp/Common.hpp>
#include <k3/runtime/cpp/Network.hpp>

namespace K3
{
  using namespace std;
  using namespace boost;
  using namespace boost::iostreams;

  using std::shared_ptr;
  
  using Asio::NEndpoint;
  using Asio::NConnection;

  //--------------------------
  // IO handles

  // TODO: IOHandle.close() method
  template<typename Value>
  class IOHandle : public virtual LogMT
  {
  public:
    IOHandle(shared_ptr<WireDesc<Value> > wd) : LogMT("IOHandle"), wireDesc(wd) {}

    virtual bool hasRead() = 0;
    virtual shared_ptr<Value> doRead() = 0;

    virtual bool hasWrite() = 0;
    virtual void doWrite(Value& v) = 0;

  protected:
    shared_ptr<WireDesc<Value> > wireDesc;
  };

  class split_line_filter : public line_filter {
  public:
    split_line_filter() {}
    string do_filter(const string& line) { return line; }
  };

  class LineInputHandle : public virtual LogMT
  {
  public:
    template<typename Source>
    LineInputHandle(const Source& src) : LogMT("LineInputHandle") {
      input = shared_ptr<filtering_istream>(new filtering_istream());
      input->push(split_line_filter());
      input->push<Source>(src);
    }

    bool hasRead() { return input->good(); }
    
    shared_ptr<string> doRead() {
      stringstream r;
      (*input) >> r.rdbuf();
      return shared_ptr<string>(new string(r.str()));
    }

    bool hasWrite() {
      BOOST_LOG(*this) << "Invalid write operation on input handle";
      return false;
    }
    
    void doWrite(string& data) {
      BOOST_LOG(*this) << "Invalid write operation on input handle";
    }

  protected:
    shared_ptr<filtering_istream> input;
  };

  class LineOutputHandle : public virtual LogMT
  {
  public:
    template<typename Sink>
    LineOutputHandle(const Sink& sink) : LogMT("LineOutputHandle") {
      output = shared_ptr<filtering_ostream>(new filtering_ostream());
      output->push(split_line_filter());
      output->push<Sink>(sink);
    }

    bool hasRead() {
      BOOST_LOG(*this) << "Invalid read operation on output handle";
      return false;
    }

    shared_ptr<string> doRead() {
      BOOST_LOG(*this) << "Invalid read operation on output handle";
      return shared_ptr<string>();
    }

    bool hasWrite()            { return output->good(); }
    void doWrite(string& data) { (*output) << data; }
  
  protected:
    shared_ptr<filtering_ostream> output;
  };

  template<typename Value>
  class LineBasedHandle : public IOHandle<Value>
  {
  public:
    struct Input  {};
    struct Output {};

    template<typename Source>
    LineBasedHandle(shared_ptr<WireDesc<Value> > wd, Input i, const Source& src)
      : IOHandle<Value>(wd)
    {
      inImpl = shared_ptr<LineInputHandle>(new LineInputHandle(src));
    }
    
    template<typename Sink>
    LineBasedHandle(shared_ptr<WireDesc<Value> > wd, Output o, const Sink& sink)
      : IOHandle<Value>(wd)
    {
      outImpl = shared_ptr<LineOutputHandle>(new LineOutputHandle(sink));
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
      shared_ptr<Value> r;
      if ( inImpl && this->wireDesc ) { 
        shared_ptr<string> data = inImpl->doRead();
        r = this->wireDesc->unpack(*data);
      }
      else { BOOST_LOG(*this) << "Invalid doRead on LineBasedHandle"; }
      return r;      
    }

    void doWrite(Value& v) {
      if ( outImpl && this->wireDesc ) {
        string data = this->wireDesc->pack(v);
        outImpl->doWrite(data);
      }
      else { BOOST_LOG(*this) << "Invalid doWrite on LineBasedHandle"; }
    }

  protected:
    shared_ptr<LineInputHandle> inImpl;
    shared_ptr<LineOutputHandle> outImpl;
  };

  // TODO
  //class MultiLineHandle;
  //class FrameBasedHandle;

  template<typename Value>
  class BuiltinHandle : public LineBasedHandle<Value>
  {
  public:
    struct Stdin  {};
    struct Stdout {};
    struct Stderr {};

    BuiltinHandle(shared_ptr<WireDesc<Value> > wd, Stdin s)
      : LineBasedHandle<Value>(wd, typename LineBasedHandle<Value>::Input(), cin)
    {}
    
    BuiltinHandle(shared_ptr<WireDesc<Value> > wd, Stdout s)
      : LineBasedHandle<Value>(wd, typename LineBasedHandle<Value>::Output(), cout)
    {}
    
    BuiltinHandle(shared_ptr<WireDesc<Value> > wd, Stderr s)
      : LineBasedHandle<Value>(wd, typename LineBasedHandle<Value>::Output(), cerr)
    {}
  };

  template<typename Value>
  class FileHandle : public LineBasedHandle<Value> 
  {
  public:
    FileHandle(shared_ptr<WireDesc<Value> > wd, const string& path,
               typename LineBasedHandle<Value>::Input i) 
      : LineBasedHandle<Value>(wd, i, file_source(path))
    {}

    FileHandle(shared_ptr<WireDesc<Value> > wd, const string& path,
               typename LineBasedHandle<Value>::Output o)
      : LineBasedHandle<Value>(wd, o, file_sink(path))
    {}
  };

  template<typename Value>
  class NetworkHandle : public IOHandle<Value>
  {
  public:
    NetworkHandle(shared_ptr<NConnection> c) : connection(c) {}
    NetworkHandle(shared_ptr<NEndpoint> e) : endpoint(e) {}

    bool hasRead()  { 
      BOOST_LOG(*this) << "Invalid hasRead on NetworkHandle";
      return false;
    }
    
    bool hasWrite() {
      bool r = false;
      if ( connection ) { r = connection->good(); }
      else { BOOST_LOG(*this) << "Invalid hasWrite on NetworkHandle"; }
      return r;
    }

    shared_ptr<Value> doRead() {
      BOOST_LOG(*this) << "Invalid doRead on NetworkHandle";
      return shared_ptr<Value>();
    }

    void doWrite(Value& v) {
      if ( connection && this->wireDesc ) {
        string data = this->wireDesc->pack(v);
        (*connection) << data;
      }
      else { BOOST_LOG(*this) << "Invalid doWrite on NetworkHandle"; }
    }

  protected:
    shared_ptr<NConnection> connection;
    shared_ptr<NEndpoint> endpoint;
  };
}

#endif