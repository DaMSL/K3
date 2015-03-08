#ifndef K3_RUNTIME_IOHANDLE_HPP
#define K3_RUNTIME_IOHANDLE_HPP

#include <boost/iostreams/device/file.hpp>
#include <boost/iostreams/filtering_stream.hpp>
#include <boost/iostreams/filter/line.hpp>

#include <Common.hpp>
#include <Codec.hpp>
#include <Network.hpp>

namespace K3
{
  //--------------------------
  // IO handles

  class IOHandle : public virtual LogMT
  {
  public:
    typedef tuple<shared_ptr<FrameCodec>, shared_ptr<Net::NEndpoint> > SourceDetails;
    typedef tuple<shared_ptr<FrameCodec>, shared_ptr<Net::NConnection> > SinkDetails;

    IOHandle(shared_ptr<FrameCodec> frme) : LogMT("IOHandle"), frame(frme) {}

    virtual bool isInput() = 0;
    virtual bool isOutput() = 0;
    virtual bool hasRead() = 0;
    virtual shared_ptr<string> doRead() = 0;

    virtual bool hasWrite() = 0;
    virtual void doWrite(shared_ptr<string> v) = 0;

    virtual void close() = 0;

    virtual bool builtin() = 0;
    virtual bool file() = 0;

    virtual SourceDetails networkSource() = 0;
    virtual SinkDetails networkSink() = 0;

  protected:
    shared_ptr<FrameCodec> frame;
  };

  class IStreamHandle : public virtual LogMT
  {
  public:
    template<typename Source>
    IStreamHandle(shared_ptr<FrameCodec> frme, Source& src)
      : LogMT("IStreamHandle"), frame(frme)
    {
      input_ = shared_ptr<filtering_istream>(new filtering_istream());
      input_->push(src);
    }

    bool hasRead() {
      return input_?
        ((input_->good() && frame->good()) || frame->decode_ready())
        : false;
    }

    shared_ptr<string> doRead();

    bool hasWrite() {
      BOOST_LOG(*this) << "Invalid write operation on input handle";
      return false;
    }

    void doWrite(shared_ptr<string> data) {
      BOOST_LOG(*this) << "Invalid write operation on input handle";
    }

    // Invoke the destructor on the filtering_istream, which in
    // turn closes all associated iostream filters and devices.
    void close() { if ( input_ ) { input_.reset(); } }

  protected:
    shared_ptr<filtering_istream> input_;
    shared_ptr<FrameCodec> frame;
  };

  class OStreamHandle : public virtual LogMT
  {
  public:
    template<typename Sink>
    OStreamHandle(shared_ptr<FrameCodec> frme, Sink& sink)
      : LogMT("OStreamHandle"), frame(frme)
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

    void doWrite(shared_ptr<string> data ) {
      if ( output ) {
        auto s = frame->encode(*data);
        (*output) << s;
        output->flush();
      }
    }

    void close() { if ( output ) { output.reset(); } }

  protected:
    shared_ptr<filtering_ostream> output;
    shared_ptr<FrameCodec> frame;
  };

  class StreamHandle : public IOHandle
  {
  public:
    struct Input  {};
    struct Output {};

    template<typename Source>
    StreamHandle(shared_ptr<FrameCodec> frme, Input i, Source& src)
      : LogMT("StreamHandle"), IOHandle(frme), isInput_(true)
    {
      inImpl = shared_ptr<IStreamHandle>(new IStreamHandle(frme, src));
    }

    template<typename Sink>
    StreamHandle(shared_ptr<FrameCodec>  frme, Output o, Sink& sink)
      : LogMT("StreamHandle"), IOHandle(frme), isInput_(false)
    {
      outImpl = shared_ptr<OStreamHandle>(new OStreamHandle(frme, sink));
    }

    bool isInput() { return isInput_; }
    bool isOutput() { return !isInput_; }
    // There are slightly bigger problems with the entire StreamHandle class
    // that makes it difficult to have one that does both input and output

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

    shared_ptr<string> doRead() {
      shared_ptr<string> data;
      if ( inImpl ) {
        data = inImpl->doRead();
      }
      else { BOOST_LOG(*this) << "Invalid doRead on LineBasedHandle"; }
      return data;
    }

    void doWrite(shared_ptr<string>  v) {
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
    bool isInput_;
  };


  class BuiltinHandle : public StreamHandle
  {
  public:
    struct Stdin  {};
    struct Stdout {};
    struct Stderr {};

    BuiltinHandle(shared_ptr<FrameCodec> frme, Stdin s)
      : LogMT("BuiltinHandle"), StreamHandle(frme, StreamHandle::Input(), cin), isInput_(true)
    {}

    BuiltinHandle(shared_ptr<FrameCodec> frme, Stdout s)
      : LogMT("BuiltinHandle"), StreamHandle(frme, StreamHandle::Output(), cout), isInput_(false)
    {}

    BuiltinHandle(shared_ptr<FrameCodec> frme, Stderr s)
      : LogMT("BuiltinHandle"), StreamHandle(frme, StreamHandle::Output(), cerr), isInput_(false)
    {}

    bool isInput() { return isInput_; }
    bool isOutput() { return !isInput_; }
    bool builtin () { return true; }
    bool file() { return false; }

    IOHandle::SourceDetails networkSource()
    {
      return make_tuple(shared_ptr<FrameCodec>(), shared_ptr<Net::NEndpoint>());
    }

    IOHandle::SinkDetails networkSink()
    {
      return make_tuple(shared_ptr<FrameCodec>(), shared_ptr<Net::NConnection>());
    }
  protected:
    bool isInput_;
  };

  class FileHandle : public StreamHandle
  {
  public:
    FileHandle(shared_ptr<FrameCodec> frme, shared_ptr<file_source> fs, StreamHandle::Input i)
      :  StreamHandle(frme, i, *fs), LogMT("FileHandle")
    {}

    FileHandle(shared_ptr<FrameCodec> frme, shared_ptr<file_sink> fs, StreamHandle::Output o)
      :  LogMT("FileHandle"), StreamHandle(frme, o, *fs)
    {}

    bool builtin () { return false; }
    bool file() { return true; }

    IOHandle::SourceDetails networkSource()
    {
      return make_tuple(shared_ptr<FrameCodec>(), shared_ptr<Net::NEndpoint>());
    }

    IOHandle::SinkDetails networkSink()
    {
      return make_tuple(shared_ptr<FrameCodec>(), shared_ptr<Net::NConnection>());
    }

  private:
    shared_ptr<file_source> fileSrc;
    shared_ptr<file_sink>   fileSink;
  };

  class NetworkHandle : public IOHandle
  {
  public:
    NetworkHandle(shared_ptr<FrameCodec> frme, shared_ptr<Net::NConnection> c)
      : LogMT("NetworkHandle"), connection(c), IOHandle(frme), isInput_(false)
    {}

    NetworkHandle(shared_ptr<FrameCodec> frme, shared_ptr<Net::NEndpoint> e)
      : LogMT("NetworkHandle"), endpoint(e), IOHandle(frme), isInput_(false)
    {}

    bool isInput() { return isInput_; }
    bool isOutput() { return !isInput_; }
    bool hasRead()  {
      BOOST_LOG(*this) << "Invalid hasRead on NetworkHandle";
      return false;
    }

    bool hasWrite() {
      bool r = false;
      if ( connection ) {
        r = connection->connected(); }
      else { BOOST_LOG(*this) << "Invalid hasWrite on NetworkHandle"; }
      return r;
    }

    shared_ptr<string> doRead() {
      BOOST_LOG(*this) << "Invalid doRead on NetworkHandle";
      return shared_ptr<string>();
    }

    void doWrite(shared_ptr<string>  v) {
      if ( connection && this->frame ) {
        string data = this->frame->encode(*v);
        shared_ptr<string> s = make_shared<string>(std::move(data));
        connection->write(s);
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
      shared_ptr<FrameCodec> frme = endpoint? this->frame : shared_ptr<FrameCodec>();
      return make_tuple(frme, endpoint);
    }

    IOHandle::SinkDetails networkSink()
    {
      shared_ptr<FrameCodec> frme = connection? this->frame : shared_ptr<FrameCodec>();
      return make_tuple(frme, connection);
    }

  protected:
    shared_ptr<Net::NConnection> connection;
    shared_ptr<Net::NEndpoint> endpoint;
    bool isInput_;
  };
}

#endif
