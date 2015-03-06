#ifndef K3_RUNTIME_ENDPOINT_H
#define K3_RUNTIME_ENDPOINT_H

#include <list>
#include <map>
#include <tuple>
#include <boost/thread/mutex.hpp>
#include <boost/thread/externally_locked.hpp>
#include <boost/thread/lockable_adapter.hpp>
#include <boost/variant.hpp>

#include <Common.hpp>
#include <Codec.hpp>
#include <EndpointBuffer.hpp>
#include <Network.hpp>
#include <IOHandle.hpp>
#include <Queue.hpp>

using std::runtime_error;

// TODO: rewrite endpoint and connection containers without externally_locked as this requires a strict_lock.
// Ideally we want to use a shared_lock since the most common operation will be read accesses.

namespace K3
{
  enum class EndpointNotification {
    NullEvent, FileData, FileTick, FileClose, SocketAccept, SocketData, SocketTick, SocketClose
  };

  class BufferException : public runtime_error {
  public:
    BufferException( const std::string& msg ) : runtime_error(msg) {}
    BufferException( const char* msg ) : runtime_error(msg) {}
  };

  class EndpointException : public runtime_error {
  public:
    EndpointException( const std::string& msg ) : runtime_error(msg) {}
    EndpointException( const char* msg ) : runtime_error(msg) {}
  };

  typedef std::function<void(const Address&, const TriggerId, shared_ptr<string>, const Address&)> SendFunctionPtr;

  class Endpoint;
  typedef map<Identifier, shared_ptr<Endpoint> > EndpointMap;

  static inline std::string internalEndpointPrefix() { return std::string("__");  }

  static inline Identifier connectionId(Address& addr) {
    return internalEndpointPrefix() + "_conn_" + addressAsString(addr);
  }

  static inline Identifier peerEndpointId(Address& addr) {
    return internalEndpointPrefix() + "_node_" + addressAsString(addr);
  }

  static inline bool externalEndpointId(Identifier& id) {
    std::string pfx = internalEndpointPrefix();
    return ( mismatch(pfx.begin(), pfx.end(), id.begin()).first != pfx.end() );
  }

  //----------------------------
  // I/O event notifications.

  class EndpointBindings : public LogMT {
  public:

    typedef list<tuple<Address, TriggerId>> Subscribers;
    typedef map<EndpointNotification, shared_ptr<Subscribers>> Subscriptions;

    EndpointBindings(SendFunctionPtr f) : LogMT("EndpointBindings"), sendFn(f) {}

    void attachNotifier(EndpointNotification nt, Address sub_addr, TriggerId sub_id);

    void detachNotifier(EndpointNotification nt, Address sub_addr, TriggerId sub_id);

    void notifyEvent(EndpointNotification nt, shared_ptr<string> payload);

  protected:
    SendFunctionPtr sendFn;
    Subscriptions eventSubscriptions;
  };

  //---------------------------------
  // Endpoints and their containers.

  class Endpoint
  {
  public:
    Endpoint(shared_ptr<IOHandle> ioh,
             shared_ptr<EndpointBuffer> buf,
             shared_ptr<EndpointBindings> subs)
      : handle_(ioh), buffer_(buf), subscribers_(subs)
    {
      if (handle_->isInput() && buffer_) { refreshBuffer(); }
    }

    virtual bool isInternal() = 0;

    shared_ptr<IOHandle> handle() { return handle_; }
    shared_ptr<EndpointBuffer> buffer() { return buffer_; }
    shared_ptr<EndpointBindings> subscribers() { return subscribers_; }

    // TODO: think through this type signature. How do subscribers know what to do
    // with the payload?
    void notify_subscribers(shared_ptr<string> v);

    shared_ptr<string> refreshBuffer();

    void flushBuffer();

    // An endpoint can be read if the handle can be read or the buffer isn't empty.
    bool hasRead() { return handle_->hasRead() || (buffer_ && !buffer_->empty()); }

    // An endpoint can be written to if the handle can be written to and the buffer isn't full.
    bool hasWrite() { return handle_->hasWrite() && buffer_ && !buffer_->full(); }

    bool do_push(shared_ptr<string> val, shared_ptr<MessageQueues> q, shared_ptr<MessageCodec> frame);

    // Closes the endpoint's IOHandle, while also notifying subscribers
    // of the close event.
    void close();

  protected:
    shared_ptr<IOHandle> handle_;
    shared_ptr<EndpointBuffer> buffer_;
    shared_ptr<EndpointBindings> subscribers_;

    boost::mutex mtx_;          // A mutex used for non-buffered I/O operations.
  };

  class ExternalEndpoint : public virtual Endpoint
  {
  public:
    // External endpoint constructor.
    ExternalEndpoint(shared_ptr<IOHandle> ioh,
                     shared_ptr<EndpointBuffer> buf,
                     shared_ptr<EndpointBindings> subs,
                     shared_ptr<Codec> codec)
      : Endpoint(ioh, buf, subs), codec_(codec)
    {}

    bool isInternal() { return false; }

    template<typename T> shared_ptr<T> doRead() {
      shared_ptr<T> result;
      shared_ptr<string> r = refreshBuffer();
      if ( r ) { result = specializedDecode<T>(*r); }
      return result;
    }

    template<typename T>
    Collection<R_elem<T>> doReadBlock(int blockSize)
    {
      Collection<R_elem<T>> r;
      shared_ptr<string> v;
      if ( buffer_ ) {
        int i = blockSize;
        do {
          v = buffer_->refresh(handle_,
                std::bind(&Endpoint::notify_subscribers, this, std::placeholders::_1));
          if ( v ) {
            shared_ptr<T> outv = specializedDecode<T>(*v);
            if ( outv ) {
              R_elem<T> elem(*outv);
              r.insert(std::move(elem));
            }
          }
        } while ( --i > 0 && handle_->hasRead() );
      } else {
        // Read data directly from the underlying IOHandle.
        boost::lock_guard<boost::mutex> guard(mtx_);
        int i = blockSize;
        do {
          v = handle_->doRead();
          notify_subscribers(v); // Produce a notification for every tuple in a block read.
          if ( v ) {
            shared_ptr<T> outv = specializedDecode<T>(*v);
            if ( outv ) {
              R_elem<T> elem(*outv);
              r.insert(std::move(elem));
            }
          }
        } while ( --i > 0 && handle_->hasRead() );
      }
      return r;
    }

    template<typename T>
    void doWrite(const T& v)
    {
      if ( codec_ ) {
        shared_ptr<string> bufv = make_shared<string>(std::move(specializedEncode<T>(v)));
        if ( buffer_ ) {
          bool success = buffer_->push_back(bufv);
          if ( !success ) {
            // Flush buffer, and then try to append again.
            flushBuffer();

            // Try to append again, and if this still fails, throw a buffering exception.
            success = buffer_->push_back(bufv);
          }

          if ( ! success )
            { throw BufferException("Failed to buffer value during endpoint write."); }
        } else {
          boost::lock_guard<boost::mutex> guard(mtx_);
          handle_->doWrite(bufv);
          notify_subscribers(bufv);
        }
      }
    }

    template<typename T>
    void doWrite(shared_ptr<T> v_ptr) {
      if ( v_ptr ) { doWrite<T>(*v_ptr); }
    }

  protected:
    shared_ptr<Codec> codec_;

    template<typename T> shared_ptr<T> specializedDecode(const string& s) {
      shared_ptr<T> result;
      if ( codec_ ) {
        if ( codec_->format() == Codec::CodecFormat::K3 ) {
          shared_ptr<K3Codec> k3codec = std::dynamic_pointer_cast<K3Codec, Codec>(codec_);
          result = k3codec->decode<T>(s);
        } else if ( codec_->format() == Codec::CodecFormat::K3B ) {
          shared_ptr<K3BCodec> k3bcodec = std::dynamic_pointer_cast<K3BCodec, Codec>(codec_);
          result = k3bcodec->decode<T>(s);
        } else if ( codec_->format() == Codec::CodecFormat::CSV ) {
          shared_ptr<CSVCodec> csvcodec = std::dynamic_pointer_cast<CSVCodec, Codec>(codec_);
          result = csvcodec->decode<T>(s);
        }
        // TODO: JSON, YAML formats.
      }
      return result;
    }

    template<typename T> string specializedEncode(const T& v) {
      if ( codec_ ) {
        if ( codec_->format() == Codec::CodecFormat::K3 ) {
          shared_ptr<K3Codec> k3codec = std::dynamic_pointer_cast<K3Codec, Codec>(codec_);
          return k3codec->encode<T>(v);
        } else if ( codec_->format() == Codec::CodecFormat::K3B ) {
          shared_ptr<K3BCodec> k3bcodec = std::dynamic_pointer_cast<K3BCodec, Codec>(codec_);
          return k3bcodec->encode<T>(v);
        } else if ( codec_->format() == Codec::CodecFormat::CSV ) {
          shared_ptr<CSVCodec> csvcodec = std::dynamic_pointer_cast<CSVCodec, Codec>(codec_);
          return csvcodec->encode<T>(v);
        }
        // TODO: JSON, YAML formats.
      }
      return std::string();
    }
  };

  class InternalEndpoint : public virtual Endpoint
  {
  public:
    // Internal endpoint constructor.
    InternalEndpoint(shared_ptr<IOHandle> ioh,
                     shared_ptr<EndpointBuffer> buf,
                     shared_ptr<EndpointBindings> subs,
                     shared_ptr<MessageCodec> msgcodec)
      : Endpoint(ioh, buf, subs), msgcodec_(msgcodec)
    {}

    bool isInternal() { return true; }

    shared_ptr<RemoteMessage> doRead() {
      shared_ptr<RemoteMessage> result;
      shared_ptr<string> r = refreshBuffer();
      if ( msgcodec_ && r ) { result = msgcodec_->decode(*r); }
      return result;
    }

    Collection<R_elem<RemoteMessage>> doReadBlock(int blockSize);

    void doWrite(const RemoteMessage& v);

    void doWrite(shared_ptr<RemoteMessage> v_ptr) {
      if ( v_ptr ) { doWrite(*v_ptr); }
    }

  protected:
    shared_ptr<MessageCodec> msgcodec_;
  };


  class EndpointState : public boost::basic_lockable_adapter<boost::mutex>
  {
  public:
    typedef boost::basic_lockable_adapter<boost::mutex> eplockable;

    using ConcurrentEndpointMap =
      boost::externally_locked<shared_ptr<EndpointMap>,EndpointState>;

    using CodecDetails = boost::variant<shared_ptr<Codec>, shared_ptr<MessageCodec>>;

    using EndpointDetails = tuple<shared_ptr<IOHandle>,
                                  shared_ptr<EndpointBuffer>,
                                  shared_ptr<EndpointBindings>>;

    EndpointState()
      : eplockable(), epsLogger(new LogMT("EndpointState")),
        internalEndpoints(emptyEndpointMap()),
        externalEndpoints(emptyEndpointMap())
    {}

    void addEndpoint(Identifier id, EndpointDetails details, CodecDetails cdetails) {
      addEndpoint(id, details, cdetails, externalEndpointId(id));
    }

    void removeEndpoint(Identifier id) {
      if ( externalEndpointId(id) ) {
        removeEndpoint(id, externalEndpoints);
      } else {
        removeEndpoint(id, internalEndpoints);
      }
    }

    void clearEndpoints() {
      clearEndpoints(internalEndpoints);
      clearEndpoints(externalEndpoints);
      return;
    }

    void clearEndpoints(shared_ptr<ConcurrentEndpointMap> m);

    shared_ptr<InternalEndpoint> getInternalEndpoint(Identifier id);

    shared_ptr<ExternalEndpoint> getExternalEndpoint(Identifier id);

    size_t numEndpoints();

    void logEndpoints();

  protected:
    shared_ptr<LogMT> epsLogger;
    shared_ptr<ConcurrentEndpointMap> internalEndpoints;
    shared_ptr<ConcurrentEndpointMap> externalEndpoints;

    shared_ptr<ConcurrentEndpointMap> emptyEndpointMap()
    {
      shared_ptr<EndpointMap> m = shared_ptr<EndpointMap>(new EndpointMap());
      return shared_ptr<ConcurrentEndpointMap>(new ConcurrentEndpointMap(*this, m));
    }


    void addEndpoint(Identifier id, EndpointDetails details, CodecDetails cdetails, bool isExternal)
    {
      boost::strict_lock<EndpointState> guard(*this);

      shared_ptr<ConcurrentEndpointMap> epMap = isExternal? externalEndpoints : internalEndpoints;
      auto lb = epMap->get(guard)->lower_bound(id);

      if ( lb == epMap->get(guard)->end() || id != lb->first )
      {
        shared_ptr<Endpoint> ep = isExternal?
          ( std::static_pointer_cast<Endpoint, ExternalEndpoint>(
              make_shared<ExternalEndpoint>(get<0>(details), get<1>(details), get<2>(details),
                                            boost::get<shared_ptr<Codec>>(cdetails))) )

          : ( std::static_pointer_cast<Endpoint, InternalEndpoint>(
                make_shared<InternalEndpoint>(get<0>(details), get<1>(details), get<2>(details),
                                              boost::get<shared_ptr<MessageCodec>>(cdetails))) );

        epMap->get(guard)->insert(lb, make_pair(id, ep));
      } else if ( epsLogger ) {
        BOOST_LOG(*epsLogger) << "Invalid attempt to add a duplicate endpoint for " << id;
      }
    }

    void removeEndpoint(Identifier id, shared_ptr<ConcurrentEndpointMap> epMap)
    {
      boost::strict_lock<EndpointState> guard(*this);
      epMap->get(guard)->erase(id);
    }

    shared_ptr<Endpoint> getEndpoint(Identifier id, shared_ptr<ConcurrentEndpointMap> epMap)
    {
      boost::strict_lock<EndpointState> guard(*this);
      shared_ptr<Endpoint> r;
      auto it = epMap->get(guard)->find(id);
      if ( it != epMap->get(guard)->end() ) { r = it->second; }
      return r;
    }
  };


  ////-------------------------------------
  //// Connections and their containers.

  class ConnectionState : public boost::shared_lockable_adapter<boost::shared_mutex>, public virtual LogMT
  {
  protected:
    // Connection maps are not thread-safe themselves, but are only
    // ever used by ConnectionState methods (which are thread-safe).
    class ConnectionMap : public virtual LogMT {
    public:
      ConnectionMap() : LogMT("ConnectionMap") {}

      ConnectionMap(shared_ptr<Net::NContext> ctxt): LogMT("ConnectionMap"), context_(ctxt) {}

      bool addConnection(Address& addr, shared_ptr<Net::NConnection> c);

      shared_ptr<Net::NConnection> getConnection(Address& addr);

      void removeConnection(Address& addr) { cache.erase(addr); }

      void clearConnections() { cache.clear(); }

      size_t size() { return cache.size(); }

    protected:
      shared_ptr<Net::NContext> context_;
      map<Address, shared_ptr<Net::NConnection> > cache;
   };

  public:
    typedef boost::shared_lockable_adapter<boost::shared_mutex> shlockable;

    typedef boost::externally_locked<shared_ptr<ConnectionMap>, ConnectionState>
              ConcurrentConnectionMap;


    ConnectionState(shared_ptr<Net::NContext> ctxt)
      : shlockable(), LogMT("ConnectionState"),
        networkCtxt(ctxt),
        internalConnections(uninitializedConnectionMap()),
        externalConnections(emptyConnectionMap(ctxt))
    {}

    ConnectionState(shared_ptr<Net::NContext> ctxt, bool simulation) : ConnectionState(ctxt)
    {
      if ( !simulation ) { internalConnections = emptyConnectionMap(ctxt); }
    }

    void lock()          { shlockable::lock(); }
    void lock_shared()   { shlockable::lock_shared(); }
    void unlock()        { shlockable::unlock(); }
    void unlock_shared() { shlockable::unlock_shared(); }

    bool hasInternalConnections();

    shared_ptr<Net::NConnection> addConnection(Address addr, bool internal);

    void removeConnection(Address addr);

    void removeConnection(Address addr, bool internal);

    // TODO: Ideally, this should be a shared lock.
    // TODO: investigate why does clang not like a shared_lock_guard here, while EndpointState::getEndpoint is fine.
    shared_ptr<Net::NConnection> getConnection(Address addr);

    // TODO: Ideally, this should be a shared lock.
    shared_ptr<Net::NConnection> getConnection(Address addr, bool internal);

    // TODO
    // virtual shared_ptr<Net::NConnection> getEstablishedConnection(Address addr) = 0;
    // virtual shared_ptr<Net::NConnection> getEstablishedConnection(Address addr, bool internal) = 0;

    void clearConnections();

    void clearConnections(bool internal);

    size_t numConnections();

  protected:
    shared_ptr<Net::NContext> networkCtxt;
    shared_ptr<ConcurrentConnectionMap> internalConnections;
    shared_ptr<ConcurrentConnectionMap> externalConnections;

    shared_ptr<ConcurrentConnectionMap> uninitializedConnectionMap() {
      return shared_ptr<ConcurrentConnectionMap>(new ConcurrentConnectionMap(*this));
    }

    shared_ptr<ConcurrentConnectionMap>
    emptyConnectionMap(shared_ptr<Net::NContext> ctxt);

    shared_ptr<ConcurrentConnectionMap> mapForId(Identifier& id) {
      return externalEndpointId(id) ? externalConnections : internalConnections;
    }

    // TODO: implement an alternative using a shared_lock_guard
    shared_ptr<ConcurrentConnectionMap>
    mapForAddress(Address& addr, boost::strict_lock<ConnectionState>& guard);

    // TODO: implement an alternative using a shared_lock_guard
    shared_ptr<Net::NConnection>
    getConnection(Address& addr,
                  shared_ptr<ConcurrentConnectionMap> connections,
                  boost::strict_lock<ConnectionState>& guard);
  };
};

#endif
// vim: set sw=2 ts=2 sts=2:
