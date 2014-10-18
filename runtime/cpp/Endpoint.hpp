#ifndef K3_RUNTIME_ENDPOINT_H
#define K3_RUNTIME_ENDPOINT_H

#include <list>
#include <map>
#include <tuple>
#include <boost/thread/mutex.hpp>
#include <boost/thread/externally_locked.hpp>
#include <boost/thread/lockable_adapter.hpp>

#include <Common.hpp>
#include <Codec.hpp>
#include <Network.hpp>
#include <IOHandle.hpp>
#include <Queue.hpp>

using std::runtime_error;

// TODO: rewrite endpoint and connection containers without externally_locked as this requires a strict_lock.
// Ideally we want to use a shared_lock since the most common operation will be read accesses.

namespace K3
{
  typedef tuple<int, int> BufferSpec;

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

  typedef std::function<void(const Address&, const TriggerId, shared_ptr<Value>)> SendFunctionPtr;

  class Endpoint;
  typedef map<Identifier, shared_ptr<Endpoint> > EndpointMap;

  static inline int bufferMaxSize(BufferSpec& spec)   { return get<0>(spec); }
  static inline int bufferBatchSize(BufferSpec& spec) { return get<1>(spec); }

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


  //------------------------
  // Endpoint buffers.
  // In contrast to the Haskell engine, in C++ we implement the concept of the
  // BufferContents datatype inline in the EndpointBuffer class. This is due
  // to the difference in the concurrency abstractions (e.g., MVar vs. externally_locked).

  class EndpointBuffer {
  public:
    typedef std::function<void(shared_ptr<Value>)> NotifyFn;

    EndpointBuffer() {}

    virtual bool empty() = 0;
    virtual bool full() = 0;
    virtual size_t size() = 0;
    virtual size_t capacity() = 0;

    // Appends to this buffer, returning if the append succeeds.
    virtual bool push_back(shared_ptr<Value> v) = 0;

    // Maybe Removes a value from the buffer and returns it
    virtual shared_ptr<Value> pop() = 0;

    // Attempt to pull a value from the provided IOHandle
    // into the buffer. Returns a Maybe Value
    virtual shared_ptr<Value> refresh(shared_ptr<IOHandle>, NotifyFn) = 0;

    // Flush the contents of the buffer out to the provided IOHandle
    virtual void flush(shared_ptr<IOHandle>, NotifyFn) = 0;

    // Transfer the contents of the buffer into provided MessageQueues
    // Using the provided InternalCodec to convert from Value to Message
    virtual bool transfer(shared_ptr<MessageQueues>, shared_ptr<InternalCodec>, NotifyFn)= 0;
  };


  class ScalarEPBufferMT : public EndpointBuffer, public boost::basic_lockable_adapter<boost::mutex> {
  public:
    typedef ScalarEPBufferMT LockB;

    ScalarEPBufferMT() : EndpointBuffer(), contents(*this) {}
    // Metadata
    bool   empty()    {
      boost::strict_lock<LockB> guard(*this);
      return !(contents.get(guard));
    }
    bool   full()     {
      boost::strict_lock<LockB> guard(*this);
      return static_cast<bool>(contents.get(guard));
    }
    size_t size()     {
      boost::strict_lock<LockB> guard(*this);
      return contents.get(guard) ? 1 : 0;
    }
    size_t capacity() { return 1; }

    // Buffer Operations
    bool push_back(shared_ptr<Value> v);

    shared_ptr<Value> pop();

    shared_ptr<Value> refresh(shared_ptr<IOHandle> ioh, NotifyFn notify);

    void flush(shared_ptr<IOHandle> ioh, NotifyFn notify);

    bool transfer(shared_ptr<MessageQueues> queues, shared_ptr<InternalCodec> cdec, NotifyFn notify);

   protected:
    boost::externally_locked<shared_ptr<Value>, LockB> contents;
  };

  class ScalarEPBufferST : public EndpointBuffer {
  public:
    ScalarEPBufferST() : EndpointBuffer() {}
    // Metadata
    bool   empty()    { return !contents; }
    bool   full()     { return static_cast<bool>(contents); }
    size_t size()     { return contents ? 1 : 0; }
    size_t capacity() { return 1; }

    // Buffer Operations
    bool push_back(shared_ptr<Value> v);

    shared_ptr<Value> pop();

    shared_ptr<Value> refresh(shared_ptr<IOHandle> ioh, NotifyFn notify);

    void flush(shared_ptr<IOHandle> ioh, NotifyFn notify);

    bool transfer(shared_ptr<MessageQueues> queues, shared_ptr<InternalCodec> cdec, NotifyFn notify);

   protected:
    shared_ptr<Value> contents;
  };

  class ContainerEPBufferST : public EndpointBuffer {
  public:
    ContainerEPBufferST(BufferSpec s) : EndpointBuffer(), spec(s) {
      contents = shared_ptr<list<Value>>(new list<Value>());
    }

    bool   empty() { return contents? contents->empty() : true; }
    bool   full()  { return size() >= bufferMaxSize(spec); }
    size_t size()  { return empty()? 0 : contents->size(); }
    size_t capacity();

    bool push_back(shared_ptr<Value> v);

    shared_ptr<Value> pop();

    shared_ptr<Value> refresh(shared_ptr<IOHandle> ioh, NotifyFn notify);

    // Default flush: do not force, wait for batch
    void flush(shared_ptr<IOHandle> ioh, NotifyFn notify) { flush(ioh,notify,false); }

    // flush overloaded with force flag to ignore batching semantics
    void flush(shared_ptr<IOHandle> ioh, NotifyFn notify, bool force);

    // Default transfer: do not force, wait for batch
    bool transfer(shared_ptr<MessageQueues> queues, shared_ptr<InternalCodec> cdec, NotifyFn notify) {
      return transfer(queues,cdec,notify,false);
    }

    // transfer overloaded with force flag to ignore batching semantics
    bool transfer(shared_ptr<MessageQueues> queues, shared_ptr<InternalCodec> cdec, NotifyFn notify, bool force);

   protected:
    shared_ptr<list<Value>> contents;
    BufferSpec spec;

    int batchSize() { int r = bufferMaxSize(spec); return r <=0? 1 : r;}
    bool batchAvailable() { return contents? contents->size() >= batchSize(): false;}
  };

  //----------------------------
  // I/O event notifications.

  class EndpointBindings : public LogMT {
  public:

    typedef list<tuple<Address, TriggerId>> Subscribers;
    typedef map<EndpointNotification, shared_ptr<Subscribers>> Subscriptions;

    EndpointBindings(SendFunctionPtr f) : LogMT("EndpointBindings"), sendFn(f) {}

    void attachNotifier(EndpointNotification nt, Address sub_addr, TriggerId sub_id);

    void detachNotifier(EndpointNotification nt, Address sub_addr, TriggerId sub_id);

    void notifyEvent(EndpointNotification nt, shared_ptr<Value> payload);

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
      if (handle_->isInput()) {
        refreshBuffer();
      }
    }

    shared_ptr<IOHandle> handle() { return handle_; }
    shared_ptr<EndpointBuffer> buffer() { return buffer_; }
    shared_ptr<EndpointBindings> subscribers() { return subscribers_; }

    void notify_subscribers(shared_ptr<Value> v);

    // An endpoint can be read if the handle can be read or the buffer isn't empty.
    bool hasRead() { return handle_->hasRead() || !buffer_->empty(); }

    // An endpoint can be written to if the handle can be written to and the buffer isn't full.
    bool hasWrite() { return handle_->hasWrite() && !buffer_->full(); }

    shared_ptr<Value> refreshBuffer();

    void flushBuffer();

    shared_ptr<Value> doRead() { return refreshBuffer(); }

    void doWrite(Value& v) { doWrite(make_shared<Value>(v)); }

    void doWrite(shared_ptr<Value> v_ptr);

    bool do_push(shared_ptr<Value> val, shared_ptr<MessageQueues> q, shared_ptr<InternalCodec> codec);

    // Closes the endpoint's IOHandle, while also notifying subscribers
    // of the close event.
    void close();;

  protected:
    shared_ptr<IOHandle> handle_;
    shared_ptr<EndpointBuffer> buffer_;
    shared_ptr<EndpointBindings> subscribers_;
  };


  class EndpointState : public boost::basic_lockable_adapter<boost::mutex>
  {
  public:
    typedef boost::basic_lockable_adapter<boost::mutex> eplockable;

    using ConcurrentEndpointMap =
      boost::externally_locked<shared_ptr<EndpointMap>,EndpointState>;

    using EndpointDetails = tuple<shared_ptr<IOHandle>,
                                 shared_ptr<EndpointBuffer>,
                                 shared_ptr<EndpointBindings> >;

    EndpointState()
      : eplockable(), epsLogger(new LogMT("EndpointState")),
        internalEndpoints(emptyEndpointMap()),
        externalEndpoints(emptyEndpointMap())
    {}

    void addEndpoint(Identifier id, EndpointDetails details) {
      if ( externalEndpointId(id) ) {
        addEndpoint(id, details, externalEndpoints);
      } else {
        addEndpoint(id, details, internalEndpoints);
      }
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

    shared_ptr<Endpoint> getInternalEndpoint(Identifier id);

    shared_ptr<Endpoint> getExternalEndpoint(Identifier id);

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


    void addEndpoint(Identifier id, EndpointDetails details,
                     shared_ptr<ConcurrentEndpointMap> epMap)
    {
      boost::strict_lock<EndpointState> guard(*this);
      auto lb = epMap->get(guard)->lower_bound(id);

      if ( lb == epMap->get(guard)->end() || id != lb->first )
      {
        shared_ptr<Endpoint> ep =
          shared_ptr<Endpoint>(new Endpoint(get<0>(details), get<1>(details), get<2>(details)));

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

    shared_ptr<Endpoint>
    getEndpoint(Identifier id, shared_ptr<ConcurrentEndpointMap> epMap)
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
