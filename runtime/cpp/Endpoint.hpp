#ifndef K3_RUNTIME_ENDPOINT_H
#define K3_RUNTIME_ENDPOINT_H

#include <list>
#include <map>
#include <memory>
#include <tuple>
//#include <boost/thread/externally_locked.hpp>
#include <boost/thread/lockable_adapter.hpp>
#include <boost/thread/shared_lock_guard.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <runtime/cpp/Common.hpp>
#include <runtime/cpp/Network.hpp>
#include <runtime/cpp/IOHandle.hpp>
#include <runtime/cpp/Queue.hpp>

// TODO: rewrite endpoint and connection containers without externally_locked as this requires a strict_lock.
// Ideally we want to use a shared_lock since the most common operation will be read accesses.

namespace K3
{
  using namespace std;

  using boost::mutex;
  using boost::strict_lock;
  using boost::shared_mutex;
  using boost::shared_lockable_adapter;
  using boost::shared_lock_guard;
  using boost::externally_locked;
  using boost::basic_lockable_adapter;

  typedef tuple<int, int> BufferSpec;

  enum class EndpointNotification {
    NullEvent, FileData, FileTick, FileClose, SocketAccept, SocketData, SocketTick, SocketClose
  };

  class Endpoint;
  typedef map<Identifier, shared_ptr<Endpoint> > EndpointMap;

  int bufferMaxSize(BufferSpec& spec)   { return get<0>(spec); }
  int bufferBatchSize(BufferSpec& spec) { return get<1>(spec); }

  string internalEndpointPrefix() { return string("__");  }

  Identifier connectionId(Address& addr) {
    return internalEndpointPrefix() + "_conn_" + addressAsString(addr);
  }

  Identifier peerEndpointId(Address& addr) {
    return internalEndpointPrefix() + "_node_" + addressAsString(addr);
  }

  bool externalEndpointId(Identifier& id) {
    string pfx = internalEndpointPrefix();
    return ( mismatch(pfx.begin(), pfx.end(), id.begin()).first != pfx.end() );
  }


  //------------------------
  // Endpoint buffers.
  // In contrast to the Haskell engine, in C++ we implement the concept of the
  // BufferContents datatype inline in the EndpointBuffer class. This is due
  // to the difference in the concurrency abstractions (e.g., MVar vs. externally_locked).

  class EndpointBuffer : public LogMT {
  public:
    EndpointBuffer() : LogMT("Endpoint Buffer") {}

    virtual bool empty() = 0;
    virtual bool full() = 0;
    virtual size_t size() = 0;
    virtual size_t capacity() = 0;

    // Iterator interface to the endpoint buffer.
    //virtual iterator<shared_ptr<Value> > begin() = 0;
    //virtual iterator<shared_ptr<Value> > end() = 0;

    // Appends to this buffer, returning if the append succeeds.
    virtual bool push_back(shared_ptr<Value> v) = 0;

    // Maybe Removes a value from the buffer and returns it 
    virtual shared_ptr<Value> pop() = 0;

    // Attempt to pull a value from the provided IOHandle
    // into the buffer. Returns a Maybe Value
    virtual tuple<shared_ptr<Value>, EndpointNotification> refresh(shared_ptr<IOHandle>) = 0;

    // Flush the contents of the buffer out to the provided IOHandle
    virtual EndpointNotification flush(shared_ptr<IOHandle>) = 0;

    // Transfer the contents of the buffer into provided MessageQueues
    // Using the provided InternalCodec to convert from Value to Message
    virtual void transfer(shared_ptr<MessageQueues>, shared_ptr<InternalCodec>)= 0;
  };

  class ScalarEPBufferST : public EndpointBuffer, public LogMT {
  public:
    ScalarEPBufferST() : EndpointBuffer(), LogMT("ScalarEPBufferST") {}
    // Metadata
    bool   empty()    { return !contents; }
    bool   full()     { return static_cast<bool>(contents); }
    size_t size()     { return contents ? 1 : 0; }
    size_t capacity() { return 1; }

    // Buffer Operations
    bool push_back(shared_ptr<Value> v) {
      // Failure:
      if (this->full()) {
        return false;
      }
      // Success:
      contents = v;
      return true;

    }

    shared_ptr<Value> pop() {
      shared_ptr<Value> v;
      if (!this->empty()) {
        // Success:
        v = contents;
        contents = shared_ptr<Value>();
      }
      // In case of failure, v is a null pointer
      return v;
    }

    tuple<shared_ptr<Value>, EndpointNotification> refresh(shared_ptr<IOHandle> ioh) {
      shared_ptr<Value> r;
      EndpointNotification nt = EndpointNotification::NullEvent;

      // Read from the buffer if possible
      if (!(this->empty())) {
        r = this->pop();
      }

      // If there is more data in the underlying IOHandle
      // use it to populate the buffer
      if (ioh->hasRead()) {
        shared_ptr<Value> v = ioh->doRead();
        this->push_back(v);
        nt = (ioh->builtin() || ioh->file())?
               EndpointNotification::FileData : EndpointNotification::SocketData;
      }

     return make_tuple(r, nt);
    }

    EndpointNotification flush(shared_ptr<IOHandle> ioh) {
      // Default to a NullEvent
      EndpointNotification nt = EndpointNotification::NullEvent;
      // pop() a value and write to the handle if possible,
      // registering the proper notification type
      if (!this->empty()) {
        ioh->doWrite(*(this->pop()));
        nt = (ioh->builtin() || ioh->file())?
               EndpointNotification::FileData : EndpointNotification::SocketData;
      }
      return nt;
    }

    void transfer(shared_ptr<MessageQueues> queues, shared_ptr<InternalCodec> cdec) {
      if(!this->empty()) {
        Message msg = cdec->read_message(*(this->pop()));
        queues->enqueue(msg);
      }
    }
   protected:
    shared_ptr<Value> contents;
  };

  class ContainerEPBufferST : public EndpointBuffer, public LogMT {
  public:
    ContainerEPBufferST(BufferSpec s) : spec(s), EndpointBuffer(),
    LogMT("ScalarEPBufferST") {
      contents = shared_ptr<list<Value>>(new list<Value>());
    }

    bool   empty() { return contents? contents->empty() : true; }
    bool   full()  { return size() >= bufferMaxSize(spec); }
    size_t size()  { return empty()? 0 : contents->size(); }
    size_t capacity() {
      if (!contents) {
        return 0;
      }
      int s = bufferMaxSize(spec);
      return s <= 0? contents->max_size() : s;
    }

    bool push_back(shared_ptr<Value> v) {
      // Failure if contents is null or full
      if (!contents || full()) {
        return false;
      }
      // Success
      contents->push_back(*v);
      return true;
    }

    shared_ptr<Value> pop() {
      shared_ptr<Value> v;
      if (!empty()) {
        v = make_shared<Value>(contents->front());
        contents->pop_front();
      }
      return v;
    }

    tuple<shared_ptr<Value>, EndpointNotification> refresh(shared_ptr<IOHandle> ioh) {
      shared_ptr<Value> r;
      EndpointNotification nt = EndpointNotification::NullEvent;

      // Read from the buffer if possible
      if (!(this->empty())) {
        r = this->pop();
      }

      // If there is more data in the underlying IOHandle
      // use it to populate the buffer
      if (ioh->hasRead()) {
        shared_ptr<Value> v = ioh->doRead();
        this->push_back(v);
        nt = (ioh->builtin() || ioh->file())?
               EndpointNotification::FileData : EndpointNotification::SocketData;
      }

     return make_tuple(r, nt);
    }

    EndpointNotification flush(shared_ptr<IOHandle> ioh) {
      // Default to a NullEvent
      EndpointNotification nt = EndpointNotification::NullEvent;
      // pop() a value and write to the handle if possible,
      // registering the proper notification type
      if (!this->empty()) {
        ioh->doWrite(*(this->pop()));
        nt = (ioh->builtin() || ioh->file())?
               EndpointNotification::FileData : EndpointNotification::SocketData;
      }
      return nt;
    }

    void transfer(shared_ptr<MessageQueues> queues, shared_ptr<InternalCodec> cdec) {
      if(!this->empty()) {
        Message msg = cdec->read_message(*(this->pop()));
        queues->enqueue(msg);
      }
    }

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

    typedef std::function<void(const Address&, const Identifier&, shared_ptr<Value>)> SendFunctionPtr;
    typedef list<tuple<Address, Identifier>> Subscribers;
    typedef map<EndpointNotification, shared_ptr<Subscribers>> Subscriptions;

    EndpointBindings(SendFunctionPtr f) : LogMT("EndpointBindings"), sendFn(f) {}

    void attachNotifier(EndpointNotification nt, Identifier sub_id, Address sub_addr) {
      shared_ptr<Subscribers> s = eventSubscriptions[nt];
        if (!s) {
          s = shared_ptr<Subscribers>(new Subscribers());
          eventSubscriptions[nt] = s;
        }

        s->push_back(make_tuple(sub_id, sub_addr));
      }

    void detachNotifier(EndpointNotification nt, Identifier sub_id, Address sub_addr) {
      auto it = eventSubscriptions.find(nt);
      if ( it != eventSubscriptions.end() ) {
        shared_ptr<Subscribers> s = it->second;
        if (s) {
          s->remove_if(
            [&sub_id, &sub_addr](const tuple<Identifier, Address>& t){
              return get<0>(t) == sub_id && get<1>(t) == sub_addr;
            });
        }
      }
    }

    void notifyEvent(EndpointNotification nt, shared_ptr<Value> payload) {
      auto it = eventSubscriptions.find(nt);
      if (it != eventSubscriptions.end()) {
        shared_ptr<Subscribers> s = it->second;
        if (s) {
          for (tuple<Identifier, Address> t : *s) {
            sendFn(get<0>(t), get<1>(t), payload);
          }
        }
      }
    }

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
             shared_ptr<EndpointBindings> subs,
             std::function<void(shared_ptr<Value>)> callbackFn)
      : handle_(ioh), buffer_(buf), subscribers_(subs), callbackFn_(callbackFn)
    {
      refreshBuffer();
    }

    shared_ptr<IOHandle> handle() { return handle_; }
    shared_ptr<EndpointBuffer> buffer() { return buffer_; }
    shared_ptr<EndpointBindings> subscribers() { return subscribers_; }

    // An endpoint can be read if the handle can be read or the buffer isn't empty.
    bool hasRead() {
        return handle_->hasRead() || !buffer_->empty();
    }

    // An endpoint can be written to if the handle can be written to and the buffer isn't full.
    bool hasWrite() {
        return handle_->hasWrite() && !buffer_->full();
    }

    tuple<shared_ptr<Value>, EndpointNotification> refreshBuffer() {
        return buffer_->refresh(handle_);
    }
    shared_ptr<Value> doRead() {
        // TODO concurrency?
        tuple<shared_ptr<Value>, EndpointNotification> readResult;

        readResult = refreshBuffer();

        // Notify those subscribers who need to be notified of the event.
        subscribers_->notifyEvent(get<1>(readResult));

        // Return the read result.
        return get<0>(readResult);
    }

    void doWrite(Value& v) {
        bool result = buffer_->push_back(make_shared<Value>(v));

        if (result) {
            // TODO: Append failed, flush?
        } else {
            // TODO: Success, now what?
        }

        return;
    }

    // TODO
    void enqueueToEndpoint(shared_ptr<Value> val) {}

  protected:
    shared_ptr<IOHandle> handle_;
    shared_ptr<EndpointBuffer> buffer_;
    shared_ptr<EndpointBindings> subscribers_;
    std::function<void(shared_ptr<Value>)> callbackFn_;
  };


  //class EndpointState : public shared_lockable_adapter<shared_mutex>, public virtual LogMT
  //{
  //public:
  //  typedef shared_lockable_adapter<shared_mutex> eplockable;
  //
  //  using ConcurrentEndpointMap =
  //    externally_locked<shared_ptr<EndpointMap>,EndpointState> ;

  //  using EndpointDetails = tuple<shared_ptr<IOHandle>,
  //                                shared_ptr<EndpointBuffer>,
  //                                shared_ptr<EndpointBindings> >;

  //  EndpointState()
  //    : eplockable(), LogMT("EndpointState"),
  //      internalEndpoints(emptyEndpointMap()),
  //      externalEndpoints(emptyEndpointMap())
  //  {}

  //  void addEndpoint(Identifier id, EndpointDetails details) {
  //    if ( !externalEndpointId(id) ) {
  //      addEndpoint(id, details, internalEndpoints);
  //    } else {
  //      string errorMsg = "Invalid internal endpoint identifier";
  //      logAt(trivial::error, errorMsg);
  //      throw runtime_error(errorMsg);
  //    }
  //  }
  //  void removeEndpoint(Identifier id) {
  //    if ( !externalEndpointId(id) ) {
  //      removeEndpoint(id, externalEndpoints);
  //    } else {
  //      removeEndpoint(id, internalEndpoints);
  //    }
  //  }

  //  // TODO: endpoint id validation.
  //  shared_ptr<Endpoint> getInternalEndpoint(Identifier id) {
  //    return getEndpoint(id, internalEndpoints);
  //  }

  //  shared_ptr<Endpoint> getExternalEndpoint(Identifier id) {
  //    return getEndpoint(id, externalEndpoints);
  //  }

  //  size_t numEndpoints() {
  //    shared_lock_guard<EndpointState> guard(*this);
  //    return externalEndpoints->get(guard)->size() + internalEndpoints->get(guard)->size();
  //  }

  //protected:
  //  shared_ptr<ConcurrentEndpointMap> internalEndpoints;
  //  shared_ptr<ConcurrentEndpointMap> externalEndpoints;

  //  shared_ptr<ConcurrentEndpointMap> emptyEndpointMap()
  //  {
  //    shared_ptr<EndpointMap> m = shared_ptr<EndpointMap>(new EndpointMap());
  //    return shared_ptr<ConcurrentEndpointMap>(new ConcurrentEndpointMap(*this, m));
  //  }


  //  void addEndpoint(Identifier id, EndpointDetails details,
  //                   shared_ptr<ConcurrentEndpointMap> epMap)
  //  {
  //    strict_lock<EndpointState> guard(*this);
  //    auto lb = epMap->get(guard)->lower_bound(id);
  //    if ( lb == epMap->get(guard)->end() || id != lb->first )
  //    {
  //      shared_ptr<Endpoint> ep =
  //        // TODO use a proper callback fn instead of nullptr
  //        shared_ptr<Endpoint>(new Endpoint(get<0>(details), get<1>(details), get<2>(details), nullptr));

  //      epMap->get(guard)->insert(lb, make_pair(id, ep));
  //    } else {
  //      BOOST_LOG(*this) << "Invalid attempt to add a duplicate endpoint for " << id;
  //    }
  //  }

  //  void removeEndpoint(Identifier id, shared_ptr<ConcurrentEndpointMap> epMap)
  //  {
  //    strict_lock<EndpointState> guard(*this);
  //    epMap->get(guard)->erase(id);
  //  }

  //  shared_ptr<Endpoint>
  //  getEndpoint(Identifier id, shared_ptr<ConcurrentEndpointMap> epMap)
  //  {
  //    shared_lock_guard<EndpointState> guard(*this);
  //    shared_ptr<Endpoint> r;
  //    auto it = epMap->get(guard)->find(id);
  //    if ( it != epMap->get(guard)->end() ) { r = it->second; }
  //    return r;
  //  }

  //};


  ////-------------------------------------
  //// Connections and their containers.

  //class ConnectionState : public shared_lockable_adapter<shared_mutex>,
  //                        public virtual LogMT
  //{
  //protected:
  //  // Connection maps are not thread-safe themselves, but are only
  //  // ever used by ConnectionState methods (which are thread-safe).
  //  class ConnectionMap : public virtual LogMT {
  //  public:
  //    ConnectionMap() : LogMT("ConnectionMap") {}
  //
  //    ConnectionMap(shared_ptr<Net::NContext> ctxt)
  //      : LogMT("ConnectionMap"), context_(ctxt)
  //    {}

  //    bool addConnection(Address& addr, shared_ptr<Net::NConnection> c)
  //    {
  //      bool r = false;
  //      auto lb = cache.lower_bound(addr);
  //      if ( lb == cache.end() || addr != lb->first ) {
  //        auto it = cache.insert(lb, make_pair(addr, c));
  //        r = it->first == addr;
  //      } else {
  //        BOOST_LOG(*this) << "Invalid attempt to add a duplicate endpoint for " << addressAsString(addr);
  //      }
  //      return r;
  //    }

  //    shared_ptr<Net::NConnection> getConnection(Address& addr)
  //    {
  //      shared_ptr<Net::NConnection> r;
  //      try {
  //        r = cache.at(addr);
  //      } catch (const out_of_range& oor) {}
  //      if ( !r ) { BOOST_LOG(*this) << "No connection found for " << addressAsString(addr); }
  //      return r;
  //    }

  //    void removeConnection(Address& addr) { cache.erase(addr); }

  //    void clearConnections() { cache.clear(); }

  //    size_t size() { return cache.size(); }

  //  protected:
  //    shared_ptr<Net::NContext> context_;
  //    map<Address, shared_ptr<Net::NConnection> > cache;
  //  };

  //public:
  //  typedef shared_lockable_adapter<shared_mutex> shlockable;
  //
  //  typedef externally_locked<shared_ptr<ConnectionMap>, ConnectionState>
  //            ConcurrentConnectionMap;


  //  ConnectionState(shared_ptr<Net::NContext> ctxt)
  //    : shlockable(), LogMT("ConnectionState"),
  //      networkCtxt(ctxt),
  //      internalConnections(uninitializedConnectionMap()),
  //      externalConnections(emptyConnectionMap(ctxt))
  //  {}

  //  ConnectionState(shared_ptr<Net::NContext> ctxt, bool simulation) : ConnectionState(ctxt)
  //  {
  //    if ( !simulation ) { internalConnections = emptyConnectionMap(ctxt); }
  //  }

  //  void lock()          { shlockable::lock(); }
  //  void lock_shared()   { shlockable::lock_shared(); }
  //  void unlock()        { shlockable::unlock(); }
  //  void unlock_shared() { shlockable::unlock_shared(); }

  //  bool hasInternalConnections() {
  //    bool r = false;
  //    if ( internalConnections ) {
  //      strict_lock<ConnectionState> guard(*this);
  //      r = internalConnections->get(guard)? true : false;
  //    }
  //    return r;
  //  }

  //  shared_ptr<Net::NConnection> addConnection(Address addr, bool internal)
  //  {
  //    strict_lock<ConnectionState> guard(*this);
  //    shared_ptr<ConcurrentConnectionMap> cMap =
  //      internal? internalConnections : externalConnections;

  //    shared_ptr<Net::NConnection> conn =
  //      shared_ptr<Net::NConnection>(new Net::NConnection(networkCtxt, addr));
  //
  //    return (cMap && cMap->get(guard)->addConnection(addr, conn))? conn : shared_ptr<Net::NConnection>();
  //  }

  //  void removeConnection(Address addr)
  //  {
  //    strict_lock<ConnectionState> guard(*this);
  //    shared_ptr<ConcurrentConnectionMap> cMap = mapForAddress(addr, guard);
  //    if ( cMap ) {
  //      cMap->get(guard)->removeConnection(addr);
  //    } else {
  //      BOOST_LOG(*this) << "No connection to " << addressAsString(addr) << " found for removal";
  //    }
  //  }
  //
  //  void removeConnection(Address addr, bool internal)
  //  {
  //    strict_lock<ConnectionState> guard(*this);
  //    shared_ptr<ConcurrentConnectionMap> cMap = internal? internalConnections : externalConnections;
  //    if ( cMap ) {
  //      cMap->get(guard)->removeConnection(addr);
  //    } else {
  //      BOOST_LOG(*this) << "No connection to " << addressAsString(addr) << " found for removal";
  //    }
  //  }

  //  // TODO: Ideally, this should be a shared lock.
  //  // TODO: investigate why does clang not like a shared_lock_guard here, while EndpointState::getEndpoint is fine.
  //  shared_ptr<Net::NConnection> getConnection(Address addr)
  //  {
  //    strict_lock<ConnectionState> guard(*this);
  //    shared_ptr<ConcurrentConnectionMap> cMap = mapForAddress(addr, guard);
  //    return getConnection(addr, cMap, guard);
  //  }

  //  // TODO: Ideally, this should be a shared lock.
  //  shared_ptr<Net::NConnection> getConnection(Address addr, bool internal)
  //  {
  //    strict_lock<ConnectionState> guard(*this);
  //    shared_ptr<ConcurrentConnectionMap> cMap = internal? internalConnections : externalConnections;
  //    return getConnection(addr, cMap, guard);
  //  }

  //  // TODO
  //  virtual shared_ptr<Net::NConnection> getEstablishedConnection(Address addr) = 0;
  //  virtual shared_ptr<Net::NConnection> getEstablishedConnection(Address addr, bool internal) = 0;

  //  void clearConnections() {
  //    strict_lock<ConnectionState> guard(*this);
  //    if ( internalConnections ) { internalConnections->get(guard)->clearConnections(); }
  //    if ( externalConnections ) { externalConnections->get(guard)->clearConnections(); }
  //  }
  //
  //  void clearConnections(bool internal) {
  //    strict_lock<ConnectionState> guard(*this);
  //    shared_ptr<ConcurrentConnectionMap> cMap =
  //      internal ? internalConnections : externalConnections;
  //    if ( cMap ) { cMap->get(guard)->clearConnections(); }
  //  };

  //  size_t numConnections() {
  //    strict_lock<ConnectionState> guard(*this);
  //    size_t r = 0;
  //    if ( internalConnections ) { r += internalConnections->get(guard)->size(); }
  //    if ( externalConnections ) { r += externalConnections->get(guard)->size(); }
  //    return r;
  //  }

  //protected:
  //  shared_ptr<Net::NContext> networkCtxt;
  //  shared_ptr<ConcurrentConnectionMap> internalConnections;
  //  shared_ptr<ConcurrentConnectionMap> externalConnections;

  //  shared_ptr<ConcurrentConnectionMap> uninitializedConnectionMap() {
  //    return shared_ptr<ConcurrentConnectionMap>(new ConcurrentConnectionMap(*this));
  //  }

  //  shared_ptr<ConcurrentConnectionMap>
  //  emptyConnectionMap(shared_ptr<Net::NContext> ctxt)
  //  {
  //    shared_ptr<ConnectionMap> mp(new ConnectionMap(ctxt));
  //    return shared_ptr<ConcurrentConnectionMap>(new ConcurrentConnectionMap(*this, mp));
  //  }

  //  shared_ptr<ConcurrentConnectionMap> mapForId(Identifier& id) {
  //    return externalEndpointId(id) ? externalConnections : internalConnections;
  //  }

  //  // TODO: implement an alternative using a shared_lock_guard
  //  shared_ptr<ConcurrentConnectionMap>
  //  mapForAddress(Address& addr, strict_lock<ConnectionState>& guard)
  //  {
  //    shared_ptr<ConcurrentConnectionMap> r;
  //    if ( internalConnections && getConnection(addr, internalConnections, guard) ) {
  //      r = internalConnections;
  //      if ( !r && externalConnections && getConnection(addr, externalConnections, guard) ) {
  //        r = externalConnections;
  //      }
  //    }
  //    return r;
  //  }

  //  // TODO: implement an alternative using a shared_lock_guard
  //  shared_ptr<Net::NConnection>
  //  getConnection(Address& addr,
  //                shared_ptr<ConcurrentConnectionMap> connections,
  //                strict_lock<ConnectionState>& guard)
  //  {
  //    shared_ptr<Net::NConnection> r;
  //    if ( connections ) { r = connections->get(guard)->getConnection(addr); }
  //    return r;
  //  }
  //};
}

#endif
