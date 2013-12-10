#ifndef K3_RUNTIME_ENDPOINT_H
#define K3_RUNTIME_ENDPOINT_H

#include <list>
#include <map>
#include <memory>
#include <tuple>
#include <boost/thread/externally_locked.hpp>
#include <boost/thread/lockable_adapter.hpp>
#include <boost/thread/shared_lock_guard.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <k3/runtime/cpp/Common.hpp>
#include <k3/runtime/cpp/Network.hpp>
#include <k3/runtime/cpp/IOHandle.hpp>

// TODO: rewrite endpoint and connection containers without externally_locked as this requires a strict_lock.
// Ideally we want to use a shared_lock since the most common operation will be read accesses.

// TODO: remove dependency on Net namespace alias, lifting usage to
// K3::NEndpoint<Net::NContext> and K3::NConnection<Net::NContext> in 
// EndpointState and ConnectionState.

namespace K3
{
  using namespace std;
  using namespace boost;

  namespace Net = K3::Asio;

  using std::shared_ptr;
  using boost::mutex;

  typedef tuple<int, int> BufferSpec;

  enum class EndpointNotification { FileData, FileClose, SocketAccept, SocketData, SocketClose };
  template<typename Value> class Endpoint;
  template<typename Value> using EndpointMap      = map<Identifier, shared_ptr<Endpoint<Value> > >;
  template<typename Value> using EndpointBindings = map<EndpointNotification, Value>;

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


  /*
  //------------------------
  // Buffer contents.
  template<typename Value>
  class BufferContents {
    // TODO: common buffer content methods.
  };

  template<typename Value>
  class SingletonBuffer : public BufferContents<Value> {
  public:
    shared_ptr<Value> contents;
  };

  template<typename Value>
  class MultiBuffer : public BufferContents<Value> {
  public:
    list<Value> contents;
    BufferSpec spec;
  };
  */

  //------------------------
  // Endpoint buffers.
  template<typename Value>
  class EndpointBuffer {
  public:

    virtual bool empty() = 0;
    virtual bool full() = 0;
    virtual size_t size() = 0;
    virtual size_t capacity() = 0;

    // Appends to this buffer, returning the value if the append fails.
    virtual shared_ptr<Value> append(shared_ptr<Value> v) = 0;

    // Transfers from this buffer into the given queues.
    virtual void enqueue(shared_ptr<MessageQueues> queues) = 0;

    // TODO
    // Writes the content of this buffer to the given IO handle. 
    // Returns a notification if the write is successfully performed.
    virtual shared_ptr<EndpointNotification>
    flush(shared_ptr<IOHandle<Value> > ioh) = 0;
    
    // Refresh this buffer by reading a value from the IO handle.
    // If the buffer is full, a value is returned. Also, a notification
    // is returned if the read is successfully performed.
    virtual tuple<shared_ptr<Value>, shared_ptr<EndpointNotification> >
    refresh(shared_ptr<IOHandle<Value> > ioh) = 0;
  };

  /*
  // TODO: locked endpoint buffer access.
  template<typename Value>
  class ExclusiveBuffer : public EndpointBuffer<Value> {};

  template<typename Value>
  class SharedBuffer : public EndpointBuffer<Value> {};
  */


  //---------------------------------
  // Endpoints and their containers.

  template<typename Value>
  class Endpoint
  {
  public:
    Endpoint(shared_ptr<IOHandle<Value> > ioh,
             shared_ptr<EndpointBuffer<Value> > buf,
             shared_ptr<EndpointBindings<Value> > subs)
      : handle_(ioh), buffer_(buf), subscribers_(subs)
    {}

    shared_ptr<IOHandle<Value> > handle() { return handle_; }
    shared_ptr<EndpointBuffer<Value> > buffer() { return buffer_; }
    shared_ptr<EndpointBindings<Value> > subscribers() { return subscribers_; }

  protected:
    shared_ptr<IOHandle<Value> > handle_;
    shared_ptr<EndpointBuffer<Value> > buffer_;
    shared_ptr<EndpointBindings<Value> > subscribers_;
  };

  template<typename Value>
  class EndpointState : public shared_lockable_adapter<shared_mutex>, public virtual LogMT
  {
  public:
    typedef shared_lockable_adapter<shared_mutex> eplockable;
    
    typedef externally_locked<shared_ptr<EndpointMap<Value> >, EndpointState>
              ConcurrentEndpointMap;
    
    typedef tuple<shared_ptr<IOHandle<Value> >, 
                  shared_ptr<EndpointBuffer<Value> >,
                  shared_ptr<EndpointBindings<Value> > >
              EndpointDetails;

    EndpointState() 
      : eplockable(), LogMT("EndpointState"),
        internalEndpoints(emptyEndpointMap()),
        externalEndpoints(emptyEndpointMap())
    {}

    shared_ptr<ConcurrentEndpointMap> emptyEndpointMap() {
      return shared_ptr<ConcurrentEndpointMap>(
          new ConcurrentEndpointMap(
                *this, shared_ptr<EndpointMap<Value> >(new EndpointMap<Value>())));
    }

    void addEndpoint(Identifier id, EndpointDetails details)
    {
      strict_lock<EndpointState> guard(*this);
      shared_ptr<ConcurrentEndpointMap> epMap = mapForId(id);
      auto lb = epMap->get(guard)->lower_bound(id);
      if ( lb == epMap->get(guard)->end() || id != lb->first ) {
        shared_ptr<Endpoint<Value> > ep = shared_ptr<Endpoint<Value> >(
          new Endpoint<Value>(get<0>(details), get<1>(details), get<2>(details)));      
        epMap->get(guard)->insert(lb, make_pair(id, ep));
      } else {
        BOOST_LOG(*this) << "Invalid attempt to add a duplicate endpoint for " << id;
      }
    }

    void removeEndpoint(Identifier id)
    {
      strict_lock<EndpointState> guard(*this);
      shared_ptr<ConcurrentEndpointMap> epMap = mapForId(id);
      epMap->get(guard)->erase(id);
    }

    shared_ptr<Endpoint<Value> > getEndpoint(Identifier id)
    {
      shared_lock_guard<EndpointState> guard(*this);
      shared_ptr<Endpoint<Value> > r;
      shared_ptr<ConcurrentEndpointMap> epMap = mapForId(id);
      auto it = epMap->get(guard)->find(id);
      if ( it != epMap->get(guard)->end() ) { r = it->second; }
      return r;
    }

    size_t numEndpoints() {
      shared_lock_guard<EndpointState> guard(*this);
      return externalEndpoints->get(guard)->size() + internalEndpoints->get(guard)->size();
    }

  protected:
    shared_ptr<ConcurrentEndpointMap> internalEndpoints;
    shared_ptr<ConcurrentEndpointMap> externalEndpoints;

    shared_ptr<ConcurrentEndpointMap> mapForId(Identifier& id) {
      return externalEndpointId(id) ? externalEndpoints : internalEndpoints;
    }
  };


  //-------------------------------------
  // Connections and their containers.

  // TODO: generalize from Net::NConnection to K3::NConnection or parameterized type.
  // TODO: addresses for anchors as needed.
  class ConnectionState : public shared_lockable_adapter<shared_mutex>,
                          public virtual LogMT
  {
  protected:
    // Connection maps are not thread-safe themselves, but are only
    // ever used by ConnectionState methods (which are thread-safe).
    class ConnectionMap : public virtual LogMT {
    public:
      ConnectionMap() : LogMT("ConnectionMap") {}
      
      ConnectionMap(Address anchorAddr, shared_ptr<Net::NEndpoint> anchorEP)
        : LogMT("ConnectionMap"), anchorAddress(anchorAddr), anchorEndpoint(anchorEP)
      {}

      bool addConnection(Address& addr, shared_ptr<Net::NConnection> c)
      {
        bool r = false;
        auto lb = cache.lower_bound(addr);
        if ( lb == cache.end() || addr != lb->first ) {
          auto it = cache.insert(lb, make_pair(addr, c));
          r = it->first == addr;
        } else {
          BOOST_LOG(*this) << "Invalid attempt to add a duplicate endpoint for " << addressAsString(addr);
        }
        return r;
      }

      shared_ptr<Net::NConnection> getConnection(Address& addr)
      {
        shared_ptr<Net::NConnection> r;
        try {
          r = cache.at(addr);
        } catch (const out_of_range& oor) {}
        if ( !r ) { BOOST_LOG(*this) << "No connection found for " << addressAsString(addr); }
        return r;
      }

      void removeConnection(Address& addr) { cache.erase(addr); }

      void clearConnections() { cache.clear(); }

      size_t size() { return cache.size(); }

    protected:
      Address anchorAddress;
      shared_ptr<Net::NEndpoint> anchorEndpoint;
      map<Address, shared_ptr<Net::NConnection> > cache;
    };

  public:
    typedef shared_lockable_adapter<shared_mutex> shlockable;
    
    typedef externally_locked<shared_ptr<ConnectionMap>, ConnectionState>
              ConcurrentConnectionMap;


    ConnectionState(shared_ptr<Net::NContext> ctxt)
      : shlockable(), LogMT("ConnectionState"),
        networkCtxt(ctxt),
        internalConnections(uninitializedConnectionMap()),
        externalConnections(emptyConnectionMap())
    {}

    ConnectionState(shared_ptr<Net::NContext> ctxt, bool simulation) : ConnectionState(ctxt)
    {
      if ( !simulation ) { internalConnections = emptyConnectionMap(); }
    }

    void lock()          { shlockable::lock(); }
    void lock_shared()   { shlockable::lock_shared(); }
    void unlock()        { shlockable::unlock(); }
    void unlock_shared() { shlockable::unlock_shared(); }

    bool hasInternalConnections() {
      bool r = false;
      if ( internalConnections ) {
        strict_lock<ConnectionState> guard(*this);
        r = internalConnections->get(guard)? true : false;
      }
      return r;
    }

    shared_ptr<Net::NConnection> addConnection(Address addr, bool internal)
    {
      strict_lock<ConnectionState> guard(*this);
      shared_ptr<ConcurrentConnectionMap> cMap =
        internal? internalConnections : externalConnections;

      shared_ptr<Net::NConnection> conn =
        shared_ptr<Net::NConnection>(new Net::NConnection(networkCtxt, addr));
      
      return (cMap && cMap->get(guard)->addConnection(addr, conn))? conn : shared_ptr<Net::NConnection>();
    }

    void removeConnection(Address addr)
    {
      strict_lock<ConnectionState> guard(*this);
      shared_ptr<ConcurrentConnectionMap> cMap = mapForAddress(addr, guard);
      if ( cMap ) {
        cMap->get(guard)->removeConnection(addr);
      } else {
        BOOST_LOG(*this) << "No connection to " << addressAsString(addr) << " found for removal";
      }
    }
    
    void removeConnection(Address addr, bool internal)
    {
      strict_lock<ConnectionState> guard(*this);
      shared_ptr<ConcurrentConnectionMap> cMap = internal? internalConnections : externalConnections;
      if ( cMap ) {
        cMap->get(guard)->removeConnection(addr);
      } else {
        BOOST_LOG(*this) << "No connection to " << addressAsString(addr) << " found for removal";
      }      
    }

    // TODO: Ideally, this should be a shared lock.
    // TODO: investiage why does clang not like a shared_lock_guard here, while EndpointState::getEndpoint is fine.
    shared_ptr<Net::NConnection> getConnection(Address addr)
    {
      strict_lock<ConnectionState> guard(*this);
      shared_ptr<ConcurrentConnectionMap> cMap = mapForAddress(addr, guard);
      return getConnection(addr, cMap, guard);
    }

    // TODO: Ideally, this should be a shared lock.
    shared_ptr<Net::NConnection> getConnection(Address addr, bool internal) 
    {
      strict_lock<ConnectionState> guard(*this);
      shared_ptr<ConcurrentConnectionMap> cMap = internal? internalConnections : externalConnections;
      return getConnection(addr, cMap, guard);
    }

    // TODO
    virtual shared_ptr<Net::NConnection> getEstablishedConnection(Address addr) = 0;    
    virtual shared_ptr<Net::NConnection> getEstablishedConnection(Address addr, bool internal) = 0;

    void clearConnections() {
      strict_lock<ConnectionState> guard(*this);
      if ( internalConnections ) { internalConnections->get(guard)->clearConnections(); }
      if ( externalConnections ) { externalConnections->get(guard)->clearConnections(); }
    }
    
    void clearConnections(bool internal) {
      strict_lock<ConnectionState> guard(*this);
      shared_ptr<ConcurrentConnectionMap> cMap = 
        internal ? internalConnections : externalConnections;
      if ( cMap ) { cMap->get(guard)->clearConnections(); }
    };

    size_t numConnections() {
      strict_lock<ConnectionState> guard(*this);
      size_t r = 0;
      if ( internalConnections ) { r += internalConnections->get(guard)->size(); }
      if ( externalConnections ) { r += externalConnections->get(guard)->size(); }
      return r;
    }

  protected:
    shared_ptr<Net::NContext> networkCtxt;
    shared_ptr<ConcurrentConnectionMap> internalConnections;
    shared_ptr<ConcurrentConnectionMap> externalConnections;

    shared_ptr<ConcurrentConnectionMap> uninitializedConnectionMap() {
      return shared_ptr<ConcurrentConnectionMap>(new ConcurrentConnectionMap(*this));
    }

    shared_ptr<ConcurrentConnectionMap> emptyConnectionMap() {
      shared_ptr<ConnectionMap> mp(new ConnectionMap());
      return shared_ptr<ConcurrentConnectionMap>(new ConcurrentConnectionMap(*this, mp));
    }

    shared_ptr<ConcurrentConnectionMap> mapForId(Identifier& id) {
      return externalEndpointId(id) ? externalConnections : internalConnections;
    }

    // TODO: implement an alternative using a shared_lock_guard
    shared_ptr<ConcurrentConnectionMap>
    mapForAddress(Address& addr, strict_lock<ConnectionState>& guard)
    {
      shared_ptr<ConcurrentConnectionMap> r;
      if ( internalConnections && getConnection(addr, internalConnections, guard) ) {
        r = internalConnections;
        if ( !r && externalConnections && getConnection(addr, externalConnections, guard) ) {
          r = externalConnections;
        }
      }
      return r;
    }

    // TODO: implement an alternative using a shared_lock_guard
    shared_ptr<Net::NConnection>
    getConnection(Address& addr,
                  shared_ptr<ConcurrentConnectionMap> connections,
                  strict_lock<ConnectionState>& guard)
    {
      shared_ptr<Net::NConnection> r;
      if ( connections ) { r = connections->get(guard)->getConnection(addr); }
      return r;
    }
  };
}

#endif