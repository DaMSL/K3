#ifndef K3_RUNTIME_ENDPOINT_H
#define K3_RUNTIME_ENDPOINT_H

#include <list>
#include <map>
#include <memory>
#include <tuple>
#include <boost/thread/mutex.hpp>
#include <boost/thread/lockable_adapter.hpp>
#include <boost/thread/externally_locked.hpp>
#include <k3/runtime/cpp/Common.hpp>
#include <k3/runtime/cpp/Network.hpp>
#include <k3/runtime/cpp/IOHandle.hpp>

namespace K3
{
  using namespace std;
  using namespace boost;

  using std::shared_ptr;
  using boost::mutex;

  using Asio::NEndpoint;
  using Asio::NConnection;

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
    // TODO: common endpoint buffer methods.
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
  // TODO: thread-safe methods.

  template<typename Value>
  class Endpoint {
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

  // TODO: shared (i.e., multi-reader) lock.
  template<typename Value>
  class EndpointState : public basic_lockable_adapter<mutex>
  {
  public:
    typedef basic_lockable_adapter<mutex> eplockable;
    
    typedef externally_locked<shared_ptr<EndpointMap<Value> >, EndpointState>
              ConcurrentEndpointMap;
    
    typedef tuple<shared_ptr<IOHandle<Value> >, 
                  shared_ptr<EndpointBuffer<Value> >,
                  shared_ptr<EndpointBindings<Value> > >
              EndpointDetails;

    EndpointState() : eplockable(),
                      internalEndpoints(emptyEndpointMap()),
                      externalEndpoints(emptyEndpointMap())
    {}

    shared_ptr<ConcurrentEndpointMap> emptyEndpointMap() {
      return shared_ptr<ConcurrentEndpointMap>(
          new ConcurrentEndpointMap(
                *this, shared_ptr<EndpointMap<Value> >(new EndpointMap<Value>())));
    }

    virtual void addEndpoint(Identifier id, EndpointDetails details) = 0;

    virtual void removeEndpoint(Identifier id) = 0;

    virtual shared_ptr<Endpoint<Value> > getEndpoint(Identifier id, bool internal) = 0;

    size_t numEndpoints() {}

  protected:
    shared_ptr<ConcurrentEndpointMap> internalEndpoints;
    shared_ptr<ConcurrentEndpointMap> externalEndpoints;
  };


  //-------------------------------------
  // Connections and their containers.
  // TODO: thread-safe methods.

  class ConnectionMap {
  public:
    Address anchorAddress;
    shared_ptr<NEndpoint> anchorEndpoint;
    map<Address, shared_ptr<NConnection> > cache;
  };

  // TODO: addresses for anchors as needed.
  class ConnectionState : public basic_lockable_adapter<mutex>
  {
  public:
    typedef basic_lockable_adapter<mutex> clocklable;
    
    typedef externally_locked<shared_ptr<ConnectionMap>, ConnectionState>
              ConcurrentConnectionMap;
    
    ConnectionState()
      : clocklable(),
        internalConnections(uninitializedConnectionMap()),
        externalConnections(emptyConnectionMap())
    {}

    ConnectionState(bool simulation) : ConnectionState()
    {
      if ( !simulation ) { internalConnections = emptyConnectionMap(); }
    }

    bool hasInternalConnections() {
      bool r = false;
      if ( internalConnections ) {
        strict_lock<ConnectionState> guard(*this);
        r = internalConnections->get(guard)? true : false;
      }
      return r;
    }

    shared_ptr<ConcurrentConnectionMap> uninitializedConnectionMap() {
      return shared_ptr<ConcurrentConnectionMap>(new ConcurrentConnectionMap(*this));
    }

    shared_ptr<ConcurrentConnectionMap> emptyConnectionMap() {
      shared_ptr<ConnectionMap> mp(new ConnectionMap());
      ConcurrentConnectionMap cmp(*this, mp);
      return shared_ptr<ConcurrentConnectionMap>(new ConcurrentConnectionMap(cmp));
    }

    virtual shared_ptr<NConnection> addConnection(Address addr) = 0;
    virtual shared_ptr<NConnection> addConnection(Identifier id, Address addr) = 0;
    virtual shared_ptr<NConnection> addConnection(Address addr, bool internal) = 0;

    virtual void removeConnection(Address addr) = 0;
    virtual void removeConnection(Address addr, bool internal) = 0;

    virtual shared_ptr<NConnection> getConnection(Address addr) = 0;
    virtual shared_ptr<NConnection> getConnection(Address addr, bool internal) = 0;

    virtual shared_ptr<NConnection> getEstablishedConnection(Address addr) = 0;
    virtual shared_ptr<NConnection> getEstablishedConnection(Address addr, bool internal) = 0;

    virtual void clearConnections(bool internal) = 0;

    virtual size_t numInternalConnections() = 0;
    virtual size_t numExternalConnections() = 0;

  protected:
    shared_ptr<ConcurrentConnectionMap> internalConnections;
    shared_ptr<ConcurrentConnectionMap> externalConnections;
  };
}

#endif