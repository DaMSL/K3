#ifndef K3_RUNTIME_ENDPOINT_H
#define K3_RUNTIME_ENDPOINT_H

#include <list>
#include <map>
#include <tuple>
#include <boost/shared_ptr.hpp>
#include <k3/runtime/Common.hpp>

namespace K3 {

  using namespace std;
  using namespace boost;

  class Endpoint;
  typedef tuple<int, int> BufferSpec;
  typedef map<Identifier, Endpoint> EEndpoints;

  string internalEndpointPrefix() { return string("__");  }
  
  Identifier connectionId(Address& addr) {
    return internalEndpointPrefix + "_conn_" + addressAsString(addr);
  }

  Identifier peerEndpointId(Address& addr) {
    return internalEndpointPrefix + "_node_" + addressAsString(addr);
  }

  bool externalEndpointId(Identifier& id) {
    return ( mismatch(internalEndpointPrefix, id.begin()).first != internalEndpointPrefix.end() );
  }

  //--------------------------
  // Low-level networking.
  // TODO: NEndpoint
  // TODO: NConnection
  // TODO: nanomsg, Boost ASIO network implementations.

  //--------------------------
  // IO handles
  int bufferMaxSize(BufferSpec& spec)   { return get<0>(spec); }
  int bufferBatchSize(BufferSpec& spec) { return get<1>(spec); }

  template<typename Value>
  class IOHandle {
  public:
    bool hasRead()      = 0;
    bool hasWrite()     = 0;
    Value read()        = 0;
    void write(Value v) = 0;

    shared_ptr<WireDesc<Value> > wireDesc;
  };

  template<typename Value>
  class BuiltinHandle : public IOHandle<Value> {
  public:
    enum BuiltinId { Stdin, Stdout, Stderr };
    BuiltinId handleId;
  };

  template<typename Value>
  class FileHandle : public IOHandle<Value> {};

  template<typename Value>
  class NetworkHandle : public IOHandle<Value> {
  public:
    shared_ptr<NEndpoint>   endpoint;
    shared_ptr<NConnection> connection;
  };

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


  //------------------------
  // Endpoint buffers.
  template<typename Value>
  class EndpointBuffer {
    // TODO: common endpoint buffer methods.
  };

  // TODO: locked endpoint buffer access.
  template<typename Value>
  class ExclusiveBuffer : public EndpointBuffer<Value> {};

  template<typename Value>
  class SharedBuffer : public EndpointBuffer<Value> {};


  //---------------------------------
  // Endpoint bindings and endpoints.
  enum EndpointNotification { FileData, FileClose, SocketAccept, SocketData, SocketClose };
  template<typename Value> class EndpointBindings : public map<EndpointNotification, Value> {};

  class Endpoint {
  public:
    Endpoint() {}
    IOHandle handle;
    EndpointBuffer buffer;
    EndpointBindings subscribers;
  };


  //-------------------------------------
  // Endpoint and connection containers
  // TODO: thread-safe implementations.
  
  class EConnectionMap {
  public:
    tuple<Address, shared_ptr<NEndpoint> > anchor;
    map<Address, NConnection> cache;
  };
  
  class EndpointState {
  public:
    EEndpointState() {}
    EEndpoints internalEndpoints;
    EEndpoints externalEndpoints;

    size_t numEndpoints() {}
  };

  // TODO: addresses for anchors as needed.
  class ConnectionState {
  public:
    ConnectionState() {}

    ConnectionState(bool simulation) {
      if ( !simulation ) {
        internalConnections = shared_ptr<EConnectionMap>(new EConnectionMap());
      }
    }

    bool hasInternalConnections() { return internalConnections? true : false; }

    shared_ptr<EConnectionMap> internalConnections;
    EConnectionMap externalConnections;
  };
}

#endif