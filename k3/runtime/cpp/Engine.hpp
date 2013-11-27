#ifndef K3_ENGINE_H
#define K3_ENGINE_H

#include <map>
#include <list>
#include <tuple>
#include <boost/shared_ptr.hpp>

namespace K3 {

  using namespace std;
  using namespace boost;

  typedef string Identifier;
  typedef tuple<string, int> Address;

  string addressHost(Address& addr) { return get<0>(addr); }
  int    addressPort(Address& addr) { return get<1>(addr); }

  template<typename Value>
  class Message : public tuple<Address, Identifier, Value> {};

  //--------------------
  // Wire descriptions

  // TODO: FrameDesc
  // TODO: protobuf, msgpack, json WireDesc implementations.

  template<typename Value>
  class WireDesc {
  public:
    WireDesc() {}
    virtual string pack(Value& v)    = 0;
    virtual Value  unpack(string& s) = 0;
    virtual FrameDesc frame()        = 0;
  };


  //------------
  // Queues
  template<typename Message>
  class MessageQueues {
  public:
    enum QueueType { Peer, MultiPeer, MultiTrigger };
    
    typedef tuple<Address, list<tuple<Identifier, Message> > > PeerMessages;
    typedef map<Address, list<tuple<Identifier, Message> > >   MultiPeerMessages;
    typedef map<tuple<Address, Identifier>, list<Message> >    MultiTriggerMessages;

    MessageQueues() {}
    MessageQueues(QueueType qt) {}

  protected:
    shared_ptr<PeerMessages>         peerMsgs;
    shared_ptr<MultiPeerMessages>    multiPeerMsgs;
    shared_ptr<MultiTriggerMessages> multiTriggerMsgs;
  };


  //------------
  // Workers
  class Workers {
  public:
    enum WorkersType { Uniprocess, MultiThreaded, MultiProcess };
    typedef int WorkerId;
    typedef int ProcessId;

    Workers() {}
    Workers(WorkersType wt) {}

  protected:
    shared_ptr<WorkerId> uniProcessor;
    shared_ptr<list<WorkerId> > threadedProcessor;
    shared_ptr<list<ProcessId> > multiProcessor;
  };

  //------------
  // Listeners
  typedef map<Identifier, ThreadId> Listeners;

  //--------------------------
  // Low-level networking.
  // TODO: NEndpoint
  // TODO: NConnection
  // TODO: nanomsg, Boost ASIO network implementations.

  //--------------------------
  // IO handles
  typedef tuple<int, int> BufferSpec;
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
  typedef map<Identifier, Endpoint> EEndpoints;
  
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
  };

  class ConnectionState {
  public:
    ConnectionState() {}
    EConnectionMap internalConnections;
    EConnectionMap externalConnections;
  };


  //-------------------
  // Message processor

  template<typename Result, typename Error>
  class MPStatus {
  public:
    MPStatus() {}
    MPStatus(Result& res) {}
    MPStatus(Error& err) {}
    shared_ptr<Result> valid;
    shared_ptr<Error>  invalid;
  };

  template<typename Value, typename Result, typename Error>
  class MessageProcessor {
    Result initialize() {}
    Result process(Message<Value> msg, Result& prev) {}
    Result finalize(Result res) {}
    MPStatus<Result, Error> status(Result res) {}
    MPStatus<Result, Error> report() {}
  };

  //---------------
  // Configuration
  class EngineConfiguration {
  public:
    Address address;
    BufferSpec defaultBufferSpec;
    int connectionRetries;
    bool waitForNetwork;
  };

  //---------------
  // Control
  class EngineControl {
  public:
    // TODO: control primitives.
  };

  //------------
  // Engine
  template<typename Value>
  class Engine {
  public:
    Engine() {}
    
    Engine(bool simulation, SystemEnvironment sysEnv, WireDesc<Value> wd)
    {
      if ( simulation ) {
        // Simulation engine initialization.
      } else {
        // Network engine initialization.
      }
    }

    // TODO: engine API.
    void send() {}

    void enqueue() {}
    void dequeue() {}

    void openBuiltin() {}
    void openFile() {}
    void openSocket() {}
    void close() {}

    bool  hasRead() {}
    bool  hasWrite() {}
    Value doRead() {}
    void  doWrite(Value v) {}

    void processMessage() {}
    void runMessages() {}

    void runEngine() {}
    void forkEngine() {}
    void waitForEngine() {}
    void terminateEngine() {}
    void cleanupEngine() {}

  protected:
    shared_ptr<EngineConfiguration>        config;    
    shared_ptr<EngineControl>              control;
    shared_ptr<WireDesc<Value> >           valueFormat;
    shared_ptr<WireDesc<Message<Value> > > msgFormat;
    shared_ptr<MessageQueues<Value> >      queues;
    shared_ptr<Workers>                    workers;
    shared_ptr<Listeners>                  listeners;
    shared_ptr<EndpointState>              endpoints;
    shared_ptr<ConnectionState>            connections;
  };

}

#endif