#ifndef K3_RUNTIME_ENGINE_H
#define K3_RUNTIME_ENGINE_H

#include <map>
#include <list>
#include <tuple>
#include <boost/shared_ptr.hpp>
#include <k3/runtime/Common.hpp>

namespace K3 {

  using namespace std;
  using namespace boost;

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
    EngineConfiguration() { defaultConfiguration(); }
    EngineConfiguration(Address& addr) { configureWithAddress(addr); }

    void defaultConfiguration() {
      address           = defaultAddress;
      defaultBufferSpec = BufferSpec(100,10);
      connectionRetries = 5;
      waitForNetwork    = false;
    }
    
    void configureWithAddress(Address addr) {
      address = addr;
      defaultBufferSpec = BufferSpec(100,10);
      connectionRetries = 5;
      waitForNetwork    = false;
    }

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
    
    Engine(bool simulation, SystemEnvironment& sysEnv, WireDesc<Value>& wd)
    {
      list<Address> peerAddrs = deployedNodes(sysEnv);
      Address initialAddress;
      if ( !peerAddrs.empty() ) {
        initialAddress = peerAddrs.front();
      } else {
        // Error
      }

      if ( simulation ) {
        // Simulation engine initialization.
        config      = shared_ptr<EngineConfiguration>(new EngineConfiguration(initialAddress));
        ctrl        = shared_ptr<EngineControl>(new EngineControl()); // TODO: control.
        deployment  = shared_ptr<SystemEnvironment>(new SystemEnvironment(sysEnv));

        valueFormat = shared_ptr<WireDesc<Value> >(new WireDesc<Value>(wd));
        msgFormat   = internalizeWireDesc(wd);
        
        queues      = peerAddrs.size() <= 1 ? simpleQueues<Value>(initialAddress)
                                            : perPeerQueues<Value>(peerAddrs);

        workers     = shared_ptr<WorkerPool>(new InlinePool());
        listeners   = shared_ptr<Listeners>(new Listeners());
        endpoints   = shared_ptr<EndpointState>(new EndpointState());

        // TODO: address for external anchor as needed.
        connections = shared_ptr<ConnectionState>(new ConnectionState(true));
      } 
      else {
        // Network engine initialization.
        config      = shared_ptr<EngineConfiguration>(new EngineConfiguration(initialAddress));
        ctrl        = shared_ptr<EngineControl>(new EngineControl()); // TODO: control.
        deployment  = shared_ptr<SystemEnvironment>(new SystemEnvironment(sysEnv));

        valueFormat = shared_ptr<WireDesc<Value> >(new WireDesc<Value>(wd));
        msgFormat   = internalizeWireDesc(wd);
        
        queues      = perPeerQueues<Value>(peerAddrs);

        workers     = shared_ptr<WorkerPool>(new InlinePool());
        listeners   = shared_ptr<Listeners>(new Listeners());
        endpoints   = shared_ptr<EndpointState>(new EndpointState());

        // TODO: address for internal and external anchor as needed.
        connections = shared_ptr<ConnectionState>(new ConnectionState(false));

        // TODO: start network listeners.
      }
    }

    // TODO: engine API.
    void send(Address addr, Identifier triggerId, Value v) {}

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

    // TODO
    shared_ptr<WireDesc<Message<Value> > internalizeWireDesc(WireDesc<Value>& wd) {
      return shared_ptr<WireDesc<Message<Value> >();
    }

    // Engine statistics.
    list<Address> nodes() { 
      list<Address> r;
      if ( deployment ) { r = deployedNodes(*deployment); }
      return r;
    }

    tuple<size_t, size_t> statistics() {
      return make_tuple(queues? queues->size() : 0,
                        endpoints? endpoints->numEndpoints() : 0);
    }

    bool simulation() {
      if ( connections ) { return connections->hasInternalConnections(); }
      // Error
      return false;
    }

    typedef map<Identifier, Listener> Listeners;

  protected:
    void startListener() {}

    shared_ptr<EngineConfiguration>        config;    
    shared_ptr<EngineControl>              control;
    shared_ptr<SystemEnvironment>          deployment;
    shared_ptr<WireDesc<Value> >           valueFormat;
    shared_ptr<WireDesc<Message<Value> > > msgFormat;
    shared_ptr<MessageQueues<Value> >      queues;
    shared_ptr<WorkerPool>                 workers;
    shared_ptr<Listeners>                  listeners;
    shared_ptr<EndpointState>              endpoints;
    shared_ptr<ConnectionState>            connections;
  };

}

#endif