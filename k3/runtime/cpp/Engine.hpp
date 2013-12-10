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

  using Net = K3::Asio;

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
  class EngineControl : public virtual LogMT
  {
  public:
    EngineControl(shared_ptr<EngineConfiguration> config)
      : LogMT("EngineControl"), configuration(config)
    {
      terminateV        = shared_ptr<atomic_bool>(new atomic_bool(false));
      listenerCounter   = shared_ptr<ListenerCounter>(new ListenerCounter());
      waitMutex         = shared_ptr<mutex>(new mutex());
      waitCondition     = shared_ptr<condition_variable>(new condition_variable());
      msgAvailMutex     = shared_ptr<mutex>(new mutex());
      msgAvailCondition = shared_ptr<condition_variable>(new condition_variable());
    }

    bool terminate() { 
      bool done = networkDone() || !config->waitForNetwork;
      return done && (terminateV? terminateV->load() : false);
    }

    bool networkDone() { return listenerCounter? (*listenerCounter)() : false; }
    
    // Wait for a notification that the engine associated
    // with this control object is finished.
    void waitForEngine()
    {
      if ( waitMutex && waitCondition ) {
        unique_lock<mutex> lock(*waitMutex);
        while ( !this->terminate() ) { waitCondition->wait(lock); }
      } else { logAt(warning, "Could not wait for engine, no condition variable available."); }
    }

    // Wait for a notification that the engine associated
    // with this control object has queued messages.
    template<typename Predicate>
    void waitForMessage(shared_ptr<Predicate> pred)
    {
      if ( p && msgAvailMutex && msgAvailCondition ) {
        unique_lock<mutex> lock(*msgAvailMutex);
        while ( (*pred)() ) { msgAvailCondition->wait(lock); }
      } else { logAt(warning, "Could not wait for message, no condition variable available."); }
    }

    shared_ptr<ListenerControl> listenerControl() {
      return shared_ptr<ListenerControl>(
              new ListenerControl(msgAvailMutex, msgAvailCondition, listenerCounter));
    }

  protected:
    // Engine configuration, indicating whether we wait for the network when terminating.
    shared_ptr<EngineConfiguration> config;

    // Engine termination indicator
    shared_ptr<atomic_bool> terminateV;

    // Network listener completion indicator.
    shared_ptr<ListenerCounter> listenerCounter;    

    // Notifications for threads waiting on the engine.
    shared_ptr<mutex> waitMutex;
    shared_ptr<condition_variable> waitCondition;

    // Notifications for engine worker threads waiting on messages.
    shared_ptr<mutex> msgAvailMutex;
    shared_ptr<condition_variable> msgAvailCondition;
  };

  //------------
  // Engine
  template<typename Value>
  class Engine : public virtual LogMT {
  public:
    Engine() : LogMT("Engine") {}

    Engine(bool simulation, SystemEnvironment& sysEnv, WireDesc<Value>& wd)
      : LogMT("Engine")
    {
      list<Address> processAddrs = deployedNodes(sysEnv);
      Address initialAddress;
      if ( !processAddrs.empty() ) {
        initialAddress = processAddrs.front();
      } else {
        logAt(warning, "No deployment peer addresses found, using a default address.");
        initialAddress = defaultAddress;
      }

      if ( simulation ) {
        // Simulation engine initialization.
        config      = shared_ptr<EngineConfiguration>(new EngineConfiguration(initialAddress));
        ctrl        = shared_ptr<EngineControl>(new EngineControl(config));
        deployment  = shared_ptr<SystemEnvironment>(new SystemEnvironment(sysEnv));

        valueFormat = shared_ptr<WireDesc<Value> >(new WireDesc<Value>(wd));
        msgFormat   = internalizeWireDesc(wd);
        
        queues      = processAddrs.size() <= 1 ? simpleQueues<Value>(initialAddress)
                                            : perPeerQueues<Value>(processAddrs);

        workers     = shared_ptr<WorkerPool>(new InlinePool());
        networkCtxt = shared_ptr<Net::NContext>(new NContext(initialAddress));
        listeners   = shared_ptr<Listeners>(new Listeners()); // TODO
        endpoints   = shared_ptr<EndpointState>(new EndpointState(networkCtxt));
        connections = shared_ptr<ConnectionState>(new ConnectionState(networkCtxt, true));
      } 
      else {
        // Network engine initialization.
        config      = shared_ptr<EngineConfiguration>(new EngineConfiguration(initialAddress));
        ctrl        = shared_ptr<EngineControl>(new EngineControl(config));
        deployment  = shared_ptr<SystemEnvironment>(new SystemEnvironment(sysEnv));

        valueFormat = shared_ptr<WireDesc<Value> >(new WireDesc<Value>(wd));
        msgFormat   = internalizeWireDesc(wd);
        
        queues      = perPeerQueues<Value>(processAddrs);

        workers     = shared_ptr<WorkerPool>(new InlinePool());
        networkCtxt = shared_ptr<Net::NContext>(new NContext(initialAddress));
        listeners   = shared_ptr<Listeners>(new Listeners()); // TODO
        endpoints   = shared_ptr<EndpointState>(new EndpointState(networkCtxt));
        connections = shared_ptr<ConnectionState>(new ConnectionState(networkCtxt, false));

        // TODO: start network listeners. Note this means opening up the listener sockets,
        // while the openSocket() method is the function that actually starts the thread.
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

    // TODO: IOHandle constructor functions, i.e. openFileHandle, openSocketHandle, etc.

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
      else { logAt(error, "Invalid system environment."); }
      return r;
    }

    tuple<size_t, size_t> statistics() {
      return make_tuple(queues? queues->size() : 0,
                        endpoints? endpoints->numEndpoints() : 0);
    }

    bool simulation() {
      if ( connections ) { return connections->hasInternalConnections(); }
      else { logAt(error, "Invalid connection state."); }
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
    shared_ptr<NContext>                   networkCtxt;
    shared_ptr<Listeners>                  listeners;
    shared_ptr<EndpointState>              endpoints;
    shared_ptr<ConnectionState>            connections;
  };

}

#endif