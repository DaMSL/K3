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

  //---------------
  // Configuration

  class EngineConfiguration {
  public:
    EngineConfiguration() { defaultConfiguration(); }
    EngineConfiguration(Address& addr) { configureWithAddress(addr); }

    Address    address()           { return address_; }
    BufferSpec defaultBufferSpec() { return defaultBufferSpec_; }
    int        connectionRetries() { return connectionRetries_; }
    bool       waitForNetwork()    { return waitForNetwork_; }
  protected:

    void defaultConfiguration() {
      address_           = defaultAddress;
      defaultBufferSpec_ = BufferSpec(100,10);
      connectionRetries_ = 5;
      waitForNetwork_    = false;
    }
    
    void configureWithAddress(Address addr) {
      address_           = addr;
      defaultBufferSpec_ = BufferSpec(100,10);
      connectionRetries_ = 5;
      waitForNetwork_    = false;
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
    EngineControl(shared_ptr<EngineConfiguration> conf)
      : LogMT("EngineControl"), config(conf)
    {
      terminateV        = shared_ptr<atomic_bool>(new atomic_bool(false));
      listenerCounter   = shared_ptr<ListenerCounter>(new ListenerCounter());
      waitMutex         = shared_ptr<mutex>(new mutex());
      waitCondition     = shared_ptr<condition_variable>(new condition_variable());
      msgAvailMutex     = shared_ptr<mutex>(new mutex());
      msgAvailCondition = shared_ptr<condition_variable>(new condition_variable());
    }

    bool terminate() { 
      bool done = networkDone() || !config->waitForNetwork();
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
  class Engine : public virtual LogMT
  {
  public:
    typedef map<Identifier, Listener<Value, Message<Value> > > Listeners;
    typedef function<void(Address, Identifier, Value)> SendFunction; // TODO: ref or rvalue-ref for value arg.

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

    //-----------
    // Messaging.

    // TODO: ref or rvalue-ref for value argument.
    void send(Address addr, Identifier triggerId, Value v)
    {
      if ( deployment ) {
        bool shortCircuit = isDeployedNode(*deployment, addr) || simulation();
        Message msg(addr, triggerId, v);
        
        if ( shortCircuit ) {
          // Directly enqueue.
          // TODO: ensure we avoid copying the value.
          queues->enqueue(m);
        } else {
          // Get connection and send a message on it.
          Identifier eid = connectionId(addr);
          bool sent = false;

          for (int i = 0; !sent && i < config->connectionRetries(); ++i) 
          {
            shared_ptr<Endpoint<Message<Value>, Value> > ep = endpoints->getInternalEndpoint(eid);
            if ( ep && ep->hasWrite() ) {
              ep->doWrite(m); 
              sent = true;
            } else { 
              openSocketInternal(eid, addr, IOMode::Write); 
            }
          }

          if ( !sent ) {
            string errorMsg = "Failed to send a message to " + m.target();
            logAt(trivial::error, errorMsg);
            throw runtime_error(errorMsg);
          }
        }
      } else {
        string errorMsg = "Invalid engine deployment";
        logAt(trivial::error, errorMsg);
        throw runtime_error(errorMsg);
      }
    }

    SendFunction sendFunction() { 
      return [this](Address a, Identifier i, Value v){ this->send(addr,i,v); }
    }

    //---------------------------------------
    // Internal and external channel methods.

    void openBuiltin(Identifier eid, string builtinId, shared_ptr<WireDesc<Value> > wd) {
      externalEndpointId(eid) ? 
        genericOpenBuiltin<Value, Value>(eid, builtinId, wd)
        : invalidEndpointIdentifier("external", eid);
    }

    void openFile(Identifier eid, string path, shared_ptr<WireDesc<Value> > wd, string mode) {
      externalEndpointId(eid) ? 
        genericOpenFile<Value, Value>(eid, path, wd, mode);
        : invalidEndpointIdentifier("external", eid);
    }

    // TODO: listener state?
    void openSocket(Identifier eid, Address addr, shared_ptr<WireDesc<Value> > wd, string mode) {
      externalEndpointId(eid) ? 
        genericOpenSocket<Value, Value>(wid, addr, wd, mode);
        : invalidEndpointIdentifier("external", eid);
    }
    
    void close(Identifier eid) { 
      externalEndpointId(eid)? 
        genericClose<Value, Value>(eid, endpoints->getExternalEndpoint(eid))
        : invalidEndpointIdentifier("external", eid);
    }

    void openBuiltinInternal(Identifier eid, string builtinId) {
      !externalEndpointId(eid)?
        genericOpenBuiltin<Message<Value>, Value>(eid, builtinId, msgFormat)
        : invalidEndpointIdentifier("internal", eid);
    }

    void openFileInternal(Identifier eid, string path, string mode) {
      !externalEndpointId(eid)?
        genericOpenFile<Message<Value>, Value>(eid, path, msgFormat, mode)
        : invalidEndpointIdentifier("internal", eid);
    }

    // TODO: listener state.
    void openSocketInternal(Identifier eid, Address addr, string mode) {
      !externalEndpointId(eid)?
        genericOpenSocket<Message<Value>, Value>(eid, addr, msgFormat, mode)
        : invalidEndpointIdentifier("internal", eid);
    }

    void closeInternal(Identifier eid) {
      !externalEndpointId(eid)? 
        genericClose<Message<Value>, Value>(eid, endpoints->getInternalEndpoint(eid)) 
        : invalidEndpointIdentifier("internal", eid);      
    }

    bool hasRead(Identifier eid) {
        if (externalEndpointId(eid)) {
            return endpoints->getExternalEndpoint(eid)->hasRead();
        } else {
            return endpoints->getInternalEndpoint(eid)->hasRead();
        }
    }

    Value doReadExternal(Identifier eid) {
        return endpoints->getExternalEndpoint(eid)->doRead();
    }

    Message<Value> doReadInternal() {
        return endpoints->getInternalEndpoint(eid)->doRead();
    }

    bool hasWrite(Identifier eid) {
        if (externalEndpointId(eid)) {
            return endpoints->getExternalEndpoint(eid)->hasWrite();
        } else {
            return endpoints->getInternalEndpoint(eid)->hasWrite();
        }
    }

    void doWriteExternal(Identifier eid, Value v) {
        return endpoints->getExternalEndpoint(eid)->doWrite(v);
    }

    void doWriteInternal(Identifier eid, Message<Value> v) {
        return endpoints->getInternalEndpoint(eid)->doWrite(v);
    }

    //-----------------------
    // Engine execution loop

    MPStatus processMessage(MessageProcessor<Message<Value>> mp) {

        // Get a message from the engine queues.
        shared_ptr<Message<Value>> next_message = queues->dequeue();

        if (next_message) {

            // If there was a message, return the result of processing that message.
            return mp.process(*next_message);
        } else {

            // Otherwise return a Done, indicating no messages in the queues.
            return LoopStatus::Done;
        }
    }
    }

    // FIXME: This is just a transliteration of the Haskell engine logic, and can probably be
    // refactored a bit.
    void runMessages(MessageProcessor<Message<Value>> mp, MPStatus st) {
        MPStatus next_status;
        switch (st) {

            // If we are not in error, process the next message.
            case LoopStatus::Continue:
                next_status = processMessage(mp);
                break;

            // If we were in error, exit out with error.
            case LoopStatus::Error:
                return;

            // If there are no messages on the queues,
            //  - If the terminate flag has been set, exit out normally.
            //  - Otherwise, wait for a message and continue.
            case LoopStatus::Done:

                if (ctrl->terminate()) {
                    return;
                }

                // FIXME: Timeout for waiting on messages?
                ctrl->waitForMessage([] () { return true; });
                next_status = LoopStatus::Continue;
                break;
        }

        // Recurse on the next message.
        runMessages(mp, next_status);
    }

    void runEngine() {}
    void forkEngine() {}
    void waitForEngine() {}
    void terminateEngine() {}
    void cleanupEngine() {}

    //-------------------
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

  protected:
    shared_ptr<EngineConfiguration>               config;    
    shared_ptr<EngineControl>                     control;
    shared_ptr<SystemEnvironment>                 deployment;
    shared_ptr<WireDesc<Value> >                  valueFormat;
    shared_ptr<WireDesc<Message<Value> > >        msgFormat;
    shared_ptr<MessageQueues<Message<Value> > >   queues;
    shared_ptr<WorkerPool>                        workers;
    shared_ptr<NContext>                          networkCtxt;
    shared_ptr<Listeners>                         listeners;
    shared_ptr<EndpointState<Value> >             endpoints;
    shared_ptr<ConnectionState>                   connections;

    void invalidEndpointIdentifier(string idType, Identifier& eid) {
      string errorMsg = "Invalid " + idType + " endpoint identifier: " + eid;
      logAt(trivial::error, errorMsg);
      throw runtime_error(errorMsg);      
    }

    // TODO
    shared_ptr<WireDesc<Message<Value> > internalizeWireDesc(WireDesc<Value>& wd) {
      return shared_ptr<WireDesc<Message<Value> >();
    }

    Builtin builtin(string builtinId) {
      Builtin r;
      if ( builtinId == "stdin" )       { r = Builtin::Stdin; }
      else if ( builtinId == "stdout" ) { r = Builtin::Stdout; }
      else if ( builtinId == "stderr" ) { r = Builtin::Stderr; }
      else { 
        logAt(trivial::error, string("Invalid builtin: ") + builtinId);
        throw runtime_error(string("Invalid builtin: ") + builtinId);
      }
      return r;
    }

    // Converts a K3 channel mode into a native file descriptor mode.
    IOMode ioMode(string k3Mode) {
      IOMode r;
      if ( k3Mode == "r" )       { r = IOMode::Read; }
      else if ( k3Mode == "w"  ) { r = IOMode::Write; }
      else if ( k3Mode == "a"  ) { r = IOMode::Append; }
      else if ( k3Mode == "rw" ) { r = IOMode::ReadWrite; }
      else {
        logAt(trivial::error, string("Invalid IO mode: ") + k3Mode);
        throw runtime_error(string("Invalid IO mode: ") + k3Mode);
      }
      return r;
    }

    // TODO: for all of the genericOpen* endpoint constructors below, revisit:
    // i. no K3 type specified for type-safe I/O as with Haskell engine.
    // ii. buffer type with concurrent engine.
    template<typename EndpointValue, typename EventValue>
    void genericOpenBuiltin(string id, string builtinId, shared_ptr<WireDesc<EndpointValue> > wd)
    {
      if ( endpoints ) {
        Builtin b = builtin(builtinId);

        // Create the IO Handle
        shared_ptr<IOHandle<EndpointValue> > ioh = openBuiltinHandle(b, wd);

        // Add the endpoint to the given endpoint state.
        shared_ptr<EndpointBuffer<EndpointValue> > buf;

        shared_ptr<EndpointBindings<EventValue> > bindings = 
            shared_ptr<EndpointBindings<EventValue> >(new EndpointBindings(sendFunction()));

        switch (b) {
          case Builtin::Stdout:
          case Builtin::Stderr:
            buf = shared_ptr<EndpointBindings<EndpointValue> >(new ScalarEPBufferST());
            break;
          case Builtin::Stdin:
            break;
        }
        
        endpoints->addEndpoint(id, make_tuple(ioh, buf, bindings));
      }
      else { logAt(trivial::error, "Unintialized engine endpoints"); }
    }

    template<typename EndpointValue, typename EventValue>
    void genericOpenFile(string id, string path, shared_ptr<WireDesc<EndpointValue> > wd, string mode)
    {
      if ( endpoints ) {
        // Create the IO Handle
        shared_ptr<IOHandle<EndpointValue> > ioh = openFileHandle(path, wd, ioMode(mode));

        // Add the endpoint to the given endpoint state.
        shared_ptr<EndpointBuffer<EndpointValue> > buf =
          shared_ptr<EndpointBindings<EndpointValue> >(new ScalarEPBufferST());

        shared_ptr<EndpointBindings<EventValue> > bindings =
          shared_ptr<EndpointBindings<EventValue> >(new EndpointBindings(sendFunction()));

        endpoints->addEndpoint(id, make_tuple(ioh, buf, bindings));

        // TODO: Prime buffers as needed with a read/refresh.
      } else { logAt(trivial::error, "Unintialized engine endpoints"); }
    }

    template<typename EndpointValue, typename EventValue>
    void genericOpenSocket(string id, Address addr, shared_ptr<WireDesc<EndpointValue> > wd, string mode,
                           xxxListenerState)
    {
      if ( endpoints ) {
        IOMode handleMode = ioMode(mode);

        // Create the IO Handle.
        shared_ptr<IOHandle<EndpointValue> > ioh = openSocketHandle(addr, wd, handleMode);

        // Add the endpoint.
        shared_ptr<EndpointBuffer<EndpointValue> > buf =
          shared_ptr<EndpointBindings<EndpointValue> >(
            new ContainerEPBufferMT(config->defaultBufferSpec()));

        shared_ptr<EndpointBindings<EventValue> > bindings =
          shared_ptr<EndpointBindings<EventValue> >(new EndpointBindings(sendFunction()));

        endpoints->addEndpoint(id, make_tuple(ioh, buf, bindings));

        // Mode-based handling of endpoint vs connection, e.g., to start network listener.
        switch ( handleMode ) {
          case IOMode::Read:
            startListener();
            break;
          case IOMode::Write:
          case IOMode::Append:
          case IOMode::ReadWrite:
            break;
        }

      } else { logAt(trivial::error, "Unintialized engine endpoints"); }
    }

    // TODO: deregistration?
    template<typename EndpointValue, typename EventValue>
    void genericClose(Identifier eid, shared_ptr<Endpoint<EndpointValue, EventValue> > ep) 
    {
      // Close the endpoint
      if ( ep ) { ep->close(); }

      // TODO: Deregister the listener if this is a network source (needed in C++?)

      // Remove the endpoint
      endpoints->removeEndpoint(eid);

      // Notify the endpoint subscribers.
      if ( ep ) { 
        shared_ptr<IOHandle<EndpointValue> > ioh = ep->handle();
        EndpointNotification nt = (ioh->builtin() || ioh->file())?
          EndpointNotification::FileClose : EndpointNotification::SocketClose;
        
        ep->subscribers()->notifyEvent(nt);
      }
    }

    // TODO
    void startListener() {
      // Create a listener object, which in turn will start the thread for receiving messages.

      // Register the listener (track in map, and update control).
    }

    //-----------------------
    // IOHandle constructors.

    template<typename HandleValue>
    shared_ptr<IOHandle<HandleValue> > 
    openBuiltinHandle(shared_ptr<WireDesc<HandleValue> > wd, Builtin b) 
    {
      shared_ptr<IOHandle<HandleValue> r;
      switch (b) {
        case Stdin:
          r = shared_ptr<IOHandle<HandleValue> >(new BuiltinHandle(wd, BuiltinHandle::Stdin()));
          break;
        case Stdout:
          r = shared_ptr<IOHandle<HandleValue> >(new BuiltinHandle(wd, BuiltinHandle::Stdout()));
          break;
        case Stderr:
          r = shared_ptr<IOHandle<HandleValue> >(new BuiltinHandle(wd, BuiltinHandle::Stderr()));
          break;
      }
      return r;
    }

    template<typename HandleValue>
    shared_ptr<IOHandle<HandleValue> >
    openFileHandle(shared_ptr<WireDesc<HandleValue> > wd, const string& path, IOMode m) 
    {
      shared_ptr<IOHandle<HandleValue> > r;
      switch ( m ) {
        case IOMode::Read:
          r = shared_ptr<IOHandle<HandleValue> >(
                new FileHandle(wd, path, LineBasedHandle<HandleValue>::Input()));
          break;
        
        case IOMode::Write:
          r = shared_ptr<IOHandle<HandleValue> >(
                new FileHandle(wd, path, LineBasedHandle<HandleValue>::Output()));
          break;
        
        case Append:
        case ReadWrite:
          string errorMsg = "Unsupported open mode for file handle";
          logAt(trivial::error, errorMsg);
          throw runtime_error(errorMsg);
          break;
      }
      return r;
    }

    template<typename HandleValue>
    shared_ptr<IOHandle<HandleValue> > 
    openSocketHandle(shared_ptr<WireDesc<HandleValue> > wd, const Address& addr, IOMode m) {
      shared_ptr<IOHandle<HandleValue> > r;
      switch ( m ) {
        case IOMode::Read:
          // TODO
          r = shared_ptr<IOHandle<HandleValue> >(new NetworkHandle(xxxNEndpoint));
          break;

        case IOMode::Write:
          // TODO
          r = shared_ptr<IOHandle<HandleValue> >(new NetworkHandle(xxxNConnection));
          break;

        case IOMode::Append:
        case IOMode::ReadWrite:
          string errorMsg = "Unsupported open mode for network handle";
          logAt(trivial::error, errorMsg);
          throw runtime_error(errorMsg);
          break;
      }
      return r;
    }
  };

}

#endif
