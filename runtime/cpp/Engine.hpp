#ifndef K3_RUNTIME_ENGINE_H
#define K3_RUNTIME_ENGINE_H

#include <atomic>
#include <functional>
#include <list>
#include <map>
#include <memory>
#include <tuple>

#include "Common.hpp"
#include "Endpoint.hpp"
#include "MessageProcessor.hpp"

namespace K3 {

  using namespace std;

  using std::atomic;

  namespace Net = K3::Asio;

  //-------------------
  // Utility functions

  Identifier listenerId(Address& addr) {
    return string("__") + "_listener_" + addressAsString(addr);
  }

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

    Address address_;
    BufferSpec defaultBufferSpec_;
    int connectionRetries_;
    bool waitForNetwork_;
  };


  //---------------
  // Control

  class EngineControl : public virtual LogMT
  {
  public:
    EngineControl(shared_ptr<EngineConfiguration> conf)
      : LogMT("EngineControl"), config(conf)
    {
      terminateV        = shared_ptr<atomic<bool>>(new atomic<bool>(false));
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
      if ( pred && msgAvailMutex && msgAvailCondition ) {
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
    shared_ptr<atomic<bool>> terminateV;

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

  class Engine : public LogMT {
  public:
    typedef map<Identifier, shared_ptr<Net::Listener<Net::NContext, Net::NEndpoint>>> Listeners;

    Engine() : LogMT("Engine") {}

    Engine(
      bool simulation,
      SystemEnvironment& sys_env,
      shared_ptr<InternalCodec> _internal_codec,
      shared_ptr<ExternalCodec> _external_codec
    ):
      LogMT("Engine"), internal_codec(_internal_codec), external_codec(_external_codec) {

      list<Address> processAddrs = deployedNodes(sys_env);
      Address initialAddress;

      if (!processAddrs.empty()) {
        initialAddress = processAddrs.front();
      } else {
        logAt(warning, "No deployment peer addresses found, using a default address.");
        initialAddress = defaultAddress;
      }

      config      = shared_ptr<EngineConfiguration>(new EngineConfiguration(initialAddress));
      control     = shared_ptr<EngineControl>(new EngineControl(config));
      deployment  = shared_ptr<SystemEnvironment>(new SystemEnvironment(sys_env));

      workers     = shared_ptr<WorkerPool>(new InlinePool());
      network_ctxt = shared_ptr<Net::NContext>(new Net::NContext());
      endpoints   = shared_ptr<EndpointState>(new EndpointState());
      
      listeners     = shared_ptr<Listeners>(new Listeners());
      listener_ctrl = shared_ptr<ListenerControl>(new ListenerControl());

      if ( simulation ) {
        // Simulation engine initialization.

        if (processAddrs.size() <= 1) {
          queues = simpleQueues(initialAddress);
        } else {
          queues = perPeerQueues(processAddrs);
        }

        connections = shared_ptr<ConnectionState>(new ConnectionState(network_ctxt, true));
      }
      else {
        // Network engine initialization.
        queues      = perPeerQueues(processAddrs);
        connections = shared_ptr<ConnectionState>(new ConnectionState(network_ctxt, false));

        // Start network listeners for all K3 processes on this engine.
        // This opens engine sockets with an internal code, relying on openSocketInternal()
        // to construct the Listener object and the thread runs the listener's event loop.
        for (Address k3proc : processAddrs) {
          openSocketInternal(peerEndpointId(k3proc), k3proc, IOMode::Read);
        }
      }
    }

    //-----------
    // Messaging.

    // TODO: ref or rvalue-ref for value argument.
    void send(Address addr, Identifier triggerId, shared_ptr<Value> v)
    {
      if (deployment) {
        bool shortCircuit = isDeployedNode(*deployment, addr) || simulation();
        Message msg(addr, triggerId, *v);

        if ( shortCircuit ) {
          // Directly enqueue.
          // TODO: ensure we avoid copying the value.
          queues->enqueue(msg);
        } else {
          // Get connection and send a message on it.
          Identifier eid = connectionId(addr);
          bool sent = false;

          for (int i = 0; !sent && i < config->connectionRetries(); ++i) {
            shared_ptr<Endpoint> ep = endpoints->getInternalEndpoint(eid);
            if ( ep && ep->hasWrite() ) {
              ep->doWrite(make_shared<Value>(internal_codec->show_message(msg)));
              sent = true;
            } else {
              openSocketInternal(eid, addr, IOMode::Write);
            }
          }

          if ( !sent ) {
            string errorMsg = "Failed to send a message to " + msg.target();
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

    // TODO: Replace with use of std::bind.
    SendFunctionPtr sendFunction() {
      return [this](Address a, Identifier i, shared_ptr<Value> v){ this->send(a,i,v); };
    }

    //---------------------------------------
    // Internal and external channel methods.

    void openBuiltin(Identifier eid, string builtinId, shared_ptr<Codec> codec) {
      externalEndpointId(eid) ?
        genericOpenBuiltin(eid, builtinId, codec)
        : invalidEndpointIdentifier("external", eid);
    }

    void openFile(Identifier eid, string path, shared_ptr<Codec> codec, string mode) {
      externalEndpointId(eid) ?
        genericOpenFile(eid, path, codec, mode)
        : invalidEndpointIdentifier("external", eid);
    }

    // TODO: listener state?
    void openSocket(Identifier eid, Address addr, shared_ptr<Codec> codec, string mode) {
      externalEndpointId(eid) ?
        genericOpenSocket(eid, addr, codec, mode)
        : invalidEndpointIdentifier("external", eid);
    }

    void close(Identifier eid) {
      externalEndpointId(eid)?
        genericClose(eid, endpoints->getExternalEndpoint(eid))
        : invalidEndpointIdentifier("external", eid);
    }

    void openBuiltinInternal(Identifier eid, string builtinId) {
      !externalEndpointId(eid)?
        genericOpenBuiltin(eid, builtinId, internal_codec)
        : invalidEndpointIdentifier("internal", eid);
    }

    void openFileInternal(Identifier eid, string path, string mode) {
      !externalEndpointId(eid)?
        genericOpenFile(eid, path, internal_codec, mode)
        : invalidEndpointIdentifier("internal", eid);
    }

    // TODO: listener state.
    void openSocketInternal(Identifier eid, Address addr, IOMode mode) {
      !externalEndpointId(eid)?
        genericOpenSocket(eid, addr, internal_codec, mode)
        : invalidEndpointIdentifier("internal", eid);
    }

    void closeInternal(Identifier eid) {
      !externalEndpointId(eid)?
        genericClose(eid, endpoints->getInternalEndpoint(eid))
        : invalidEndpointIdentifier("internal", eid);
    }

    bool hasRead(Identifier eid) {
        if (externalEndpointId(eid)) {
            return endpoints->getExternalEndpoint(eid)->hasRead();
        } else {
            return endpoints->getInternalEndpoint(eid)->hasRead();
        }
    }

    shared_ptr<Value> doReadExternal(Identifier eid) {
        return endpoints->getExternalEndpoint(eid)->doRead();
    }

    Message doReadInternal(Identifier eid) {
        return internal_codec->read_message(*endpoints->getInternalEndpoint(eid)->doRead());
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

    void doWriteInternal(Identifier eid, Message v) {
        return endpoints->getInternalEndpoint(eid)->doWrite(
                  make_shared<Value>(internal_codec->show_message(v)));
    }

    //-----------------------
    // Engine execution loop

    MPStatus processMessage(MessageProcessor mp)
    {
      // Get a message from the engine queues.
      shared_ptr<Message> next_message = queues->dequeue();

      if (next_message) {
        // If there was a message, return the result of processing that message.
        return mp.process(*next_message);
      } else {
        // Otherwise return a Done, indicating no messages in the queues.
        return LoopStatus::Done;
      }
    }

    // FIXME: This is just a transliteration of the Haskell engine logic, and can probably be
    // refactored a bit.
    void runMessages(MessageProcessor mp, MPStatus st)
    {
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
          if (control->terminate()) {
              return;
          }

          // FIXME: Timeout for waiting on messages?
          control->waitForMessage([] () { return true; });
          next_status = LoopStatus::Continue;
          break;
      }

      // Recurse on the next message.
      runMessages(mp, next_status);
    }

    void runEngine(MessageProcessor mp) {
      // TODO MessageProcessor initialize() is empty.
      // In the Haskell code base, this is where the K3 AST
      // is passed to the MessageProcessor
      mp.initialize()

      // TODO Check PoolType for Uniprocess, Multithreaded..etc
      // Following code is for Uniprocess mode only:
      // TODO Dummy ID. Need to log actual ThreadID
      workers->setId(1);

      runMessages(mp, mp.status());
    }

    // Return a new thread running runEngine()
    // with the provided MessageProcessor
    thread forkEngine(MessageProcessor<Message> mp) {
      thread engineThread(runEngine, mp);
      return engineThread;
    }

    // Delegate wait to EngineControl
    void waitForEngine() {
      control->waitForEngine();
    }

    // Set the EngineControl's terminateV to true
    void terminateEngine() {
      control->terminateV->store(true);
    }

    // Clear the Engine's connections and endpointis
    void cleanupEngine() {
      if (connections) {
        connections->clearConnections();
      }
      if (endpoints) {
        // TODO: clearEndpoints() does not exists.
        // It should call removeEndpoint() on all endpoints
        // in the internal and external endpoint maps
        endpoints->clearEndpoints();
      }
    }

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
    shared_ptr<EngineConfiguration> config;
    shared_ptr<EngineControl>       control;
    shared_ptr<SystemEnvironment>   deployment;
    shared_ptr<InternalCodec>       internal_codec;
    shared_ptr<ExternalCodec>       external_codec;
    shared_ptr<MessageQueues>       queues;
    shared_ptr<WorkerPool>          workers;
    shared_ptr<Net::NContext>       network_ctxt;
    
    // Endpoint and collection tracked by the engine.
    shared_ptr<EndpointState>       endpoints;
    shared_ptr<ConnectionState>     connections;
    
    // Listeners tracked by the engine.
    shared_ptr<Listeners>           listeners;
    shared_ptr<ListenerControl>     listener_ctrl;

    void invalidEndpointIdentifier(string idType, Identifier& eid) {
      string errorMsg = "Invalid " + idType + " endpoint identifier: " + eid;
      logAt(trivial::error, errorMsg);
      throw runtime_error(errorMsg);
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
    void genericOpenBuiltin(string id, string builtinid, shared_ptr<Codec> codec) {
      if (endpoints) {
        Builtin b = builtin(builtinId);

        // Create the IO Handle
        shared_ptr<IOHandle> ioh = openBuiltinHandle(b, codec);

        // Add the endpoint to the given endpoint state.
        shared_ptr<EndpointBuffer> buf;

        shared_ptr<EndpointBindings > bindings =
            shared_ptr<EndpointBindings>(new EndpointBindings(sendFunction()));

        switch (b) {
          case Builtin::Stdout:
          case Builtin::Stderr:
            buf = shared_ptr<EndpointBindings>(new ScalarEPBufferST());
            break;
          case Builtin::Stdin:
            break;
        }

        endpoints->addEndpoint(id, make_tuple(ioh, buf, bindings));
      }
      else { logAt(trivial::error, "Unintialized engine endpoints"); }
    }

    void genericOpenFile(string id, string path, shared_ptr<Codec> codec, string mode) {
      if ( endpoints ) {
        // Create the IO Handle
        shared_ptr<IOHandle> ioh = openFileHandle(path, codec, ioMode(mode));

        // Add the endpoint to the given endpoint state.
        shared_ptr<EndpointBuffer> buf = shared_ptr<EndpointBindings>(new ScalarEPBufferST());

        shared_ptr<EndpointBindings> bindings = shared_ptr<EndpointBindings>(new EndpointBindings(sendFunction()));

        endpoints->addEndpoint(id, make_tuple(ioh, buf, bindings));

        // TODO: Prime buffers as needed with a read/refresh.
      } else { logAt(trivial::error, "Unintialized engine endpoints"); }
    }

    void genericOpenSocket(string id, Address addr, shared_ptr<Codec> codec, string mode) {
      if (endpoints) {
        IOMode handleMode = ioMode(mode);

        // Create the IO Handle.
        shared_ptr<IOHandle> ioh = openSocketHandle(addr, codec, handleMode);

        // Add the endpoint.
        shared_ptr<EndpointBuffer> buf = shared_ptr<EndpointBindings>(
            new ContainerEPBufferMT(config->defaultBufferSpec()));

        shared_ptr<EndpointBindings> bindings = shared_ptr<EndpointBindings>(new EndpointBindings(sendFunction()));

        endpoints->addEndpoint(id, make_tuple(ioh, buf, bindings));

        // Mode-based handling of endpoint vs connection, e.g., to start network listener.
        switch (handleMode) {
          case IOMode::Read:
            startListener(addr, externalEndpointId? getExternalEndpoint(id) : getInternalEndpoint(id));
            break;
          case IOMode::Write:
          case IOMode::Append:
          case IOMode::ReadWrite:
            break;
        }

      } else { logAt(trivial::error, "Unintialized engine endpoints"); }
    }

    // TODO: deregistration?
    void genericClose(Identifier eid, shared_ptr<Endpoint> ep) {
      // Close the endpoint
      if (ep) { ep->close(); }

      // TODO: Deregister the listener if this is a network source (needed in C++?)

      // Remove the endpoint
      endpoints->removeEndpoint(eid);

      // Notify the endpoint subscribers.
      if ( ep ) {
        shared_ptr<IOHandle> ioh = ep->handle();
        EndpointNotification nt = (ioh->builtin() || ioh->file())?
          EndpointNotification::FileClose : EndpointNotification::SocketClose;

        ep->subscribers()->notifyEvent(nt);
      }
    }

    // Creates and registers a listener instance for the given address and network endpoint.
    void startListener(Address& listenerAddr, shared_ptr<Endpoint> ep)
    {
      if ( listeners ) {
        // Create a listener object, which in turn will start the
        // thread for receiving messages.
        Identifier lstnr_name = listenerId(listenerAddr);

        shared_ptr<Net::Listener> lstnr = new shared_ptr<Listener>(
          new Listener(lstnr_name, network_ctxt, queues, ep, listener_ctrl, internal_codec))

        // Register the listener (track in map, and update control).
        (*listeners)[lstnr_name] = lstnr;
      } else {
        logAt(trivial::error, "Unintialized engine listeners");
      }
    }

    //-----------------------
    // IOHandle constructors.

    shared_ptr<IOHandle> openBuiltinHandle(shared_ptr<Codec> codec, Builtin b) {
      shared_ptr<IOHandle> r;
      switch (b) {
        case Stdin:
          r = shared_ptr<IOHandle>(new BuiltinHandle(codec, BuiltinHandle::Stdin()));
          break;
        case Stdout:
          r = shared_ptr<IOHandle>(new BuiltinHandle(codec, BuiltinHandle::Stdout()));
          break;
        case Stderr:
          r = shared_ptr<IOHandle>(new BuiltinHandle(codec, BuiltinHandle::Stderr()));
          break;
      }
      return r;
    }

    shared_ptr<IOHandle> openFileHandle(shared_ptr<Codec> codec, const string& path, IOMode m) {
      shared_ptr<IOHandle> r;
      switch (m) {
        case IOMode::Read:
          r = shared_ptr<IOHandle>(new FileHandle(codec, path, LineBasedHandle::Input()));
          break;
        case IOMode::Write:
          r = shared_ptr<IOHandle>(new FileHandle(codec, path, LineBasedHandle::Output()));
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

    shared_ptr<IOHandle> openSocketHandle(shared_ptr<Codec> codec, const Address& addr, IOMode m) {
      shared_ptr<IOHandle> r;
      switch ( m ) {
        case IOMode::Read:
          // TODO: check
          shared_ptr<Net::NEndpoint> nep = make_shared<Net::NEndpoint>(Net::NEndpoint(network_ctxt, addr));
          r = make_shared<IOHandle>(NetworkHandle(codec, nep));
          break;
        case IOMode::Write:
          // TODO: check
          shared_ptr<Net::NConnection> nc = make_shared<Net::NConnection>(Net::NConnection(network_ctxt, addr));
          r = make_shared<IOHandle>(NetworkHandle(codec, nc));
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
// vim: set sts=2 ts=2 sw=2:
