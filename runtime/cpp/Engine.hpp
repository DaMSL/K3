#ifndef K3_RUNTIME_ENGINE_H
#define K3_RUNTIME_ENGINE_H

#include <atomic>
#include <string>
#include <list>
#include <map>
#include <exception>
#include <tuple>

#include "Common.hpp"
#include "Endpoint.hpp"
#include "Listener.hpp"
#include "Message.hpp"
#include "MessageProcessor.hpp"
#include "Options.hpp"

namespace K3 {

  namespace Net = K3::Asio;

  //-------------------
  // Utility functions

  static inline Identifier listenerId(Address& addr) {
    return std::string("__") + "_listener_" + addressAsString(addr);
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
      connectionRetries_ = 1000;
      waitForNetwork_    = false;
    }

    void configureWithAddress(Address addr) {
      address_           = addr;
      defaultBufferSpec_ = BufferSpec(100,10);
      connectionRetries_ = 100;
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
      terminateV        = shared_ptr<std::atomic<bool>>(new std::atomic<bool>(false));
      listenerCounter   = shared_ptr<ListenerCounter>(new ListenerCounter());
      waitMutex         = shared_ptr<boost::mutex>(new boost::mutex());
      waitCondition     = shared_ptr<boost::condition_variable>(new boost::condition_variable());
      msgAvailMutex     = shared_ptr<boost::mutex>(new boost::mutex());
      msgAvailCondition = shared_ptr<boost::condition_variable>(new boost::condition_variable());
    }

    bool terminate() {
      bool net_done = networkDone();
      bool force_term = !config->waitForNetwork() && (terminateV? terminateV->load() : false);
      return net_done || force_term;
    }

    void set_terminate() {
      terminateV->store(true);
    }

    bool networkDone() { return listenerCounter? (*listenerCounter)() == 0 : false; }

    // Wait for a notification that the engine associated
    // with this control object is finished.
    void waitForEngine()
    {
      if ( waitMutex && waitCondition ) {
        boost::unique_lock<boost::mutex> lock(*waitMutex);
        while ( !this->terminate() ) { waitCondition->wait(lock); }
      } else { logAt(boost::log::trivial::warning, "Could not wait for engine, no condition variable available."); }
    }

    // Wait for a notification that the engine associated
    // with this control object has queued messages.
    template <class Predicate>
    void waitForMessage(Predicate pred)
    {
      if (msgAvailMutex && msgAvailCondition) {
        boost::unique_lock<boost::mutex> lock(*msgAvailMutex);
        while (pred()) { msgAvailCondition->wait(lock); }
      } else { logAt(boost::log::trivial::warning, "Could not wait for message, no condition variable available."); }
    }

    shared_ptr<ListenerControl> listenerControl() {
      return shared_ptr<ListenerControl>(
              new ListenerControl(msgAvailMutex, msgAvailCondition, listenerCounter));
    }

    void messageAvail() {
      msgAvailCondition->notify_one();
    }

  protected:
    // Engine configuration, indicating whether we wait for the network when terminating.
    shared_ptr<EngineConfiguration> config;

    // Engine termination indicator
    shared_ptr<std::atomic<bool>> terminateV;

    // Network listener completion indicator.
    shared_ptr<ListenerCounter> listenerCounter;

    // Notifications for threads waiting on the engine.
    shared_ptr<boost::mutex> waitMutex;
    shared_ptr<boost::condition_variable> waitCondition;

    // Notifications for engine worker threads waiting on messages.
    shared_ptr<boost::mutex> msgAvailMutex;
    shared_ptr<boost::condition_variable> msgAvailCondition;
  };


  //------------
  // Engine

  class Engine : public LogMT {
  public:
    typedef map<Identifier, shared_ptr<Net::Listener>> Listeners;
    Engine() : LogMT("Engine") {}

    Engine(
      bool simulation,
      SystemEnvironment& sys_env,
      shared_ptr<InternalCodec> _internal_codec,
      string log_level
    ): LogMT("Engine") {
      configure(simulation, sys_env, _internal_codec, log_level);
    }

    void configure(bool simulation, SystemEnvironment& sys_env, shared_ptr<InternalCodec> _internal_codec, string log_level);

    //-----------
    // Messaging.

    // TODO: rvalue-ref overload for value argument.
    void send(Address addr, TriggerId triggerId, shared_ptr<Dispatcher> d);

    // TODO: avoid destructing tuple here
    void send(Message& m) {
      send(m.address(), m.id(), m.dispatcher());
    }

    void send(shared_ptr<Message> m) {
      send(m->address(), m->id(), m->dispatcher());
    }

    // TODO: Replace with use of std::bind.
    SendFunctionPtr sendFunction() {
      return [this](Address a, TriggerId i, shared_ptr<Value> v)
          { send(RemoteMessage(a, i, *v).toMessage()); };
    }

    //---------------------------------------
    // Internal and external channel methods.

    void openBuiltin(Identifier eid, string builtinId) {
      externalEndpointId(eid) ?
        genericOpenBuiltin(eid, builtinId)
        : invalidEndpointIdentifier("external", eid);
    }

    void openFile(Identifier eid, string path, IOMode mode) {
      externalEndpointId(eid) ?
        genericOpenFile(eid, path, mode)
        : invalidEndpointIdentifier("external", eid);
    }

    void openSocket(Identifier eid, Address addr, IOMode mode) {
      externalEndpointId(eid) ?
        genericOpenSocket(eid, addr, mode)
        : invalidEndpointIdentifier("external", eid);
    }

    void close(Identifier eid) {
      externalEndpointId(eid)?
        genericClose(eid, endpoints->getExternalEndpoint(eid))
        : invalidEndpointIdentifier("external", eid);
    }

    void openBuiltinInternal(Identifier eid, string builtinId) {
      !externalEndpointId(eid)?
        genericOpenBuiltin(eid, builtinId)
        : invalidEndpointIdentifier("internal", eid);
    }

    void openFileInternal(Identifier eid, string path, IOMode mode) {
      !externalEndpointId(eid)?
        genericOpenFile(eid, path, mode)
        : invalidEndpointIdentifier("internal", eid);
    }

    void openSocketInternal(Identifier eid, Address addr, IOMode mode) {
      !externalEndpointId(eid)?
        genericOpenSocket(eid, addr, mode)
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

    RemoteMessage doReadInternal(Identifier eid) {
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

    void doWriteInternal(Identifier eid, RemoteMessage m) {
      return endpoints->getInternalEndpoint(eid)->doWrite(
                make_shared<Value>(internal_codec->show_message(m)));
    }

    //-----------------------
    // Engine execution loop

    MPStatus processMessage(shared_ptr<MessageProcessor> mp);

    void runMessages(shared_ptr<MessageProcessor>& mp, MPStatus init_st);

    void runEngine(shared_ptr<MessageProcessor> mp);

    // Return a new thread running runEngine()
    // with the provided MessageProcessor
    shared_ptr<boost::thread> forkEngine(shared_ptr<MessageProcessor> mp);

    // Delegate wait to EngineControl
    void waitForEngine() {
      control->waitForEngine();
    }

    // Set the EngineControl's terminateV to true
    void terminateEngine() {
      control->set_terminate();
    }

    void forceTerminateEngine() {
      terminateEngine();
      control->messageAvail();
      cleanupEngine();
    }

    // Clear the Engine's connections and endpointis
    void cleanupEngine() {
      network_ctxt->service->stop();
      network_ctxt->service_threads->join_all();

      // TODO the following code never finishes running (deadlock?) :

      // if (connections) {
      //   connections->clearConnections();
      // }
      // if (endpoints) {
      //    //TODO: clearEndpoints() does not exists.
      //    //It should call removeEndpoint() on all endpoints
      //    // in the internal and external endpoint maps
      //    //endpoints->clearEndpoints();
      // }
    }

    //-------------------
    // Engine statistics.

    list<Address> nodes() {
      list<Address> r;
      if ( deployment ) { r = deployedNodes(*deployment); }
      else { logAt(boost::log::trivial::error, "Invalid system environment."); }
      return r;
    }

    tuple<size_t, size_t> statistics() {
      return make_tuple(queues? queues->size() : 0,
                        endpoints? endpoints->numEndpoints() : 0);
    }

    bool simulation() {
      if ( connections ) {
        return !connections->hasInternalConnections(); }
      else { logAt(boost::log::trivial::error, "Invalid connection state."); }
      return false;
    }

    /* Paul's hacky functions */
    unsigned getCollectionCount() { return collectionCount; }
    void incrementCollectionCount() { collectionCount += 1; }
    Address getAddress() { return config->address(); }

    // Converts a K3 channel mode into a native file descriptor mode.
    IOMode ioMode(string k3Mode);
  protected:
    bool                            log_enabled;
    std::ofstream                   log_stream;
    shared_ptr<EngineConfiguration> config;
    shared_ptr<EngineControl>       control;
    shared_ptr<SystemEnvironment>   deployment;
    shared_ptr<InternalCodec>       internal_codec;
    shared_ptr<MessageQueues>       queues;
    // shared_ptr<WorkerPool>          workers;
    shared_ptr<Net::NContext>       network_ctxt;

    // Endpoint and collection tracked by the engine.
    shared_ptr<EndpointState>       endpoints;
    shared_ptr<ConnectionState>     connections;

    // Listeners tracked by the engine.
    shared_ptr<Listeners>           listeners;
    unsigned                        collectionCount;

    void logMessageLoop(string s);

    void invalidEndpointIdentifier(string idType, const Identifier& eid) {
      string errorMsg = "Invalid " + idType + " endpoint identifier: " + eid;
      logAt(boost::log::trivial::error, errorMsg);
      throw runtime_error(errorMsg);
    }

    Builtin builtin(string builtinId);



    // TODO: for all of the genericOpen* endpoint constructors below, revisit:
    // i. no K3 type specified for type-safe I/O as with Haskell engine.
    // ii. buffer type with concurrent engine.
    void genericOpenBuiltin(string id, string builtinId);

    void genericOpenFile(string id, string path, IOMode mode);

    void genericOpenSocket(string id, Address addr, IOMode handleMode);

    void genericClose(Identifier eid, shared_ptr<Endpoint> ep);

    // Creates and registers a listener instance for the given address and network endpoint.
    void startListener(Address& listenerAddr, shared_ptr<Endpoint> ep);

    void stopListener(Identifier listener_name);

    //-----------------------
    // IOHandle constructors.

    shared_ptr<IOHandle> openBuiltinHandle(Builtin b, shared_ptr<Codec> codec);

    shared_ptr<IOHandle> openFileHandle(const string& path, shared_ptr<Codec> codec, IOMode m);

    shared_ptr<IOHandle> openSocketHandle(const Address& addr, shared_ptr<Codec> codec, IOMode m);
  };

} // namespace K3

#endif // K3_RUNTIME_ENGINE_H
// vim: set sts=2 ts=2 sw=2:
