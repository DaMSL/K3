#ifndef K3_RUNTIME_ENGINE_H
#define K3_RUNTIME_ENGINE_H

#include <atomic>
#include <string>
#include <list>
#include <map>
#include <exception>
#include <tuple>
#include <ctime>

#include "Common.hpp"
#include "Endpoint.hpp"
#include "Listener.hpp"
#include "Message.hpp"
#include "MessageProcessor.hpp"
#include "Options.hpp"

namespace K3 {

  template <>
  std::size_t hash_value(const int& t);

  namespace Net = K3::Asio;

  using std::shared_ptr;
  using std::tuple;
  using std::ofstream;

  //-------------------
  // Utility functions

  static inline Identifier listenerId(Address& addr) {
    return std::string("__") + "_listener_" + addressAsString(addr);
  }

  static inline std::string currentTime() {
    time_t now = time(0);
    tm *ltm = localtime(&now);
    std::ostringstream oss;
    oss << ltm->tm_hour << ":" << ltm->tm_min << ":" << ltm->tm_sec;
    return oss.str();
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


    shared_ptr<ListenerControl> listenerControl() {
      return shared_ptr<ListenerControl>(
              new ListenerControl(listenerCounter));
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
      shared_ptr<MessageCodec> _msgcodec,
      string log_level,
      string log_path,
      string result_v,
      string result_p,
      shared_ptr<const MessageQueues> qs
    ): LogMT("Engine") {
      configure(simulation, sys_env, _msgcodec, log_level, log_path, result_v, result_p, qs);
    }

    void configure(bool simulation, SystemEnvironment& sys_env, shared_ptr<MessageCodec> _msgcodec, string log_level,string log_path, string result_var, string result_path, shared_ptr<const MessageQueues> qs);

    //-----------
    // Messaging.

    // TODO: rvalue-ref overload for value argument.
    void send(Address addr, TriggerId triggerId, shared_ptr<Dispatcher> d, Address source);

    // TODO: avoid destructing tuple here
    void send(Message& m) {
      send(m.address(), m.id(), m.dispatcher(), m.source());
    }

    void send(shared_ptr<Message> m) {
      send(m->address(), m->id(), m->dispatcher(), m->source());
    }

    // TODO: Replace with use of std::bind.
    SendFunctionPtr sendFunction() {
      return [this](Address a, TriggerId i, shared_ptr<Value> v, Address src)
          { send(RemoteMessage(a, i, *v, src).toMessage()); };
    }

    //---------------------------------------
    // Internal and external channel methods.

    void openBuiltin(Identifier eid, string builtinId, string format) {
      externalEndpointId(eid) ?
        genericOpenBuiltin(eid, builtinId, format)
        : invalidEndpointIdentifier("external", eid);
    }

    void openFile(Identifier eid, string path, string format, IOMode mode) {
      externalEndpointId(eid) ?
        genericOpenFile(eid, path, format, mode)
        : invalidEndpointIdentifier("external", eid);
    }

    void openSocket(Identifier eid, Address addr, string format, IOMode mode) {
      externalEndpointId(eid) ?
        genericOpenSocket(eid, addr, format, mode)
        : invalidEndpointIdentifier("external", eid);
    }

    void close(Identifier eid) {
      externalEndpointId(eid)?
        genericClose(eid, endpoints->getExternalEndpoint(eid))
        : invalidEndpointIdentifier("external", eid);
    }

    void openBuiltinInternal(Identifier eid, string builtinId) {
      !externalEndpointId(eid)?
        genericOpenBuiltin(eid, builtinId, "internal")
        : invalidEndpointIdentifier("internal", eid);
    }

    void openFileInternal(Identifier eid, string path, IOMode mode) {
      !externalEndpointId(eid)?
        genericOpenFile(eid, path, "internal", mode)
        : invalidEndpointIdentifier("internal", eid);
    }

    void openSocketInternal(Identifier eid, Address addr, IOMode mode) {
      !externalEndpointId(eid)?
        genericOpenSocket(eid, addr, "internal", mode)
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

    template<typename T> shared_ptr<T> doReadExternal(Identifier eid) {
      return endpoints->getExternalEndpoint(eid)->doRead<T>();
    }

    shared_ptr<RemoteMessage> doReadInternal(Identifier eid) {
      return endpoints->getInternalEndpoint(eid)->doRead();
    }

    template<typename T>
    Collection<R_elem<T>> doReadExternalBlock (Identifier eid, int blockSize) {
      return endpoints->getExternalEndpoint(eid)->doReadBlock<T>(blockSize);
    }

    bool hasWrite(Identifier eid) {
      if (externalEndpointId(eid)) {
        return endpoints->getExternalEndpoint(eid)->hasWrite();
      } else {
        return endpoints->getInternalEndpoint(eid)->hasWrite();
      }
    }

    template<typename T>
    void doWriteExternal(Identifier eid, const T& v) {
      endpoints->getExternalEndpoint(eid)->doWrite<T>(v);
      endpoints->getExternalEndpoint(eid)->flushBuffer();
      return;
    }

    void doWriteInternal(Identifier eid, const RemoteMessage& m) {
      return endpoints->getInternalEndpoint(eid)->doWrite(m);
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
      queues->messageAvail(*me);
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

    // JSON logging
    void logJson(std::string time, const Address& peer, std::string trig, std::string msg_contents, std::map<std::string, std::string> env, const Address& msgSource) {
            auto& event_stream = *std::get<0>(log_streams[peer]);

            // Log message Event:
            // message_id, dest_peer, trigger_name
            // source_peer, contents, timestamp
            event_stream << message_counter << "|";
            event_stream << K3::serialization::json::encode<Address>(peer) << "|";
            event_stream << "\"" << trig << "\"|";
            event_stream << K3::serialization::json::encode<Address>(msgSource) << "|";
            event_stream << msg_contents << "|";
            event_stream << time_milli() << std::endl;

            // Log Global state
            auto& global_stream = *std::get<1>(log_streams[peer]);
            auto s = std::to_string(message_counter) + "|" + K3::serialization::json::encode<Address>(peer) + "|";
            for (const auto& tup : env) {
              global_stream << s << tup.first << "|" << tup.second << std::endl;
            }
    }

    void logResult(shared_ptr<MessageProcessor>& mp) {
      if (result_var != "") {
        auto a = *me;
        auto dir = result_path != "" ? result_path : ".";
        auto s = dir + "/" + addressAsString(a) + "_Result.txt";
        std::ofstream ofs;
        ofs.open(s);
        auto m = mp->json_bindings(a);
        if (m.count(result_var) != 0) {
          ofs << m[result_var] << std::endl;
        } else {
          throw std::runtime_error(
              "Cannot log result variable, does not exist: " + result_var);
        }
      }
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

    bool logEnabled() { return log_enabled; }
    bool logJsonEnabled() { return log_json; }

  protected:
    shared_ptr<EngineConfiguration> config;
    shared_ptr<EngineControl>       control;
    shared_ptr<Address>             me;
    shared_ptr<MessageCodec>        msgcodec;
    shared_ptr<const MessageQueues> queues;
    // shared_ptr<WorkerPool>       workers;
    shared_ptr<Net::NContext>       network_ctxt;

    // Endpoint and collection tracked by the engine.
    shared_ptr<EndpointState>       endpoints;
    shared_ptr<ConnectionState>     connections;

    // Listeners tracked by the engine.
    shared_ptr<Listeners>           listeners;
    unsigned                        collectionCount;

    // Log info
    bool                            log_enabled;
    bool                            log_json;
    // Tuple of (eventLog, globalsLog)

    std::map<Address, tuple<shared_ptr<ofstream>, shared_ptr<ofstream>>> log_streams;
    string                          log_path;
    string                          result_var;
    string                          result_path;


    unsigned int                    message_counter;

    void logMessageLoop(string s);

    void invalidEndpointIdentifier(string idType, const Identifier& eid) {
      string errorMsg = "Invalid " + idType + " endpoint identifier: " + eid;
      logAt(boost::log::trivial::error, errorMsg);
      throw runtime_error(errorMsg);
    }

    Builtin builtin(string builtinId);

    // Format selection.
    EndpointState::CodecDetails codecOfFormat(string format);

    // TODO: for all of the genericOpen* endpoint constructors below, revisit:
    // i. no K3 type specified for type-safe I/O as with Haskell engine.
    // ii. buffer type with concurrent engine.
    void genericOpenBuiltin(string id, string builtinId, string format);

    void genericOpenFile(string id, string path, string format, IOMode mode);

    void genericOpenSocket(string id, Address addr, string format, IOMode handleMode);

    void genericClose(Identifier eid, shared_ptr<Endpoint> ep);

    // Creates and registers a listener instance for the given address and network endpoint.
    void startListener(Address& listenerAddr, shared_ptr<Endpoint> ep);

    void stopListener(Identifier listener_name);

    //-----------------------
    // IOHandle constructors.

    shared_ptr<IOHandle> openBuiltinHandle(Builtin b, shared_ptr<FrameCodec> frame);

    shared_ptr<IOHandle> openFileHandle(const string& path, shared_ptr<FrameCodec> frame, IOMode m);

    shared_ptr<IOHandle> openSocketHandle(const Address& addr, shared_ptr<FrameCodec> frame, IOMode m);
  };

  template <>
  std::size_t hash_value(int const& b);
} // namespace K3

#endif // K3_RUNTIME_ENGINE_H
// vim: set sts=2 ts=2 sw=2:
