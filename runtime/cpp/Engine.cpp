#include "Engine.hpp"
#include <stdlib.h>

using namespace boost::log;
using namespace boost::log::trivial;
using std::shared_ptr;
using std::tuple;
using std::ofstream;
using boost::thread;

namespace K3 {

    void Engine::configure(bool simulation, SystemEnvironment& sys_env, shared_ptr<MessageCodec> _msgcodec,
                           string log_l, string log_p, string result_v, string result_p, shared_ptr<const MessageQueues> qs)
    {
      queues = qs;
      msgcodec = _msgcodec;
      log_enabled = false;
      log_json = false;
      if (log_l == "final") {
        log_final = true;
      } else if (log_l != "") {
        log_enabled = true;
      }
      if (log_p != "") { log_json = true; }
      auto dir = log_p != "" ? log_p : ".";
      log_path = dir;

      result_var = result_v;
      result_path = result_p;

      list<Address> processAddrs = deployedNodes(sys_env);
      Address initialAddress;

      if (processAddrs.size() > 1) {
        throw std::runtime_error("Only 1 peer per engine allowed");
      }

      if (log_json) {
        for (const auto& addr : processAddrs) {
          auto s1 = log_path + "/" + addressAsString(addr) + "_Messages.dsv";
          auto s2 = log_path + "/" + addressAsString(addr) + "_Globals.dsv";
          log_streams[addr] = make_tuple(make_shared<ofstream>(s1), make_shared<ofstream>(s2));
        }
      }

      if (!processAddrs.empty()) {
        initialAddress = processAddrs.front();
      } else {
        logAt(warning, "No deployment peer addresses found, using a default address.");
        initialAddress = defaultAddress;
      }

      config       = make_shared<EngineConfiguration>(initialAddress);
      control      = make_shared<EngineControl>(config);
      // workers     = shared_ptr<WorkerPool>(new InlinePool());
      network_ctxt = make_shared<Net::NContext>();
      me           = make_shared<Address>(initialAddress);
      endpoints    = make_shared<EndpointState>();
      listeners    = make_shared<Listeners>();
      collectionCount   = 0;
      message_counter = 0;

      // Network engine initialization.
      connections = make_shared<ConnectionState>(network_ctxt, false);

      // Start network listeners for all K3 processes on this engine.
      // This opens engine sockets with an internal frame, relying on openSocketInternal()
      // to construct the Listener object and the thread runs the listener's event loop.
      for (Address k3proc : processAddrs) {
        openSocketInternal(peerEndpointId(k3proc), k3proc, IOMode::Read);
      }
    }

    //-----------
    // Messaging.

    // TODO: rvalue-ref overload for value argument.
    void Engine::send(Address addr, TriggerId triggerId,
                      shared_ptr<Dispatcher> disp, Address src) {
      if (queues) {
        bool local_address = queues->isLocal(addr);
        bool shortCircuit = local_address;

        if (shortCircuit) {
          // Directly enqueue.
          // TODO: ensure we avoid copying the dispatcher
          Message msg(addr, triggerId, disp, src);
          queues->enqueue(msg);
        } else {
          RemoteMessage rMsg(addr, triggerId, disp->pack(), src);

          // Get connection and send a message on it.
          Identifier eid = connectionId(addr);
          bool sent = false;

          for (int i = 0; !sent && i < config->connectionRetries(); ++i) {
            shared_ptr<InternalEndpoint> ep = endpoints->getInternalEndpoint(eid);

            if (ep && ep->hasWrite()) {
              ep->doWrite(rMsg);
              ep->flushBuffer();
              sent = true;
            } else {
              if (ep && !ep->hasWrite()) {
                if (log_enabled)
                  logAt(trivial::trace,
                        eid + "is not ready for write. Sleeping...");
                boost::this_thread::sleep_for(boost::chrono::milliseconds(20));

              } else {
                if (log_enabled)
                  logAt(trivial::trace, "Creating endpoint: " + eid);
                // Peer may not be accepting connections yet, wait:
                try {
                  if (i > 0) {
                    boost::this_thread::sleep_for(
                        boost::chrono::milliseconds(200));
                    if (log_enabled)
                      logAt(trivial::trace, "Retry: Creating endpoint.");
                  }
                  openSocketInternal(eid, addr, IOMode::Write);
                } catch (std::runtime_error e) {
                  // Try again next iteration.
                }
              }
            }
          }

          if (!sent) {
            string errorMsg = "Failed to send a message to " + rMsg.target();
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

    //-----------------------
    // Engine execution loop
    void Engine::logEnvironment(const Address& a) {
      logAt(trivial::trace, "Environment: ");
      std::map<std::string, std::string> env = mp_->bindings(a);
      std::map<std::string, std::string>::iterator iter;
      for (iter = env.begin(); iter != env.end(); ++iter) {
         std::string id = iter->first;
         std::string val = iter->second;
         logAt(trivial::trace, "  " + id + " = " + val);
      }
    }

    void Engine::logFinalEnvironment(const Address& a) {
      if (log_final) {
        logEnvironment(a);
      }
    }

    MPStatus Engine::processMessage(shared_ptr<MessageProcessor> mp)
    {
      // Get a message from the engine queues.
      shared_ptr<Message> next_message = queues->dequeue(*me);

      if (next_message) {
        message_counter++;
        // Log Message
        if (log_enabled) {
          std::string target = next_message->target();
          std::shared_ptr<Dispatcher> d = next_message->dispatcher();
          std::string contents = d->pack();
          std::string sep = "======================================";
          logAt(trivial::trace, sep);
          logAt(trivial::trace, "Message for: " + target);
          logAt(trivial::trace, "Contents: " + contents);
        }

        // If there was a message, return the result of processing that message.
        LoopStatus res =  mp->process(*next_message);

        // Log Env
        if (log_enabled) {
          logEnvironment(next_message->address());
        }

        return res;
      } else {
        // Otherwise return a Done, indicating no messages in the queues.
        return LoopStatus::Done;
      }
    }

    void Engine::runMessages(shared_ptr<MessageProcessor>& mp, MPStatus init_st)
    {
      mp_ = mp;
      MPStatus curr_status = init_st;
      MPStatus next_status;
      logAt(trivial::trace, "Starting the Message Processing Loop");

      while(true) {
        if (control->terminate()) {
            logAt(trivial::trace, "Finished Message Processing Loop.");
            return;
        }
        switch (curr_status) {
          // If we are not in error, process the next message.
          case LoopStatus::Continue:
            next_status = processMessage(mp);
            break;

          // If we were in error, exit out with error.
          case LoopStatus::Error:
            //TODO silent error?
            return;

          // If there are no messages on the queues,
          //  - If the terminate flag has been set, exit out normally.
          //  - Otherwise, wait for a message and continue.
          case LoopStatus::Done:
            queues->waitForMessage(*me,
              [=] () {
                return !control->terminate() && queues->empty(*me);
              });

            // Otherwise continue
            next_status = LoopStatus::Continue;
            break;
        }
        curr_status = next_status;
      }
    }

    void Engine::runEngine(shared_ptr<MessageProcessor> mp) {
      // TODO MessageProcessor initialize() is empty.
      // In the Haskell code base, this is where the K3 AST
      // is passed to the MessageProcessor
      mp->initialize();

      // TODO Check PoolType for Uniprocess, Multithreaded..etc
      // Following code is for Uniprocess mode only:
      // TODO Dummy ID. Need to log actual ThreadID
      // workers->setId(1);

      runMessages(mp, mp->status());
      logResult(mp);
    }

    // Return a new thread running runEngine()
    // with the provided MessageProcessor
    shared_ptr<thread> Engine::forkEngine(shared_ptr<MessageProcessor> mp) {
      using std::placeholders::_1;

      std::function<void(shared_ptr<MessageProcessor>)> _runEngine = std::bind(
        &Engine::runEngine, this, _1
      );

      shared_ptr<thread> engineThread = make_shared<thread>(_runEngine, mp);

      return engineThread;
    }

    Builtin Engine::builtin(string builtinId) {
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
    IOMode Engine::ioMode(string k3Mode) {
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

    EndpointState::CodecDetails Engine::codecOfFormat(string format) {
      EndpointState::CodecDetails r;
      bool set = false;

      if ( format == "k3" ) {
        r = dynamic_pointer_cast<Codec, K3Codec>(make_shared<K3Codec>(Codec::CodecFormat::K3));
        set = true;
      } else if ( format == "k3b" ) {
        r = dynamic_pointer_cast<Codec, K3BCodec>(make_shared<K3BCodec>(Codec::CodecFormat::K3B));
        set = true;
      } else if ( format == "k3yb" ) {
        r = dynamic_pointer_cast<Codec, K3YBCodec>(make_shared<K3YBCodec>(Codec::CodecFormat::K3YB));
        set = true;
      } else if ( format == "csv" ) {
        r = dynamic_pointer_cast<Codec, CSVCodec>(make_shared<CSVCodec>(Codec::CodecFormat::CSV));
        set = true;
      } else if ( format == "psv" ) {
        r = dynamic_pointer_cast<Codec, PSVCodec>(make_shared<PSVCodec>(Codec::CodecFormat::PSV));
        set = true;
      } else if ( format == "k3x" ) {
        r = dynamic_pointer_cast<Codec, K3XCodec>(make_shared<K3XCodec>(Codec::CodecFormat::K3X));
        set = true;
      } else if ( format == "internal" ) {
        r = msgcodec;
        set = true;
      }

      if ( set ) { return r; }
      string errorMsg = "Invalid endpoint format " + format;
      logAt(boost::log::trivial::error, errorMsg);
      throw runtime_error(errorMsg);
    }

    // TODO: for all of the genericOpen* endpoint constructors below, revisit:
    // i. no K3 type specified for type-safe I/O as with Haskell engine.
    // ii. buffer type with concurrent engine.
    void Engine::genericOpenBuiltin(string id, string builtinId, string format)
    {
      if (endpoints) {
        shared_ptr<FrameCodec> frame = make_shared<DelimiterFrameCodec>('\n');

        Builtin b = builtin(builtinId);

        // Create the IO Handle
        shared_ptr<IOHandle> ioh = openBuiltinHandle(b, frame);

        // Add the endpoint to the given endpoint state.
        shared_ptr<EndpointBuffer> buf;

        shared_ptr<EndpointBindings> bindings = make_shared<EndpointBindings>(sendFunction());

        switch (b) {
          case Builtin::Stdout:
          case Builtin::Stderr:
            buf = std::static_pointer_cast<EndpointBuffer, ScalarEPBufferMT>(make_shared<ScalarEPBufferMT>());
            break;
          case Builtin::Stdin:
            break;
        }

        endpoints->addEndpoint(id, make_tuple(ioh, buf, bindings), codecOfFormat(format));
      }
      else { logAt(trivial::error, "Unintialized engine endpoints"); }
    }

    void Engine::genericOpenFile(string id, string path, string format, IOMode mode)
    {
      if ( endpoints ) {
        shared_ptr<FrameCodec> frame = make_shared<DelimiterFrameCodec>('\n');

        // Create the IO Handle
        shared_ptr<IOHandle> ioh = openFileHandle(path, frame, mode);

        // Add the endpoint to the given endpoint state.
        // For now, files have no endpoint buffer.

        //shared_ptr<EndpointBuffer> buf =
        //  std::static_pointer_cast<EndpointBuffer,ScalarEPBufferST>(make_shared<ScalarEPBufferST>());

        shared_ptr<EndpointBuffer> buf;
        shared_ptr<EndpointBindings> bindings = make_shared<EndpointBindings>(sendFunction());

        endpoints->addEndpoint(id, make_tuple(ioh, buf, bindings), codecOfFormat(format));

        // TODO: Prime buffers as needed with a read/refresh.
      } else { logAt(trivial::error, "Unintialized engine endpoints"); }
    }

    void Engine::genericOpenSocket(string id, Address addr, string format, IOMode handleMode)
    {
      if (endpoints) {
        shared_ptr<FrameCodec> frame = make_shared<LengthHeaderFrameCodec>();

        // Create the IO Handle.
        shared_ptr<IOHandle> ioh = openSocketHandle(addr, frame, handleMode);

        // Add the endpoint.
        shared_ptr<EndpointBuffer> buf =
          std::static_pointer_cast<EndpointBuffer, ScalarEPBufferMT>(make_shared<ScalarEPBufferMT>());

        shared_ptr<EndpointBindings> bindings = make_shared<EndpointBindings>(sendFunction());

        endpoints->addEndpoint(id, make_tuple(ioh, buf, bindings), codecOfFormat(format));

        // Mode-based handling of endpoint vs connection, e.g., to start network listener.
        switch (handleMode) {
          case IOMode::Read:
            if (externalEndpointId(id)) {
              startListener(addr, endpoints->getExternalEndpoint(id));
            } else {
              startListener(addr, endpoints->getInternalEndpoint(id));
            }
            break;
          case IOMode::Write:
          case IOMode::Append:
          case IOMode::ReadWrite:
            break;
        }

      } else { logAt(trivial::error, "Unintialized engine endpoints"); }
    }

    void Engine::genericClose(Identifier eid, shared_ptr<Endpoint> ep) {
      // Close the endpoint
      if (ep) {
        // Deregister the listener if this is a network source
        if ( get<1>(ep->handle()->networkSource()) ) {
          stopListener(eid);
        }
        ep->close();
      }

      // Remove the endpoint
      endpoints->removeEndpoint(eid);
    }

    // Creates and registers a listener instance for the given address and network endpoint.
    void Engine::startListener(Address& listenerAddr, shared_ptr<Endpoint> ep)
    {
      if ( listeners ) {
        // Create a listener object, which in turn will start the
        // thread for receiving messages.
        Identifier lstnr_name = listenerId(listenerAddr);

        shared_ptr<Net::Listener> lstnr =
          make_shared<Net::Listener>(
            lstnr_name, network_ctxt, queues, ep, control->listenerControl(), msgcodec);

        // Register the listener (track in map, and update control).
        (*listeners)[lstnr_name] = lstnr;

        // Update listener control to increment network done counter.
        lstnr->control()->counter()->registerListener();

      } else {
        logAt(trivial::error, "Unintialized engine listeners");
      }
    }

    void Engine::stopListener(Identifier listener_name) {
      if ( listeners ) {

        try {
          // Update listener control to decrement network done counter.
          shared_ptr<Net::Listener> lstnr = listeners->at(listener_name);
          if ( lstnr ) { lstnr->control()->counter()->deregisterListener(); }
          listeners->erase(listener_name);
        } catch ( std::out_of_range& oor ) {
          logAt(trivial::error, "Invalid listener identifier "+listener_name);
        }

      } else {
        logAt(trivial::error, "Unintialized engine listeners");
      }
    }

    //-----------------------
    // IOHandle constructors.

    shared_ptr<IOHandle> Engine::openBuiltinHandle(Builtin b, shared_ptr<FrameCodec> frame) {
      shared_ptr<IOHandle> r;
      switch (b) {
        case Builtin::Stdin:
          r = shared_ptr<IOHandle>(new BuiltinHandle(frame, BuiltinHandle::Stdin()));
          break;
        case Builtin::Stdout:
          r = shared_ptr<IOHandle>(new BuiltinHandle(frame, BuiltinHandle::Stdout()));
          break;
        case Builtin::Stderr:
          r = shared_ptr<IOHandle>(new BuiltinHandle(frame, BuiltinHandle::Stderr()));
          break;
      }
      return r;
    }

    shared_ptr<IOHandle> Engine::openFileHandle(const string& path, shared_ptr<FrameCodec> frame, IOMode m) {
      shared_ptr<IOHandle> r;
      switch (m) {
        case IOMode::Read:
          r = make_shared<FileHandle>(frame, make_shared<file_source>(path), StreamHandle::Input());
          break;
        case IOMode::Write:
          r = make_shared<FileHandle>(frame, make_shared<file_sink>(path), StreamHandle::Output());
          break;
        case IOMode::Append:
          r = make_shared<FileHandle>(frame, make_shared<file_sink>(path, std::ios::app), StreamHandle::Output());
          break;
        case IOMode::ReadWrite:
          string errorMsg = "Unsupported open mode for file handle";
          logAt(trivial::error, errorMsg);
          throw runtime_error(errorMsg);
          break;
      }
      return r;
    }

    shared_ptr<IOHandle> Engine::openSocketHandle(const Address& addr, shared_ptr<FrameCodec> frame, IOMode m) {
      shared_ptr<IOHandle> r;
      switch ( m ) {
        case IOMode::Read: {
          shared_ptr<Net::NEndpoint> n_ep = make_shared<Net::NEndpoint>(network_ctxt, addr);
          r = make_shared<NetworkHandle>(NetworkHandle(frame, n_ep));
          break;
        }
        case IOMode::Write: {
          shared_ptr<Net::NConnection> conn = make_shared<Net::NConnection>(network_ctxt, addr);
          r = make_shared<NetworkHandle>(NetworkHandle(frame, conn));
          break;
        }

        case IOMode::Append:
        case IOMode::ReadWrite: {
            string errorMsg = "Unsupported open mode for network handle";
            logAt(trivial::error, errorMsg);
            throw runtime_error(errorMsg);
            break;
        }
      }
      return r;
    }

    void Engine::logMessageLoop(string s) { if (log_enabled) { logAt(trivial::trace, s); } }

  template <>
  std::size_t hash_value(int const& b) {
   const unsigned int fnv_prime = 0x811C9DC5;
   unsigned int hash = 0;
   const char* p = (const char *) &b;
   for(std::size_t i = 0; i < sizeof(int); i++)
   {
      hash *= fnv_prime;
      hash ^= p[i];
   }
   return hash;
  }
}
