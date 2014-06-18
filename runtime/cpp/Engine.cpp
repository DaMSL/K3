#include "Engine.hpp"

namespace K3 {

    void Engine::configure(bool simulation, SystemEnvironment& sys_env, shared_ptr<InternalCodec> _internal_codec) {
      internal_codec = _internal_codec;
      list<Address> processAddrs = deployedNodes(sys_env);
      Address initialAddress;

      if (!processAddrs.empty()) {
        initialAddress = processAddrs.front();
      } else {
        logAt(warning, "No deployment peer addresses found, using a default address.");
        initialAddress = defaultAddress;
      }

      config       = shared_ptr<EngineConfiguration>(new EngineConfiguration(initialAddress));
      control      = shared_ptr<EngineControl>(new EngineControl(config));
      deployment   = shared_ptr<SystemEnvironment>(new SystemEnvironment(sys_env));
      // workers     = shared_ptr<WorkerPool>(new InlinePool());
      network_ctxt = shared_ptr<Net::NContext>(new Net::NContext());
      endpoints    = shared_ptr<EndpointState>(new EndpointState());
      listeners    = shared_ptr<Listeners>(new Listeners());
      collectionCount   = 0;

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
        // This opens engine sockets with an internal codec, relying on openSocketInternal()
        // to construct the Listener object and the thread runs the listener's event loop.
        for (Address k3proc : processAddrs) {
          openSocketInternal(peerEndpointId(k3proc), k3proc, IOMode::Read);
        }
      }
    }
    
    //-----------
    // Messaging.

    // TODO: rvalue-ref overload for value argument.
    void Engine::send(Address addr, Identifier triggerId, const Value& v)
    {
      if (deployment) {
        bool local_address = isDeployedNode(*deployment, addr);
        bool shortCircuit =  local_address || simulation();
        Message msg(addr, triggerId, v);
   
        if ( shortCircuit ) {
          // Directly enqueue.
          // TODO: ensure we avoid copying the value.
          queues->enqueue(msg);
          control->messageAvail();
        } else {
          // Get connection and send a message on it.
          Identifier eid = connectionId(addr);
          bool sent = false;

          for (int i = 0; !sent && i < config->connectionRetries(); ++i) {
            shared_ptr<Endpoint> ep = endpoints->getInternalEndpoint(eid);

            if ( ep && ep->hasWrite() ) {
              shared_ptr<Value> v = make_shared<Value>(internal_codec->show_message(msg));
              ep->doWrite(v);
              ep->flushBuffer();
              sent = true;
            } else {
              if (ep && !ep->hasWrite()) {
                logAt(trivial::trace, eid + "is not ready for write. Sleeping...");
                boost::this_thread::sleep_for( boost::chrono::seconds(1) );

              }
              else {
                logAt(trivial::trace, "Creating endpoint: " + eid);
                openSocketInternal(eid, addr, IOMode::Write);
              }
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
    
    //-----------------------
    // Engine execution loop

    MPStatus Engine::processMessage(shared_ptr<MessageProcessor> mp)
    {
      // Log queues?

      // Get a message from the engine queues.
      shared_ptr<Message> next_message = queues->dequeue();

      if (next_message) {
        // Log Message
        std::string target = next_message->target();
        std::string contents = next_message->contents();
        std::string sep = "======================================";
        logAt(trivial::trace, sep);
        logAt(trivial::trace, "Message for: " + target);
        logAt(trivial::trace, "Contents: " + contents);

        // If there was a message, return the result of processing that message.
        LoopStatus res =  mp->process(*next_message);

        // Log Env
        logAt(trivial::trace, "Environment: ");
        std::map<std::string, std::string> env = mp->bindings(next_message->address());
        std::map<std::string, std::string>::iterator iter;
        for (iter = env.begin(); iter != env.end(); ++iter) {
           std::string id = iter->first;
           std::string val = iter->second;
           logAt(trivial::trace, "  " + id + " = " + val);
        }

        return res;

      } else {
        // Otherwise return a Done, indicating no messages in the queues.
        return LoopStatus::Done;
      }
    }

    void Engine::runMessages(shared_ptr<MessageProcessor>& mp, MPStatus init_st)
    {
      MPStatus curr_status = init_st;
      MPStatus next_status;
      logAt(trivial::trace, "Starting the Message Processing Loop");
      while(true) {
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
            if (control->terminate()) {
                logAt(trivial::trace, "Finished Message Processing Loop.");
                return;
            }

            logAt(trivial::trace, "Waiting for Messages...");
            control->waitForMessage(
              [=] () {
                return !control->terminate() && queues->size() < 1;
              });

            // Check for termination signal after waiting is finished
            if (control->terminate()) {
              logAt(trivial::trace, "Received Termination Signal. Exiting Message Processing Loop.");
              return;
            }
            // Otherwise continue
            next_status = LoopStatus::Continue;
            break;
        }
        curr_status = next_status;
      }
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

    // TODO: for all of the genericOpen* endpoint constructors below, revisit:
    // i. no K3 type specified for type-safe I/O as with Haskell engine.
    // ii. buffer type with concurrent engine.
    void Engine::genericOpenBuiltin(string id, string builtinId) {
      if (endpoints) {
        shared_ptr<Codec> codec = shared_ptr<DelimiterCodec>(new DelimiterCodec('\n'));

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
            buf = shared_ptr<EndpointBuffer>(new ScalarEPBufferMT());
            break;
          case Builtin::Stdin:
            break;
        }

        endpoints->addEndpoint(id, make_tuple(ioh, buf, bindings));
      }
      else { logAt(trivial::error, "Unintialized engine endpoints"); }
    }

    void Engine::genericOpenFile(string id, string path, IOMode mode) {
      if ( endpoints ) {
        shared_ptr<Codec> codec =  shared_ptr<DelimiterCodec>(new DelimiterCodec('\n'));

        // Create the IO Handle
        shared_ptr<IOHandle> ioh = openFileHandle(path, codec, mode);

        // Add the endpoint to the given endpoint state.
        shared_ptr<EndpointBuffer> buf = shared_ptr<EndpointBuffer>(new ScalarEPBufferMT());

        shared_ptr<EndpointBindings> bindings =
          shared_ptr<EndpointBindings>(new EndpointBindings(sendFunction()));

        endpoints->addEndpoint(id, make_tuple(ioh, buf, bindings));

        // TODO: Prime buffers as needed with a read/refresh.
      } else { logAt(trivial::error, "Unintialized engine endpoints"); }
    }

    void Engine::genericOpenSocket(string id, Address addr, IOMode handleMode) {
      if (endpoints) {
        shared_ptr<Codec> codec =  shared_ptr<LengthHeaderCodec>(new LengthHeaderCodec());

        // Create the IO Handle.
        shared_ptr<IOHandle> ioh = openSocketHandle(addr, codec, handleMode);

        // Add the endpoint.
        shared_ptr<EndpointBuffer> buf = shared_ptr<EndpointBuffer>(
            new ScalarEPBufferMT());

        shared_ptr<EndpointBindings> bindings =
          shared_ptr<EndpointBindings>(new EndpointBindings(sendFunction()));

        endpoints->addEndpoint(id, make_tuple(ioh, buf, bindings));

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
          shared_ptr<Net::Listener>(
            new Net::Listener(lstnr_name, network_ctxt, queues, ep,
                              control->listenerControl(), internal_codec));

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

    shared_ptr<IOHandle> Engine::openBuiltinHandle(Builtin b, shared_ptr<Codec> codec) {
      shared_ptr<IOHandle> r;
      switch (b) {
        case Builtin::Stdin:
          r = shared_ptr<IOHandle>(new BuiltinHandle(codec, BuiltinHandle::Stdin()));
          break;
        case Builtin::Stdout:
          r = shared_ptr<IOHandle>(new BuiltinHandle(codec, BuiltinHandle::Stdout()));
          break;
        case Builtin::Stderr:
          r = shared_ptr<IOHandle>(new BuiltinHandle(codec, BuiltinHandle::Stderr()));
          break;
      }
      return r;
    }

    shared_ptr<IOHandle> Engine::openFileHandle(const string& path, shared_ptr<Codec> codec, IOMode m) {
      shared_ptr<IOHandle> r;
      switch (m) {
        case IOMode::Read:
          r = make_shared<FileHandle>(codec, make_shared<file_source>(path), StreamHandle::Input());
          break;
        case IOMode::Write:
          r = make_shared<FileHandle>(codec, make_shared<file_sink>(path), StreamHandle::Output());
          break;
        case IOMode::Append:
          r = make_shared<FileHandle>(codec, make_shared<file_sink>(path, std::ios::app), StreamHandle::Output());
          break;
        case IOMode::ReadWrite:
          string errorMsg = "Unsupported open mode for file handle";
          logAt(trivial::error, errorMsg);
          throw runtime_error(errorMsg);
          break;
      }
      return r;
    }

    shared_ptr<IOHandle> Engine::openSocketHandle(const Address& addr, shared_ptr<Codec> codec, IOMode m) {
      shared_ptr<IOHandle> r;
      switch ( m ) {
        case IOMode::Read: {
          shared_ptr<Net::NEndpoint> n_ep = shared_ptr<Net::NEndpoint>(new Net::NEndpoint(network_ctxt, addr));
          r = make_shared<NetworkHandle>(NetworkHandle(codec, n_ep));
          break;
        }
        case IOMode::Write: {
          shared_ptr<Net::NConnection> conn = shared_ptr<Net::NConnection>(new Net::NConnection(network_ctxt, addr));
          r = make_shared<NetworkHandle>(NetworkHandle(codec, conn));
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

}
