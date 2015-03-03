#include "Endpoint.hpp"

using namespace boost::log;
using namespace boost;
using std::out_of_range;

namespace K3
{
    bool ScalarEPBufferMT::push_back(shared_ptr<Value> v) {
      strict_lock<LockB> guard(*this);
      // Failure:
      if (!v || (contents.get(guard))) {
        return false;
      }
      // Success:
      contents.get(guard) = v;
      return true;
    }

    shared_ptr<Value> ScalarEPBufferMT::pop() {
      strict_lock<LockB> guard(*this);
      shared_ptr<Value> v;
      if (contents.get(guard)) {
        // Success:
        v = contents.get(guard);
        contents.get(guard).reset();
      }
      // In case of failure, v is a null pointer
      return v;
    }

    shared_ptr<Value> ScalarEPBufferMT::refresh(shared_ptr<IOHandle> ioh, NotifyFn notify)
    {
      strict_lock<LockB> guard(*this);
      shared_ptr<Value> r;

      // Read from the buffer if possible
      if (contents.get(guard)) {
        r = contents.get(guard);
        contents.get(guard).reset();
        notify(r);
      }

      // If there is more data in the underlying IOHandle
      // use it to populate the buffer
      if (ioh->hasRead()) {
        shared_ptr<Value> v = ioh->doRead();
        contents.get(guard) = v;
      }

     return r;
    }

    void ScalarEPBufferMT::flush(shared_ptr<IOHandle> ioh, NotifyFn notify) {
      // pop() a value and write to the handle if possible
      strict_lock<LockB> guard(*this);
      if (contents.get(guard)) {
        shared_ptr<Value> v = contents.get(guard);
        contents.get(guard).reset();
        ioh->doWrite(v);
        notify(v);
      }
    }

    bool ScalarEPBufferMT::transfer(shared_ptr<MessageQueues> queues,
                                    shared_ptr<InternalFraming> frame,
                                    NotifyFn notify) {
      strict_lock<LockB> guard(*this);
      bool transferred = false;
      if(contents.get(guard)) {
        shared_ptr<Value> v = contents.get(guard);
        contents.get(guard).reset();
        if (queues && frame) {
          Message msg = *(frame->read_message(*v).toMessage());
          queues->enqueue(msg);
          transferred = true;
        }
        notify(v);
      }
      return transferred;
    }

    // Buffer Operations
    bool ScalarEPBufferST::push_back(shared_ptr<Value> v) {
      // Failure:
      if (!v || this->full()) {
        return false;
      }
      // Success:
      contents = v;
      return true;
    }

    shared_ptr<Value> ScalarEPBufferST::pop() {
      shared_ptr<Value> v;
      if (!this->empty()) {
        // Success:
        v = contents;
        contents = shared_ptr<Value>();
      }
      // In case of failure, v is a null pointer
      return v;
    }

    shared_ptr<Value> ScalarEPBufferST::refresh(shared_ptr<IOHandle> ioh, NotifyFn notify)
    {
      shared_ptr<Value> r;

      // Read from the buffer if possible
      if (!(this->empty())) {
        r = this->pop();
        notify(r);
      }

      // If there is more data in the underlying IOHandle
      // use it to populate the buffer
      if (ioh->hasRead()) {
        shared_ptr<Value> v = ioh->doRead();
        this->push_back(v);
      }

     return r;
    }

    void ScalarEPBufferST::flush(shared_ptr<IOHandle> ioh, NotifyFn notify) {
      // pop() a value and write to the handle if possible
      if (!this->empty()) {
        shared_ptr<Value> v = this->pop();
        ioh->doWrite(v);
        notify(v);
      }
    }

    bool ScalarEPBufferST::transfer(shared_ptr<MessageQueues> queues,
                                    shared_ptr<InternalFraming> frame,
                                    NotifyFn notify) {
      bool transferred = false;
      if(!this->empty()) {
        shared_ptr<Value> v = this->pop();
        if (queues && frame) {
          Message msg = *(frame->read_message(*v).toMessage());
          queues->enqueue(msg);
          transferred = true;
        }
        notify(v);
      }
      return transferred;
    }

    size_t ContainerEPBufferST::capacity() {
      if (!contents) {
        return 0;
      }
      int s = bufferMaxSize(spec);
      return s <= 0? contents->max_size() : s;
    }

    bool ContainerEPBufferST::push_back(shared_ptr<Value> v) {
      // Failure if contents is null or full
      if (!v || !contents || full()) {
        return false;
      }

      // Success
      contents->push_back(*v);
      return true;
    }

    shared_ptr<Value> ContainerEPBufferST::pop() {
      shared_ptr<Value> v;
      if (!empty()) {
        v = make_shared<Value>(contents->front());
        contents->pop_front();
      }
      return v;
    }

    shared_ptr<Value> ContainerEPBufferST::refresh(shared_ptr<IOHandle> ioh,
                                                   NotifyFn notify) {
      shared_ptr<Value> r;

      // Read from the buffer if possible
      if (!(this->empty())) {
        r = this->pop();
        notify(r);
      }

      // If there is more data in the underlying IOHandle
      // use it to populate the buffer. try to batch
      int n = batchSize();
      for(int i=0; !full() && ioh->hasRead() && i < n; i++) {
        shared_ptr<Value> v = ioh->doRead();
        this->push_back(v);
      }
      return r;
    }

    // flush overloaded with force flag to ignore batching semantics
    void ContainerEPBufferST::flush(shared_ptr<IOHandle> ioh,
                                    NotifyFn notify,
                                    bool force) {
      while (batchAvailable() || force) {
        int n = batchSize();
        for (int i=0; i < n; i++) {
          if (force && empty()) { return; }
          shared_ptr<Value> v = this->pop();
          ioh->doWrite(v);
          notify(v);
        }
      }
    }

    // transfer overloaded with force flag to ignore batching semantics
    bool ContainerEPBufferST::transfer(shared_ptr<MessageQueues> queues,
                                       shared_ptr<InternalFraming> frame,
                                       NotifyFn notify,
                                       bool force) {
      // Transfer as many full batches as possible
      bool transferred = false;
      while (batchAvailable() || force) {
        int n = batchSize();
        for (int i=0; i < n; i++) {
          if (force && empty()) { return transferred; }
          shared_ptr<Value> v = this->pop();
          if (queues && frame) {
            RemoteMessage rMsg = frame->read_message(*v);
            queues->enqueue(*(rMsg.toMessage()));
            transferred = true;
          }
          notify(v);
        }
      }
      return transferred;
    }

    void EndpointBindings::attachNotifier(EndpointNotification nt,
                                          Address sub_addr,
                                          TriggerId sub_id) {
      shared_ptr<Subscribers> s = eventSubscriptions[nt];
      if (!s) {
        s = shared_ptr<Subscribers>(new Subscribers());
        eventSubscriptions[nt] = s;
      }

      s->push_back(make_tuple(sub_addr, sub_id));
    }

    void EndpointBindings::detachNotifier(EndpointNotification nt,
                                          Address sub_addr,
                                          TriggerId sub_id) {
      auto it = eventSubscriptions.find(nt);
      if ( it != eventSubscriptions.end() ) {
        shared_ptr<Subscribers> s = it->second;
        if (s) {
          s->remove_if(
            [&sub_id, &sub_addr](const tuple<Address, TriggerId>& t){
              return get<0>(t) == sub_addr && get<1>(t) == sub_id;
            });
        }
      }
    }

    void EndpointBindings::notifyEvent(EndpointNotification nt, shared_ptr<Value> payload) {
      auto it = eventSubscriptions.find(nt);
      if (it != eventSubscriptions.end()) {
        shared_ptr<Subscribers> s = it->second;
        if (s) {
          for (tuple<Address, TriggerId> t : *s) {
            // TODO we need to add the source of the message (4th argument)
            sendFn(get<0>(t), get<1>(t), payload, defaultAddress);
          }
        }
      }
    }

    void Endpoint::notify_subscribers(shared_ptr<Value> v) {
      EndpointNotification nt =
        (handle_->builtin() || handle_->file())?
          EndpointNotification::FileData : EndpointNotification::SocketData;
      subscribers_->notifyEvent(nt, v);
    }

    Collection<R_elem<K3::base_string>> Endpoint::doReadBlock(int blockSize) {
      Collection<R_elem<K3::base_string>> r;
      shared_ptr<Value> v;
      if ( buffer_ ) {
        int i = blockSize;
        do {
          v = buffer_->refresh(handle_,
                std::bind(&Endpoint::notify_subscribers, this, std::placeholders::_1));
          if ( v ) {
            R_elem<K3::base_string> elem(K3::base_string(std::move(*v)));
            r.insert(std::move(elem));
          }
        } while ( --i > 0 && handle_->hasRead() );
      } else {
        // Read data directly from the underlying IOHandle.
        boost::lock_guard<boost::mutex> guard(mtx_);
        int i = blockSize;
        do {
          v = handle_->doRead();
          notify_subscribers(v); // Produce a notification for every tuple in a block read.
          if ( v ) {
            R_elem<K3::base_string> elem(K3::base_string(std::move(*v)));
            r.insert(std::move(elem));
          }
        } while ( --i > 0 && handle_->hasRead() );
      }
      return r;
    }

    shared_ptr<Value> Endpoint::refreshBuffer() {
      shared_ptr<Value> r;
      if ( buffer_ ) {
        r = buffer_->refresh(handle_,
              std::bind(&Endpoint::notify_subscribers, this, std::placeholders::_1));
      } else {
        // Read data directly from the underlying IOHandle.
        boost::lock_guard<boost::mutex> guard(mtx_);
        if (handle_->hasRead()) {
          r = handle_->doRead();
          notify_subscribers(r);
        }
      }
      return r;
    }

    void Endpoint::flushBuffer() {
      if ( buffer_ ) {
        buffer_->flush(handle_,
          std::bind(&Endpoint::notify_subscribers, this, std::placeholders::_1));
      }
    }

    void Endpoint::doWrite(shared_ptr<Value> v_ptr) {
      if ( buffer_ ) {
        bool success = buffer_->push_back(v_ptr);
        if ( !success ) {
          // Flush buffer, and then try to append again.
          flushBuffer();

          // Try to append again, and if this still fails, throw a buffering exception.
          success = buffer_->push_back(v_ptr);
        }

        if ( ! success )
          { throw BufferException("Failed to buffer value during endpoint write."); }
      } else {
        boost::lock_guard<boost::mutex> guard(mtx_);
        handle_->doWrite(v_ptr);
        notify_subscribers(v_ptr);
      }
    }

    bool Endpoint::do_push(shared_ptr<Value> val,
                           shared_ptr<MessageQueues> q,
                           shared_ptr<InternalFraming> codec) {

      // Skip the endpoint buffer.
      // deserialize and directly enqueue
      q->enqueue(*(codec->read_message(*val).toMessage()));
      return true;
    }

    // Closes the endpoint's IOHandle, while also notifying subscribers
    // of the close event.
    void Endpoint::close() {
      if(handle_->isOutput())
        flushBuffer();

      EndpointNotification nt = (handle_->builtin() || handle_->file())?
        EndpointNotification::FileClose : EndpointNotification::SocketClose;

      subscribers_->notifyEvent(nt, nullptr);
      handle_->close();
    };

    void EndpointState::clearEndpoints(shared_ptr<ConcurrentEndpointMap> m) {
      list<Identifier> endpoint_names;

      strict_lock<EndpointState> guard(*this);

      for (pair<Identifier, shared_ptr<Endpoint>> p: *(m->get(guard))) {
        endpoint_names.push_back(p.first);
      }

      for (Identifier i: endpoint_names) {
        removeEndpoint(i);
      }

      return;
    }

    shared_ptr<Endpoint> EndpointState::getInternalEndpoint(Identifier id) {
      if ( externalEndpointId(id) ) {
        epsLogger->logAt(trivial::error, "Invalid request for internal endpoint: "+id);
        return shared_ptr<Endpoint>();
      }
      return getEndpoint(id, internalEndpoints);
    }

    shared_ptr<Endpoint> EndpointState::getExternalEndpoint(Identifier id) {
      if ( !externalEndpointId(id) ) {
        epsLogger->logAt(trivial::error, "Invalid request for external endpoint: "+id);
        return shared_ptr<Endpoint>();
      }
      return getEndpoint(id, externalEndpoints);
    }

    size_t EndpointState::numEndpoints() {
      strict_lock<EndpointState> guard(*this);
      return externalEndpoints->get(guard)->size() + internalEndpoints->get(guard)->size();
    }

    void EndpointState::logEndpoints() {
      strict_lock<EndpointState> guard(*this);
      BOOST_LOG(*epsLogger) << "Internal Endpoints (" << internalEndpoints->get(guard)->size() << ") @ " << internalEndpoints << ":";
      for (const std::pair<Identifier, shared_ptr<Endpoint> >& something: *(internalEndpoints->get(guard)) )
      {
        BOOST_LOG(*epsLogger) << "\t" << something.first;
      }

      BOOST_LOG(*epsLogger) << "External Endpoints (" << externalEndpoints->get(guard)->size() << ") @ " << externalEndpoints << ":";
      for (const std::pair<Identifier, shared_ptr<Endpoint> >& something: *(externalEndpoints->get(guard)) )
      {
        BOOST_LOG(*epsLogger) << "\t" << something.first;
      }
    }

    bool ConnectionState::ConnectionMap::addConnection(Address& addr, shared_ptr<Net::NConnection> c) {
      bool r = false;
      auto lb = cache.lower_bound(addr);
      if ( lb == cache.end() || addr != lb->first ) {
        auto it = cache.insert(lb, make_pair(addr, c));
        r = it->first == addr;
      }
      else {
        BOOST_LOG(*this) << "Invalid attempt to add a duplicate endpoint for " << addressAsString(addr);
      }
      return r;
    }

    shared_ptr<Net::NConnection> ConnectionState::ConnectionMap::getConnection(Address& addr)
    {
      shared_ptr<Net::NConnection> r;
      try {
        r = cache.at(addr);
      } catch (const out_of_range& oor) {}
      if ( !r ) { BOOST_LOG(*this) << "No connection found for " << addressAsString(addr); }
      return r;
    }

    bool ConnectionState::hasInternalConnections() {
      bool r = false;
      if ( internalConnections ) {
        strict_lock<ConnectionState> guard(*this);
        r = internalConnections->get(guard)? true : false;
      }
      return r;
    }

    shared_ptr<Net::NConnection> ConnectionState::addConnection(Address addr, bool internal)
    {
      strict_lock<ConnectionState> guard(*this);
      shared_ptr<ConnectionState::ConcurrentConnectionMap> cMap =
        internal? internalConnections : externalConnections;

      shared_ptr<Net::NConnection> conn =
        shared_ptr<Net::NConnection>(new Net::NConnection(networkCtxt, addr));

      return (cMap && cMap->get(guard)->addConnection(addr, conn))? conn : shared_ptr<Net::NConnection>();
    }

    void ConnectionState::removeConnection(Address addr)
    {
      strict_lock<ConnectionState> guard(*this);
      shared_ptr<ConnectionState::ConcurrentConnectionMap> cMap = mapForAddress(addr, guard);
      if ( cMap ) {
        cMap->get(guard)->removeConnection(addr);
      } else {
        BOOST_LOG(*this) << "No connection to " << addressAsString(addr) << " found for removal";
      }
    }

    void ConnectionState::removeConnection(Address addr, bool internal)
    {
      strict_lock<ConnectionState> guard(*this);
      shared_ptr<ConnectionState::ConcurrentConnectionMap> cMap = internal? internalConnections : externalConnections;
      if ( cMap ) {
        cMap->get(guard)->removeConnection(addr);
      } else {
        BOOST_LOG(*this) << "No connection to " << addressAsString(addr) << " found for removal";
      }
    }

    // TODO: Ideally, this should be a shared lock.
    // TODO: investigate why does clang not like a shared_lock_guard here, while EndpointState::getEndpoint is fine.
    shared_ptr<Net::NConnection> ConnectionState::getConnection(Address addr)
    {
      strict_lock<ConnectionState> guard(*this);
      shared_ptr<ConnectionState::ConcurrentConnectionMap> cMap = mapForAddress(addr, guard);
      return getConnection(addr, cMap, guard);
    }

    // TODO: Ideally, this should be a shared lock.
    shared_ptr<Net::NConnection> ConnectionState::getConnection(Address addr, bool internal) {
      strict_lock<ConnectionState> guard(*this);
      shared_ptr<ConnectionState::ConcurrentConnectionMap> cMap = internal? internalConnections : externalConnections;
      return getConnection(addr, cMap, guard);
    }

    void ConnectionState::clearConnections() {
      strict_lock<ConnectionState> guard(*this);
      if ( internalConnections ) { internalConnections->get(guard)->clearConnections(); }
      if ( externalConnections ) { externalConnections->get(guard)->clearConnections(); }
    }

    void ConnectionState::clearConnections(bool internal) {
      strict_lock<ConnectionState> guard(*this);
      shared_ptr<ConnectionState::ConcurrentConnectionMap> cMap =
        internal ? internalConnections : externalConnections;
      if ( cMap ) { cMap->get(guard)->clearConnections(); }
    };

    size_t ConnectionState::numConnections() {
      strict_lock<ConnectionState> guard(*this);
      size_t r = 0;
      if ( internalConnections ) { r += internalConnections->get(guard)->size(); }
      if ( externalConnections ) { r += externalConnections->get(guard)->size(); }
      return r;
    }

    shared_ptr<ConnectionState::ConcurrentConnectionMap>
    ConnectionState::emptyConnectionMap(shared_ptr<Net::NContext> ctxt)
    {
      shared_ptr<ConnectionMap> mp(new ConnectionMap(ctxt));
      return shared_ptr<ConnectionState::ConcurrentConnectionMap>
        (new ConnectionState::ConcurrentConnectionMap(*this, mp));
    }

    // TODO: implement an alternative using a shared_lock_guard
    shared_ptr<ConnectionState::ConcurrentConnectionMap>
    ConnectionState::mapForAddress(Address& addr, strict_lock<ConnectionState>& guard)
    {
      shared_ptr<ConnectionState::ConcurrentConnectionMap> r;
      if ( internalConnections && getConnection(addr, internalConnections, guard) ) {
        r = internalConnections;
        if ( !r && externalConnections && getConnection(addr, externalConnections, guard) ) {
          r = externalConnections;
        }
      }
      return r;
    }

    // TODO: implement an alternative using a shared_lock_guard
    shared_ptr<Net::NConnection>
    ConnectionState::getConnection(Address& addr,
                  shared_ptr<ConnectionState::ConcurrentConnectionMap> connections,
                  strict_lock<ConnectionState>& guard)
    {
      shared_ptr<Net::NConnection> r;
      if ( connections ) { r = connections->get(guard)->getConnection(addr); }
      return r;
    }

}


