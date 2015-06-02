#include "Endpoint.hpp"

using namespace boost::log;
using namespace boost;
using std::out_of_range;

namespace K3
{
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

    void EndpointBindings::notifyEvent(EndpointNotification nt, shared_ptr<string> payload) {
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

    // --------------------------
    // Endpoint methods
    //

    void Endpoint::notify_subscribers(shared_ptr<string> v) {
      EndpointNotification nt =
        (handle_->builtin() || handle_->file())?
          EndpointNotification::FileData : EndpointNotification::SocketData;
      subscribers_->notifyEvent(nt, v);
    }

    shared_ptr<string> Endpoint::refreshBuffer() {
      shared_ptr<string> r;
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

    bool Endpoint::do_push(shared_ptr<string> val,
                           shared_ptr<const MessageQueues> q,
                           shared_ptr<MessageCodec> frame) {

      // Skip the endpoint buffer.
      // deserialize and directly enqueue
      q->enqueue(*(frame->decode(*val)->toMessage()));
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

    // --------------------------
    // Internal endpoint methods
    //

    Collection<R_elem<RemoteMessage>> InternalEndpoint::doReadBlock(int blockSize) {
      Collection<R_elem<RemoteMessage>> r;
      shared_ptr<string> v;
      if ( msgcodec_ ) {
        if ( buffer_ ) {
          int i = blockSize;
          do {
            v = buffer_->refresh(handle_,
                  std::bind(&Endpoint::notify_subscribers, this, std::placeholders::_1));
            if ( v ) {
              shared_ptr<RemoteMessage> outv = msgcodec_->decode(*v);
              if ( outv ) {
                R_elem<RemoteMessage> elem(*outv);
                r.insert(std::move(elem));
              }
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
              shared_ptr<RemoteMessage> outv = msgcodec_->decode(*v);
              if ( outv ) {
                R_elem<RemoteMessage> elem(*outv);
                r.insert(std::move(elem));
              }
            }
          } while ( --i > 0 && handle_->hasRead() );
        }
      }
      return r;
    }

    void InternalEndpoint::doWrite(const RemoteMessage& v) {
      if ( msgcodec_ ) {
        shared_ptr<string> bufv = make_shared<string>(std::move(msgcodec_->encode(v)));
        if ( buffer_ ) {
          bool success = buffer_->push_back(bufv);
          if ( !success ) {
            // Flush buffer, and then try to append again.
            flushBuffer();

            // Try to append again, and if this still fails, throw a buffering exception.
            success = buffer_->push_back(bufv);
          }

          if ( ! success )
            { throw BufferException("Failed to buffer value during endpoint write."); }
        } else {
          boost::lock_guard<boost::mutex> guard(mtx_);
          handle_->doWrite(bufv);
          notify_subscribers(bufv);
        }
      }
    }


    // ----------------------------
    // EndpointState methods
    //

    void EndpointState::clearEndpoints(shared_ptr<ConcurrentEndpointMap> m) {
      list<Identifier> endpoint_names;

      strict_lock<EndpointState> guard(*this);

      for (pair<Identifier, shared_ptr<Endpoint>> p: *(m->get(guard))) {
        endpoint_names.push_back(p.first);
      }

      for (Identifier i: endpoint_names) {
        removeEndpoint(i);
      }
    }

    shared_ptr<InternalEndpoint> EndpointState::getInternalEndpoint(Identifier id) {
      if ( externalEndpointId(id) ) {
        epsLogger->logAt(trivial::error, "Invalid request for internal endpoint: "+id);
        return shared_ptr<InternalEndpoint>();
      }
      return dynamic_pointer_cast<InternalEndpoint, Endpoint>(getEndpoint(id, internalEndpoints));
    }

    shared_ptr<ExternalEndpoint> EndpointState::getExternalEndpoint(Identifier id) {
      if ( !externalEndpointId(id) ) {
        epsLogger->logAt(trivial::error, "Invalid request for external endpoint: "+id);
        return shared_ptr<ExternalEndpoint>();
      }
      return dynamic_pointer_cast<ExternalEndpoint, Endpoint>(getEndpoint(id, externalEndpoints));
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


