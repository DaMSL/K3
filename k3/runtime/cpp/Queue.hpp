#ifndef K3_RUNTIME_QUEUE_H
#define K3_RUNTIME_QUEUE_H

#include <list>
#include <map>
#include <tuple>
#include <boost/shared_ptr.hpp>
#include <k3/runtime/Common.hpp>

namespace K3 {

  using namespace std;
  using namespace boost;

  //------------
  // Queues
  template<typename Value, typename QueueIndex, typename Queue>
  class MessageQueues {
  public:
    MessageQueues() {}

    void enqueue(Message<Value> m)
    {
      if ( validTarget(m) ) { enqueue(m, queue(m)); }
      else {
        // Error
      }
    }

    // TODO: fair queueing policy.
    shared_ptr<Message<Value> > dequeue()
    {
      shared_ptr<Message<Value> > r;
      tuple<QueueIndex, shared_ptr<Queue> > idxQ = nonEmptyQueue();
      if ( get<1>(idxQ) ) { r = dequeue(idxQ); }
      else {
        // Error
      }
      return r;
    }

    virtual void enqueue() = 0;
    virtual shared_ptr<Message<Value> > dequeue() = 0;

    virtual size_t size() = 0;

  protected:
    bool validTarget(Message<Value>& m) = 0;
    
    shared_ptr<Queue> queue(Message<Value>& m) = 0;
    tuple<QueueIndex, shared_ptr<Queue> > nonEmptyQueue() = 0;

    void enqueue(Message<Value>& m, shared_ptr<Queue> q);
    shared_ptr<Message<Value> > dequeue(tuple<QueueIndex, shared_ptr<Queue> > q) = 0;
  };


  template<typename Value>
  class SinglePeerQueue
    : public MessageQueues<Value, Address, list<tuple<Identifier, Value> > >
  {
  public:
    typedef Address QueueKey;
    typedef list<tuple<Identifier, Value> > Queue;
    typedef tuple<QueueKey, shared_ptr<Queue> > PeerMessages;

    SinglePeerQueue() {}
    SinglePeerQueue(Address addr) : peerMsgs(addr, shared_ptr<Queue>(new Queue())) {}

    size_t size() { return peerMsgs.size(); }

  protected:
    PeerMessages peerMsgs;
    
    bool validTarget(Message<Value>& m) { return m.address() == get<0>(peerMsgs); }
    
    shared_ptr<Queue> queue(Message<Value>& m) { return get<1>(peerMsgs); }
    
    tuple<QueueKey, shared_ptr<Queue> > nonEmptyQueue() {
      shared_ptr<Queue> r = get<1>(peerMsgs);
      return make_tuple(get<0>(peerMsgs), r->empty()? shared_ptr<Queue>() : r);
    }

    void enqueue(Message<Value>& m, shared_ptr<Queue> q) {
      if ( q ) { q->insert(make_tuple(m.id(), m.contents())); }
      else {
        // Error
      }      
    }

    shared_ptr<Message<Value> > dequeue(tuple<QueueKey, shared_ptr<Queue> > idxQ) {
      shared_ptr<Message<Value> > r;
      shared_ptr<Queue> q = get<1>(idxQ);
      if ( q && !q->empty() ) {
        tuple<Identifier, Value>& entry = q->front();
        r = shared_ptr<Message<Value> >(
              new Message<Value>(get<0>(idxQ), get<0>(entry), get<1>(entry)));
        q->pop_front();
      }
      else {
        // Error
      }
      return r;
    }
  };


  template<typename Value>
  class MultiPeerQueue
    : public MessageQueues<Value, Address, list<tuple<Identifier, Value> > >
  {
  public:
    typedef Address QueueKey;
    typedef list<tuple<Identifier, Value> > Queue;
    typedef map<QueueKey, shared_ptr<Queue> > MultiPeerMessages;

    MultiPeerQueue() {}
    MultiPeerQueue(list<Address>& addresses) {
      list<Address>::iterator it = addresses.begin();
      list<Address>::iterator end = addresses.end();
      for (; it != end; ++it) {
        multiPeerMsgs.insert(make_pair(*it, shared_ptr<Queue>(new Queue)));
      }
    }

    size_t size() {
      size_t r = 0;
      for_each(multiPeerMsgs, r += (arg1->second ? arg1->second->size() : 0));
      return r;
    }

  protected:
    MultiPeerMessages multiPeerMsgs;
    
    bool validTarget(Message<Value>& m) {
      return multiPeerMsgs.find(m.address()) != multiPeerMsgs.end();
    }

    shared_ptr<Queue> queue(Message<Value>& m) { return multiPeerMsgs[m.address()]; }

    tuple<QueueKey, shared_ptr<Queue> >& nonEmptyQueue()
    {
      tuple<QueueKey, shared_ptr<Queue> > r;
      MultiPeerMessages::iterator it =
        find_if(multiPeerMsgs.begin(), multiPeerMsgs.end(), mem_fun(&Queue::empty));
      if ( it != multiPeerMsgs.end() ) { r = make_tuple(it->first, it->second); }
      return r;
    }
    
    void enqueue(Message<Value>& m, shared_ptr<Queue> q) {
      if ( q ) { q->insert(make_tuple(m.id(), m.contents())); }
      else {
        // Error
      }
    }
    
    shared_ptr<Message<Value> > dequeue(tuple<QueueKey, shared_ptr<Queue> > idxQ) {
      shared_ptr<Message<Value> > r;
      shared_ptr<Queue> q = get<1>(idxQ);
      if ( q && !q->empty() ) {
        tuple<Identifier, Value>& entry = q->front();
        Address& addr  = get<0>(idxQ);
        Identifier& id = get<0>(entry);
        Value& v       = get<1>(entry);
        r = shared_ptr<Message<Value> >(new Message<Value>(addr, id, v));
        q->pop_front();
      } else {
        // Error
      }
      return r;
    }
  };


  template<typename Value>
  class MultiTriggerQueue
    : public MessageQueues<Value, tuple<Address, Identifier>, list<Value> >
  {
  public:
    typedef tuple<Address, Identifier> QueueKey;
    typedef list<Value> Queue;
    typedef map<QueueKey, Queue> MultiTriggerMessages;

    MultiTriggerMessages() {}

    MultiTriggerMessages(list<Address>& addresses, list<Identifier>& triggerIds) {
      for_each(addresses, lambda(_addr = arg1)[
        for_each(triggerIds, lambda(_id = arg1)[
          multiTriggerMsgs.insert(
            make_pair(make_tuple(_addr, _id), shared_ptr<Queue>(new Queue())))])])
    }

    size_t size() {
      size_t r = 0;
      for_each(multiTriggerMsgs, r += (arg1->second ? arg1->second->size() : 0));
      return r;
    }

  protected:
    MultiTriggerMessages multiTriggerMsgs;
    
    bool validTarget(Message<Value>& m) {
      return multiTriggerMsgs.find(make_tuple(m.address(), m.id())) != multiTriggerMsgs.end();
    }

    shared_ptr<Queue> queue(Message<Value>& m) {
      return multiTriggerMsgs[make_tuple(m.address(), m.id())];
    }

    tuple<QueueKey, shared_ptr<Queue> > nonEmptyQueue() {
      tuple<QueueKey, shared_ptr<Queue> > r;
      MultiTriggerMessages::iterator it =
        find_if(multiTriggerMsgs.begin(), multiTriggerMsgs.end(), mem_fun(&Queue::empty));
      if ( it != multiTriggerMsgs.end() ) { r = make_tuple(it->first, it->second); }
      return r;      
    }

    void enqueue(Message<Value>& m, shared_ptr<Queue> q) {
      if ( q ) { q->insert(m.contents()); }
      else {
        // Error
      }
    }
    
    shared_ptr<Message<Value> > dequeue(tuple<QueueKey, shared_ptr<Queue> > idxQ) {
      shared_ptr<Message<Value> > r;
      shared_ptr<Queue> q = get<1>(idxQ);
      if ( q && !q->empty() ) {
        Address& addr  = get<0>(get<0>(idxQ));
        Identifier& id = get<1>(get<0>(idxQ));
        Value& entry   = q->front();
        r = shared_ptr<Message<Value> >(new Message<Value>(addr, id, entry));
        q->pop_front();
      } else {
        // Error
      }
      return r;
    }    
  };


  //--------------------
  // Queue constructors.

  template<typename Value>
  shared_ptr<MessageQueues<Value> > simpleQueues(Address addr)
  {
    return shared_ptr<MessageQueues<Value> >(new SinglePeerQueue(addr));
  }

  template<typename Value>
  shared_ptr<MessageQueues<Value> > perPeerQueues(list<Address> addresses)
  {
    return shared_ptr<MessageQueues<Value> >(new MultiPeerMessages(addresses));
  }

  template<typename Value>
  shared_ptr<MessageQueues<Value> > perTriggerQueues(list<Address> addresses, list<Identifier> triggerIds)
  {
    return shared_ptr<MessageQueues<Value> >(new MultiTriggerMessages(addresses, triggerIds));
  }
}

#endif