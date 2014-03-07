#ifndef K3_RUNTIME_QUEUE_H
#define K3_RUNTIME_QUEUE_H

#include <list>
#include <map>
#include <memory>
#include <tuple>
#include <boost/lockfree/queue.hpp>
#include <boost/log/sources/record_ostream.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/lockable_adapter.hpp>
#include <boost/thread/externally_locked.hpp>
#include <runtime/cpp/Common.hpp>

namespace K3 {

  using namespace std;
  using namespace boost::lockfree;
  using boost::strict_lock;
  using boost::basic_lockable_adapter;
  using boost::externally_locked;
  using boost::mutex;

  //-------------
  // Queue types.

  class MsgQueue {
  public:
    virtual bool push(Message& v) = 0;
    virtual bool pop(Message& v) = 0;
    virtual bool empty() = 0;
  };

  class LockfreeMsgQueue : public MsgQueue {
  public:
    LockfreeMsgQueue() {}    
    bool empty()        { return queue.empty(); }
    bool push(Message& v) { return queue.push(v); }
    bool pop(Message& v)  { return queue.pop(v); }
  protected:
    boost::lockfree::queue<Message> queue;
  };

  class LockingMsgQueue
    : public MsgQueue, public basic_lockable_adapter<mutex>
  {
  public:
    typedef basic_lockable_adapter<mutex> qlockable;
    LockingMsgQueue () : qlockable(), queue(*this) {}

    bool empty() {
      strict_lock<LockingMsgQueue> guard(*this);
      return queue.get(guard).empty();
    }

    bool push(Message& v) {
      strict_lock<LockingMsgQueue> guard(*this);
      queue.get(guard).push(v);
      return true;
    }
    
    bool pop(Message& v) {
      strict_lock<LockingMsgQueue> guard(*this);
      bool r = false;
      if ( !queue.get(guard).empty() ) {
        v = queue.get(guard).front();
        queue.get(guard).pop();
        r = true;
      }
      return r;
    }

  protected:
    externally_locked<queue<Message>, LockingMsgQueue> queue;
  };

  //------------------
  // Queue containers.

  class MessageQueues : public virtual LogMT {
  public:
    MessageQueues() : LogMT("queue") {}
    virtual void enqueue(Message m) = 0;
    virtual shared_ptr<Message> dequeue() = 0;
  };

  template<typename QueueIndex, typename Queue>
  class IndexedMessageQueues : public MessageQueues 
  {
  public:
    IndexedMessageQueues() {}

    // TODO: use a ref / rvalue ref to avoid copying
    void enqueue(Message m)
    {
      if ( validTarget(m) ) { enqueue(m, queue(m)); }
      else {
        BOOST_LOG(*this) << "Invalid message target: " << m.address() << ":" << m.id();
      }
    }

    // TODO: fair queueing policy.
    shared_ptr<Message>  dequeue()
    {
      shared_ptr<Message> r;
      tuple<QueueIndex, shared_ptr<Queue> > idxQ = nonEmptyQueue();
      if ( get<1>(idxQ) ) { r = dequeue(idxQ); }
      else {
        BOOST_LOG(*this) << "Invalid non-empty queue on dequeue";
      }
      return r;
    }

    virtual size_t size() = 0;

  protected:
    bool validTarget(Message& m) = 0;
    
    shared_ptr<Queue> queue(Message& m) = 0;
    tuple<QueueIndex, shared_ptr<Queue> > nonEmptyQueue() = 0;

    void enqueue(Message& m, shared_ptr<Queue> q);
    shared_ptr<Message>  dequeue(const tuple<QueueIndex, shared_ptr<Queue> >& q) = 0;
  };


  class SinglePeerQueue
     public IndexedMessageQueues<Address, MsgQueue<tuple<Identifier, Value> > >
  {
  public:
    typedef Address QueueKey;
    typedef LockfreeMsgQueue<tuple<Identifier, Value> > Queue;
    typedef tuple<QueueKey, shared_ptr<Queue> > PeerMessages;

    SinglePeerQueue() {}
    SinglePeerQueue(Address addr) : peerMsgs(addr, shared_ptr<Queue>(new Queue())) {}

    size_t size() { return peerMsgs.size(); }

  protected:
    PeerMessages peerMsgs;
    
    bool validTarget(Message& m) { return m.address() == get<0>(peerMsgs); }
    
    shared_ptr<Queue> queue(Message& m) { return get<1>(peerMsgs); }
    
    tuple<QueueKey, shared_ptr<Queue> > nonEmptyQueue()
    {
      shared_ptr<Queue> r = get<1>(peerMsgs);
      return make_tuple(get<0>(peerMsgs), r->empty()? shared_ptr<Queue>() : r);
    }

    void enqueue(Message& m, shared_ptr<Queue> q)
    {
      if ( !(q && q->push(make_tuple(m.id(), m.contents()))) ) {
        BOOST_LOG(*this) << "Invalid destination queue during enqueue";
      }
    }

    shared_ptr<Message> dequeue(const tuple<QueueKey, shared_ptr<Queue> >& idxQ)
    {
      shared_ptr<Message>  r;
      tuple<Identifier, Value> entry;
      
      shared_ptr<Queue> q = get<1>(idxQ);
      if ( q && q->pop(entry) ) {
        Address& addr  = get<0>(idxQ);
        Identifier& id = get<0>(entry);
        Value& v       = get<1>(entry);        
        r = shared_ptr<Message>(new Message(addr, id, v));
      } else {
        BOOST_LOG(*this) << "Invalid source queue during dequeue";
      }
      return r;
    }
  };

  // TODO: for dynamic changes to the queues container, use a shared lock
  class MultiPeerQueue
    : public IndexedMessageQueues<Address, MsgQueue<tuple<Identifier, Value> > >
  {
  public:
    typedef Address QueueKey;
    typedef LockfreeMsgQueue<tuple<Identifier, Value> > Queue;
    typedef map<QueueKey, shared_ptr<Queue> > MultiPeerMessages;

    MultiPeerQueue() {}
    MultiPeerQueue(const list<Address>& addresses) {
      for ( auto x : addresses ) {
        multiPeerMsgs.insert(make_pair(x, shared_ptr<Queue>(new Queue())));
      }
    }

    size_t size() {
      size_t r = 0;
      for ( auto x : multiPeerMsgs ) { r += x.second? x.second->size() : 0; }
      return r;
    }

  protected:
    MultiPeerMessages multiPeerMsgs;
    
    bool validTarget(Message& m) {
      return multiPeerMsgs.find(m.address()) != multiPeerMsgs.end();
    }

    shared_ptr<Queue> queue(Message& m) { return multiPeerMsgs[m.address()]; }

    tuple<QueueKey, shared_ptr<Queue> >& nonEmptyQueue()
    {
      tuple<QueueKey, shared_ptr<Queue> > r;

      typename MultiPeerMessages::iterator it =
        find_if(multiPeerMsgs.begin(), multiPeerMsgs.end(),
          [](typename MultiPeerMessages::value_type& x){ return x.second.empty(); });

      if ( it != multiPeerMsgs.end() ) { r = make_tuple(it->first, it->second); }
      return r;
    }
    
    void enqueue(Message m, shared_ptr<Queue> q) {
      if ( !(q && q->push(make_tuple(m.id(), m.contents()))) ) {
        BOOST_LOG(*this) << "Invalid destination queue during enqueue";
      }
    }
    
    shared_ptr<Message> dequeue(const tuple<QueueKey, shared_ptr<Queue> >& idxQ)
    {
      shared_ptr<Message> r;
      tuple<Identifier, Value> entry;
      
      shared_ptr<Queue> q = get<1>(idxQ);
      if ( q && q->pop() ) {
        Address& addr  = get<0>(idxQ);
        Identifier& id = get<0>(entry);
        Value& v       = get<1>(entry);
        r = shared_ptr<Message >(new Message(addr, id, v));
      } else {
        BOOST_LOG(*this) << "Invalid source queue during dequeue";
      }
      return r;
    }
  };


  // TODO: for dynamic changes to the queues container, use a shared lock
  template<typename Value>
  class MultiTriggerQueue
    : public IndexedMessageQueues<Value, tuple<Address, Identifier>, MsgQueue >
  {
  public:
    typedef tuple<Address, Identifier> QueueKey;
    typedef LockfreeMsgQueue<Value> Queue;
    typedef map<QueueKey, Queue> MultiTriggerMessages;

    MultiTriggerQueue() {}

    MultiTriggerQueue(const list<Address>& addresses, const list<Identifier>& triggerIds)
    {
      for ( auto addr : addresses ) {
        for ( auto id : triggerIds ) {
          multiTriggerMsgs.insert(
            make_pair(make_tuple(addr, id), shared_ptr<Queue>(new Queue())));
        }
      }
    }

    size_t size() {
      size_t r = 0;
      for ( auto x : multiTriggerMsgs ) { r += x.second? x.second.size() : 0; }
      return r;
    }

  protected:
    MultiTriggerMessages multiTriggerMsgs;
    
    bool validTarget(Message& m) {
      return multiTriggerMsgs.find(make_tuple(m.address(), m.id())) != multiTriggerMsgs.end();
    }

    shared_ptr<Queue> queue(Message& m) {
      return multiTriggerMsgs[make_tuple(m.address(), m.id())];
    }

    tuple<QueueKey, shared_ptr<Queue> > nonEmptyQueue()
    {
      tuple<QueueKey, shared_ptr<Queue> > r;
    
      typename MultiTriggerMessages::iterator it =
        find_if(multiTriggerMsgs.begin(), multiTriggerMsgs.end(), 
          [](typename MultiTriggerMessages::value_type& x) { return x.second.empty(); });
      
      if ( it != multiTriggerMsgs.end() ) { r = make_tuple(it->first, it->second); }
      return r;      
    }

    void enqueue(Message& m, shared_ptr<Queue> q) {
      if ( !(q && q->push(m.contents())) ) {
        BOOST_LOG(*this) << "Invalid destination queue during enqueue";
      }
    }
    
    shared_ptr<Message > dequeue(const tuple<QueueKey, shared_ptr<Queue> >& idxQ)
    {
      shared_ptr<Message > r;
      Value entry;
      
      shared_ptr<Queue> q = get<1>(idxQ);
      if ( q && q->pop(entry) ) {
        Address& addr  = get<0>(get<0>(idxQ));
        Identifier& id = get<1>(get<0>(idxQ));
        r = shared_ptr<Message >(new Message(addr, id, entry));
      } else {
        BOOST_LOG(*this) << "Invalid source queue during dequeue";
      }
      return r;
    }    
  };


  //--------------------
  // Queue constructors.

  template<typename Value>
  shared_ptr<MessageQueues<Message > > simpleQueues(Address addr)
  {
    return shared_ptr<MessageQueues<Message > >(new SinglePeerQueue<Value>(addr));
  }

  template<typename Value>
  shared_ptr<MessageQueues<Message > > perPeerQueues(const list<Address>& addresses)
  {
    return shared_ptr<MessageQueues<Message > >(new MultiPeerQueue<Value>(addresses));
  }

  template<typename Value>
  shared_ptr<MessageQueues<Message > >
  perTriggerQueues(const list<Address>& addresses, const list<Identifier>& triggerIds)
  {
    return shared_ptr<MessageQueues<Message > >(new MultiTriggerQueue<Value>(addresses, triggerIds));
  }
}

#endif
