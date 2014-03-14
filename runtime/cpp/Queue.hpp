#ifndef K3_RUNTIME_QUEUE_H
#define K3_RUNTIME_QUEUE_H

#include <list>
#include <map>
#include <memory>
#include <queue>
#include <tuple>
#include <boost/lockfree/queue.hpp>
#include <boost/log/sources/record_ostream.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/lockable_adapter.hpp>
#include <boost/thread/externally_locked.hpp>
#include <runtime/cpp/Common.hpp>

namespace K3 {

  using namespace std;
  using namespace boost;

  template<typename T> using shared_ptr = std::shared_ptr<T>;
  using mutex = boost::mutex;

  //-------------
  // Queue types.

  template<typename Value>
  class MsgQueue {
  public:
    virtual bool push(Value& v) = 0;
    virtual bool pop(Value& v) = 0;
    virtual bool empty() = 0;
    virtual size_t size() = 0;
  };

  template<typename Value>
  class LockfreeMsgQueue : public MsgQueue<Value> {
  public:
    LockfreeMsgQueue() {}    
    bool empty()        { return queue.empty(); }
    bool push(Value& v) { ++qSize; return queue.push(v); }
    bool pop(Value& v)  { --qSize; return queue.pop(v); }
    size_t size()       { return qSize; }
  protected:
    boost::lockfree::queue<Value> queue;
    size_t qSize; // TODO: synchronize.
  };


  template<typename Value>
  class LockingMsgQueue
    : public MsgQueue<Value>, public basic_lockable_adapter<mutex>
  {
  public:
    typedef basic_lockable_adapter<mutex> qlockable;
    LockingMsgQueue () : qlockable(), queue(*this) {}

    bool empty() {
      strict_lock<LockingMsgQueue> guard(*this);
      return queue.get(guard).empty();
    }

    bool push(Value& v) {
      strict_lock<LockingMsgQueue> guard(*this);
      ++qSize;
      queue.get(guard).push(v);
      return true;
    }
    
    bool pop(Value& v) {
      strict_lock<LockingMsgQueue> guard(*this);
      bool r = false;
      if ( !queue.get(guard).empty() ) {
        --qSize;
        v = queue.get(guard).front();
        queue.get(guard).pop();
        r = true;
      }
      return r;
    }

    size_t size() { return qSize; }

  protected:
    externally_locked<queue<Value>, LockingMsgQueue> queue;
    size_t qSize; // TODO: synchronize
  };


  //------------------
  // Queue containers.

  class MessageQueues : public virtual LogMT {
  public:
    MessageQueues() : LogMT("queue") {}
    virtual void enqueue(Message m) = 0; // TODO: use a ref / rvalue ref to avoid copying
    virtual shared_ptr<Message> dequeue() = 0;
  };


  template<typename QueueIndex, typename Queue>
  class IndexedMessageQueues : public MessageQueues
  {
  public:
    IndexedMessageQueues() : LogMT("IndexedMessageQueues") {}

    // TODO: use a ref / rvalue ref to avoid copying
    void enqueue(Message m)
    {
      if ( validTarget(m) ) { enqueue(m, queue(m)); }
      else {
        BOOST_LOG(*this) << "Invalid message target: " << addressAsString(m.address()) << ":" << m.id();
      }
    }

    // TODO: fair queueing policy.
    shared_ptr<Message> dequeue()
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
    virtual bool validTarget(Message& m) = 0;
    
    virtual shared_ptr<Queue> queue(Message& m) = 0;
    virtual tuple<QueueIndex, shared_ptr<Queue> > nonEmptyQueue() = 0;

    virtual void enqueue(Message& m, shared_ptr<Queue> q) = 0;
    virtual shared_ptr<Message> dequeue(const tuple<QueueIndex, shared_ptr<Queue> >& q) = 0;
  };


  class SinglePeerQueue
    : public IndexedMessageQueues<Address, MsgQueue<tuple<Identifier, Value> > >
  {
  public:
    typedef Address QueueKey;
    //typedef LockfreeMsgQueue<tuple<Identifier, Value> > Queue;
    typedef LockingMsgQueue<tuple<Identifier, Value> > Queue;
    typedef tuple<QueueKey, shared_ptr<Queue> > PeerMessages;

    SinglePeerQueue() : LogMT("SinglePeerQueue") {}
    SinglePeerQueue(Address addr) : LogMT("SinglePeerQueue"), peerMsgs(addr, shared_ptr<Queue>(new Queue())) {}

    size_t size() { return get<1>(peerMsgs)->size(); }

  protected:
    typedef MsgQueue<tuple<Identifier, Value> > BaseQueue;
    PeerMessages peerMsgs;
    
    bool validTarget(Message& m) { return m.address() == get<0>(peerMsgs); }
    
    shared_ptr<BaseQueue> queue(Message& m) { return dynamic_pointer_cast<BaseQueue, Queue>(get<1>(peerMsgs)); }
    
    tuple<QueueKey, shared_ptr<BaseQueue> > nonEmptyQueue()
    {
      shared_ptr<Queue> r = get<1>(peerMsgs);
      shared_ptr<BaseQueue> br =
        r->empty()? shared_ptr<BaseQueue>() : dynamic_pointer_cast<BaseQueue, Queue>(r);
      return make_tuple(get<0>(peerMsgs), br);
    }

    void enqueue(Message& m, shared_ptr<BaseQueue> q)
    {
      tuple<Identifier, Value> entry = make_tuple(m.id(), m.contents());
      if ( !(q && q->push(entry)) ) {
        BOOST_LOG(*this) << "Invalid destination queue during enqueue";
      }
    }

    shared_ptr<Message> dequeue(const tuple<QueueKey, shared_ptr<BaseQueue> >& idxQ)
    {
      shared_ptr<Message>  r;
      tuple<Identifier, Value> entry;
      
      shared_ptr<BaseQueue> q = get<1>(idxQ);
      if ( q && q->pop(entry) ) {
        const Address& addr  = get<0>(idxQ);
        const Identifier& id = get<0>(entry);
        const Value& v       = get<1>(entry);        
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
    //typedef LockfreeMsgQueue<tuple<Identifier, Value> > Queue;
    typedef LockingMsgQueue<tuple<Identifier, Value> > Queue;
    typedef map<QueueKey, shared_ptr<Queue> > MultiPeerMessages;

    MultiPeerQueue() : LogMT("MultiPeerQueue") {}
    MultiPeerQueue(const list<Address>& addresses) : LogMT("MultiPeerQueue") {
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
    typedef MsgQueue<tuple<Identifier, Value> > BaseQueue;
    MultiPeerMessages multiPeerMsgs;
    
    bool validTarget(Message& m) {
      return multiPeerMsgs.find(m.address()) != multiPeerMsgs.end();
    }

    shared_ptr<BaseQueue> queue(Message& m) { 
      return dynamic_pointer_cast<BaseQueue, Queue>(multiPeerMsgs[m.address()]);
    }

    tuple<QueueKey, shared_ptr<BaseQueue> > nonEmptyQueue()
    {
      tuple<QueueKey, shared_ptr<BaseQueue> > r;

      MultiPeerMessages::iterator it =
        find_if(multiPeerMsgs.begin(), multiPeerMsgs.end(),
          [](MultiPeerMessages::value_type& x){ return x.second->empty(); });

      if ( it != multiPeerMsgs.end() ) { 
        r = make_tuple(it->first, dynamic_pointer_cast<BaseQueue, Queue>(it->second));
      }
      return r;
    }
    
    void enqueue(Message& m, shared_ptr<BaseQueue> q) 
    {
      tuple<Identifier, Value> entry = make_tuple(m.id(), m.contents());
      if ( !(q && q->push(entry)) ) {
        BOOST_LOG(*this) << "Invalid destination queue during enqueue";
      }
    }
    
    shared_ptr<Message> dequeue(const tuple<QueueKey, shared_ptr<BaseQueue> >& idxQ)
    {
      shared_ptr<Message> r;
      tuple<Identifier, Value> entry;
      
      shared_ptr<BaseQueue> q = get<1>(idxQ);
      if ( q && q->pop(entry) ) {
        const Address& addr  = get<0>(idxQ);
        const Identifier& id = get<0>(entry);
        const Value& v       = get<1>(entry);
        r = shared_ptr<Message >(new Message(addr, id, v));
      } else {
        BOOST_LOG(*this) << "Invalid source queue during dequeue";
      }
      return r;
    }
  };


  // TODO: for dynamic changes to the queues container, use a shared lock
  class MultiTriggerQueue
    : public IndexedMessageQueues<tuple<Address, Identifier>, MsgQueue<Value> >
  {
  public:
    typedef tuple<Address, Identifier> QueueKey;
    //typedef LockfreeMsgQueue<Value> Queue;
    typedef LockingMsgQueue<Value> Queue;
    typedef map<QueueKey, shared_ptr<Queue> > MultiTriggerMessages;

    MultiTriggerQueue() : LogMT("MultiTriggerQueue") {}

    MultiTriggerQueue(const list<Address>& addresses, const list<Identifier>& triggerIds)
      : LogMT("MultiTriggerQueue")
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
      for ( auto x : multiTriggerMsgs ) { r += x.second? x.second->size() : 0; }
      return r;
    }

  protected:
    typedef MsgQueue<Value> BaseQueue;
    MultiTriggerMessages multiTriggerMsgs;
    
    bool validTarget(Message& m) {
      return multiTriggerMsgs.find(make_tuple(m.address(), m.id())) != multiTriggerMsgs.end();
    }

    shared_ptr<BaseQueue> queue(Message& m) {
      return dynamic_pointer_cast<BaseQueue, Queue>(multiTriggerMsgs[make_tuple(m.address(), m.id())]);
    }

    tuple<QueueKey, shared_ptr<BaseQueue> > nonEmptyQueue()
    {
      tuple<QueueKey, shared_ptr<BaseQueue> > r;
    
      MultiTriggerMessages::iterator it =
        find_if(multiTriggerMsgs.begin(), multiTriggerMsgs.end(), 
          [](MultiTriggerMessages::value_type& x) { return x.second->empty(); });
      
      if ( it != multiTriggerMsgs.end() ) {
        r = make_tuple(it->first, dynamic_pointer_cast<BaseQueue, Queue>(it->second));
      }
      return r;      
    }

    void enqueue(Message& m, shared_ptr<BaseQueue> q) {
      if ( !(q && q->push(m.contents())) ) {
        BOOST_LOG(*this) << "Invalid destination queue during enqueue";
      }
    }
    
    shared_ptr<Message > dequeue(const tuple<QueueKey, shared_ptr<BaseQueue> >& idxQ)
    {
      shared_ptr<Message > r;
      Value entry;
      
      shared_ptr<BaseQueue> q = get<1>(idxQ);
      if ( q && q->pop(entry) ) {
        const Address& addr  = get<0>(get<0>(idxQ));
        const Identifier& id = get<1>(get<0>(idxQ));
        r = shared_ptr<Message >(new Message(addr, id, entry));
      } else {
        BOOST_LOG(*this) << "Invalid source queue during dequeue";
      }
      return r;
    }    
  };


  //--------------------
  // Queue constructors.

  shared_ptr<MessageQueues> simpleQueues(Address addr)
  {
    return shared_ptr<MessageQueues>(new SinglePeerQueue(addr));
  }

  shared_ptr<MessageQueues> perPeerQueues(const list<Address>& addresses)
  {
    return shared_ptr<MessageQueues>(new MultiPeerQueue(addresses));
  }

  shared_ptr<MessageQueues>
  perTriggerQueues(const list<Address>& addresses, const list<Identifier>& triggerIds)
  {
    return shared_ptr<MessageQueues>(new MultiTriggerQueue(addresses, triggerIds));
  }
}

#endif
