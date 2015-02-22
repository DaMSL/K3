#ifndef K3_RUNTIME_QUEUE_H
#define K3_RUNTIME_QUEUE_H

#include <list>
#include <map>
#include <queue>
#include <tuple>
#include <boost/lockfree/queue.hpp>
#include <boost/log/sources/record_ostream.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/lockable_adapter.hpp>
#include <boost/thread/externally_locked.hpp>

#include "Common.hpp"
#include "Message.hpp"

using mutex = boost::mutex;
using std::dynamic_pointer_cast;
namespace K3 {

  //-------------
  // Queue types.

  class MsgQueue
    : public boost::basic_lockable_adapter<mutex> {
  public:
    typedef boost::basic_lockable_adapter<mutex> qlockable;
    MsgQueue () : qlockable(), queue(*this) {}

    bool empty() {
      boost::strict_lock<MsgQueue> guard(*this);
      return queue.get(guard).empty();
    }

    bool push(const Message& v) {
      boost::strict_lock<MsgQueue> guard(*this);
      ++qSize;
      queue.get(guard).push(v);
      return true;
    }

    shared_ptr<Message> pop() {
      boost::strict_lock<MsgQueue> guard(*this);
      shared_ptr<Message> r;
      if ( !queue.get(guard).empty() ) {
        --qSize;
	// TODO std::move or store pointers in queue
        r = make_shared<Message>(queue.get(guard).front());
        queue.get(guard).pop();
      }
      return r;
    }

    size_t size() { return qSize; }

  protected:
    boost::externally_locked<std::queue<Message>, MsgQueue> queue;
    size_t qSize; // TODO: synchronize
  };


  // TODO: r-ref overload for enqueue
  // TODO: for dynamic changes to the queues container, use a shared lock
  class MessageQueues
    : public virtual LogMT 
  {
  public:
    typedef map<Address, shared_ptr<MsgQueue> > MultiPeerMessages;

    MessageQueues() : LogMT("MessageQueues") {}
    MessageQueues(const list<Address>& addresses) : LogMT("MessageQueues") {
      for (auto x : addresses) {
        addQueue(x);
      }
    }

    void addQueue(const Address& a) {
        multiPeerMsgs.insert(make_pair(a, shared_ptr<MsgQueue>(new MsgQueue())));
    }
    
    void enqueue(Message& m)
    {
      if (validTarget(m)) { enqueue(m, queue(m.address())); }
      else {
        BOOST_LOG(*this) << "Invalid message target:" << m.id() << "@" << K3::addressAsString(m.address());
      }
    }
    
    shared_ptr<Message> dequeue(const Address& a)
    {
      shared_ptr<MsgQueue> q = queue(a);

      // upon failure: return nullptr (Nothing)
      return q->pop();
    }

    bool isLocal(const Address& a) {
      return queue(a) ? true : false;
    }

    size_t size(const Address& a) {
      size_t r = 0;
      shared_ptr<MsgQueue> q = queue(a);
      return q ? q->size() : 0;
    }

  protected:
    MultiPeerMessages multiPeerMsgs;

    bool validTarget(const Message& m) const {
      return multiPeerMsgs.find(m.address()) != multiPeerMsgs.end();
    }

    shared_ptr<MsgQueue> queue(const Address& m) {
      shared_ptr<MsgQueue> bqueue = multiPeerMsgs[m];
      return bqueue;
    }

    void enqueue(Message& m, shared_ptr<MsgQueue> q)
    {
      if (q) {
        q->push(m);
      }
      else { 
        BOOST_LOG(*this) << "Invalid destination queue during enqueue";
      }
    }
  };


  //--------------------
  // Queue constructors.

  static inline shared_ptr<MessageQueues> perPeerQueues(const list<Address>& addresses)
  {
    return shared_ptr<MessageQueues>(new MessageQueues(addresses));
  }

}

#endif
