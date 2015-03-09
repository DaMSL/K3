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
    MsgQueue () : qlockable(), queue(*this) {
      msgAvailMutex     = shared_ptr<boost::mutex>(new boost::mutex());
      msgAvailCondition = shared_ptr<boost::condition_variable>(new boost::condition_variable());

    }

    bool empty() {
      boost::strict_lock<MsgQueue> guard(*this);
      return queue.get(guard).empty();
    }

    bool push(const Message& v) {
      boost::strict_lock<MsgQueue> guard(*this);
      queue.get(guard).push(v);
      return true;
    }

    shared_ptr<Message> pop() {
      boost::strict_lock<MsgQueue> guard(*this);
      shared_ptr<Message> r;
      if ( !queue.get(guard).empty() ) {
	// TODO std::move or store pointers in queue
        r = make_shared<Message>(queue.get(guard).front());
        queue.get(guard).pop();
      }
      return r;
    }

    // Wait for a notification that the engine associated
    // with this control object has queued messages.
    template <class Predicate>
    void waitForMessage(Predicate pred) {
      if (msgAvailMutex && msgAvailCondition) {
        boost::unique_lock<boost::mutex> lock(*msgAvailMutex);
        while (pred()) { msgAvailCondition->wait(lock); }
      }
    }

    void messageAvail() {
      msgAvailCondition->notify_one();
    }

  protected:
    boost::externally_locked<std::queue<Message>, MsgQueue> queue;
    shared_ptr<boost::mutex> msgAvailMutex;
    shared_ptr<boost::condition_variable> msgAvailCondition;

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

    bool empty(const Address& a) {
      shared_ptr<MsgQueue> q = queue(a);
      if (q) {
        return q->empty();
      }
      fail(a);
      return false;
    }

    template <class Predicate>
    void waitForMessage(const Address& a, Predicate pred) {
      shared_ptr<MsgQueue> q = queue(a);
      if (q) {
        q->waitForMessage(pred);
      }
      fail(a);
    }

    void messageAvail(const Address& a) {
      shared_ptr<MsgQueue> q = queue(a);
      if (q) {
        q->messageAvail();
      }
      fail(a);
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
        q->messageAvail();
      }
      fail(m.address());
    }

    void fail(const Address& a) {
       throw std::runtime_error("Failed to find queue for address" + addressAsString(a));
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
