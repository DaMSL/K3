#ifndef K3_RUNTIME_QUEUE_H
#define K3_RUNTIME_QUEUE_H

#include <list>
#include <map>
#include <queue>
#include <tuple>

#include <boost/lockfree/queue.hpp>
#include <boost/log/sources/record_ostream.hpp>
#include <boost/thread/recursive_mutex.hpp>
#include <boost/thread/lockable_adapter.hpp>
#include <boost/thread/externally_locked.hpp>

#include "Common.hpp"
#include "Message.hpp"

using mutex = boost::recursive_mutex;
using std::dynamic_pointer_cast;
namespace K3 {

  class MsgQueue : public boost::basic_lockable_adapter<mutex> {
   public:
    typedef boost::basic_lockable_adapter<mutex> qlockable;
    MsgQueue () : qlockable(), queue(*this) {
      msgAvailCondition = shared_ptr<boost::condition_variable_any>(new boost::condition_variable_any());
    }

    bool empty() {
      boost::strict_lock<MsgQueue> guard(*this);
      return queue.get(guard).empty();
    }

    bool push(const Message& v) {
      boost::strict_lock<MsgQueue> guard(*this);
      queue.get(guard).push(v);
      msgAvailCondition->notify_one();
      return true;
    }

    shared_ptr<Message> pop() {
      boost::strict_lock<MsgQueue> guard(*this);
      shared_ptr<Message> r;
      if ( !queue.get(guard).empty() ) {
        r = make_shared<Message>(queue.get(guard).front());
        queue.get(guard).pop();
      }
      return r;
    }

    template <class Predicate>
    void waitForMessage(Predicate pred) {
      if ( msgAvailCondition ) {
        boost::unique_lock<MsgQueue> lock(*this);
        while ( pred() ) {
	  msgAvailCondition->wait(lock);
	}
      }
    }

    void messageAvail() {
      boost::strict_lock<MsgQueue> guard(*this);
      msgAvailCondition->notify_one();
    }

  protected:
    boost::externally_locked<std::queue<Message>, MsgQueue> queue;
    shared_ptr<boost::condition_variable_any> msgAvailCondition;
  };

  class MessageQueues : public virtual LogMT {
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

    void enqueue(const Message& m) const {
      if ( validTarget(m) ) {
        enqueue(m, queue(m.address()));
      }
      else {
	fail(m.address());
      }
    }

    shared_ptr<Message> dequeue(const Address& a) const
    {
      shared_ptr<MsgQueue> q = queue(a);
      return q->pop();
    }

    bool isLocal(const Address& a) const {
      return queue(a) ? true : false;
    }

    bool empty(const Address& a) const {
      shared_ptr<MsgQueue> q = queue(a);
      if (q) {
        return q->empty();
      }
      fail(a);
      return false;
    }

    template <class Predicate>
    void waitForMessage(const Address& a, Predicate pred) const {
      shared_ptr<MsgQueue> q = queue(a);
      if (q) {
        q->waitForMessage(pred);
      }
      else {
        fail(a);
      }
    }

    void messageAvail(const Address& a) const {
      shared_ptr<MsgQueue> q = queue(a);
      if (q) {
        q->messageAvail();
      }
      else {
        fail(a);
      }
    }

  protected:
    MultiPeerMessages multiPeerMsgs;

    bool validTarget(const Message& m) const {
      return multiPeerMsgs.find(m.address()) != multiPeerMsgs.end();
    }

    shared_ptr<MsgQueue> queue(const Address& m) const {
      shared_ptr<MsgQueue> bqueue;
      auto it  = multiPeerMsgs.find(m);
      if ( it != multiPeerMsgs.end() ) {
        bqueue = it->second;;
      }
      return bqueue;
    }

    void enqueue(const Message& m, shared_ptr<MsgQueue> q) const {
      if (q) {
        q->push(m);
      }
      else {
        fail(m.address());
      }
    }

    void fail(const Address& a) const {
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
