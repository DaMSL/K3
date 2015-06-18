#ifndef K3_QUEUE
#define K3_QUEUE

// A lockfree Queue for holding Messages
// dequeue() is blocking, allowing a Peer to wait for messages

#include <memory>
#include <queue>

#include "concurrentqueue/blockingconcurrentqueue.h"
#include <boost/thread/thread.hpp>
#include <boost/thread/recursive_mutex.hpp>
#include <boost/thread/lockable_adapter.hpp>
#include <boost/thread/externally_locked.hpp>

#include "types/Message.hpp"

namespace K3 {

// TODO(jbw) Consider bulk dequeues
class Queue {
public:
  virtual void enqueue(std::unique_ptr<Message> m) = 0;
  virtual std::unique_ptr<Message> dequeue() = 0;
  virtual size_t dequeueBulk(vector<std::unique_ptr<Message>>&) = 0;
};

class LockFreeQueue : public Queue {
 public:
  LockFreeQueue() {}

  void enqueue(std::unique_ptr<Message> m) { queue_.enqueue(std::move(m)); }

  size_t dequeueBulk(vector<std::unique_ptr<Message>>& ms) {
    return queue_.wait_dequeue_bulk(ms.data(), ms.size());
  }

  std::unique_ptr<Message> dequeue() {
    std::unique_ptr<Message> m;
    queue_.wait_dequeue(m);
    return std::move(m);
  }

 protected:
  moodycamel::BlockingConcurrentQueue<std::unique_ptr<Message>> queue_;
};

using mutex = boost::recursive_mutex;
using std::dynamic_pointer_cast;

//class LockedQueue : public Queue, public boost::basic_lockable_adapter<mutex>  {
//  public:
//   typedef boost::basic_lockable_adapter<mutex> qlockable;
//   LockedQueue () : qlockable(), queue_(*this) {
//     condition_ = make_shared<boost::condition_variable_any>();
//   }
//
//   bool empty() {
//     boost::strict_lock<LockedQueue> guard(*this);
//     return queue_.get(guard).empty();
//   }
//
//   void enqueue(std::unique_ptr<Message> m) {
//     boost::strict_lock<LockedQueue> guard(*this);
//     queue_.get(guard).push(m);
//     condition_->notify_one();
//   }
//
//   void waitForMessage() {
//     boost::std::unique_lock<LockedQueue> guard(*this);
//     while (this->empty()) {
//       condition_->wait(guard);
//     }
//   }
//
//   std::unique_ptr<Message> dequeue() {
//     waitForMessage();
//     boost::strict_lock<LockedQueue> guard(*this);
//     std::unique_ptr<Message> r;
//     r = queue_.get(guard).front();
//     queue_.get(guard).pop();
//     return r;
//   }
//
// protected:
//   mutex mutex_;
//   boost::externally_locked<std::queue<std::unique_ptr<Message>>, LockedQueue> queue_;
//   shared_ptr<boost::condition_variable_any> condition_;
//};


}  // namespace K3

#endif
