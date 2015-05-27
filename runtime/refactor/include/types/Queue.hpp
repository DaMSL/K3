#ifndef K3_QUEUE
#define K3_QUEUE

// A lockfree Queue for holding Messages
// dequeue() is blocking, allowing a Peer to wait for messages

#include <memory>

#include "concurrentqueue/blockingconcurrentqueue.h"

#include "types/Message.hpp"

namespace K3 {

// TODO(jbw) Consider bulk dequeues
class Queue {
 public:
  Queue() {}

  void enqueue(shared_ptr<Message> m) { queue_.enqueue(m); }

  shared_ptr<Message> dequeue() {
    shared_ptr<Message> m;
    queue_.wait_dequeue(m);
    return m;
  }

 protected:
  moodycamel::BlockingConcurrentQueue<shared_ptr<Message>> queue_;
};

}  // namespace K3

#endif
