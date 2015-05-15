#ifndef K3_QUEUE
#define K3_QUEUE

// A lockfree Queue for holding Messages
// dequeue() is blocking, allowing a Peer to wait for messages

#include <memory>

#include "concurrentqueue/blockingconcurrentqueue.h"

#include "Message.hpp"

using std::unique_ptr;
using std::make_unique;

class Queue {
 public:
  void enqueue(unique_ptr<Message> m) {
    queue_.enqueue(std::move(m));
  }

  unique_ptr<Message> dequeue() {
    unique_ptr<Message> m;
    queue_.wait_dequeue(m);
    return std::move(m);
  }

 protected:
  moodycamel::BlockingConcurrentQueue<unique_ptr<Message>> queue_;
};

#endif
