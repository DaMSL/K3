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

class Queue {
 public:
  Queue() {}

  void enqueue(std::unique_ptr<Message> m) { queue_.enqueue(std::move(m)); }

  size_t dequeueBulk(moodycamel::ConsumerToken& ctok, vector<std::unique_ptr<Message>>& ms) {
    return queue_.wait_dequeue_bulk(ctok, ms.data(), ms.size());
  }

  std::unique_ptr<Message> dequeue() {
    std::unique_ptr<Message> m;
    queue_.wait_dequeue(m);
    return std::move(m);
  }

  shared_ptr<moodycamel::ProducerToken> producerToken() {
    return make_shared<moodycamel::ProducerToken>(queue_);
  }

  shared_ptr<moodycamel::ConsumerToken> consumerToken() {
    return make_shared<moodycamel::ConsumerToken>(queue_);
  }

 protected:
  moodycamel::BlockingConcurrentQueue<std::unique_ptr<Message>> queue_;
};

}  // namespace K3

#endif
