#include <vector>
#include "types/Queue.hpp"
#include <concurrentqueue/blockingconcurrentqueue.h>

namespace K3 {

Queue::Queue() {}

void Queue::enqueue(Pool::unique_ptr<Dispatcher> m) { queue_.enqueue(std::move(m)); }

void Queue::enqueue(shared_ptr<moodycamel::ProducerToken> token, Pool::unique_ptr<Dispatcher> m) {
  queue_.enqueue(*token, std::move(m));
}

size_t Queue::dequeueBulk(vector<Pool::unique_ptr<Dispatcher>>& ms) {
  return queue_.wait_dequeue_bulk(ms.data(), ms.size());
}

Pool::unique_ptr<Dispatcher> Queue::dequeue() {
  Pool::unique_ptr<Dispatcher> m;
  queue_.wait_dequeue(m);
  return std::move(m);
}

shared_ptr<moodycamel::ProducerToken> Queue::newProducerToken() {
  return make_shared<moodycamel::ProducerToken>(queue_);
}

}  // namespace K3
