#include <vector>
#include "types/Queue.hpp"
#include <concurrentqueue/concurrentqueue.h>
#include "core/Peer.hpp"

namespace K3 {

Queue::Queue() : queue_() {}

void Queue::enqueue(Pool::unique_ptr<Dispatcher> m) { queue_.enqueue(std::move(m)); }

void Queue::enqueue(shared_ptr<moodycamel::ProducerToken> token, Pool::unique_ptr<Dispatcher> m) {
  queue_.enqueue(*token, std::move(m));
}

void Queue::enqueueBulk(vector<Pool::unique_ptr<Dispatcher>>& ms) {
  typedef std::vector<Pool::unique_ptr<Dispatcher>>::iterator iter_t;
  if (ms.size() > 0) {
    queue_.enqueue_bulk(std::move_iterator<iter_t>(ms.begin()), ms.size());
  }
}

size_t Queue::dequeueBulk(vector<Pool::unique_ptr<Dispatcher>>& ms) {
  return queue_.try_dequeue_bulk(ms.data(), ms.size());
}

Pool::unique_ptr<Dispatcher> Queue::dequeue() {
  Pool::unique_ptr<Dispatcher> m;
  queue_.try_dequeue(m);
  return std::move(m);
}

shared_ptr<moodycamel::ProducerToken> Queue::newProducerToken() {
  return make_shared<moodycamel::ProducerToken>(queue_);
}

}  // namespace K3
