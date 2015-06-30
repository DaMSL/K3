#include <vector>
#include "types/Queue.hpp"

namespace K3 {

Queue::Queue() {}

void Queue::enqueue(std::unique_ptr<Dispatcher> m) { queue_.enqueue(std::move(m)); }

size_t Queue::dequeueBulk(vector<std::unique_ptr<Dispatcher>>& ms) {
  return queue_.wait_dequeue_bulk(ms.data(), ms.size());
}

std::unique_ptr<Dispatcher> Queue::dequeue() {
  std::unique_ptr<Dispatcher> m;
  queue_.wait_dequeue(m);
  return std::move(m);
}

}  // namespace K3
