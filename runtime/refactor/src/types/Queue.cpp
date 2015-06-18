#include <vector>
#include "types/Queue.hpp"

namespace K3 {

Queue::Queue() {}

void Queue::enqueue(std::unique_ptr<Message> m) { queue_.enqueue(std::move(m)); }

size_t Queue::dequeueBulk(vector<std::unique_ptr<Message>>& ms) {
  return queue_.wait_dequeue_bulk(ms.data(), ms.size());
}

std::unique_ptr<Message> Queue::dequeue() {
  std::unique_ptr<Message> m;
  queue_.wait_dequeue(m);
  return std::move(m);
}

}  // namespace K3
