#ifndef K3_QUEUE
#define K3_QUEUE

#include <memory>
#include <queue>

#include <concurrentqueue/blockingconcurrentqueue.h>

#include "types/Dispatcher.hpp"

namespace K3 {

class Queue { 
 public:
  Queue();
  void enqueue(std::unique_ptr<Dispatcher> m);
  std::unique_ptr<Dispatcher> dequeue();
  size_t dequeueBulk(vector<std::unique_ptr<Dispatcher>>& ms);

 protected:
  moodycamel::BlockingConcurrentQueue<std::unique_ptr<Dispatcher>> queue_;
};

}  // namespace K3

#endif
