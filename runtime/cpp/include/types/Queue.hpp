#ifndef K3_QUEUE
#define K3_QUEUE

#include <queue>


#include <concurrentqueue/blockingconcurrentqueue.h>

#include "types/Pool.hpp"

#include "types/Dispatcher.hpp"

namespace K3 {
class Queue {
 public:
  Queue();
  void enqueue(Pool::unique_ptr<Dispatcher> m);
  void enqueue(shared_ptr<moodycamel::ProducerToken> token, Pool::unique_ptr<Dispatcher> m);
  Pool::unique_ptr<Dispatcher> dequeue();
  size_t dequeueBulk(vector<Pool::unique_ptr<Dispatcher>>& ms);
  shared_ptr<moodycamel::ProducerToken> newProducerToken();

 protected:
  moodycamel::BlockingConcurrentQueue<Pool::unique_ptr<Dispatcher>> queue_;
};

}  // namespace K3

#endif
