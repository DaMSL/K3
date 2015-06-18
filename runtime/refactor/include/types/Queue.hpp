#ifndef K3_QUEUE
#define K3_QUEUE

#include <memory>
#include <queue>

#include <concurrentqueue/blockingconcurrentqueue.h>

#include "types/Message.hpp"

namespace K3 {

class Queue { 
 public:
  Queue();
  void enqueue(std::unique_ptr<Message> m);
  std::unique_ptr<Message> dequeue();
  size_t dequeueBulk(vector<std::unique_ptr<Message>>& ms);

 protected:
  moodycamel::BlockingConcurrentQueue<std::unique_ptr<Message>> queue_;
};

}  // namespace K3

#endif
