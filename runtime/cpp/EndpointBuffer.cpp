#include "EndpointBuffer.hpp"

using namespace boost::log;
using namespace boost;

namespace K3
{
  bool ScalarEPBufferMT::push_back(shared_ptr<string> v) {
    strict_lock<LockB> guard(*this);
    // Failure:
    if (!v || (contents.get(guard))) {
      return false;
    }
    // Success:
    contents.get(guard) = v;
    return true;
  }

  shared_ptr<string> ScalarEPBufferMT::pop() {
    strict_lock<LockB> guard(*this);
    shared_ptr<string> v;
    if (contents.get(guard)) {
      // Success:
      v = contents.get(guard);
      contents.get(guard).reset();
    }
    // In case of failure, v is a null pointer
    return v;
  }

  shared_ptr<string> ScalarEPBufferMT::refresh(shared_ptr<IOHandle> ioh, NotifyFn notify)
  {
    strict_lock<LockB> guard(*this);
    shared_ptr<string> r;

    // Read from the buffer if possible
    if (contents.get(guard)) {
      r = contents.get(guard);
      contents.get(guard).reset();
      notify(r);
    }

    // If there is more data in the underlying IOHandle
    // use it to populate the buffer
    if (ioh->hasRead()) {
      shared_ptr<string> v = ioh->doRead();
      contents.get(guard) = v;
    }

   return r;
  }

  void ScalarEPBufferMT::flush(shared_ptr<IOHandle> ioh, NotifyFn notify) {
    // pop() a value and write to the handle if possible
    strict_lock<LockB> guard(*this);
    if (contents.get(guard)) {
      shared_ptr<string> v = contents.get(guard);
      contents.get(guard).reset();
      ioh->doWrite(v);
      notify(v);
    }
  }

  bool ScalarEPBufferMT::transfer(shared_ptr<MessageQueues> queues,
                                  shared_ptr<MessageCodec> frame,
                                  NotifyFn notify)
  {
    strict_lock<LockB> guard(*this);
    bool transferred = false;
    if(contents.get(guard)) {
      shared_ptr<string> v = contents.get(guard);
      contents.get(guard).reset();
      if (queues && frame) {
        Message msg = *(frame->decode(*v)->toMessage());
        queues->enqueue(msg);
        transferred = true;
      }
      notify(v);
    }
    return transferred;
  }

  // Buffer Operations
  bool ScalarEPBufferST::push_back(shared_ptr<string> v) {
    // Failure:
    if (!v || this->full()) {
      return false;
    }
    // Success:
    contents = v;
    return true;
  }

  shared_ptr<string> ScalarEPBufferST::pop() {
    shared_ptr<string> v;
    if (!this->empty()) {
      // Success:
      v = contents;
      contents = shared_ptr<string>();
    }
    // In case of failure, v is a null pointer
    return v;
  }

  shared_ptr<string> ScalarEPBufferST::refresh(shared_ptr<IOHandle> ioh, NotifyFn notify)
  {
    shared_ptr<string> r;

    // Read from the buffer if possible
    if (!(this->empty())) {
      r = this->pop();
      notify(r);
    }

    // If there is more data in the underlying IOHandle
    // use it to populate the buffer
    if (ioh->hasRead()) {
      shared_ptr<string> v = ioh->doRead();
      this->push_back(v);
    }

   return r;
  }

  void ScalarEPBufferST::flush(shared_ptr<IOHandle> ioh, NotifyFn notify) {
    // pop() a value and write to the handle if possible
    if (!this->empty()) {
      shared_ptr<string> v = this->pop();
      ioh->doWrite(v);
      notify(v);
    }
  }

  bool ScalarEPBufferST::transfer(shared_ptr<MessageQueues> queues,
                                  shared_ptr<MessageCodec> frame,
                                  NotifyFn notify) {
    bool transferred = false;
    if(!this->empty()) {
      shared_ptr<string> v = this->pop();
      if (queues && frame) {
        Message msg = *(frame->decode(*v)->toMessage());
        queues->enqueue(msg);
        transferred = true;
      }
      notify(v);
    }
    return transferred;
  }

  size_t ContainerEPBufferST::capacity() {
    if (!contents) {
      return 0;
    }
    int s = bufferMaxSize(spec);
    return s <= 0? contents->max_size() : s;
  }

  bool ContainerEPBufferST::push_back(shared_ptr<string> v) {
    // Failure if contents is null or full
    if (!v || !contents || full()) {
      return false;
    }

    // Success
    contents->push_back(*v);
    return true;
  }

  shared_ptr<string> ContainerEPBufferST::pop() {
    shared_ptr<string> v;
    if (!empty()) {
      v = make_shared<string>(contents->front());
      contents->pop_front();
    }
    return v;
  }

  shared_ptr<string> ContainerEPBufferST::refresh(shared_ptr<IOHandle> ioh,
                                                  NotifyFn notify)
  {
    shared_ptr<string> r;

    // Read from the buffer if possible
    if (!(this->empty())) {
      r = this->pop();
      notify(r);
    }

    // If there is more data in the underlying IOHandle
    // use it to populate the buffer. try to batch
    int n = batchSize();
    for(int i=0; !full() && ioh->hasRead() && i < n; i++) {
      shared_ptr<string> v = ioh->doRead();
      this->push_back(v);
    }
    return r;
  }

  // flush overloaded with force flag to ignore batching semantics
  void ContainerEPBufferST::flush(shared_ptr<IOHandle> ioh,
                                  NotifyFn notify,
                                  bool force) {
    while (batchAvailable() || force) {
      int n = batchSize();
      for (int i=0; i < n; i++) {
        if (force && empty()) { return; }
        shared_ptr<string> v = this->pop();
        ioh->doWrite(v);
        notify(v);
      }
    }
  }

  // transfer overloaded with force flag to ignore batching semantics
  bool ContainerEPBufferST::transfer(shared_ptr<MessageQueues> queues,
                                     shared_ptr<MessageCodec> frame,
                                     NotifyFn notify,
                                     bool force)
  {
    // Transfer as many full batches as possible
    bool transferred = false;
    while (batchAvailable() || force) {
      int n = batchSize();
      for (int i=0; i < n; i++) {
        if (force && empty()) { return transferred; }
        shared_ptr<string> v = this->pop();
        if (queues && frame) {
          shared_ptr<Message> msg = frame->decode(*v)->toMessage();
          queues->enqueue(*msg);
          transferred = true;
        }
        notify(v);
      }
    }
    return transferred;
  }
}