#ifndef K3_RUNTIME_ENDPOINT_BUFFER_H
#define K3_RUNTIME_ENDPOINT_BUFFER_H

#include <boost/thread/mutex.hpp>
#include <boost/thread/externally_locked.hpp>
#include <boost/thread/lockable_adapter.hpp>

#include <Common.hpp>
#include <Codec.hpp>
#include <IOHandle.hpp>
#include <Queue.hpp>

namespace K3
{
  typedef tuple<int, int> BufferSpec;
  static inline int bufferMaxSize(BufferSpec& spec)   { return get<0>(spec); }
  static inline int bufferBatchSize(BufferSpec& spec) { return get<1>(spec); }

  //------------------------
  // Endpoint buffers.
  // In contrast to the Haskell engine, in C++ we implement the concept of the
  // BufferContents datatype inline in the EndpointBuffer class. This is due
  // to the difference in the concurrency abstractions (e.g., MVar vs. externally_locked).

  class EndpointBuffer {
  public:
    typedef std::function<void(shared_ptr<string>)> NotifyFn;

    EndpointBuffer() {}

    virtual bool empty() = 0;
    virtual bool full() = 0;
    virtual size_t size() = 0;
    virtual size_t capacity() = 0;

    // Appends to this buffer, returning if the append succeeds.
    virtual bool push_back(shared_ptr<string> v) = 0;

    // Maybe Removes a value from the buffer and returns it
    virtual shared_ptr<string> pop() = 0;

    // Attempt to pull a value from the provided IOHandle
    // into the buffer. Returns a Maybe string
    virtual shared_ptr<string> refresh(shared_ptr<IOHandle>, NotifyFn) = 0;

    // Flush the contents of the buffer out to the provided IOHandle
    virtual void flush(shared_ptr<IOHandle>, NotifyFn) = 0;

    // Transfer the contents of the buffer into provided MessageQueues
    // Using the provided MessageCodec to convert from string to Message
    virtual bool transfer(shared_ptr<const MessageQueues>, shared_ptr<MessageCodec>, NotifyFn)= 0;
  };


  class ScalarEPBufferMT : public EndpointBuffer, public boost::basic_lockable_adapter<boost::mutex> {
  public:
    typedef ScalarEPBufferMT LockB;

    ScalarEPBufferMT() : EndpointBuffer(), contents(*this) {}
    // Metadata
    bool   empty()    {
      boost::strict_lock<LockB> guard(*this);
      return !(contents.get(guard));
    }
    bool   full()     {
      boost::strict_lock<LockB> guard(*this);
      return static_cast<bool>(contents.get(guard));
    }
    size_t size()     {
      boost::strict_lock<LockB> guard(*this);
      return contents.get(guard) ? 1 : 0;
    }
    size_t capacity() { return 1; }

    // Buffer Operations
    bool push_back(shared_ptr<string> v);

    shared_ptr<string> pop();

    shared_ptr<string> refresh(shared_ptr<IOHandle> ioh, NotifyFn notify);

    void flush(shared_ptr<IOHandle> ioh, NotifyFn notify);

    bool transfer(shared_ptr<const MessageQueues> queues, shared_ptr<MessageCodec> frame, NotifyFn notify);

   protected:
    boost::externally_locked<shared_ptr<string>, LockB> contents;
  };

  class ScalarEPBufferST : public EndpointBuffer {
  public:
    ScalarEPBufferST() : EndpointBuffer() {}
    // Metadata
    bool   empty()    { return !contents; }
    bool   full()     { return static_cast<bool>(contents); }
    size_t size()     { return contents ? 1 : 0; }
    size_t capacity() { return 1; }

    // Buffer Operations
    bool push_back(shared_ptr<string> v);

    shared_ptr<string> pop();

    shared_ptr<string> refresh(shared_ptr<IOHandle> ioh, NotifyFn notify);

    void flush(shared_ptr<IOHandle> ioh, NotifyFn notify);

    bool transfer(shared_ptr<const MessageQueues> queues, shared_ptr<MessageCodec> frame, NotifyFn notify);

   protected:
    shared_ptr<string> contents;
  };

  class ContainerEPBufferST : public EndpointBuffer {
  public:
    ContainerEPBufferST(BufferSpec s) : EndpointBuffer(), spec(s) {
      contents = shared_ptr<list<string>>(new list<string>());
    }

    bool   empty() { return contents? contents->empty() : true; }
    bool   full()  { return size() >= bufferMaxSize(spec); }
    size_t size()  { return empty()? 0 : contents->size(); }
    size_t capacity();

    bool push_back(shared_ptr<string> v);

    shared_ptr<string> pop();

    shared_ptr<string> refresh(shared_ptr<IOHandle> ioh, NotifyFn notify);

    // Default flush: do not force, wait for batch
    void flush(shared_ptr<IOHandle> ioh, NotifyFn notify) { flush(ioh,notify,false); }

    // flush overloaded with force flag to ignore batching semantics
    void flush(shared_ptr<IOHandle> ioh, NotifyFn notify, bool force);

    // Default transfer: do not force, wait for batch
    bool transfer(shared_ptr<const MessageQueues> queues, shared_ptr<MessageCodec> frame, NotifyFn notify) {
      return transfer(queues, frame, notify, false);
    }

    // transfer overloaded with force flag to ignore batching semantics
    bool transfer(shared_ptr<const MessageQueues> queues, shared_ptr<MessageCodec> frame, NotifyFn notify, bool force);

   protected:
    shared_ptr<list<string>> contents;
    BufferSpec spec;

    int batchSize() { int r = bufferMaxSize(spec); return r <=0? 1 : r;}
    bool batchAvailable() { return contents? contents->size() >= batchSize(): false;}
  };

}

#endif
