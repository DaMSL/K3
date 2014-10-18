#ifndef K3_RUNTIME_WORKER_H
#define K3_RUNTIME_WORKER_H

#include <list>

namespace K3 {

  //------------
  // Workers
  class WorkerPool {
  public:
    enum PoolType { Uniprocess, MultiThreaded, MultiProcess };
    typedef int WorkerId;
    typedef int ProcessId;

    WorkerPool() {}
    WorkerPool(PoolType wt) {}

    virtual void run() = 0;
  };
  class InlinePool : public WorkerPool {
  public:
    void run() {}
    // Set the ID of the single worker
    void setId(WorkerId x) {
      if (!uniProcessor) {
        uniProcessor = make_shared<WorkerId>(x);
      } else {
        *uniProcessor = x;
      }
    }
    // Get the ID of the single worker, or -1 if it is not yet initialized
    WorkerId getId() {
      if (!uniProcessor) {
        return -1;
      } else {
        return *uniProcessor;
      }
    }
  protected:
    shared_ptr<WorkerId> uniProcessor;
  };

  class ThreadPool : public WorkerPool {
  public:
    void run() {}

  protected:
    shared_ptr<list<WorkerId> > threadedProcessor;
  };

  class ProcessPool : public WorkerPool {
  public:
    void run() {}

  protected:
    shared_ptr<list<ProcessId> > multiProcessor;
  };

}

#endif
