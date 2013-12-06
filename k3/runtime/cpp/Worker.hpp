#ifndef K3_RUNTIME_WORKER_H
#define K3_RUNTIME_WORKER_H

#include <list>
#include <boost/shared_ptr.hpp>

namespace K3 {

  using namespace std;
  using namespace boost;

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