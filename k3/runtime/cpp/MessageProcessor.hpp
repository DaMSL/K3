#ifndef K3_RUNTIME_MESSAGEPROC_H
#define K3_RUNTIME_MESSAGEPROC_H

namespace K3
{
  //-------------------
  // Message processor

  template<typename Result, typename Error>
  class MPStatus {
  public:
    MPStatus() {}
    MPStatus(Result& res) {}
    MPStatus(Error& err) {}
    shared_ptr<Result> valid;
    shared_ptr<Error>  invalid;
  };

  template<typename Value, typename Result, typename Error>
  class MessageProcessor {
    Result initialize() {}
    Result process(Message<Value> msg, Result& prev) {}
    Result finalize(Result res) {}
    MPStatus<Result, Error> status(Result res) {}
    MPStatus<Result, Error> report() {}
  };
}

#endif