#ifndef K3_RUNTIME_MESSAGEPROC_H
#define K3_RUNTIME_MESSAGEPROC_H

#include <k3/runtime/Dispatch.hpp>

namespace K3
{
  //-------------------
  // Message processor

  enum class MPStatus = { Continue, Error, Done };

  template <typename Error, typename Result>
  class MPStatus {
  public:
    MPStatus tag;
    Error error;
    Result result;
  };

  template<typename Environment>
  class MessageProcessor 
  {
  public:
    MessageProcessor(Environment e): env(e), _status(MPStatus::Continue) {}
    virtual void initialize() {}
    virtual void finalize() {}
    virtual void MPStatus process(Message msg) = 0;
    MPStatus status() { return _status };

    Environment env;
    MPStatus _status;
  };

  // Message Processor used by generated code. Processing is done by dispatch messages using a
  // generated table of triggers. The trigger wrapper functions perform the deserialization of the
  // message payload themeselves.
  class DispatchMessageProcessor : public MessageProcessor {
  public:
    DispatchMessageProcessor(TriggerDispatch td): table(td) {}

    MPStatus process(Message msg)
    {
      TriggerWrapper tw;

      // Look the trigger up in the dispatch table, error out if not present.
      try {
          tw = table.at(msg.id());
      } catch (std::out_of_range e) {
          return LoopStatus::Error;
      }

      // Call the trigger.
      tw(msg.contents());

      // Message was processed, signal the engine to continue.
      // TODO: Propagate trigger errors to engine, K3 error semantics?
      return LoopStatus::Continue;
    }

  private:
    TriggerDispatch table;
  };
}

#endif
