#ifndef K3_RUNTIME_MESSAGEPROC_H
#define K3_RUNTIME_MESSAGEPROC_H

#include <k3/runtime/Dispatch.hpp>

namespace K3
{
  //-------------------
  // Message processor

  enum class MPStatus = { Continue, Error, Done };

  template<typename Value>
  class MessageProcessor {
    public:
    MPStatus process(Message<Value> msg) {}
  };

  // Message Processor used by generated code. Processing is done by dispatch messages using a
  // generated table of triggers. The trigger wrapper functions perform the deserialization of the
  // message payload themeselves.
  template <typename Value>
  class DispatchMessageProcessor : public MessageProcessor {
  public:
    DispatchMessageProcessor(TriggerDispatch td): table(td) {}

    MPStatus process(Message<Value> msg) {

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
  }
}

#endif
