#ifndef K3_RUNTIME_MESSAGEPROC_H
#define K3_RUNTIME_MESSAGEPROC_H

#include <runtime/cpp/Dispatch.hpp>

namespace K3
{
  //-------------------
  // Message processor

  enum class LoopStatus { Continue, Error, Done };

  // template <typename Error, typename Result>
  // class MPStatus {
    // public:
      // LoopStatus tag;
      // Error error;
      // Result result;
  // };

  class MessageProcessor {
    public:
      MessageProcessor(): _status(LoopStatus::Continue) {}
      virtual void initialize() {}
      virtual void finalize() {}
      virtual LoopStatus process(Message msg) = 0;
      LoopStatus status() { return _status; };
    private:
      LoopStatus _status;
  };

  using MPStatus = LoopStatus;

  using NativeMessageProcessor = MessageProcessor;

  template <class E>
  class VirtualizedMessageProcessor: public MessageProcessor {
    public:
      VirtualizedMessageProcessor(E e): MessageProcessor(), env(e) {}
    private:
      E env;
  };

  // Message Processor used by generated code. Processing is done by dispatch messages using a
  // generated table of triggers. The trigger wrapper functions perform the deserialization of the
  // message payload themeselves.
  class DispatchMessageProcessor : public NativeMessageProcessor {
    public:
      DispatchMessageProcessor(TriggerDispatch td): table(td) {}

      LoopStatus process(Message msg)
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
