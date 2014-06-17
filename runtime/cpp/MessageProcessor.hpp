#ifndef K3_RUNTIME_MESSAGEPROC_H
#define K3_RUNTIME_MESSAGEPROC_H

#include <Dispatch.hpp>

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

  using EnvStrFunction = std::function<std::map<std::string,std::string>()>;

  class MessageProcessor {

    public:
      MessageProcessor(EnvStrFunction f): _status(LoopStatus::Continue), _get_env(f) {}
      virtual void initialize() {}
      virtual void finalize() {}
      virtual LoopStatus process( Message msg) = 0;
      LoopStatus status() { return _status; };
      std::map<std::string, std::string> get_env() { auto x = _get_env(); return x;};
    private:
      LoopStatus _status;
      EnvStrFunction _get_env;

  };

  using MPStatus = LoopStatus;

  using NativeMessageProcessor = MessageProcessor;

  template <class E>
  class VirtualizedMessageProcessor: public MessageProcessor {
    public:
      VirtualizedMessageProcessor(E e, EnvStrFunction f): MessageProcessor(f), env(e) {}
    private:
      E env;
  };

  // Message Processor used by generated code. Processing is done by dispatch messages using a
  // generated table of triggers. The trigger wrapper functions perform the deserialization of the
  // message payload themeselves.
  class DispatchMessageProcessor : public NativeMessageProcessor {
    public:
      DispatchMessageProcessor(TriggerDispatch td)
        : NativeMessageProcessor(empty_map), table(td)
      {}

      DispatchMessageProcessor(TriggerDispatch td, EnvStrFunction f)
        : NativeMessageProcessor(f), table(td)
      {}

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
      static std::map<std::string, std::string> empty_map() { return std::map<std::string, std::string>(); };
    };
}



#endif
