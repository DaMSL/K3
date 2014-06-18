#ifndef K3_RUNTIME_MESSAGEPROC_H
#define K3_RUNTIME_MESSAGEPROC_H

#include <map>
#include <string>
#include <functional>
#include <memory>

#include "Context.hpp"
#include "Dispatch.hpp"

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
      MessageProcessor(): _status(LoopStatus::Continue) {}
      virtual void initialize() {}
      virtual void finalize() {}
      virtual LoopStatus process( Message msg) = 0;
      LoopStatus status() { return _status; };

      virtual map<string, string> bindings(Address) = 0;
    private:
      LoopStatus _status;
  };

  using MPStatus = LoopStatus;

  class NativeMessageProcessor: public MessageProcessor {
   public:
    NativeMessageProcessor(EnvStrFunction get_env): MessageProcessor(),  _get_env(get_env) {}

    map<string, string> bindings(Address) override {
      return _get_env();
    }

   private:
    EnvStrFunction _get_env;
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

  class Engine;

  class virtualizing_message_processor: public MessageProcessor {
   public:
    virtualizing_message_processor(): MessageProcessor() {}

    virtualizing_message_processor(std::map<Address, shared_ptr<__k3_context>> m):
      MessageProcessor(), contexts(m) {}

    void add_context(Address a, std::shared_ptr<__k3_context> p) {
      contexts[a] = p;
    }

    LoopStatus process(Message msg) {
      try {
        contexts[msg.address()]->__dispatch(msg.contents());
      } catch(std::out_of_range e) {
        return LoopStatus::Error;
      }

      return LoopStatus::Continue;
    }

    std::map<std::string, std::string> bindings(Address a) override {
      return contexts[a]->__prettify();
    }

   private:
    std::map<Address, shared_ptr<__k3_context>> contexts;
  };
}

#endif
