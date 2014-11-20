#ifndef K3_RUNTIME_MESSAGEPROC_H
#define K3_RUNTIME_MESSAGEPROC_H

#include <map>
#include <string>
#include <functional>

#include "Common.hpp"
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
      virtual map<string, string> json_bindings(Address) = 0;
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

    map<string, string> json_bindings(Address) override {
      throw std::runtime_error("Json bindinds not implemented for Native MP");
    }
   private:
    EnvStrFunction _get_env;
  };

  // Message Processor used by generated code.
  // class DispatchMessageProcessor : public NativeMessageProcessor {
  //   public:
  //     DispatchMessageProcessor()
  //       : NativeMessageProcessor(empty_map) {}

  //     DispatchMessageProcessor(EnvStrFunction f)
  //       : NativeMessageProcessor(f) {}

  //     LoopStatus process(Message msg)
  //     {
  //       msg.dispatcher()->dispatch();

  //       // Message was processed, signal the engine to continue.
  //       // TODO: Propagate trigger errors to engine, K3 error semantics?
  //       return LoopStatus::Continue;
  //     }

  //   private:
  //     static std::map<std::string, std::string> empty_map() { return std::map<std::string, std::string>(); };
  //   };

  class Engine;

  class virtualizing_message_processor: public MessageProcessor {
   public:
    virtualizing_message_processor(): MessageProcessor() {}

    virtualizing_message_processor(std::map<Address, shared_ptr<__k3_context>> m):
      MessageProcessor(), contexts(m) {}

    void add_context(Address a, shared_ptr<__k3_context> p) {
      contexts[a] = p;
    }

    LoopStatus process(Message msg) {
      try {
	msg.dispatcher()->call_dispatch(*contexts[msg.address()], msg.id());
      } catch(std::out_of_range e) {
        return LoopStatus::Error;
      }

      return LoopStatus::Continue;
    }

    std::map<std::string, std::string> bindings(Address a) override {
      return contexts[a]->__prettify();
    }

    std::map<std::string, std::string> json_bindings(Address a) override {
      return contexts[a]->__jsonify();
    }
   private:
    std::map<Address, shared_ptr<__k3_context>> contexts;
  };
}

#endif
