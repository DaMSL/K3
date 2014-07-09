#ifndef K3_RUNTIME_DISPATCH_H
#define K3_RUNTIME_DISPATCH_H

#include <map>
#include <string>
#include <memory>

#include "Common.hpp"
#include "Serialization.hpp"

namespace K3 {
  
    //------------
    // Dispatcher class
    //
    // Every trigger type must have a Dispatcher that can be inserted
    // in a queue
    class Dispatcher {
      public:
        virtual void dispatch() const = 0;
        virtual std::string pack() const = 0; 
        virtual void unpack(const std::string &msg) = 0;
        virtual ~Dispatcher() {}
    };

    template<typename T>
    class DispatcherImpl : public Dispatcher {
      public:
        typedef unit_t (*trigFunc)(T);

        DispatcherImpl(trigFunc f, T arg) : _func(f), _arg(arg) {}
        DispatcherImpl(trigFunc f) : _func(f) {}

        void dispatch() const { _func(_arg); }

        void unpack(const std::string &msg) { _arg = *BoostSerializer::unpack<T>(msg); }

        string pack() const { return BoostSerializer::pack<T>(_arg); }

        T _arg;
        trigFunc _func;
    };

    // A TriggerDispatch table maps trigger names to the corresponding generated TriggerWrapper
    // function.
    using TriggerDispatch = std::vector<std::tuple<std::shared_ptr<Dispatcher>, Identifier> >;

#ifndef MAIN_PROGRAM
    extern
#endif
    TriggerDispatch dispatch_table;

} // namespace K3
#endif

