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
        virtual Value pack() const = 0; 
        virtual void unpack(const Value &msg) = 0;
        virtual Dispatcher* clone() = 0;
        virtual ~Dispatcher() {}
    };

    template<typename T>
    class ValDispatcher : public Dispatcher {
      public:
        typedef unit_t (*trigFunc)(T);

        ValDispatcher(trigFunc f, const T& arg) : _func(f), _arg(arg) {}
        ValDispatcher(trigFunc f) : _func(f) {}

        void dispatch() const { _func(_arg); }

        void unpack(const Value &msg) { _arg = *BoostSerializer::unpack<T>(msg); }

        Dispatcher* clone() { return new ValDispatcher<T>(_func, _arg); }

        Value pack() const { return BoostSerializer::pack<T>(_arg); }

        T _arg;
        trigFunc _func;
    };

    template<typename T>
    class RefDispatcher : public Dispatcher {
      public:
        typedef unit_t (*trigFunc)(const T&);

        RefDispatcher(trigFunc f, const T& arg) : _func(f), _arg(arg) {}
        RefDispatcher(trigFunc f) : _func(f) {}

        void dispatch() const { _func(_arg); }

        void unpack(const Value &msg) { throw std::runtime_error("RefDispatcher cannot unpack"); }

        Dispatcher* clone() { return new RefDispatcher<T>(_func, _arg); }

        Value pack() const { return BoostSerializer::pack<T>(_arg); }

        const T &_arg;
        trigFunc _func;
    };

    template<typename T>
    class SharedDispatcher : public Dispatcher {
      public:
        typedef unit_t (*trigFunc)(const std::shared_ptr<T>);

        SharedDispatcher(trigFunc f, std::shared_ptr<T> arg) : _func(f), _arg(arg) {}
        SharedDispatcher(trigFunc f) : _func(f) {}

        void dispatch() const { _func(_arg); }

        void unpack(const Value &msg) { _arg = std::shared_ptr<T>(BoostSerializer::unpack<T>(msg)); }

        Dispatcher* clone() { return new SharedDispatcher<T>(_func, _arg); }

        Value pack() const { return BoostSerializer::pack<T>(*_arg); }

        std::shared_ptr<T> _arg;
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

