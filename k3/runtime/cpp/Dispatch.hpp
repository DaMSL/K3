#ifndef K3_RUNTIME_DISPATCH_H
#define K3_RUNTIME_DISPATCH_H

#include <functional>
#include <map>

namespace K3 {
    // A TriggerWrapper is a function which wraps a trigger with the functionality necessary to
    // deserialize the message payload, cast it to the trigger's appropriate argument type, and
    // invoke the trigger function.
    using TriggerWrapper = function<void(string)>;

    // A TriggerDispatch table maps trigger names to the corresponding generated TriggerWrapper
    // function.
    using TriggerDispatch = map<Identifier, TriggerWrapper>;
}
#endif

