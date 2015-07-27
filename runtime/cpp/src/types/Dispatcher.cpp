#include "types/Dispatcher.hpp"

namespace K3 {

base_string Dispatcher::jsonify() const { return "{type: UNKNOWN_DISPATCHER}"; }

void SentinelDispatcher::operator()() { throw EndOfProgramException(); }
base_string SentinelDispatcher::jsonify() const { return "{type: SENTINEL}"; }
}  // namespace K3
