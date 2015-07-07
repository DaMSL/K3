#include "types/Dispatcher.hpp"

namespace K3 {
void SentinelDispatcher::operator()() { throw EndOfProgramException(); }
base_string SentinelDispatcher::jsonify() const { return "{type: SENTINEL}"; }
}  // namespace K3
