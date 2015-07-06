#include "types/Dispatcher.hpp"

namespace K3 {
void SentinelDispatcher::operator()() { throw EndOfProgramException(); }
}  // namespace K3
