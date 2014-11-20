#ifndef K3_RUNTIME_MESSAGE_H
#define K3_RUNTIME_MESSAGE_H

#include "Common.hpp"
#include "Dispatch.hpp"

namespace K3 {

  //-------------
  // Local Messages (inside a system)

  // (destination peer, trigger, payload, source peer)
  class Message : public std::tuple<Address, TriggerId, shared_ptr<Dispatcher>, Address > {
  public:
    Message(Address addr, TriggerId id, shared_ptr<Dispatcher> d, Address src)
      : std::tuple<Address, TriggerId, shared_ptr<Dispatcher>, Address>(std::move(addr), id, d, std::move(src)) {}

    const Address&  address()  const { return std::get<0>(*this); }
    TriggerId id()             const { return std::get<1>(*this); }
    const shared_ptr<Dispatcher> dispatcher() const { return std::get<2>(*this); }
    const Address& source() const { return std::get<3>(*this); }
    string target() const {
      return __k3_context::__get_trigger_name(id()) + "@" + addressAsString(address());
    }
  };


  //-------------
  // Remote Messages (between nodes)

  class RemoteMessage : public std::tuple<Address, TriggerId, Value, Address> {
  public:
    RemoteMessage(Address addr, TriggerId id, const Value& v, Address src)
      : std::tuple<Address, TriggerId, Value, Address>(std::move(addr), id, v, std::move(src))
    {}

    RemoteMessage(Address addr, TriggerId id, Value&& v, Address src)
      : std::tuple<Address, TriggerId, Value, Address>(std::move(addr), id, std::forward<Value>(v), std::move(src))
    {}

    const Address&    address()  const { return std::get<0>(*this); }
    TriggerId id()               const { return std::get<1>(*this); }
    const Value& contents()      const { return std::get<2>(*this); }
    const Address& source()      const { return std::get<3>(*this); }
    std::string target() const {
      return __k3_context::__get_trigger_name(id()) + "@" + addressAsString(address()); 
    }

    // TODO: error reporting if not found
    const shared_ptr<Message> toMessage() const {
      auto *d = __k3_context::__get_clonable_dispatcher(id())->clone();
      d->unpack(contents());
      return make_shared<Message>(address(), id(), shared_ptr<Dispatcher>(d), source());
    }
  };

} // namespace K3

#endif // K3_RUNTIME_MESSAGE_H
