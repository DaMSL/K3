#ifndef K3_RUNTIME_MESSAGE_H
#define K3_RUNTIME_MESSAGE_H

#include <tuple>
#include <string>

#include "Common.hpp"
#include "Dispatch.hpp"

namespace K3 {
  
  //-------------
  // Local Messages (inside a system)

  class Message : public std::tuple<Address, Identifier, boost::shared_ptr<Dispatcher> > {
  public:
    Message(Address addr, Identifier id, Dispatcher& d)
      : std::tuple<Address, Identifier, boost::shared_ptr<Dispatcher> >(std::move(addr), std::move(id), boost::shared_ptr<Dispatcher>(&d)) {}

    Message(Address addr, Identifier id, std::shared_ptr<Dispatcher> d)
      : std::tuple<Address, Identifier, boost::shared_ptr<Dispatcher> >(std::move(addr), std::move(id), make_shared_ptr(d)) {}

    Message(Address addr, Identifier id, boost::shared_ptr<Dispatcher> d)
      : std::tuple<Address, Identifier, boost::shared_ptr<Dispatcher> >(std::move(addr), std::move(id), d) {}


    const Address&    address()    const { return std::get<0>(*this); }
    const Identifier& id()         const { return std::get<1>(*this); }
    const boost::shared_ptr<Dispatcher> dispatcher() const { return std::get<2>(*this); }
    const std::string target()     const { return id() + "@" + addressAsString(address()); }
  };


  //-------------
  // Remote Messages (between nodes)
  
  class RemoteMessage : public std::tuple<Address, Identifier, Value> {
  public:
    RemoteMessage(Address addr, Identifier id, const Value& v)
      : std::tuple<Address, Identifier, Value>(std::move(addr), std::move(id), v)
    {}

    RemoteMessage(Address addr, Identifier id, Value&& v)
      : std::tuple<Address, Identifier, Value>(std::move(addr), std::move(id), std::forward<Value>(v))
    {}

    RemoteMessage(Address&& addr, Identifier&& id, Value&& v)
      : std::tuple<Address, Identifier, Value>(std::forward<Address>(addr),
                                          std::forward<Identifier>(id),
                                          std::forward<Value>(v))
    {}

    const Address&    address()  const { return std::get<0>(*this); }
    const Identifier& id()       const { return std::get<1>(*this); }
    const Value& contents()      const { return std::get<2>(*this); }
    const std::string target()   const { return id() + "@" + addressAsString(address()); }
    // TODO: error reporting if not found
    const std::shared_ptr<Message> toMessage() const {
      return std::make_shared<Message>(address(), id(), dispatch_table[id()]);
    }
  };
  
} // namespace K3

#endif // K3_RUNTIME_MESSAGE_H
