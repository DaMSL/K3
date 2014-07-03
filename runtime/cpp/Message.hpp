#ifndef K3_RUNTIME_MESSAGE_H
#define K3_RUNTIME_MESSAGE_H

  //-------------
  // Remote Messages (between nodes)

namespace K3 {

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
    Message& toMessage() const;
  };
  
  //-------------
  // Local Messages (inside a system)

  // TODO: use reference
  class Message : public std::tuple<Address, Identifier, Dispatcher> {
  public:
    Message(Address addr, Identifier id, const Dispatcher& d)
      : std::tuple<Address, Identifier, Dispatcher>(std::move(addr), std::move(id), d)
    {}

    Message(Address addr, Identifier id, Dispatcher&& d)
      : std::tuple<Address, Identifier, Dispatcher>(std::move(addr), std::move(id), std::forward<Dispatcher>(d))
    {}

    Message(Address&& addr, Identifier&& id, Dispatcher&& d)
      : std::tuple<Address, Identifier, Dispatcher>(std::forward<Address>(addr),
                                   std::forward<Identifier>(id),
                                   std::forward<Dispatcher>(d))
    {}

    const Address&    address()  const { return std::get<0>(*this); }
    const Identifier& id()       const { return std::get<1>(*this); }
    const Dispatcher& dispatcher() const { return std::get<2>(*this); }
    const std::string target()   const { return id() + "@" + addressAsString(address()); }

    // TODO: error reporting if not found
    const std::shared_ptr<Message> toMessage() const {
      return std::make_shared<Message>(address(), id(), dispatch_table[id()]);
    }
  };

} // namespace K3

#endif // K3_RUNTIME_MESSAGE_H
