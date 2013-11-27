#ifndef K3_RUNTIME_COMMON_H
#define K3_RUNTIME_COMMON_H

#include <tuple>

namespace K3 {

  using namespace std;
  using namespace boost;

  typedef string Identifier;
  typedef tuple<string, int> Address;

  //---------------
  // Addresses.
  Address defaultAddress("127.0.0.1", 40000);

  string addressHost(Address& addr) { return get<0>(addr); }
  int    addressPort(Address& addr) { return get<1>(addr); }
  
  string addressAsString(Address& addr) {
    return addressHost(addr) + to_string(addressPort(addr));
  }

  Address internalSendAddress(Address addr) {
    return Address(addressHost(addr), addressPort(addr)+1);
  }

  Address externalSendAddress(Address addr) {
    return Address(addressHost(addr), addressPort(addr)+2);
  }

  //-------------
  // Messages.
  template<typename Value>
  class Message : public tuple<Address, Identifier, Value> {
  public:
    Message(Address addr, Identifer id, Value v) : tuple<Address, Identifier, Value>(addr, id, v) {}
    
    Address    address()  { return get<0>(*this); }
    Identifier id()       { return get<1>(*this); }
    Value      contents() { return get<2>(*this); }
  };

  //--------------------
  // System environment.

  // Literals are native values rather than an AST reprensentation as in Haskell.
  typedef boost::any Literal;
  typedef map<Identifier, Literal> PeerBootstrap;
  typedef map<Address, PeerBootstrap> SystemEnvironment;

  list<Address> deployedNodes(SystemEnvironment& sysEnv) {
    list<Address> r;
    for_each(sysEnv, r.push_back(arg1->first))
    return r;
  }

}

#endif