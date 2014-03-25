#include <Common.hpp>
#include <Engine.hpp>
#include <Dispatch.hpp>
#include <MessageProcessor.hpp>

#include <functional>
#include <iostream>
#include <sstream>

#include <boost/regex.hpp>

#include <xUnit++/xUnit++.h>

namespace K3 {

std::string localhost = "127.0.0.1";
Address peer1;
Address peer2;
Address peer3;
Address rendezvous;

int nodeCounter(0);


using std::cout;
using std::endl;
using std::bind;
using std::string;
using std::shared_ptr;


// "Trigger" Declarations
void join(shared_ptr<Engine> engine, string message_contents) {
  Message m = Message(rendezvous, "register", "1");
  engine->send(m);
}

void register2(shared_ptr<Engine> engine, string message_contents) {
  int n = std::stoi(message_contents);
  nodeCounter += n;
}

// MP setup
TriggerDispatch buildTable(shared_ptr<Engine> engine) {
  TriggerDispatch table = TriggerDispatch();
  table["join"] = bind(&K3::join, engine, std::placeholders::_1);
  table["register"] = bind(&K3::register2, engine, std::placeholders::_1);
  return table;
}

shared_ptr<MessageProcessor> buildMP(shared_ptr<Engine> engine) {
  auto table = buildTable(engine);
  shared_ptr<MessageProcessor> mp = make_shared<DispatchMessageProcessor>(DispatchMessageProcessor(table));
  return mp;
}

// Engine setup
shared_ptr<Engine> buildEngine() {
  // Configure engine components
  bool simulation = true;



  list<Address> peers = list<Address>();
  peers.push_back(peer1);
  peers.push_back(peer2);
  peers.push_back(peer3);


  SystemEnvironment s_env = defaultEnvironment(peers);
  shared_ptr<InternalCodec> i_cdec = make_shared<DefaultInternalCodec>(DefaultInternalCodec());
  shared_ptr<ExternalCodec> e_cdec = make_shared<DefaultCodec>(DefaultCodec());

  // Construct an engine
  Engine engine = Engine(simulation, s_env, i_cdec, e_cdec);
  return make_shared<Engine>(engine);
}

}

FACT("CountPeers with 3 peers should count 3") {
  K3::peer1 = K3::make_address(K3::localhost, 3000);
  K3::peer2 = K3::make_address(K3::localhost, 3001);
  K3::peer3 = K3::make_address(K3::localhost, 3002);
  K3::rendezvous = K3::peer1;

  auto engine = K3::buildEngine();
  auto mp = K3::buildMP(engine);
  K3::Message m1 = K3::Message(K3::peer1, "join", "()");
  K3::Message m2 = K3::Message(K3::peer2, "join", "()");
  K3::Message m3 = K3::Message(K3::peer3, "join", "()");
  engine->send(m1);
  engine->send(m2);
  engine->send(m3);

  // Run Engine
  engine->runEngine(mp);

  // Check the result (the 6th fib number should = 8)
  Assert.Equal(3, K3::nodeCounter);
}
