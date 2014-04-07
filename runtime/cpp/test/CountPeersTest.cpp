#include <Common.hpp>
#include <Engine.hpp>
#include <Dispatch.hpp>
#include <MessageProcessor.hpp>

#include <functional>
#include <iostream>
#include <sstream>

#include <boost/regex.hpp>
#include <boost/thread/thread.hpp>

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
shared_ptr<Engine> buildEngine(bool simulation, SystemEnvironment s_env) {
  // Configure engine components
  shared_ptr<InternalCodec> i_cdec = make_shared<LengthHeaderInternalCodec>(LengthHeaderInternalCodec());

  // Construct an engine
  Engine engine = Engine(simulation, s_env, i_cdec);
  return make_shared<Engine>(engine);
}

}

FACT("Simulation mode CountPeers with 3 peers should count 3") {
  K3::nodeCounter = 0;
  K3::peer1 = K3::make_address(K3::localhost, 3000);
  K3::peer2 = K3::make_address(K3::localhost, 3001);
  K3::peer3 = K3::make_address(K3::localhost, 3002);
  K3::rendezvous = K3::peer1;
  std::list<K3::Address> peers = std::list<K3::Address>();
  peers.push_back(K3::peer1);
  peers.push_back(K3::peer2);
  peers.push_back(K3::peer3);
  K3::SystemEnvironment s_env = K3::defaultEnvironment(peers);

  auto engine = K3::buildEngine(true, s_env);
  auto mp = K3::buildMP(engine);
  K3::Message m1 = K3::Message(K3::peer1, "join", "()");
  K3::Message m2 = K3::Message(K3::peer2, "join", "()");
  K3::Message m3 = K3::Message(K3::peer3, "join", "()");
  engine->send(m1);
  engine->send(m2);
  engine->send(m3);

  // Run Engine
  engine->runEngine(mp);

  // Check the result
  Assert.Equal(3, K3::nodeCounter);
}

FACT("Network mode CountPeers with 3 peers should count 3") {
  K3::nodeCounter = 0;
  using std::shared_ptr;
  using boost::thread;
  using boost::thread_group;
  // Create peers
  K3::peer1 = K3::make_address(K3::localhost, 3000);
  K3::peer2 = K3::make_address(K3::localhost, 3005);
  K3::peer3 = K3::make_address(K3::localhost, 3002);
  K3::rendezvous = K3::peer1;
  // Create engines
  auto engine1 = K3::buildEngine(false, K3::defaultEnvironment(K3::peer1));
  auto engine2 = K3::buildEngine(false, K3::defaultEnvironment(K3::peer2));
  auto engine3 = K3::buildEngine(false, K3::defaultEnvironment(K3::peer3));
  // Create MPs
  auto mp1 = K3::buildMP(engine1);
  auto mp2 = K3::buildMP(engine2);
  auto mp3 = K3::buildMP(engine3);
  // Create initial messages (source)
  K3::Message m1 = K3::Message(K3::peer1, "join", "()");
  K3::Message m2 = K3::Message(K3::peer2, "join", "()");
  K3::Message m3 = K3::Message(K3::peer3, "join", "()");
  engine1->send(m1);
  engine2->send(m2);
  engine3->send(m3);
  // Fork a thread for each engine
  auto service_threads = std::shared_ptr<thread_group>(new thread_group());
  shared_ptr<thread> t1 = engine1->forkEngine(mp1);
  shared_ptr<thread> t2 = engine2->forkEngine(mp2);
  shared_ptr<thread> t3 = engine3->forkEngine(mp3);

  service_threads->add_thread(t1.get());
  service_threads->add_thread(t2.get());
  service_threads->add_thread(t3.get());

  boost::this_thread::sleep_for( boost::chrono::seconds(3) );
  engine1->forceTerminateEngine();
  engine2->forceTerminateEngine();
  engine3->forceTerminateEngine();
  service_threads->join_all();
  service_threads->remove_thread(t1.get());
  service_threads->remove_thread(t2.get());
  service_threads->remove_thread(t3.get());

  // Check the result
  Assert.Equal(3, K3::nodeCounter);
}

FACT("Network mode CountPeers with 100k messages per 3 peers should count 300k") {
  K3::nodeCounter = 0;
  using boost::thread;
  using boost::thread_group;
  using std::shared_ptr;
  // Create peers
  K3::peer1 = K3::make_address(K3::localhost, 3000);
  K3::peer2 = K3::make_address(K3::localhost, 3005);
  K3::peer3 = K3::make_address(K3::localhost, 3002);
  K3::rendezvous = K3::peer1;
  // Create engines
  auto engine1 = K3::buildEngine(false, K3::defaultEnvironment(K3::peer1));
  auto engine2 = K3::buildEngine(false, K3::defaultEnvironment(K3::peer2));
  auto engine3 = K3::buildEngine(false, K3::defaultEnvironment(K3::peer3));
  // Create MPs
  auto mp1 = K3::buildMP(engine1);
  auto mp2 = K3::buildMP(engine2);
  auto mp3 = K3::buildMP(engine3);
  // Create initial messages (source)
  K3::Message m1 = K3::Message(K3::peer1, "join", "()");
  K3::Message m2 = K3::Message(K3::peer2, "join", "()");
  K3::Message m3 = K3::Message(K3::peer3, "join", "()");
  for (int i = 0; i < 100000; i++) {
    engine1->send(m1);
    engine2->send(m2);
    engine3->send(m3);
  }
  // Fork a thread for each engine
  auto service_threads = std::shared_ptr<thread_group>(new thread_group());
  shared_ptr<thread> t1 = engine1->forkEngine(mp1);
  shared_ptr<thread> t2 = engine2->forkEngine(mp2);
  shared_ptr<thread> t3 = engine3->forkEngine(mp3);

  service_threads->add_thread(t1.get());
  service_threads->add_thread(t2.get());
  service_threads->add_thread(t3.get());

  boost::this_thread::sleep_for( boost::chrono::seconds(15) );
  engine1->forceTerminateEngine();
  engine2->forceTerminateEngine();
  engine3->forceTerminateEngine();
  service_threads->join_all();
  service_threads->remove_thread(t1.get());
  service_threads->remove_thread(t2.get());
  service_threads->remove_thread(t3.get());

  // Check the result
  Assert.Equal(300000, K3::nodeCounter);
}
