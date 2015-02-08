#include <Common.hpp>
#include <Engine.hpp>
#include <Dispatch.hpp>
#include <MessageProcessor.hpp>
#include <test/TestUtils.hpp>

#include <functional>
#include <iostream>
#include <sstream>

#include <boost/regex.hpp>
#include <boost/thread/thread.hpp>

#include <xUnit++/xUnit++.h>

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
  auto table = countPeersTable(engine);
  auto mp = K3::buildMP(engine,table);
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
  auto mp1 = K3::buildMP(engine1,countPeersTable(engine1));
  auto mp2 = K3::buildMP(engine2,countPeersTable(engine2));
  auto mp3 = K3::buildMP(engine3,countPeersTable(engine3));
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

  int timeout = 3;
  int i = 0;
  int desired = 3;
  while ((K3::nodeCounter < desired) && i < timeout) {
    boost::this_thread::sleep_for( boost::chrono::seconds(1) );
    i++;
  }
  engine1->forceTerminateEngine();
  engine2->forceTerminateEngine();
  engine3->forceTerminateEngine();
  service_threads->join_all();
  service_threads->remove_thread(t1.get());
  service_threads->remove_thread(t2.get());
  service_threads->remove_thread(t3.get());

  // Check the result
  Assert.Equal(desired, K3::nodeCounter);
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
  auto mp1 = K3::buildMP(engine1, countPeersTable(engine1));
  auto mp2 = K3::buildMP(engine2, countPeersTable(engine2));
  auto mp3 = K3::buildMP(engine3, countPeersTable(engine3));
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

  int timeout = 600;
  int i =0;
  int desired = 300000;

  shared_ptr<thread> t1 = engine1->forkEngine(mp1);
  shared_ptr<thread> t2 = engine2->forkEngine(mp2);
  shared_ptr<thread> t3 = engine3->forkEngine(mp3);

  service_threads->add_thread(t1.get());
  service_threads->add_thread(t2.get());
  service_threads->add_thread(t3.get());

  while ((K3::nodeCounter < desired) && i < timeout) {
    boost::this_thread::sleep_for( boost::chrono::seconds(1) );
    i++;
  }

  engine1->forceTerminateEngine();
  engine2->forceTerminateEngine();
  engine3->forceTerminateEngine();
  service_threads->join_all();
  service_threads->remove_thread(t1.get());
  service_threads->remove_thread(t2.get());
  service_threads->remove_thread(t3.get());

  // Check the result
  Assert.Equal(desired, K3::nodeCounter);
}
