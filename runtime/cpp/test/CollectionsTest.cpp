#include <Common.hpp>
#include <Engine.hpp>
#include <Dispatch.hpp>
#include <MessageProcessor.hpp>
#include <dataspace/StlDS.hpp>
#include <Serialization.hpp>
#include <Collections.hpp>
#include <test/TestUtils.hpp>

#include <functional>
#include <iostream>
#include <sstream>
#include <list>
#include <set>
#include <string>

#include <boost/serialization/list.hpp>
#include <boost/serialization/set.hpp>
#include <boost/regex.hpp>
#include <boost/thread/thread.hpp>

#include <xUnit++/xUnit++.h>

namespace K3 {

using std::cout;
using std::endl;
using std::bind;
using std::string;
using std::shared_ptr;

// Int collections
typedef K3::Collection<ListDS,int> int_c;
int_c sent = int_c(nullptr);
shared_ptr<int_c> received = shared_ptr<int_c>(new int_c(nullptr));

// "Trigger" Declarations
// Build a collection and send it to the "receiveCollection" trigger
void sendCollection(shared_ptr<Engine> engine, string message_contents) {
  K3::Collection<ListDS, int> c = K3::Collection<ListDS, int>(nullptr);
  for (int i =0; i < 100; i++) {
    c.insert(i);
  }

  std::string s = K3::BoostSerializer::pack(c);
  Message m = Message(K3::rendezvous, "receiveCollection", s);
  engine->send(m);
  sent = c;
}

// Unpack the collection and store it globally
void receiveCollection(shared_ptr<Engine> engine, string message_contents) {
  std::shared_ptr<int_c> p = K3::BoostSerializer::unpack_with_engine<int_c>(message_contents, nullptr);
  received = p;
}

TriggerDispatch buildTable(shared_ptr<Engine> engine) {
  TriggerDispatch table = TriggerDispatch();
  table["sendCollection"] = bind(&K3::sendCollection, engine, std::placeholders::_1);
  table["receiveCollection"] = bind(&K3::receiveCollection, engine, std::placeholders::_1);
  return table;
}

}

FACT("Collection Send simulation mode") { 
  K3::peer1 = K3::make_address(K3::localhost, 3000);
  K3::peer2 = K3::make_address(K3::localhost, 3001);
  K3::rendezvous = K3::peer1;
  std::list<K3::Address> peers = std::list<K3::Address>();
  peers.push_back(K3::peer1);
  peers.push_back(K3::peer2);
  K3::SystemEnvironment s_env = K3::defaultEnvironment(peers);

  auto engine = K3::buildEngine(true, s_env);
  auto mp = K3::buildMP(engine, buildTable(engine));
  K3::Message m2 = K3::Message(K3::peer2, "join", "()");
  engine->send(m2);

  // Run Engine
  engine->runEngine(mp);
  std::shared_ptr<int> i;

  // Check that the received collection is equal to the sent collection
  while (i = K3::received->peek()) {
    std::shared_ptr<int> i2 = K3::sent.peek();
    Assert.Equal(*i2, *i);
    K3::received->erase(*i);
    K3::sent.erase(*i);
  }
  bool actual = K3::sent.peek() == NULL;
  Assert.Equal(true, actual);
}

FACT("Collection send network mode") {
  using std::shared_ptr;
  using boost::thread;
  using boost::thread_group;
  // Create peers
  K3::peer1 = K3::make_address(K3::localhost, 3000);
  K3::peer2 = K3::make_address(K3::localhost, 3005);
  K3::rendezvous = K3::peer1;
  // Create engines
  auto engine1 = K3::buildEngine(false, K3::defaultEnvironment(K3::peer1));
  auto engine2 = K3::buildEngine(false, K3::defaultEnvironment(K3::peer2));
  // Create MPs
  auto mp1 = K3::buildMP(engine1, buildTable(engine1));
  auto mp2 = K3::buildMP(engine2, countPeersTable(engine2));
  // Create initial messages (source)
  K3::Message m2 = K3::Message(K3::peer2, "join", "()");

  engine2->send(m2);

  // Fork a thread for each engine
  auto service_threads = std::shared_ptr<thread_group>(new thread_group());
  shared_ptr<thread> t1 = engine1->forkEngine(mp1);
  shared_ptr<thread> t2 = engine2->forkEngine(mp2);

  service_threads->add_thread(t1.get());
  service_threads->add_thread(t2.get());

  int timeout = 3;
  int n = 0;
  while (n < timeout) {
    boost::this_thread::sleep_for( boost::chrono::seconds(1) );
    n++;
  }

  engine1->forceTerminateEngine();
  engine2->forceTerminateEngine();

  service_threads->join_all();
  service_threads->remove_thread(t1.get());
  service_threads->remove_thread(t2.get());

  // Check that the received collection is equal to the sent collection
  std::shared_ptr<int> i;
  while (i = K3::received->peek()) {
    std::shared_ptr<int> i2 = K3::sent.peek();
    Assert.Equal(*i2, *i);
    K3::received->erase(*i);
    K3::sent.erase(*i);
  }
  bool actual = K3::sent.peek() == NULL;
  Assert.Equal(true, actual);
}


