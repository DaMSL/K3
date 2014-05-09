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
typedef K3::Seq<int> int_seq;
typedef K3::Set<int> int_set;
typedef K3::Sorted<int> int_sorted;
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
  K3::received = std::shared_ptr<K3::int_c>(new K3::int_c(nullptr));
  K3::peer1 = K3::make_address(K3::localhost, 3000);
  K3::peer2 = K3::make_address(K3::localhost, 3001);
  K3::rendezvous = K3::peer1;
  std::list<K3::Address> peers = std::list<K3::Address>();
  peers.push_back(K3::peer1);
  peers.push_back(K3::peer2);
  K3::SystemEnvironment s_env = K3::defaultEnvironment(peers);

  auto engine = K3::buildEngine(true, s_env);
  auto mp = K3::buildMP(engine, K3::buildTable(engine));
  K3::Message m2 = K3::Message(K3::peer2, "sendCollection", "()");
  engine->send(m2);

  // Run Engine
  engine->runEngine(mp);
  std::shared_ptr<int> i;

  // Check that the received collection is equal to the sent collection
  while (i = K3::sent.peek()) {
    std::shared_ptr<int> i2 = K3::received->peek();
    Assert.Equal(*i, *i2);
    K3::received->erase(*i);
    K3::sent.erase(*i);
  }
  bool actual = K3::sent.peek() == NULL;
  Assert.Equal(true, actual);
}

FACT("Collection send network mode") {
  K3::received = std::shared_ptr<K3::int_c>(new K3::int_c(nullptr));
  using std::shared_ptr;
  using boost::thread;
  using boost::thread_group;
  // Create peers
  K3::peer1 = K3::make_address(K3::localhost, 3000);
  K3::peer2 = K3::make_address(K3::localhost, 3001);
  K3::rendezvous = K3::peer1;
  // Create engines
  auto engine1 = K3::buildEngine(false, K3::defaultEnvironment(K3::peer1));
  auto engine2 = K3::buildEngine(false, K3::defaultEnvironment(K3::peer2));
  // Create MPs
  auto mp1 = K3::buildMP(engine1, K3::buildTable(engine1));
  auto mp2 = K3::buildMP(engine2, K3::buildTable(engine2));
  // Create initial messages (source)
  K3::Message m2 = K3::Message(K3::peer2, "sendCollection", "()");

  engine2->send(m2);

  // Fork a thread for each engine
  auto service_threads = std::shared_ptr<thread_group>(new thread_group());
  shared_ptr<thread> t1 = engine1->forkEngine(mp1);
  shared_ptr<thread> t2 = engine2->forkEngine(mp2);

  service_threads->add_thread(t1.get());
  service_threads->add_thread(t2.get());

  int timeout = 2;
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
  while (i = K3::sent.peek()) {
    std::shared_ptr<int> i2 = K3::received->peek();
    Assert.Equal(*i, *i2);
    K3::received->erase(*i);
    K3::sent.erase(*i);
  }
  bool actual = K3::sent.peek() == NULL;
  Assert.Equal(true, actual);
}


FACT("Collection Insert/Erase/Update/Peek") {
  K3::int_c c = K3::int_c(nullptr);
  Assert.Equal(nullptr, c.peek());
  c.insert(1);
  c.insert(2);
  c.insert(3);
  c.insert(4);
  c.insert(5);
  auto first = *(c.peek());
  Assert.Equal(1, first);
  c.update(3,1);
  c.erase(2);
  c.erase(1);
  first = *(c.peek());
  Assert.Equal(1, first);
  c.update(4,1);
  c.erase(1);
  c.erase(1);
  first = *(c.peek());
  Assert.Equal(5, first);
  c.erase(5);
  Assert.Equal(nullptr, c.peek());
  c.erase(5);
}

FACT("Seq Insert/Erase/Update/Peek") {
  K3::int_seq c = K3::int_seq(nullptr);
  Assert.Equal(nullptr, c.peek());
  c.insert(1);
  c.insert(2);
  c.insert(3);
  c.insert(4);
  c.insert(5);
  auto first = *(c.peek());
  Assert.Equal(1, first);
  c.update(3,1);
  c.erase(2);
  c.erase(1);
  first = *(c.peek());
  Assert.Equal(1, first);
  c.update(4,1);
  c.erase(1);
  c.erase(1);
  first = *(c.peek());
  Assert.Equal(5, first);
  c.erase(5);
  Assert.Equal(nullptr, c.peek());
  c.erase(5);
}

FACT("Set Insert/Erase/Update/Peek/Member") {
  K3::int_set c = K3::int_set(nullptr);
  Assert.Equal(nullptr, c.peek());
  c.insert(1);
  c.insert(1);
  c.insert(2);
  c.insert(3);
  c.insert(2);
  c.insert(4);
  c.insert(5);
  c.insert(5);
  c.insert(1);
  Assert.Equal(true, c.member(5));
  Assert.Equal(true, c.member(3));
  Assert.Equal(false, c.member(0));
  c.update(3,1);
  Assert.Equal(false, c.member(3));
  c.erase(2);
  c.erase(1);
  Assert.Equal(false, c.member(2));
  Assert.Equal(false, c.member(1));
  c.update(4,1);
  c.erase(1);
  auto first = *(c.peek());
  Assert.Equal(5, first);
  c.erase(5);
  Assert.Equal(nullptr, c.peek());
  c.erase(5);
}

FACT("Sorted Insert/Erase/Update/Peek") {
  K3::int_sorted c = K3::int_sorted(nullptr);
  Assert.Equal(nullptr, c.peek());
  c.insert(3);
  c.insert(1);
  c.insert(2);
  c.insert(5);
  c.insert(4);
  auto first = *(c.peek());
  Assert.Equal(1, first);
  c.update(3,1);
  c.erase(2);
  c.erase(1);
  first = *(c.peek());
  Assert.Equal(1, first);
  c.update(4,1);
  c.erase(1);
  c.erase(1);
  first = *(c.peek());
  Assert.Equal(5, first);
  c.erase(5);
  Assert.Equal(nullptr, c.peek());
  c.erase(5);
}

FACT("Collection Split/Combine") {
  K3::int_c c = K3::int_c(nullptr);
  c.insert(1);
  c.insert(2);
  c.insert(3);
  c.insert(4);
  auto tup = c.split();
  auto p1 = std::get<0>(tup);
  auto p2 = std::get<1>(tup);
  Assert.Equal(1, *(p1.peek()));
  Assert.Equal(3, *(p2.peek()));
  p1.erase(1);
  p2.erase(3);
  auto c2 = p1.combine(p2);
  Assert.Equal(2, *(c2.peek()));
  c2.erase(2);
  Assert.Equal(4, *(c2.peek()));
}

FACT("Seq Split/Combine") {
  K3::int_seq c = K3::int_seq(nullptr);
  c.insert(1);
  c.insert(2);
  c.insert(3);
  c.insert(4);
  auto tup = c.split();
  auto p1 = std::get<0>(tup);
  auto p2 = std::get<1>(tup);
  Assert.Equal(1, *(p1.peek()));
  Assert.Equal(3, *(p2.peek()));
  p1.erase(1);
  p2.erase(3);
  auto c2 = p1.combine(p2);
  Assert.Equal(2, *(c2.peek()));
  c2.erase(2);
  Assert.Equal(4, *(c2.peek()));
}

FACT("Set Split/Combine") {
  K3::int_set c = K3::int_set(nullptr);
  c.insert(1);
  c.insert(2);
  c.insert(3);
  c.insert(4);
  auto tup = c.split();
  auto p1 = std::get<0>(tup);
  auto p2 = std::get<1>(tup);
  p1.erase(1);
  p1.erase(3);
  p2.erase(1);
  p2.erase(3);

  auto c2 = p1.combine(p2);
  Assert.Equal(true, c2.member(2));
  c2.erase(2);
  Assert.Equal(4, *(c2.peek()));

}

FACT("Sorted Split/Combine") {
  K3::int_sorted c = K3::int_sorted(nullptr);
  c.insert(1);
  c.insert(2);
  c.insert(3);
  c.insert(4);
  auto tup = c.split();
  auto p1 = std::get<0>(tup);
  auto p2 = std::get<1>(tup);
  Assert.Equal(1, *(p1.peek()));
  Assert.Equal(3, *(p2.peek()));
  p1.erase(1);
  p2.erase(3);
  auto c2 = p1.combine(p2);
  Assert.Equal(2, *(c2.peek()));
  c2.erase(2);
  Assert.Equal(4, *(c2.peek()));
}

bool isEven(int x) { return (x % 2) == 0; };

FACT("Collection Map") {
  K3::Collection<K3::ListDS,int> c = K3::Collection<K3::ListDS,int>(nullptr);
  c.insert(1);
  c.insert(2);
  c.insert(3);
  c.insert(4);
  K3::Collection<K3::ListDS,bool> c2 = c.map(isEven);
  Assert.Equal(false, *(c2.peek()));
  c2.erase(false);
  Assert.Equal(true, *(c2.peek()));
}

FACT("Seq Map") {
  K3::Seq<int> c = K3::Seq<int>(nullptr);
  c.insert(1);
  c.insert(2);
  c.insert(3);
  c.insert(4);
  K3::Seq<bool> c2 = c.map(isEven);
  Assert.Equal(false, *(c2.peek()));
  c2.erase(false);
  Assert.Equal(true, *(c2.peek()));

}

FACT("Set Map") {
  K3::Set<int> c = K3::Set<int>(nullptr);
  c.insert(1);
  c.insert(2);
  c.insert(3);
  c.insert(4);
  K3::Set<bool> c2 = c.map(isEven);
  Assert.Equal(true, c2.member(false));
  c2.erase(false);
  Assert.Equal(true, *(c2.peek()));
}

FACT("Sorted Map") {
  K3::Sorted<int> c = K3::Sorted<int>(nullptr);
  c.insert(1);
  c.insert(2);
  c.insert(3);
  c.insert(4);
  K3::Sorted<bool> c2 = c.map(isEven);
  Assert.Equal(false, *(c2.peek()));
  c2.erase(false);
  c2.erase(false);
  Assert.Equal(true, *(c2.peek()));
}

FACT("Collection Filter") {
  K3::int_c c = K3::int_c(nullptr);
  c.insert(1);
  c.insert(2);
  c.insert(3);
  c.insert(4);
  K3::int_c c2 = c.filter(isEven);
  Assert.Equal(2, *(c2.peek()));
  c2.erase(2);
  Assert.Equal(4, *(c2.peek()));
}

FACT("Seq Filter") {
  K3::int_seq c = K3::int_seq(nullptr);
  c.insert(1);
  c.insert(2);
  c.insert(3);
  c.insert(4);
  K3::int_seq c2 = c.filter(isEven);
  Assert.Equal(2, *(c2.peek()));
  c2.erase(2);
  Assert.Equal(4, *(c2.peek()));
}

FACT("Set Filter") {
  K3::int_set c = K3::int_set(nullptr);
  c.insert(1);
  c.insert(2);
  c.insert(3);
  c.insert(4);
  K3::int_set c2 = c.filter(isEven);
  Assert.Equal(2, *(c2.peek()));
  c2.erase(2);
  Assert.Equal(4, *(c2.peek()));
}

FACT("Sorted Filter") {
  K3::int_sorted c = K3::int_sorted(nullptr);
  c.insert(1);
  c.insert(2);
  c.insert(3);
  c.insert(4);
  K3::int_sorted c2 = c.filter(isEven);
  Assert.Equal(2, *(c2.peek()));
  c2.erase(2);
  Assert.Equal(4, *(c2.peek()));
}

int summer(int acc, int x) { return acc+x; };

FACT("Collection Fold") {
  K3::int_c c = K3::int_c(nullptr);
  c.insert(1);
  c.insert(2);
  c.insert(3);
  c.insert(4);
  int acc  = c.fold<int>(summer, 0);
  Assert.Equal(10, acc);
}

FACT("Seq Fold") {
  K3::int_seq c = K3::int_seq(nullptr);
  c.insert(1);
  c.insert(2);
  c.insert(3);
  c.insert(4);
  int acc  = c.fold<int>(summer, 0);
  Assert.Equal(10, acc);
}

FACT("Set Fold") {
  K3::int_set c = K3::int_set(nullptr);
  c.insert(1);
  c.insert(2);
  c.insert(3);
  c.insert(4);
  int acc  = c.fold<int>(summer, 0);
  Assert.Equal(10, acc);
}

FACT("Sorted Fold") {
  K3::int_sorted c = K3::int_sorted(nullptr);
  c.insert(1);
  c.insert(2);
  c.insert(3);
  c.insert(4);
  int acc  = c.fold<int>(summer, 0);
  Assert.Equal(10, acc);
}

FACT("Collection Group By") {
  K3::Collection<K3::ListDS, int> c = K3::Collection<K3::ListDS, int>(nullptr);
  c.insert(1);
  c.insert(2);
  c.insert(3);
  c.insert(4);
  K3::Collection<K3::ListDS, std::tuple<bool,int>> c2 = c.group_by(isEven,summer,0);
  auto tup = *(c2.peek());
  Assert.Equal(4, std::get<1>(tup));
  c2.erase(tup);
  tup = *(c2.peek());
  Assert.Equal(6, std::get<1>(tup));
}

FACT("Seq Group By") {
  K3::Seq<int> c = K3::Seq<int>(nullptr);
  c.insert(1);
  c.insert(2);
  c.insert(3);
  c.insert(4);
  K3::Seq<std::tuple<bool,int>> c2 = c.group_by(isEven,summer,0);
  auto tup = *(c2.peek());
  Assert.Equal(4, std::get<1>(tup));
  c2.erase(tup);
  tup = *(c2.peek());
  Assert.Equal(6, std::get<1>(tup));
}

// Cant have an unordered set of tuples! need a hash function.
// FACT("Set Group By") {
//   K3::Set<int> c = K3::Set<int>(nullptr);
//   c.insert(1);
//   c.insert(2);
//   c.insert(3);
//   c.insert(4);

//   K3::Set<std::tuple<bool,int>> c2 = K3::Set<std::tuple<bool,int>>(nullptr);
// }

FACT("Sorted Group By") {
  K3::Sorted<int> c = K3::Sorted<int>(nullptr);
  c.insert(1);
  c.insert(2);
  c.insert(3);
  c.insert(4);
  K3::Sorted<std::tuple<bool,int>> c2 = c.group_by(isEven,summer,0);
  auto tup = *(c2.peek());
  Assert.Equal(4, std::get<1>(tup));
  c2.erase(tup);
  tup = *(c2.peek());
  Assert.Equal(6, std::get<1>(tup));
}

//TODO nullptr! need a handle to the engine
template <template <class> class DS>
K3::Collection<DS,int> expand(int x ) { 
  K3::Collection<DS, int> result = K3::Collection<DS, int>(nullptr);
  for (int i = 0;i < x; i++) {
    result.insert(i);
  }
  return result;
}; 

FACT("Collection Ext") {
  K3::Collection<K3::ListDS, int> c = K3::Collection<K3::ListDS, int>(nullptr);
  c.insert(1);
  c.insert(2);
  K3::Collection<K3::ListDS, int> c2 = c.ext(expand<K3::ListDS>);
  Assert.Equal(0, *(c2.peek()));
  c2.erase(0);
  Assert.Equal(0, *(c2.peek()));
  c2.erase(0);
  Assert.Equal(1, *(c2.peek()));

}

FACT("Seq Ext") {
  K3::Seq<int> c = K3::Seq<int>(nullptr);
  c.insert(1);
  c.insert(2);
  K3::Seq<int> c2 = c.ext(expand<K3::ListDS>);
  Assert.Equal(0, *(c2.peek()));
  c2.erase(0);
  Assert.Equal(0, *(c2.peek()));
  c2.erase(0);
  Assert.Equal(1, *(c2.peek()));
}

FACT("Set Ext") {
  K3::Set<int> c = K3::Set<int>(nullptr);
  c.insert(1);
  c.insert(2);
  K3::Set<int> c2 = c.ext(expand<K3::SetDS>);
  Assert.Equal(0, *(c2.peek()));
  c2.erase(0);
  Assert.Equal(1, *(c2.peek()));
}

FACT("Sorted Ext") {
  K3::Sorted<int> c = K3::Sorted<int>(nullptr);
  c.insert(1);
  c.insert(2);
  K3::Sorted<int> c2 = c.ext(expand<K3::SortedDS>);
  Assert.Equal(0, *(c2.peek()));
  c2.erase(0);
  Assert.Equal(0, *(c2.peek()));
  c2.erase(0);
  Assert.Equal(1, *(c2.peek()));
}

