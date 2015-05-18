#include <list>

#include "Common.hpp"
#include "Engine.hpp"
#include "Value.hpp"

int one_dir_num = 1000000;

Address a1 = make_address("127.0.0.1", 30000);
Address a2 = make_address("127.0.0.1", 40000);

void setupEngine() {
  list<Address> peer_addrs;
  peer_addrs.push_back(a1);
  peer_addrs.push_back(a2);

  Engine& engine = Engine::getInstance();
  ContextConstructor c = [] () { return make_unique<DummyContext>(); };
  engine.initialize(peer_addrs, c);
  engine.run();
}

void stopEngine() {
  Engine::getInstance().sendSentinel();
  Engine::getInstance().join();
  Engine::getInstance().cleanup();
}

void oneDirectionalSends() {
  setupEngine();
  unique_ptr<Message> m;
  for (int i = 0; i < one_dir_num; i++) {
    m = make_unique<Message>(a2, a1, 1, make_unique<TNativeValue<int>>(i));
    Engine::getInstance().send(std::move(m));
  }
  stopEngine();
}

int main() {
  oneDirectionalSends();
}
