#ifndef K3_ENGINE
#define K3_ENGINE

#include <list>
#include <map>
#include <memory>

#include "Common.hpp"
#include "Peer.hpp"

using std::list;
using std::map;
using std::unique_ptr;
using std::make_unique;
class Message;

// TODO(jbw) make the peers_ map const after initialization
// Singleton class.
class Engine {
 public:
  static Engine& getInstance();
  void initialize(const list<Address>& peer_addrs, ContextConstructor c);
  void send(unique_ptr<Message> m);
  void run();
  void sendSentinel();
  void join();
  void cleanup();
  Peer* getPeer(const Address& a);
  void registerPeer();
  ContextConstructor* getContextConstructor();

 private:
  // No copies of Singleton
  Engine() { }
  Engine(const Engine&) = delete;
  void operator=(const Engine&) = delete;
  map<Address, unique_ptr<Peer>> peers_;
  int num_total_peers_;
  std::atomic<int> num_ready_peers_;
  unique_ptr<ContextConstructor> context_constructor_;
};

#endif
