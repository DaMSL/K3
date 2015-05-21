#ifndef K3_ENGINE
#define K3_ENGINE

#include <atomic>
#include <list>
#include <map>
#include <memory>
#include <string>

#include "Common.hpp"
#include "ProgramContext.hpp"

class Peer;

// TODO(jbw) make the peers_ map const after initialization
// Singleton class.
class Engine {
 public:
  // Core
  static Engine& getInstance();
  void run(const list<string>& peer_configs, shared_ptr<ContextFactory> f);
  void stop();
  void join();
  void send(const MessageHeader& m, shared_ptr<NativeValue> v, shared_ptr<Codec> cdec);

  // Utilities
  void toggleLocalSend(bool enable);
  shared_ptr<Peer> getPeer(const Address& addr);
  bool running();

 private:
  // No copies of Singleton
  Engine() { }
  Engine(const Engine&) = delete;
  void operator=(const Engine&) = delete;

  map<Address, shared_ptr<Peer>> peers_;
  shared_ptr<ContextFactory> context_factory_;
  int total_peers_;
  std::atomic<int> ready_peers_;
  std::atomic<bool> running_;
  bool local_sends_enabled_;
};

#endif
