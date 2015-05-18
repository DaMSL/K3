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
template <class ContextImpl>
class Engine {
 public:
  static Engine<ContextImpl>& getInstance() {
    static Engine<ContextImpl> instance;
    return instance;
  }

  void initialize(const list<Address>& peer_addrs) {
    for (auto addr : peer_addrs) {
      peers_[addr] = make_unique<Peer<ContextImpl>>(true);
    }
    num_total_peers_ = peer_addrs.size();
  }

  void send(unique_ptr<Message> m) {
    auto it = peers_.find(m->destination());
    if (it != peers_.end()) {
      it->second->enqueue(std::move(m));
    } else {
      throw std::runtime_error("Engine cannot send non-local messages yet!");
    }
  }

  void run() {
    for (auto& it : peers_) {
      it.second->run([this] () {
        registerPeer();
      });
    }

    // Busy-wait until all peers are ready
    while (num_total_peers_ > num_ready_peers_.load()) continue;

    return;
  }

  void join() {
    for (auto& it : peers_) {
      it.second->join();
    }
  }

  void cleanup() {
    peers_ = map<Address, unique_ptr<Peer<ContextImpl>>>();
    num_total_peers_ = 0;
    num_ready_peers_.store(0);
  }

  Peer<ContextImpl>* getPeer(const Address& a) {
    Peer<ContextImpl>* p;
    auto it = peers_.find(a);
    if (it != peers_.end()) {
      p = it->second.get();
    }
    return p;
  }

  void registerPeer() {
    num_ready_peers_++;
  }

 private:
  // No copies of Singleton
  Engine() { }
  Engine(const Engine&) = delete;
  void operator=(const Engine&) = delete;
  map<Address, unique_ptr<Peer<ContextImpl>>> peers_;
  int num_total_peers_;
  std::atomic<int> num_ready_peers_;
};

#endif
