#include <memory>

#include "spdlog/spdlog.h"

#include "core/Peer.hpp"
#include "core/Engine.hpp"
#include "network/NetworkManager.hpp"

namespace K3 {

Peer::Peer(const Address& addr, shared_ptr<ContextFactory> fac,
           const YAML::Node& peer_config,
           std::function<void()> ready_callback) {
  address_ = addr;
  start_processing_ = false;
  finished_ = false;
  logger_ = spdlog::get(addr.toString());
  if (!logger_) {
    logger_ = spdlog::stdout_logger_mt(addr.toString());
  }

  auto work = [this, fac, peer_config, ready_callback]() {
    queue_ = make_shared<Queue>();
    context_ = (*fac)();
    context_->__patch(peer_config);

    try {
      context_->initDecls(unit_t{});
      // Tell the engine that this peer is ready to process its role
      ready_callback();

      // The engine will flip the start_processing_ bool after
      // all peers have processed their role (sent initial message)
      while (!start_processing_) continue;

      while (true) {
        shared_ptr<Message> m = queue_->dequeue();

        if (logger_->level() == spdlog::level::trace) {
          auto it = ProgramContext::__trigger_names_.find(m->trigger());
          std::string trig = (it != ProgramContext::__trigger_names_.end()) ? it->second : "{Undefined Trigger}";
          logger_->trace() << "  [" << address_.toString() << "] Received:: @" << trig;
        }
        m->value()->dispatchIntoContext(context_.get(), m->trigger());
      }
    } catch (EndOfProgramException e) {
      finished_ = true;
      return;
    }
  };

  thread_ = make_shared<boost::thread>(work);
}

void Peer::processRole() { context_->processRole(unit_t{}); }

void Peer::start() {
  if (!context_) {
    throw std::runtime_error("Peer start(): null context ptr");
  }
  start_processing_ = true;
}

void Peer::enqueue(shared_ptr<Message> m) {
  if (!queue_) {
    throw std::runtime_error("Peer enqueue(): null queue ptr");
  }
  queue_->enqueue(std::move(m));
}

void Peer::join() {
  if (!thread_) {
    throw std::runtime_error("Peer join(): not running ");
  }

  thread_->join();
  return;
}

bool Peer::finished() { return finished_.load(); }

Address Peer::address() { return address_; }

shared_ptr<ProgramContext> Peer::getContext() { return context_; }

}  // namespace K3
