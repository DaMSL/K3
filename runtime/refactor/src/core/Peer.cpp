#include <spdlog/spdlog.h>

#include <memory>
#include <string>
#include <sstream>

#include "core/Peer.hpp"
#include "core/Engine.hpp"
#include "network/NetworkManager.hpp"

namespace K3 {

Peer::Peer(const Address& addr, shared_ptr<ContextFactory> fac,
           const YAML::Node& peer_config, std::function<void()> ready_callback,
           const string& json_path, bool json_final_only) {
  logger_ = spdlog::get(addr.toString());
  if (!logger_) {
    logger_ = spdlog::stdout_logger_mt(addr.toString());
  }
  address_ = addr;
  start_processing_ = false;
  finished_ = false;
  if (json_path != "") {
    json_globals_log_ = make_shared<std::ofstream>(
        json_path + "/" + address_.toString() + "_Globals.dsv");
    json_messages_log_ = make_shared<std::ofstream>(
        json_path + "/" + address_.toString() + "_Messages.dsv");
  }
  json_final_state_only_ = json_final_only;

  // Create work to run in new thread
  auto work = [this, fac, peer_config, ready_callback]() {
    queue_ = make_shared<Queue>();
    batch_.resize(1000);
    context_ = (*fac)();
    context_->__patch(peer_config);
    context_->initDecls(unit_t{});

    // Signal that peer is initialized, wait for 'go' signal
    ready_callback();
    while (!start_processing_) continue;

    try {
      while (true) {
        processBatch();
      }
    } catch (EndOfProgramException e) {
      finished_ = true;
      logFinalState();
    } catch (const std::exception& e) {
      logger_->error() << "Peer failed: " << e.what();
    }
  };
  thread_ = make_shared<std::thread>(work);
}

void Peer::processRole() {
  if (!context_) {
    throw std::runtime_error("Peer processRole(): null context ptr");
  }
  context_->processRole(unit_t{});
}

void Peer::start() { start_processing_ = true; }

void Peer::join() {
  if (!thread_) {
    throw std::runtime_error("Peer join(): not running ");
  }
  thread_->join();
  return;
}

void Peer::enqueue(std::unique_ptr<Dispatcher> d) {
  if (!queue_) {
    throw std::runtime_error("Peer enqueue(): null queue ptr");
  }
  queue_->enqueue(std::move(d));
}

bool Peer::finished() { return finished_.load(); }

Address Peer::address() { return address_; }

shared_ptr<ProgramContext> Peer::getContext() { return context_; }

void Peer::processBatch() {
  size_t num = queue_->dequeueBulk(batch_);
  for (int i = 0; i < num; i++) {
    auto d = std::move(batch_[i]);
    //logMessage(*m);
    (*d)();
    //logGlobals(*m);
  }
}

void Peer::logMessage(const Message& m) {
  if (logger_->level() <= spdlog::level::trace) {
    string trig = ProgramContext::__triggerName(m.trigger());
    logger_->trace() << "Received:: @" << trig;
  }
}

void Peer::logGlobals(const Message& m) {
  if (logger_->level() <= spdlog::level::trace) {
    std::ostringstream oss;
    string trig = ProgramContext::__triggerName(m.trigger());
    oss << "Processed:: @" << trig << std::endl;
    oss << "Environment: " << std::endl;
    bool first = true;
    for (const auto& it : context_->__prettify()) {
      if (!first) {
        oss << std::endl;
      } else {
        first = false;
      }
      oss << std::setw(10) << it.first << ": " << it.second;
    }
    logger_->trace() << oss.str();
  }

  if (json_globals_log_ && !json_final_state_only_) {
    for (const auto& it : context_->__jsonify()) {
      *json_globals_log_ << it.first << "|" << it.second << std::endl;
    }
  }
}

void Peer::logFinalState() {
  if (json_globals_log_ && json_final_state_only_) {
    for (const auto& it : context_->__jsonify()) {
      *json_globals_log_ << it.first << "|" << it.second << std::endl;
    }
  }
}

}  // namespace K3
