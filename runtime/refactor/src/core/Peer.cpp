#include <memory>
#include <string>
#include <sstream>

#include "spdlog/spdlog.h"

#include "core/Peer.hpp"
#include "core/Engine.hpp"
#include "network/NetworkManager.hpp"

namespace K3 {

Peer::Peer(const Address& addr, shared_ptr<ContextFactory> fac,
           const YAML::Node& peer_config, std::function<void()> ready_callback,
           const string& json_path, bool json_final_only) {
  address_ = addr;
  start_processing_ = false;
  finished_ = false;
  logger_ = spdlog::get(addr.toString());

  if (!logger_) {
    logger_ = spdlog::stdout_logger_mt(addr.toString());
  }

  if (json_path != "") {
    json_globals_log_ = make_shared<std::ofstream>(
        json_path + "/" + address_.toString() + "_Globals.dsv");
    json_messages_log_ = make_shared<std::ofstream>(
        json_path + "/" + address_.toString() + "_Messages.dsv");
  }

  json_final_state_only_ = json_final_only;
  message_counter_ = 0;

  // Peer's event processing loop
  auto work = [this, fac, peer_config, ready_callback]() {
    queue_ = make_shared<LockFreeQueue>();
    context_ = (*fac)();
    context_->__patch(peer_config);
    context_->initDecls(unit_t{});
    ready_callback();  // Signal to engine that peer is initialized

    try {
      // The engine will flip the start_processing_ bool after
      // all peers have processed their role (sent initial message)
      while (!start_processing_) continue;

      vector<std::unique_ptr<Message>> ms(1000);
      while (true) {
        //auto m = queue_->dequeue();
        size_t num = queue_->dequeueBulk(ms);
        for (int i = 0; i < num; i++) {
          auto m = std::move(ms[i]);
          logMessage(*m);
          m->value()->dispatchIntoContext(context_.get(), m->trigger(),
                                          m->source());
          logGlobals(*m);
        }
      }
    } catch (EndOfProgramException e) {
      finished_ = true;
      if (json_globals_log_ && json_final_state_only_) {
        for (const auto& it : context_->__jsonify()) {
          *json_globals_log_ << it.first << "|" << it.second << std::endl;
        }
      }
      return;
    } catch (const std::exception& e) {
      logger_->error() << "Peer failed: " << e.what();
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

void Peer::enqueue(std::unique_ptr<Message> m) {
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

void Peer::logMessage(const Message& m) {
  if (logger_->level() == spdlog::level::trace) {
    string trig = ProgramContext::__triggerName(m.trigger());
    logger_->trace() << "Received:: @" << trig;
  }
}

void Peer::logGlobals(const Message& m) {
  if (logger_->level() == spdlog::level::trace) {
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
      oss << std::setw(20) << it.first << ": " << it.second;
    }
    logger_->trace() << oss.str();
  }

  if (json_globals_log_ && !json_final_state_only_) {
    for (const auto& it : context_->__jsonify()) {
      *json_globals_log_ << it.first << "|" << it.second << std::endl;
    }
  }
}

}  // namespace K3
