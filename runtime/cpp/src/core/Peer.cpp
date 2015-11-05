#ifdef _WIN32
#include <winsock2.h>
#endif //_WIN32
#include <spdlog/spdlog.h>

#include <algorithm>
#include <memory>
#include <string>
#include <regex>
#include <sstream>

#include "core/Peer.hpp"
#include "core/Engine.hpp"
#include "core/ProgramContext.hpp"
#include "network/NetworkManager.hpp"

namespace K3 {

Peer::Peer(shared_ptr<ContextFactory> fac, const YAML::Node& config,
           std::function<void()> callback, const JSONOptions& opts) 
#ifdef BSL_ALLOC
  :
  #ifdef BSEQ
  mpool_()
  #elif BPOOLSEQ
  seqpool_(), mpool_(8, &seqpool_)
  #elif BLOCAL
  mpool_()
  #else
  mpool_(8)
  #endif
#endif
  {
  // Initialization
  address_ = serialization::yaml::meFromYAML(config);
  start_processing_ = false;
  finished_ = false;
  message_counter_ = 0;
  statistics_.resize(ProgramContext::__trigger_names_.size());
  json_opts_ = opts;

  // Log configuration
  logger_ = spdlog::get(address_.toString());
  if (!logger_) {
    logger_ = spdlog::stdout_logger_mt(address_.toString());
  }
  string json_path = json_opts_.output_folder_;
  if (json_path != "") {
    json_globals_log_ = make_shared<std::ofstream>(
        json_path + "/" + address_.toString() + "_Globals.dsv");
    json_messages_log_ = make_shared<std::ofstream>(
        json_path + "/" + address_.toString() + "_Messages.dsv");
  }

  // Create work to run in new thread
  auto work = [this, fac, config, callback]() {
  #ifdef BSL_ALLOC
    #ifdef BSEQ
    mpool = &mpool_;
    #elif BPOOLSEQ
    seqpool = &seqpool_;
    mpool = &mpool_;
    #elif BLOCAL
    mpool = &mpool_;
    #else
    mpool = &mpool_;
    #endif
  #endif
    // Queue and batch are allocated on worker thread for locality
    queue_ = make_shared<Queue>();
    batch_.resize(1000);
    context_ = (*fac)();
    context_->__patch(config);
    context_->initDecls(unit_t{});

    // Signal that peer is initialized, wait for 'go' signal
    callback();
    while (!start_processing_) continue;
    try {
      while (true) {
        processBatch();
      }
    } catch (EndOfProgramException e) {
      finished_ = true;
#ifdef K3DEBUG
      logGlobals(true);
#endif
    }

#if defined(K3_LT_SAMPLE) || defined(K3_LT_HISTOGRAM)
    lifetime::__active_lt_profiler.dump();
#endif
    //catch (const std::exception& e) {
    //  logger_->error() << "Peer failed: " << e.what();
    //}
  };

  // Execute in new thread
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

Queue& Peer::getQueue() { return *queue_; }

bool Peer::finished() { return finished_.load(); }

Address Peer::address() { return address_; }


void Peer::processBatch() {
  size_t num = queue_->dequeueBulk(batch_);

  for (int i = 0; i < num; i++) {
    auto d = std::move(batch_[i]);
#ifdef K3DEBUG
    logMessage(*d);
    TriggerID tid = d->trigger_;
    std::chrono::high_resolution_clock::time_point start_time =
        std::chrono::high_resolution_clock::now();
#endif  // K3DEBUG

    (*d)();  // Call the dispatcher (process a message)

#ifdef K3DEBUG
    statistics_[tid].total_time +=
        std::chrono::high_resolution_clock::now() - start_time;
    statistics_[tid].total_count++;
    logGlobals(false);
#endif  // K3DEBUG
  }
}

void Peer::logMessage(const Dispatcher& d) {
#ifdef K3DEBUG
  if (logger_->level() <= spdlog::level::debug) {
    string trig = ProgramContext::__triggerName(d.trigger_);
    logger_->debug() << "Received:: @" << trig;
  }

  if (json_messages_log_ && !json_opts_.final_state_only_) {
    string trig = ProgramContext::__triggerName(d.trigger_);
    if (std::regex_match(trig, std::regex(json_opts_.messages_regex_))) {
      *json_messages_log_ << message_counter_ << "|";
      *json_messages_log_ << K3::serialization::json::encode<Address>(
                                d.destination_) << "|";
      *json_messages_log_ << trig << "|";
      *json_messages_log_ << K3::serialization::json::encode<Address>(d.source_)
                          << "|";
      *json_messages_log_ << d.jsonify() << "|";
      *json_messages_log_ << currentTime() << std::endl;
    }
  }
  message_counter_++;
#endif  // K3DEBUG
}

void Peer::logGlobals(bool final) {
#ifdef K3DEBUG
  if (logger_->level() <= spdlog::level::trace) {
    std::ostringstream oss;
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

  if ((json_globals_log_ && !json_opts_.final_state_only_) ||
      (json_opts_.final_state_only_ && final)) {
    for (const auto& global : context_->__globalNames()) {
      if (std::regex_match(global.c_str(),
                           std::regex(json_opts_.globals_regex_.c_str()))) {
        *json_globals_log_ << message_counter_ << "|"
                           << K3::serialization::json::encode<Address>(address_)
                           << "|" << global << "|"
                           << context_->__jsonify(global) << std::endl;
      }
    }
  }
#endif  // K3DEBUG
}

void Peer::printStatistics() {
  // Associate each statistic with its trigger id, since sorting will reorder
  // the statistics_ vector
  for (int i = 0; i < statistics_.size(); i++) {
    statistics_[i].trig_id = ProgramContext::__triggerName(i);
  }

  double total = 0.0;
  std::cout << "===Trigger Statistics @" << addressAsString(address_)
            << "===" << std::endl;
  std::sort(statistics_.begin(), statistics_.end(),
            [](const TriggerStatistics& a, const TriggerStatistics& b) {
              return a.total_time > b.total_time;
            });

  int k = 10;
  int max = statistics_.size() < k ? statistics_.size() : k;
  for (int i = 0; i < max; i++) {
    auto count = statistics_[i].total_count;
    auto time =
        std::chrono::duration_cast<std::chrono::duration<float, std::ratio<1>>>(
            statistics_[i].total_time)
            .count();
    total += time;
    std::cout << std::setw(70) << statistics_[i].trig_id << ": " << std::setw(6)
              << count << " call(s), " << std::setw(15) << std::scientific
              << std::setprecision(15) << time << "s total time spent, "
              << std::setw(6) << std::scientific << std::setprecision(15)
              << time / count << "s average per call." << std::endl;
  }
  std::cout << "Total time in all triggers: " << total << std::endl;
}
}  // namespace K3
