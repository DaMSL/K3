#ifndef K3_PEER
#define K3_PEER

#include <atomic>
#include <string>
#include <memory>
#include <thread>
#include <chrono>
#include <vector>

#ifdef _WIN32
#include <winsock2.h>
#endif //_WIN32
#include <spdlog/spdlog.h>
#include <yaml-cpp/yaml.h>

#include "Options.hpp"
#include "types/Dispatcher.hpp"
#include "types/Queue.hpp"

#ifdef BSL_ALLOC
#include <bsl_memory.h>
#include <bsls_blockgrowth.h>
#include <bdlma_multipoolallocator.h>
#include <bdlma_sequentialallocator.h>
#include <bdlma_localsequentialallocator.h>
#include <bdlma_countingallocator.h>
#include "collections/AllCollections.hpp"
#endif

namespace K3 {

class ProgramContext;
class Peer {
 public:
  // Core Interface
  Peer(shared_ptr<ContextFactory> fac, const YAML::Node& peer_config,
       std::function<void()> ready_callback, const JSONOptions& json);
  void start();
  void processRole();
  void join();

  Queue& getQueue();

  // Utilities
  bool finished();
  Address address();
  ProgramContext& getContext() {
    return *context_;
  }
  void printStatistics();

 protected:
  // Helper Functions
  void processBatch();
  void logMessage(const Dispatcher& d);
  void logGlobals(bool final);

  // BSL Allocations
#ifdef BSL_ALLOC
  #ifdef BSEQ
  BloombergLP::bdlma::SequentialAllocator mpool_;
  #elif BPOOLSEQ
  BloombergLP::bdlma::SequentialAllocator seqpool_;
  BloombergLP::bdlma::MultipoolAllocator mpool_;
  #elif BLOCAL
  BloombergLP::bdlma::LocalSequentialAllocator<lsz> mpool_;
  #elif BCOUNT
  BloombergLP::bdlma::MultipoolAllocator backing_pool_;
  BloombergLP::bdlma::CountingAllocator mpool_;
  #else
  BloombergLP::bdlma::MultipoolAllocator mpool_;
  #endif
#endif

  // Components
  shared_ptr<ProgramContext> context_;
  shared_ptr<spdlog::logger> logger_;
  shared_ptr<std::thread> thread_;
  shared_ptr<Queue> queue_;

  // Configuration
  JSONOptions json_opts_;
  shared_ptr<std::ofstream> json_globals_log_;
  shared_ptr<std::ofstream> json_messages_log_;

  // State
  Address address_;
  std::atomic<bool> start_processing_;
  std::atomic<bool> finished_;
  vector<unique_ptr<Dispatcher>> batch_;

  // Statistics
  std::vector<TriggerStatistics> statistics_;
  int message_counter_;
};

}  // namespace K3

#endif
