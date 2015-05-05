#ifndef K3_RUNTIME_RUN_H
#define K3_RUNTIME_RUN_H

#include "Engine.hpp"
#include "Context.hpp"
#include <map>
#include <boost/thread/thread.hpp>
#include "serialization/yaml.hpp"
namespace K3 {
  template <class context>
  void runProgram(Options opt) {

    std::vector<string> peer_strs = opt.peer_strings;
    bool simulation = opt.simulation;
    std::string log_level = opt.log_level;
    std::string log_path = opt.json_path;
    std::string result_var = opt.result_var;
    std::string result_path = opt.result_path;

    std::vector<std::string> configurations;

    for (auto p: peer_strs) {
      if (p.size() > 0 && p[0] != '{') {
        for (auto d: YAML::LoadAllFromFile(p)) {
          YAML::Emitter out;
          out << d;
          configurations.push_back(std::string {out.c_str()});
        }
      } else {
        configurations.push_back(p);
      }
    }

    typedef std::map<Address, shared_ptr<__k3_context>> ctxt_map;
    typedef shared_ptr<Engine> e_ptr;
    std::list<tuple<e_ptr, ctxt_map>> engines;

    shared_ptr<MessageQueues> queues = make_shared<MessageQueues>();
    list<Address> peers;
    for (auto& s: configurations) {
      ctxt_map contexts;
      e_ptr engine = make_shared<Engine>();
      auto gc = make_shared<context>(*engine);
      gc->__patch(s);
      gc->initDecls(unit_t {});
      contexts[gc->me] = gc;
      gc->__setAddr(gc->me);
      queues->addQueue(gc->me);
      peers.push_back(gc->me);
      SystemEnvironment se = defaultEnvironment(getAddrs(contexts));
      engine->configure(simulation, se, make_shared<DefaultMessageCodec>(), log_level, log_path, result_var, result_path, queues);
      processRoles(contexts);
      auto t = tuple<e_ptr, ctxt_map>(engine, contexts);
      engines.push_back(t);
    }

     using boost::thread;
     using boost::thread_group;
     auto l = std::list<shared_ptr<thread>>();
     for (auto& t : engines) {
       auto engine = get<0>(t);
       auto contexts = get<1>(t);
       shared_ptr<thread> thread = engine->forkEngine(make_shared<virtualizing_message_processor>(contexts));
       l.push_back(thread);
     }
     // Block until completion
     for (auto th : l) {
       th->join();
     }

  }

} //Namespace K3

#endif //K3_RUNTIME_RUN_H
