#ifndef K3_RUNTIME_RUN_H
#define K3_RUNTIME_RUN_H

#include "Engine.hpp"
#include "Context.hpp"
#include <map>
#include <boost/thread/thread.hpp>

namespace K3 {
  template <class context>
  void runProgram(std::vector<string> peer_strs, bool simulation, string log_level, std::string name, bool directory_master, std::string directory_upstream) {
    if (simulation) {
      // All peers share a single engine 
      Engine engine;
      std::map<Address, shared_ptr<__k3_context>> contexts;
    
      for (auto& s : peer_strs) {
        auto gc = make_shared<context>(engine);
        gc->__patch(parse_bindings(s, name));
        contexts[gc->me] = gc;
      }
      SystemEnvironment se = defaultEnvironment(getAddrs(contexts));
      engine.configure(simulation, se, make_shared<DefaultInternalCodec>(), log_level, directory_master, directory_upstream);
      processRoles(contexts);
      engine.runEngine(make_shared<virtualizing_message_processor>(contexts));
    }
    // One engine per peer, each on its own thread
    else {
      typedef std::map<Address, shared_ptr<__k3_context>> ctxt_map;
      typedef shared_ptr<Engine> e_ptr;
      std::list<tuple<e_ptr, ctxt_map>> engines; 
     
      for (auto& s : peer_strs) {
	std::cout << "Engine for this string: " << s << std::endl;
        ctxt_map contexts;
        e_ptr engine = make_shared<Engine>(); 
        auto gc = make_shared<context>(*engine);
        gc->__patch(parse_bindings(s, name));
        contexts[gc->me] = gc;
        SystemEnvironment se = defaultEnvironment(getAddrs(contexts));
        engine->configure(simulation, se, make_shared<DefaultInternalCodec>(), log_level, directory_master, directory_upstream);
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
    
  }

} //Namespace K3

#endif //K3_RUNTIME_RUN_H
