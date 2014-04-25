#include <Common.hpp>
#include <Engine.hpp>
#include <Dispatch.hpp>
#include <MessageProcessor.hpp>

#include <functional>
#include <iostream>
#include <sstream>
#include <fstream>

#include <boost/regex.hpp>
#include <boost/thread/thread.hpp>



namespace K3 {

std::string localhost = "127.0.0.1";
Address peer1;
Address rendezvous;

int nodeCounter(0);
string message;

using std::cout;
using std::endl;
using std::bind;
using std::string;
using std::shared_ptr;


// "Trigger" Declarations
void join(shared_ptr<Engine> engine, string message_contents) {
  Message m = Message(rendezvous, "register", message);
  engine->send(m);
}

void register2(shared_ptr<Engine> engine, string message_contents) {
  int n = 1;
  nodeCounter += n;
  std::cout << "Length (mB)" << message_contents.length() / (1024*1024.0) << ". Number:" << nodeCounter<< std::endl;
}

// MP setup
TriggerDispatch buildTable(shared_ptr<Engine> engine) {
  TriggerDispatch table = TriggerDispatch();
  table["join"] = bind(&K3::join, engine, std::placeholders::_1);
  table["register"] = bind(&K3::register2, engine, std::placeholders::_1);
  return table;
}

shared_ptr<MessageProcessor> buildMP(shared_ptr<Engine> engine) {
  auto table = buildTable(engine);
  shared_ptr<MessageProcessor> mp = make_shared<DispatchMessageProcessor>(DispatchMessageProcessor(table));
  return mp;
}

// Engine setup
shared_ptr<Engine> buildEngine(bool simulation, SystemEnvironment s_env) {
  // Configure engine components
  shared_ptr<InternalCodec> i_cdec = make_shared<LengthHeaderInternalCodec>(LengthHeaderInternalCodec());

  // Construct an engine
  Engine engine = Engine(simulation, s_env, i_cdec);
  return make_shared<Engine>(engine);
}

}

void runReceiver(int num_messages) {
  K3::nodeCounter = 0;
  using boost::thread;
  using boost::thread_group;
  using std::shared_ptr;
  // Create engines
  auto engine1 = K3::buildEngine(false, K3::defaultEnvironment(K3::peer1));
  // Create MPs
  auto mp1 = K3::buildMP(engine1);
  // Fork a thread for each engine
  auto service_threads = std::shared_ptr<thread_group>(new thread_group());
  shared_ptr<thread> t1 = engine1->forkEngine(mp1);

  service_threads->add_thread(t1.get());

  int desired = num_messages;
  while ((K3::nodeCounter < desired)) {
   continue;
  }

  engine1->forceTerminateEngine();

  service_threads->join_all();
  service_threads->remove_thread(t1.get());
}

void runSender(int num_messages, int message_len) {
  K3::message = "";
  for (int i=0; i< message_len; i++) {
    K3::message += "a";
  }
  using boost::thread;
  using boost::thread_group;
  using std::shared_ptr;
  // Create peers
  K3::peer1 = K3::make_address("", 40000);
  K3::peer2 = K3::make_address(K3::localhost, 40001);
  K3::rendezvous = K3::peer1;
  // Create engines
  auto engine2 = K3::buildEngine(false, K3::defaultEnvironment(K3::peer2));
  // Create MPs
  auto mp2 = K3::buildMP(engine2);
  // Create initial messages (source)
  K3::Message m2 = K3::Message(K3::peer2, "join", "()");
  for (int i = 0; i < num_messages; i++) {
    engine2->send(m2);
  }
  engine2->runEngine();

}

int main(int argc, char** argv) {

 if (argc < 3) {
  std::cout << "usage: " << argv[0] << " num_messages" << " sender|receiver" std::endl;
  return -1;
 }



 int num_messages = std::atoi(argv[1]);
 std::string mode = argv[2];
 if (mode == "receiver") {
   runReceiver(num_messages);

 } 
 else {
   runSender(num_messages, 10);
 }

 return 0;
}
