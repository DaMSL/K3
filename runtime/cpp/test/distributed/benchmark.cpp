#include <Common.hpp>
#include <Engine.hpp>
#include <Dispatch.hpp>
#include <MessageProcessor.hpp>

#include <chrono>
#include <functional>
#include <iostream>
#include <sstream>
#include <fstream>

#include <boost/regex.hpp>
#include <boost/thread/thread.hpp>

namespace K3 {

using std::cout;
using std::endl;
using std::bind;
using std::string;
using std::shared_ptr;
 
Address rendezvous;
Address sender;
std::chrono::system_clock::time_point start, end;


int nodeCounter(0);
int numSent(0);
int desired(0);
bool received_all(false);
bool sent_all(false);
string message;

// "Trigger" Declarations
void join(shared_ptr<Engine> engine, string message_contents) {
  Message m = Message(rendezvous, "register", message);
  engine->send(m);
}

void register2(shared_ptr<Engine> engine, string message_contents) {
  int n = 1;
  nodeCounter += n;
  if (nodeCounter % (desired/100) == 0) {
    std::cout << "received count: " << nodeCounter << "/" << desired << std::endl;
  }
  if (nodeCounter == desired) {
    Message m = Message(sender, "finished", "()");
    engine->send(m);
    received_all = true;
  }

}
void finished(shared_ptr<Engine> engine, string message_contents) {
  sent_all = true;
  std::cout << "FINISHED!!!" << std::endl;
}

// MP setup
TriggerDispatch buildTable(shared_ptr<Engine> engine) {
  TriggerDispatch table = TriggerDispatch();
  table["join"] = bind(&K3::join, engine, std::placeholders::_1);
  table["register"] = bind(&K3::register2, engine, std::placeholders::_1);
  table["finished"] = bind(&K3::finished, engine, std::placeholders::_1);
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
  using boost::thread;
  using boost::thread_group;
  using std::shared_ptr;
  K3::nodeCounter = 0;

  // Create engines
  auto engine1 = K3::buildEngine(false, K3::defaultEnvironment(K3::rendezvous));
  // Create MPs
  auto mp1 = K3::buildMP(engine1);
  // Fork a thread for each engine
  auto service_threads = std::shared_ptr<thread_group>(new thread_group());
  shared_ptr<thread> t1 = engine1->forkEngine(mp1);
  service_threads->add_thread(t1.get());

  // Wait for completion
  while ((!K3::received_all)) {
   continue;
  }

  // Cleanup
  engine1->forceTerminateEngine();
  service_threads->join_all();
  service_threads->remove_thread(t1.get());
}

std::chrono::seconds runSender(int num_messages, int message_len) {
  using boost::thread;
  using boost::thread_group;
  using std::shared_ptr;
  
  // Build message
  K3::message = "";
  for (int i=0; i< message_len; i++) {
    K3::message += "a";
  }
  // Create engines
  auto engine2 = K3::buildEngine(false, K3::defaultEnvironment(K3::sender));
  // Create MPs
  auto mp2 = K3::buildMP(engine2);
  
  // Start timer
  K3::start = std::chrono::system_clock::now();

  // Fork a thread for each engine
  auto service_threads = std::shared_ptr<thread_group>(new thread_group());
  shared_ptr<thread> t1 = engine2->forkEngine(mp2);
  service_threads->add_thread(t1.get());
  
  // Send messages
  K3::Message m2 = K3::Message(K3::sender, "join", "()");
  for (int i = 0; i < num_messages; i++) {
    engine2->send(m2);
  }

  // Wait for completion
  while ((!K3::sent_all)) {
   continue;
  }

  // End timer
  K3::end = std::chrono::system_clock::now();

  // Cleanup
  engine2->forceTerminateEngine();
  service_threads->join_all();
  service_threads->remove_thread(t1.get());

  auto elapsed = K3::end - K3::start;
  return std::chrono::duration_cast<std::chrono::seconds>(elapsed);
}

int main(int argc, char** argv) {

 if (argc < 6) {
  std::cout << "usage: " << argv[0] << " num_messages message_len mode reciever_ip sender_ip";
  return -1;
 }

 int num_messages = std::atoi(argv[1]);
 int message_len = std::atoi(argv[2]);
 std::string mode = argv[3];
 auto receiver_ip = argv[4];
 auto sender_ip = argv[5];

 K3::desired = num_messages;

 K3::rendezvous = K3::make_address(receiver_ip, 3000);
 K3::sender = K3::make_address(sender_ip, 3000);

 if (mode == "receiver") {
   runReceiver(num_messages);
 } 
 else {
   // output time in seconds
   std::chrono::seconds t = runSender(num_messages, message_len);
   std::ofstream f;
   f.open("results.txt");
   f << t.count();
   f.close();
 }

 return 0;
}
