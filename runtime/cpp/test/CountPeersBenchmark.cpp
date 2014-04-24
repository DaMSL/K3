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
Address peer2;
Address peer3;
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
//  std::cout << "Length (mB)" << message_contents.length() / (1024*1024.0) << ". Number:" << nodeCounter<< std::endl;
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


void runExperiment(int num_messages, int message_len) {
  K3::nodeCounter = 0;
  K3::message = "";
  for (int i=0; i< message_len; i++) {
    K3::message += "a";
  }
  using boost::thread;
  using boost::thread_group;
  using std::shared_ptr;
  // Create peers
  K3::peer1 = K3::make_address(K3::localhost, 3000);
  K3::peer2 = K3::make_address(K3::localhost, 3005);
  K3::rendezvous = K3::peer1;
  // Create engines
  auto engine1 = K3::buildEngine(false, K3::defaultEnvironment(K3::peer1));
  auto engine2 = K3::buildEngine(false, K3::defaultEnvironment(K3::peer2));
  // Create MPs
  auto mp1 = K3::buildMP(engine1);
  auto mp2 = K3::buildMP(engine2);
  // Create initial messages (source)
  K3::Message m1 = K3::Message(K3::peer1, "join", "()");
  K3::Message m2 = K3::Message(K3::peer2, "join", "()");
  for (int i = 0; i < num_messages; i++) {
    engine2->send(m2);
  }
  // Fork a thread for each engine
  auto service_threads = std::shared_ptr<thread_group>(new thread_group());
  shared_ptr<thread> t1 = engine1->forkEngine(mp1);
  shared_ptr<thread> t2 = engine2->forkEngine(mp2);

  service_threads->add_thread(t1.get());
  service_threads->add_thread(t2.get());


  int desired = num_messages;
  while ((K3::nodeCounter < desired)) {
   continue;
  }

  engine1->forceTerminateEngine();
  engine2->forceTerminateEngine();

  service_threads->join_all();
  service_threads->remove_thread(t1.get());
  service_threads->remove_thread(t2.get());

}

int main(int argc, char** argv) {

 if (argc < 2) {
  std::cout << "usage: " << argv[0] << " num_messages" << std::endl;
  return -1;
 }
 int num_messages = std::atoi(argv[1]);
// int message_lens [] = {16, 512, 1024, 1024*128, 1024*256, 1024*512,  1024*1024};
 int message_lens [] = {1024*1024};
 int num_sizes = sizeof(message_lens) / sizeof(message_lens[0]);
 std::ofstream outfile;
 outfile.open("benchmarks.txt");
 for (int i=0; i<num_sizes; i++ ) {
   for (int j=0; j < 1; j++) {
     std::cout << "size: " << message_lens[i] << " trial: " << j << std::endl;
     boost::chrono::system_clock::time_point start = boost::chrono::system_clock::now();
     runExperiment(num_messages,message_lens[i]);
     boost::chrono::system_clock::time_point end = boost::chrono::system_clock::now();
     typedef boost::chrono::milliseconds ms;
     ms elapsed = boost::chrono::duration_cast<ms>(end - start);
     outfile << message_lens[i] << ","<< elapsed.count() << "\n";
   }
 }


 outfile.close();
 return 0;
}
