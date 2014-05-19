#include <Common.hpp>
#include <Engine.hpp>
#include <Dispatch.hpp>
#include <MessageProcessor.hpp>
#include <test/TestUtils.hpp>

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
 
Address receiver;
Address sender;
std::chrono::system_clock::time_point start, end;


int numReceived(0);
int numSent(0);
int desired(0);
bool received_all(false);
bool sent_all(false);
string message;

// "Trigger" Declarations
void send_one(shared_ptr<Engine> engine, string message_contents) {
  Message m = Message(receiver, "receive_one", message);
  engine->send(m);
}

void receive_one(shared_ptr<Engine> engine, string message_contents) {
  int n = 1;
  numReceived += n;
  if (numReceived % (desired/100) == 0) {
    std::cout << "received count: " << numReceived << "/" << desired << std::endl;
  }
  if (numReceived == desired) {
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
  table["send_one"] = bind(&K3::send_one, engine, std::placeholders::_1);
  table["receive_one"] = bind(&K3::receive_one, engine, std::placeholders::_1);
  table["finished"] = bind(&K3::finished, engine, std::placeholders::_1);
  return table;
}
}

void runReceiver(int num_messages) {
  using boost::thread;
  using boost::thread_group;
  using std::shared_ptr;
  K3::numReceived = 0;

  // Create engines
  auto engine1 = K3::buildEngine(false, K3::defaultEnvironment(K3::receiver));
  // Create MPs
  auto mp1 = K3::buildMP(engine1, K3::buildTable(engine1));
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
  auto mp2 = K3::buildMP(engine2, K3::buildTable(engine2));
  
  // Start timer
  K3::start = std::chrono::system_clock::now();

  // Fork a thread for each engine
  auto service_threads = std::shared_ptr<thread_group>(new thread_group());
  shared_ptr<thread> t1 = engine2->forkEngine(mp2);
  service_threads->add_thread(t1.get());
  
  // Send messages
  K3::Message m2 = K3::Message(K3::sender, "send_one", "()");
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

 K3::receiver = K3::make_address(receiver_ip, 3000);
 K3::sender = K3::make_address(sender_ip, 3002);

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
