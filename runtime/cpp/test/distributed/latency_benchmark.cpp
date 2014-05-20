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
#include <thread>

#include <boost/regex.hpp>
#include <boost/thread/thread.hpp>

namespace K3 {

// Using
using std::cout;
using std::endl;
using std::bind;
using std::string;
using std::shared_ptr;
using boost::thread;
using namespace std::chrono;

// Chrono
using Clock = std::chrono::high_resolution_clock;
using Time_point = Clock::time_point;
using Duration = Clock::duration;
 
// Globals
Address receiver;
Address sender;
std::chrono::system_clock::time_point start, end;
int numReceived(0);
int numSent(0);
int desired(0);
bool received_all(false);
bool sent_all(false);
std::list<std::chrono::milliseconds> latencies;
string message;

// "Trigger" Declarations
void warmup_stor(shared_ptr<Engine> engine, string message_contents) {
  std::cout << "received warmup" << std::endl;
  Message m = Message(sender, "warmup_rtos", "()");
  engine->send(m);
}

void warmup_rtos(shared_ptr<Engine> engine, string message_contents) {
  std::cout << "received warmup" << std::endl;
}

void send_now(shared_ptr<Engine> engine, string message_contents) {
  Time_point const t0 = Clock::now();
  milliseconds num_ms = duration_cast< milliseconds >( t0.time_since_epoch() );
  std::string s1 = std::to_string(num_ms.count());
  Message m = Message(receiver, "reflect_timepoint", s1);
  engine->send(m);

}

void reflect_timepoint(shared_ptr<Engine> engine, string message_contents) {
  // Reflect the timestamp back to the sender
  Message m = Message(sender, "compute_timediff", message_contents);
  engine->send(m);
  // Record new message count
  int n = 1;
  numReceived += n;
  if ((desired > 100) && (numReceived % (desired/100) == 0)) {
    std::cout << "received count: " << numReceived << "/" << desired << std::endl;
  }

}

bool skip_first(false);
void compute_timediff(shared_ptr<Engine> engine, string message_contents) {
  Time_point const t1 = Clock::now();

  // Record new message count
  int n = 1;
  numSent += n;

  // Todo, parse timepoint, compute delta
  milliseconds const num_ms = milliseconds(std::stoll(message_contents));
  auto ms3 = std::chrono::duration_cast<milliseconds>(t1.time_since_epoch());

  Duration const d(num_ms);
  Time_point const t0(d);


  auto elapsed = t1 - t0;
  auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(elapsed);
  if (!skip_first) {
    latencies.push_back(ms);
  } else {
    skip_first = false;
  }

  // Check if the sender is finished
  if (numSent == desired) {
    Message m = Message(receiver, "receiver_finished", "()");
    engine->send(m);
    sent_all = true;
  }
}

void receiver_finished(shared_ptr<Engine> engine, string message_contents) {
  received_all = true;
}

// MP setup
TriggerDispatch buildTable(shared_ptr<Engine> engine) {
  TriggerDispatch table = TriggerDispatch();
  table["warmup_stor"] = bind(&K3::warmup_stor, engine, std::placeholders::_1);
  table["warmup_rtos"] = bind(&K3::warmup_rtos, engine, std::placeholders::_1);
  table["send_now"] = bind(&K3::send_now, engine, std::placeholders::_1);
  table["reflect_timepoint"] = bind(&K3::reflect_timepoint, engine, std::placeholders::_1);
  table["compute_timediff"] = bind(&K3::compute_timediff, engine, std::placeholders::_1);
  table["receiver_finished"] = bind(&K3::receiver_finished, engine, std::placeholders::_1);
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

std::tuple<float, float> runSender(int num_messages, int message_len, int time_between) {

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
  
  // Fork a thread for each engine
  auto service_threads = std::shared_ptr<thread_group>(new thread_group());

  shared_ptr<thread> t1 = engine2->forkEngine(mp2);
   K3::Message m = K3::Message(K3::receiver, "warmup_stor", "()");
   engine2->send(m);
   boost::this_thread::sleep_for(boost::chrono::seconds(5));
   K3::Message m2 = K3::Message(K3::sender, "send_now", "()");

   service_threads->add_thread(t1.get());
  
  // Send messages
  boost::chrono::milliseconds dura( time_between );
  for (int i = 0; i < num_messages; i++) {
    engine2->send(m2);
    boost::this_thread::sleep_for( dura );
  }

  // Wait for completion
  while ((!K3::sent_all)) {
   continue;
  }

  // Compute Avg
  int total = 0;
  int count = 0;
  for (auto x : K3::latencies) {
    total += x.count();
    count += 1;
  }

  float avg = float(total) / count;
  

  // Compute Variance
  float variance = 0;
  for (auto x: K3::latencies) {
    float y = (1.0 / float(count)) * (x.count() - avg) * (x.count() - avg);
    variance += y;
  }

  
  // Cleanup
  engine2->forceTerminateEngine();
  service_threads->join_all();
  service_threads->remove_thread(t1.get());

  return std::make_tuple(avg, variance);
}

int main(int argc, char** argv) {


 if (argc < 8) {
  std::cout << "usage: " << argv[0] << " num_messages time_between(ms) mode reciever_ip receiver_port sender_ip sender_port";
  return -1;
 }

 int message_len = 10;
 int num_messages = std::atoi(argv[1]);
 int time_between = std::atoi(argv[2]);
 std::string mode = argv[3];
 auto receiver_ip = argv[4];
 auto receiver_port = std::atoi(argv[5]);
 auto sender_ip = argv[6];
 auto sender_port = std::atoi(argv[7]);
 K3::desired = num_messages;

 K3::receiver = K3::make_address(receiver_ip, receiver_port);
 K3::sender = K3::make_address(sender_ip, sender_port);

 if (mode == "receiver") {
   runReceiver(num_messages);
 } 
 else {
   auto tup = runSender(num_messages, message_len, time_between);
   float avg = std::get<0>(tup);
   float variance = std::get<1>(tup);

   std::ofstream file;
   file.open("results.txt");
   file << avg << "," << variance << "\n";
   file.close();
   std::cout << "AVG:" << avg << std::endl;
   std::cout << "VAR:" << variance << std::endl;
 }

 return 0;
}
