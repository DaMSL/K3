#include <Common.hpp>
#include <Engine.hpp>
#include <Dispatch.hpp>
#include <MessageProcessor.hpp>
#include <test/TestUtils.hpp>

#include <functional>
#include <iostream>
#include <sstream>

#include <boost/regex.hpp>

#include <xUnit++/xUnit++.h>

namespace K3 {

int final = -1;

using std::cout;
using std::endl;
using std::bind;

// "Trigger" Declarations
void start(shared_ptr<Engine> engine, string message_contents) {
  int n = std::stoi(message_contents);
  ostringstream s;
  s << "(" << n << ",0,1)";
  Message m = Message(defaultAddress, "fibonacci", s.str());
  engine->send(m);
}

void fibonacci(shared_ptr<Engine> engine, string message_contents) {
  // "bind" the triple
  static const boost::regex triple_regex("\\( *(.+) *, *(.+) *, *(.+) *\\)");
  boost::cmatch triple_match;
  if(boost::regex_match(message_contents.c_str(), triple_match, triple_regex)){
    int n = std::stoi(triple_match[1]);
    int a = std::stoi(triple_match[2]);
    int b = std::stoi(triple_match[3]);
    // Succesful bind: trigger body:
    if (n <= 1) {
      // send to result
      Message m = Message(defaultAddress, "result", std::to_string(b));
      engine->send(m);
    }
    else {
      // Send the next message
      int new_n = n - 1;
      int new_a = b;
      int new_b = a + b;
      ostringstream s;
      s << "(" << new_n << "," << new_a << "," << new_b << ")";
      Message m = Message(defaultAddress, "fibonacci", s.str());
      engine->send(m);
    }
  } 
  else {
    cout << "Couldn't bind args in fibonacci!" << endl;
  }
}

void result(shared_ptr<Engine> engine, string message_contents) {
  int n = std::stoi(message_contents);
  final = n;
}

// MP setup
TriggerDispatch buildTable(shared_ptr<Engine> engine) {
  TriggerDispatch table = TriggerDispatch();
  table["start"] = bind(&K3::start, engine, std::placeholders::_1);
  table["fibonacci"] = bind(&K3::fibonacci, engine, std::placeholders::_1);
  table["result"] = bind(&K3::result, engine, std::placeholders::_1);
  return table;
}

}

FACT("The 6th fibonacci number = 8") {
  std::string num = "6";
  auto addr = K3::defaultAddress;
  auto engine = K3::buildEngine(true, K3::defaultEnvironment(addr));
  auto mp = K3::buildMP(engine, K3::buildTable(engine));
  K3::Message m = K3::Message(K3::defaultAddress, "start", num);
  engine->send(m);

  // Run Engine
  engine->runEngine(mp);

  // Check the result (the 6th fib number should = 8)
  Assert.Equal(8, K3::final);
}
