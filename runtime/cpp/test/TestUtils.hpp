
namespace K3 {

// Read from a file line by line
template <class R>
tuple<int,string> readFile(K3::shared_ptr<R> r) {
  // Read the file line-by-line
  int count = 0;
  ostringstream os;
  while (r->hasRead()) {
    shared_ptr<string> val = r->doRead();
    if ( val ) {
      count += 1;
      os << *val;
    }
  }
  string actual = os.str();
  return make_tuple(count,actual);
}

// Program Variables
std::string localhost = "127.0.0.1";
Address peer1;
Address peer2;
Address peer3;
Address rendezvous;

int nodeCounter(0);

// "Trigger" Declarations
void join(shared_ptr<Engine> engine, string message_contents) {
  Message m = Message(rendezvous, "register", "1");
  engine->send(m);
}

void register2(shared_ptr<Engine> engine, string message_contents) {
  int n = std::stoi(message_contents);
  nodeCounter += n;
  cout << "processed #" << nodeCounter << endl;
}

// Dispatch Tables
TriggerDispatch countPeersTable(shared_ptr<Engine> engine) {
  TriggerDispatch table = TriggerDispatch();
  table["join"] = bind(&K3::join, engine, std::placeholders::_1);
  table["register"] = bind(&K3::register2, engine, std::placeholders::_1);
  return table;
}

// MP setup
shared_ptr<MessageProcessor> buildMP(shared_ptr<Engine> engine,TriggerDispatch table) {
  shared_ptr<MessageProcessor> mp = make_shared<DispatchMessageProcessor>(DispatchMessageProcessor(table));
  return mp;
}

// Engine setup
shared_ptr<Engine> buildEngine(bool simulation, SystemEnvironment s_env) {
  // Configure engine components
  shared_ptr<InternalCodec> i_cdec = make_shared<LengthHeaderInternalCodec>(LengthHeaderInternalCodec());

  // Construct an engine
  Engine engine = Engine(simulation, s_env, i_cdec);
  shared_ptr<Engine> e =  make_shared<Engine>(engine);
  return e;
}

}
