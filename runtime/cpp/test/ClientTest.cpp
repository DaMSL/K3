#include <memory>

#include <runtime/cpp/Listener.hpp>
#include <runtime/cpp/Common.hpp>
#include <runtime/cpp/Queue.hpp>
#include <boost/thread/thread.hpp>

namespace K3 {


using NContext = K3::Asio::NContext;

// Utils
void do_nothing(const Address&, const Identifier&, shared_ptr<Value>) {}

void setup() {
  // Setup context
  Identifier id = "id";
  Address server = defaultAddress;
  shared_ptr<NContext> context = shared_ptr<NContext>(new NContext());
  
  
  // setup connection
  shared_ptr<K3::Asio::NConnection> n_conn = shared_ptr<K3::Asio::NConnection>(new K3::Asio::NConnection(context, server));
  DefaultCodec cdec = DefaultCodec();
  NetworkHandle net_handle = NetworkHandle(make_shared<DefaultCodec>(cdec), n_conn);
  // Setup Endpoint
  // auto buf = make_shared<ScalarEPBufferST>(ScalarEPBufferST());
  // EndpointBindings::SendFunctionPtr func = do_nothing;
  // auto bindings = make_shared<EndpointBindings>(func); 
  // shared_ptr<Endpoint> ep = shared_ptr<Endpoint>(new Endpoint(make_shared<NetworkHandle>(net_handle), buf, bindings));

  auto buf = make_shared<ScalarEPBufferST>(ScalarEPBufferST());
  Endpoint:SendFunctionPtr func = do_nothing;
  auto bindings = make_shared<EndpointBindings>(func); 
  shared_ptr<Endpoint> ep = shared_ptr<Endpoint>(new Endpoint(make_shared<NetworkHandle>(net_handle), buf, bindings));
  context->service_threads->create_thread(*(context));

 
  while(!ep->hasWrite()) {
    cout << "waiting for hasWrite" << endl;
    boost::this_thread::sleep_for( boost::chrono::seconds(1) );
  }
  cout << "writing" << endl;
  for (int i =0; i < 15; i++) {
    Value val = "(127.0.0.1:40000, trig1, message_" + std::to_string(i) + ")";
    ep->doWrite(val);
    ep->flushBuffer();
  }
 
  
  context->service_threads->join_all();  
}


};
int main() {
  K3::setup();
  return 0;

}

