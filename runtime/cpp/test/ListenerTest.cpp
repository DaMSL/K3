#include <memory>

#include <runtime/cpp/Listener.hpp>
#include <runtime/cpp/Common.hpp>
#include <runtime/cpp/Queue.hpp>


namespace K3 {


using NContext = K3::Asio::NContext;

// Utils
void do_nothing(const Address&, const Identifier&, shared_ptr<Value>) {}

void setup() {
  // Setup context and Queues
  Identifier id = "id";
  Address me = defaultAddress;
  shared_ptr<NContext> context = shared_ptr<NContext>(new NContext());
  shared_ptr<SinglePeerQueue> qs = shared_ptr<SinglePeerQueue>(new SinglePeerQueue(me));
  // Setup network IOHandle
  K3::Asio::NEndpoint n_ep = K3::Asio::NEndpoint(context, me);
  DefaultCodec cdec = DefaultCodec();
  NetworkHandle net_handle = NetworkHandle(make_shared<DefaultCodec>(cdec), make_shared<K3::Asio::NEndpoint>(n_ep));
  // Setup Endpoint
  auto buf = make_shared<ScalarEPBufferST>(ScalarEPBufferST());
  SendFunctionPtr func = do_nothing;
  auto bindings = make_shared<EndpointBindings>(func); 
  shared_ptr<Endpoint> ep = shared_ptr<Endpoint>(new Endpoint(make_shared<NetworkHandle>(net_handle), buf, bindings));
  // Setup Listener Control
  shared_ptr<mutex> m = shared_ptr<mutex>(new mutex());
  shared_ptr<condition_variable> c = shared_ptr<condition_variable>(new condition_variable());
  shared_ptr<ListenerCounter> counter = shared_ptr<ListenerCounter>(new ListenerCounter());
  shared_ptr<ListenerControl> ctrl = shared_ptr<ListenerControl>(new ListenerControl(m,c,counter));
  // Setup Codec
  shared_ptr<DefaultInternalCodec> i_cdec = shared_ptr<DefaultInternalCodec>(new DefaultInternalCodec());
  // Create Listener
  shared_ptr<K3::Asio::Listener> l = shared_ptr<K3::Asio::Listener>(new K3::Asio::Listener(
  	id, 
  	context,
  	qs,
  	ep,
  	ctrl,
  	i_cdec
  	));
  

  cout << "Waiting for threads to finish" << endl;
  context->service_threads->join_all();
  cout << "Done" << endl;
}


};
int main() {
  K3::setup();
  return 0;

}

