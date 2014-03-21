#include <memory>

#include <runtime/cpp/Listener.hpp>
#include <runtime/cpp/Common.hpp>
#include <runtime/cpp/Queue.hpp>


namespace K3 {


using NContext = K3::Asio::NContext;

// Utils
void do_nothing(const Address&, const Identifier&, shared_ptr<Value>) {}

shared_ptr<K3::Net::Listener> setup(shared_ptr<NContext> context, Address& me, shared_ptr<SinglePeerQueue> qs) {
  // Setup context and Queues
  Identifier id = "id";
  // Setup network IOHandle
  shared_ptr<K3::Net::NEndpoint> n_ep = shared_ptr<K3::Net::NEndpoint>(new K3::Net::NEndpoint(context, me));
  LengthHeaderCodec cdec = LengthHeaderCodec();
  shared_ptr<NetworkHandle> net_handle = shared_ptr<NetworkHandle>(new NetworkHandle(make_shared<LengthHeaderCodec>(cdec), n_ep));
  // Setup Endpoint
  auto buf = make_shared<ScalarEPBufferST>(ScalarEPBufferST());
  SendFunctionPtr func = do_nothing;
  auto bindings = make_shared<EndpointBindings>(func); 
  shared_ptr<Endpoint> ep = shared_ptr<Endpoint>(new Endpoint(net_handle, buf, bindings));
  // Setup Listener Control
  shared_ptr<mutex> m = shared_ptr<mutex>(new mutex());
  shared_ptr<condition_variable> c = shared_ptr<condition_variable>(new condition_variable());
  shared_ptr<ListenerCounter> counter = shared_ptr<ListenerCounter>(new ListenerCounter());
  shared_ptr<ListenerControl> ctrl = shared_ptr<ListenerControl>(new ListenerControl(m,c,counter));
  // Setup Codec
  shared_ptr<DefaultInternalCodec> i_cdec = shared_ptr<DefaultInternalCodec>(new DefaultInternalCodec());
  // Create Listener
  shared_ptr<K3::Net::Listener> l = shared_ptr<K3::Net::Listener>(new K3::Asio::Listener(
  	id, 
  	context,
  	qs,
  	ep,
  	ctrl,
  	i_cdec
  	));

  return l;
}
shared_ptr<Endpoint> write_to_listener(shared_ptr<NContext> context) {
  // Setup context
  Identifier id = "id";
  Address server = defaultAddress;

  // setup connection
  shared_ptr<K3::Asio::NConnection> n_conn = shared_ptr<K3::Asio::NConnection>(new K3::Asio::NConnection(context, server));
  LengthHeaderCodec cdec = LengthHeaderCodec();
  NetworkHandle net_handle = NetworkHandle(make_shared<LengthHeaderCodec>(cdec), n_conn);
  auto buf = make_shared<ScalarEPBufferST>(ScalarEPBufferST());
  SendFunctionPtr func = do_nothing;
  auto bindings = make_shared<EndpointBindings>(func);
  shared_ptr<Endpoint> ep = shared_ptr<Endpoint>(new Endpoint(make_shared<NetworkHandle>(net_handle), buf, bindings));
  // Run the io_service to establish the connection
  (*context)();
  context->service->reset();

  for (int i =0; i < 15; i++) {
    Value val = "(127.0.0.1:40000, trig1, message_" + std::to_string(i) + ")";
    net_handle.doWrite(val);
    // Run the io_service to execute the async write listeners
  }
  (*context)();
  context->service->reset();
  return ep;
}

};
int main() {
  using std::cout;
  using std::endl;
  using std::shared_ptr;
  // Create (start) a Listener
  K3::Address me = K3::defaultAddress;
  shared_ptr<K3::SinglePeerQueue> qs = shared_ptr<K3::SinglePeerQueue>(new K3::SinglePeerQueue(me));
  shared_ptr<K3::Net::NContext> listener_context = shared_ptr<K3::Net::NContext>(new K3::Net::NContext());
  shared_ptr<K3::Net::Listener> listener = K3::setup(listener_context, me, qs);
  // Write to Listener
  shared_ptr<K3::Net::NContext> ep_context = shared_ptr<K3::Net::NContext>(new K3::Net::NContext());

  shared_ptr<K3::Endpoint> ep = K3::write_to_listener(ep_context);
  // Give the listener 1 second to finish its reads
  boost::this_thread::sleep_for( boost::chrono::seconds(1) );
  cout << "Cleaning up:" << endl;
  listener_context->service->stop();
  listener_context->service_threads->join_all();
  ep_context->service->stop();
  ep_context->service_threads->join_all();

  cout << "Done" << endl;
  return 0;

}

