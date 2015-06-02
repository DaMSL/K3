#include <memory>
#include <Listener.hpp>
#include <Common.hpp>
#include <Queue.hpp>

#include <xUnit++/xUnit++.h>

using std::atomic_uint;
std::shared_ptr<atomic_uint> notify_count = std::shared_ptr<atomic_uint>(new atomic_uint(0));

namespace K3 {

using NContext = K3::Asio::NContext;

// Utils
void do_nothing(const Address&, const Identifier&, shared_ptr<Value>) {}

void echo_notification(const Address& addr, const Identifier& id, shared_ptr<Value> v) {
  notify_count->fetch_add(1);
}

shared_ptr<K3::Net::Listener> setup(shared_ptr<NContext> context, Address& me, shared_ptr<MessageQueues> qs) {
  // Setup context and Queues
  Identifier id = "id";
  // Setup network IOHandle
  shared_ptr<K3::Net::NEndpoint> n_ep = shared_ptr<K3::Net::NEndpoint>(new K3::Net::NEndpoint(context, me));
  LengthHeaderFraming frme = LengthHeaderFraming();
  shared_ptr<NetworkHandle> net_handle = shared_ptr<NetworkHandle>(new NetworkHandle(make_shared<LengthHeaderFraming>(frme), n_ep));
  // Setup Endpoint
  auto buf = make_shared<ScalarEPBufferST>(ScalarEPBufferST());
  SendFunctionPtr func = echo_notification;
  auto bindings = make_shared<EndpointBindings>(func);
  shared_ptr<Endpoint> ep = shared_ptr<Endpoint>(new Endpoint(net_handle, buf, bindings));
  // Add some subscribers:
  bindings->attachNotifier(EndpointNotification::SocketData ,me, "TestTrigger");
  // Setup Listener Control
  shared_ptr<mutex> m = shared_ptr<mutex>(new mutex());
  shared_ptr<condition_variable> c = shared_ptr<condition_variable>(new condition_variable());
  shared_ptr<ListenerCounter> counter = shared_ptr<ListenerCounter>(new ListenerCounter());
  shared_ptr<ListenerControl> ctrl = shared_ptr<ListenerControl>(new ListenerControl(m,c,counter));
  // Setup Framing
  shared_ptr<DefaultInternalFraming> i_frme = shared_ptr<DefaultInternalFraming>(new DefaultInternalFraming());
  // Create Listener
  shared_ptr<K3::Net::Listener> l = shared_ptr<K3::Net::Listener>(new K3::Asio::Listener(
  	id,
  	context,
  	qs,
  	ep,
  	ctrl,
  	i_frme
  	));

  return l;
}

shared_ptr<Endpoint> write_to_listener(shared_ptr<NContext> context) {
  // Setup context
  Identifier id = "id";
  Address server = defaultAddress;

  // setup connection
  shared_ptr<K3::Asio::NConnection> n_conn = shared_ptr<K3::Asio::NConnection>(new K3::Asio::NConnection(context, server));
  LengthHeaderFraming frme = LengthHeaderFraming();
  NetworkHandle net_handle = NetworkHandle(make_shared<LengthHeaderFraming>(frme), n_conn);
  auto buf = make_shared<ScalarEPBufferST>(ScalarEPBufferST());
  SendFunctionPtr func = echo_notification;
  auto bindings = make_shared<EndpointBindings>(func);
  shared_ptr<Endpoint> ep = shared_ptr<Endpoint>(new Endpoint(make_shared<NetworkHandle>(net_handle), buf, bindings));
  // Run the io_service to establish the connection
  (*context)();
  context->service->reset();

  for (int i =0; i < 15; i++) {
    Value val = "(127.0.0.1:40000, trig1, message_" + std::to_string(i) + ")";
    net_handle.doWrite(make_shared<string>(val));
    // Run the io_service to execute the async write listeners
  }
  (*context)();
  context->service->reset();
  return ep;
}

};

FACT("Send 15 messages from Endpoint to Listener => 15 notifications, 15 messages on queue") {
  using std::cout;
  using std::endl;
  using std::shared_ptr;
  using std::make_shared;
  using std::string;

  // Create (start) a Listener
  K3::Address me = K3::defaultAddress;
  shared_ptr<K3::MessageQueues> qs = make_shared<K3::SinglePeerQueue>(K3::SinglePeerQueue(me));
  shared_ptr<K3::Net::NContext> listener_context = shared_ptr<K3::Net::NContext>(new K3::Net::NContext());
  shared_ptr<K3::Net::Listener> listener = K3::setup(listener_context, me, qs);
  // Write to Listener
  shared_ptr<K3::Net::NContext> ep_context = shared_ptr<K3::Net::NContext>(new K3::Net::NContext());
  shared_ptr<K3::Endpoint> ep = K3::write_to_listener(ep_context);

  // Give the listener 1 second to finish its reads
  boost::this_thread::sleep_for( boost::chrono::seconds(1) );
  listener_context->service->stop();
  listener_context->service_threads->join_all();
  ep_context->service->stop();
  ep_context->service_threads->join_all();

  // Make sure messages made it onto the queues
  int i = 0;
  while (qs->size() > 0) {
     K3::Message m = *(qs->dequeue());
     string expected = "message_" + std::to_string(i);
     i++;
     string actual = m.contents();
     Assert.Equal(expected, actual);
  }

  // Make sure 15 notifications occured
  int x = *notify_count;
  Assert.Equal(15, x);
}

