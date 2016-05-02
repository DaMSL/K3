#ifndef K3_LISTENER
#define K3_LISTENER

#include <memory>
#include "network/NetworkManager.hpp"
#include "IncomingConnection.hpp"
#include <concurrentqueue/blockingconcurrentqueue.h>

namespace K3 {

typedef std::function<shared_ptr<InternalIncomingConnection>(
    asio::io_service&, CodecFormat)> IncomingConnectionFactory;

class Peer;
class Listener : public std::enable_shared_from_this<Listener> {
 public:
  Listener(asio::io_service&, const Address& address, shared_ptr<Peer>,
           CodecFormat, IncomingConnectionFactory);
  template <class E> // ErrorHandler
  void acceptConnection(E&&);
  int numConnections();

 protected:
  void registerConnection(shared_ptr<InternalIncomingConnection>);

  Address address_;
  shared_ptr<Peer> peer_; // Keep the Peer alive while listener is alive
  boost::asio::ip::tcp::acceptor acceptor_;
  CodecFormat format_;
  IncomingConnectionFactory conn_factory_;
  IncomingConnectionMap in_conns_;
};

template<class E> // ErrorHandler
void Listener::acceptConnection(E&& acpt_e_handler) {
  shared_ptr<InternalIncomingConnection> conn = conn_factory_(acceptor_.get_io_service(), format_);

  // Make sure this Listener lives long enough
  acceptor_.async_accept(
      conn->getSocket(),
      [this_shared = shared_from_this(), conn, acpt_e_handler=std::forward<E>(acpt_e_handler)]
        (const boost::system::error_code& ec) mutable {
        if (ec) {
          acpt_e_handler(ec);
        } else {
          this_shared->registerConnection(conn);
          this_shared->acceptConnection(std::forward<E>(acpt_e_handler));
        }
      });
}

}  // namespace K3

#endif
