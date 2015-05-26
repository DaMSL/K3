#ifndef K3_INCOMINGCONNECTION
#define K3_INCOMINGCONNECTION

#include "Common.hpp"
#include "NetworkManager.hpp"

namespace K3 {

class IncomingConnection {
 public:
  IncomingConnection(asio::io_service&, CodecFormat);
  ~IncomingConnection();
  shared_ptr<asio::ip::tcp::socket> getSocket();
  virtual void receiveMessages(shared_ptr<MessageHandler>, shared_ptr<ErrorHandler>) = 0;

 protected:
  shared_ptr<asio::ip::tcp::socket> socket_;
  CodecFormat format_;
};

class InternalIncomingConnection : public IncomingConnection,
                                   public enable_shared_from_this<InternalIncomingConnection> {
 public:
  InternalIncomingConnection(asio::io_service&, CodecFormat);
  virtual void receiveMessages(shared_ptr<MessageHandler>, shared_ptr<ErrorHandler>);
};

class ExternalIncomingConnection : public IncomingConnection,
                                   public enable_shared_from_this<ExternalIncomingConnection> {
 public:
  ExternalIncomingConnection(asio::io_service& service,
                             CodecFormat format,
                             Address peer_addr,
                             TriggerID dest_trig)
    : IncomingConnection(service, format) {
    peer_addr_ = peer_addr;
    trigger_ = dest_trig;
  }
  virtual void receiveMessages(shared_ptr<MessageHandler>, shared_ptr<ErrorHandler>);

 protected:
  Address peer_addr_;
  TriggerID trigger_;
};

}  // namespace K3

#endif
