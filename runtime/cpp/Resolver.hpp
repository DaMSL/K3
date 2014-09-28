#ifndef K3_RUNTIME_RESOLVER_H
#define K3_RUNTIME_RESOLVER_H

#include <string>

#include <boost/thread/thread.hpp>

#include <Endpoint.hpp>

extern "C" struct redisContext;

namespace K3 {
  typedef std::string PeerID;

  class Resolver
  {
  public:
    virtual ~Resolver() = default;

    virtual void sayHello(PeerID me) = 0;
    virtual void sayGoodbye() = 0;
  };

  class RedisResolver : public EndpointBindings, public Resolver
  {
  private:
    PeerID me; // Should probably not be here, should be fetched from somewhere else
    boost::thread subscription_listener;

    bool subscription_started;
    boost::mutex mutex;
    boost::condition_variable cv;
    void startMaster(const std::string& address);
    void startSlave(const std::string& upstream);

    void subscriptionWork();
    void publishHello(const PeerID& peer, const std::string& address, const std::string& port);
    void publishGoodbye(const PeerID& peer);

  public:
    RedisResolver(SendFunctionPtr sendFn, bool master, const std::string& address);
    virtual ~RedisResolver();

    virtual void sayHello(PeerID me) override;
    virtual void sayGoodbye() override;
  };
}

#endif
// vim: set sw=2 ts=2 sts=2:
