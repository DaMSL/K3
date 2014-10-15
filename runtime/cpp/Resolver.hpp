#ifndef K3_RUNTIME_RESOLVER_H
#define K3_RUNTIME_RESOLVER_H

#include <string>
#include <unordered_map>

#include <boost/thread/thread.hpp>

#include <Common.hpp>
#include <Endpoint.hpp>

extern "C" struct redisContext;

namespace K3 {
  template<typename T>
  class Collection;

  class Resolver
  {
  public:
    EndpointBindings bindings;

    Resolver(SendFunctionPtr sendFn);
    virtual ~Resolver() = default;

    virtual void sayHello(PeerId myId, Address me) = 0;
    virtual void sayGoodbye() = 0;
    virtual Address lookup(const PeerId& id) = 0;
  };

  class RedisResolver : public Resolver, public LogMT
  {
  private:
    struct PeerRecord
    {
      PeerId name;
      PeerRecord(PeerId n) : name(n) { }

      template<class Archiver>
      void serialize(Archiver& ar, const unsigned int)
      {
        ar & name;
      }
    };

    PeerId myID; // Should probably not be here, should be fetched from somewhere else
    boost::thread subscription_listener;

    bool subscription_started;
    bool download_completed;
    boost::mutex mutex;
    boost::condition_variable cv;
    std::unordered_map<std::string, Address> directory;
    void startMaster(const std::string& address);
    void startSlave(const std::string& upstream);

    void subscriptionWork();
    void publishHello(const PeerId& peer);
    void publishHello(const Collection<PeerRecord>& peer);
    void publishGoodbye(const PeerId& peer);

  public:
    RedisResolver(SendFunctionPtr sendFn, bool master, const std::string& address);
    virtual ~RedisResolver();

    virtual void sayHello(PeerId myID, Address me) override;
    virtual void sayGoodbye() override;

    virtual Address lookup(const PeerId& id) override;
  };
}

#endif
// vim: set sw=2 ts=2 sts=2:
