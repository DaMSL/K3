#ifndef K3_RUNTIME_RESOLVER_H
#define K3_RUNTIME_RESOLVER_H

#include <string>

//#include "Common.hpp"

extern "C" struct redisContext;

namespace K3 {
  typedef std::string PeerID;

  class Resolver
  {
  public:
    virtual ~Resolver() = default;

    virtual void sayHello(const PeerID& me) = 0;
    virtual void sayGoodbye(const PeerID& me) = 0;
  };

  class RedisResolver : public Resolver
  {
  private:
    redisContext * readContext;
    void startMaster(const std::string& address);
    void startSlave(const std::string& upstream);

  public:
    RedisResolver(bool master, const std::string& address);
    virtual ~RedisResolver();

    virtual void sayHello(const PeerID& me) override;
    virtual void sayGoodbye(const PeerID& me) override;
  };
}

#endif
// vim: set sw=2 ts=2 sts=2:
