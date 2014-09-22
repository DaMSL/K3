#include <algorithm>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

#include <unistd.h>

#include <hiredis/hiredis.h>

#include "Resolver.hpp"

constexpr int REDIS_PORT    =  6379;
constexpr int SENTINEL_PORT = 26379;
#define redisCommand(...) reinterpret_cast<redisReply*>(redisCommand(__VA_ARGS__))

const std::string K3_MASTER_NAME("k3_peers");
const std::string K3_PEER_MAP("k3_peer_map");
const std::string K3_UPDATE_CHANEL("k3_peer_updates");
const std::string LOCALHOST("127.0.0.1");
const std::string GOODBYE("GOODBYE");

static inline std::string getAddress(const std::string& addr_and_port)
{
  return std::string(addr_and_port.begin(), std::find(addr_and_port.begin(), addr_and_port.end(), ' '));
}

const std::string redisServerCommand = "redis-server";
K3::RedisResolver::RedisResolver(bool master, const std::string& address)
  : readContext(nullptr)
{
    if (master)
        startMaster(address);
    else
        startSlave(address);
}

K3::RedisResolver::~RedisResolver()
{
  if( readContext ) {
    redisFree(readContext);
  }
  // TODO Shut down redis and sentinel
}

static inline void writeSentinelConfiguration(const std::string& address, const std::string& port)
{
  std::ofstream output("sentinel.conf");
  output << "\
sentinel monitor " << K3_MASTER_NAME << " " << address << " " << port << " 2\n\
sentinel down-after-milliseconds " << K3_MASTER_NAME << " 60000\n\
sentinel failover-timeout " << K3_MASTER_NAME << " 180000\n\
sentinel parallel-syncs " << K3_MASTER_NAME << " 3\n";
}
static inline void writeSentinelConfiguration(const std::vector<std::string>& master)
{
  writeSentinelConfiguration(master.at(0), master.at(1));
}

void handleRedisError(redisReply * reply)
{
  std::cerr << "Error from Redis: " << reply->str << "\n";
  exit(EXIT_FAILURE);
}

template<typename T>
T parseRedisResponse(redisReply * reply);

template<>
std::string parseRedisResponse(redisReply * reply)
{
  switch(reply->type)
  {
    case REDIS_REPLY_STRING:
      return std::string(reply->str, reply->str + reply->len);
    case REDIS_REPLY_ERROR:
      handleRedisError(reply);
    default:
      throw std::runtime_error("Unexpected Redis response");
  }
}

template<>
int parseRedisResponse(redisReply * reply)
{
  switch(reply->type)
  {
    case REDIS_REPLY_INTEGER:
      return reply->integer;
    case REDIS_REPLY_ERROR:
      handleRedisError(reply);
    default:
      throw std::runtime_error("Unexpected Redis response");
  }
}

template<>
std::vector<std::string> parseRedisResponse(redisReply * reply)
{
  switch(reply->type)
  {
    case REDIS_REPLY_ERROR:
      handleRedisError(reply);
    case REDIS_REPLY_ARRAY:
    {
      std::vector<std::string> result(reply->elements);
      for( size_t i = 0; i < reply->elements; ++i )
        result[i] = parseRedisResponse<std::string>(reply->element[i]);
      return result;
    }
    default:
    throw std::runtime_error("Unexpected Redis response");
  }
}

static void checkNoError(redisReply * reply)
{
  if (reply->type == REDIS_REPLY_ERROR)
    handleRedisError(reply);
}

static void checkSetReply(redisReply * reply)
{
  switch(reply->type)
  {
    case REDIS_REPLY_STRING:
      if (std::string("OK") != reply->str)
      {
        std::cerr << "Unexpected response from SET: " << reply->str << "\n";
        exit(EXIT_FAILURE);
      }
      break;
    case REDIS_REPLY_NIL:
      std::cerr << "SET failed due to unsatisified NX or XX\n";
      // FIXME should this be a fatal error?
      exit(EXIT_FAILURE);
      break;
    case REDIS_REPLY_ERROR:
      handleRedisError(reply);
      throw 4;
  }
}

static bool checkHSetReply(redisReply * reply)
{
  int answer = parseRedisResponse<int>(reply);
  return answer == 1;
}

static inline int checkHDeleteReply(redisReply * reply)
{
  return parseRedisResponse<int>(reply);
}

void redisHSET(redisContext * ctx, const std::string& map, const std::string& key, const std::string& value)
{
  redisReply * reply = redisCommand(ctx, "HSET %s %s %s", map.c_str(), key.c_str(), value.c_str());
  checkHSetReply(reply);
  freeReplyObject(reply);
}

void redisHDELETE(redisContext * ctx, const std::string& map, const std::string& key)
{
  redisReply * reply = redisCommand(ctx, "HDEL %s %s", map.c_str(), key.c_str());
  checkHDeleteReply(reply);
  freeReplyObject(reply);
}

void redisPUBLISH(redisContext * ctx, const std::string& channel, const std::string& value)
{
  redisReply * reply = redisCommand(ctx, "PUBLISH %s %s", channel.c_str(), value.c_str());
  parseRedisResponse<int>(reply);
  freeReplyObject(reply);
}

void redisSUBSCRIBE(redisContext * ctx, const std::string& channel)
{
  redisReply * reply = redisCommand(ctx, "SUBSCRIBE %s", channel.c_str());
  freeReplyObject(reply);
}

std::vector<std::string> redisGETALL(redisContext * ctx, const std::string& key)
{
  redisReply * reply = redisCommand(ctx, "HGETALL %s", key.c_str());
  auto result = parseRedisResponse<std::vector<std::string>>(reply);
  freeReplyObject(reply);
  return result;
}

// since redisConnect is already taken
redisContext * connectRedis(const std::string& address, int port)
{
  redisContext * ctx = redisConnect(address.c_str(), port);
  if (!ctx)
  {
    std::cout << "Error connecting to Redis\n";
    exit(EXIT_FAILURE);
  }
  // implicit ctx != nullptr
  else if (ctx->err)
  {
    std::cout << "Error connecting to Redis (" << ctx->err << ") " << ctx->errstr << "\n";
    exit(EXIT_FAILURE);
  }
  return ctx;
}

static std::vector<std::string> redisGetMasterAddress(const std::string& address)
{
  redisContext * sentinelContext = connectRedis(address, SENTINEL_PORT);

  redisReply * reply = redisCommand(sentinelContext, "SENTINEL get-master-addr-by-name %s", K3_MASTER_NAME.c_str());
  auto result = parseRedisResponse<std::vector<std::string>>(reply);

  freeReplyObject(reply);
  redisFree(sentinelContext);

  return result;
}

void K3::RedisResolver::startMaster(const std::string& my_address)
{
  if (fork() == 0)
  {
    // child
    execlp("redis-server", "redis-server", NULL);
    std::cout << "Error exec-ing redis-server\n";
    exit(EXIT_FAILURE);
  }

  sleep(2);

  if (fork() == 0)
  {
    writeSentinelConfiguration(LOCALHOST, std::to_string(REDIS_PORT));
    execlp("redis-server", "redis-sentinel", "./sentinel.conf", "--sentinel", NULL);
    std::cout << "Error exec-ing redis-sentinel\n";
    exit(EXIT_FAILURE);
  }

  // parent
  sleep(2); // need to know when redis starts up

  readContext = connectRedis(LOCALHOST.c_str(), REDIS_PORT);
}

void K3::RedisResolver::startSlave(const std::string& upstream)
{
  if (fork() == 0)
  {
    // child
    execlp("redis-server", "redis-server", "--slaveof", upstream.c_str(), NULL);
    std::cout << "Error exec-ing redis-server\n";
    exit(EXIT_FAILURE);
  }

  sleep(2);

  // Get the location of the master
  std::string upstream_address = getAddress(upstream);
  std::vector<std::string> master_address = redisGetMasterAddress(upstream_address);

  if (fork() == 0)
  {
    writeSentinelConfiguration(master_address);
    execlp("redis-server", "redis-sentinel", "./sentinel.conf", "--sentinel", NULL);
    std::cout << "Error exec-ing redis-sentinel\n";
    exit(EXIT_FAILURE);
  }
  sleep(2);

  readContext = connectRedis(LOCALHOST, REDIS_PORT);
}

void K3::RedisResolver::sayHello(const PeerID& me)
{
  const std::string my_address = "127.0.0.1 40000";

  std::vector<std::string> master_location = redisGetMasterAddress(LOCALHOST);
  std::string& master_address = master_location.at(0);
  int master_port = std::stoi(master_location.at(1));

  redisContext * writeContext = connectRedis(master_address, master_port);
  redisHSET(writeContext, K3_PEER_MAP, me, my_address);

  redisPUBLISH(writeContext, K3_UPDATE_CHANEL.c_str(), (me + " " + my_address).c_str());
  redisFree(writeContext);

  redisSUBSCRIBE(readContext, K3_UPDATE_CHANEL);

  redisContext * directoryCtx = connectRedis(LOCALHOST, REDIS_PORT);
  std::vector<std::string> directory = redisGETALL(directoryCtx, K3_PEER_MAP);

  redisFree(directoryCtx);

  // Populate the peers variable

  // Handle Updates
}

void K3::RedisResolver::sayGoodbye(const PeerID& me)
{
  std::vector<std::string> master_location = redisGetMasterAddress(LOCALHOST);
  std::string& master_address = master_location.at(0);
  int master_port = std::stoi(master_location.at(1));

  redisContext * writeContext = connectRedis(master_address, master_port);
  redisHDELETE(writeContext, K3_PEER_MAP, me.c_str());
  redisPUBLISH(writeContext, K3_UPDATE_CHANEL, (GOODBYE + " " + me).c_str());
  redisFree(writeContext);
}

// vim: set sw=2 ts=2 sts=2:
