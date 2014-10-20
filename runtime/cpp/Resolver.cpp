#include <algorithm>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

#include <unistd.h>

#include <hiredis/hiredis.h>

#include "Common.hpp"
#include "Resolver.hpp"
#include "Serialization.hpp"
#include "dataspace/Dataspace.hpp"

K3::Resolver::Resolver(SendFunctionPtr sendFn)
  : bindings(sendFn)
{
}

constexpr int REDIS_PORT    =  6379;
constexpr int SENTINEL_PORT = 26379;

#define redisCommand(ctx, ...) reinterpret_cast<redisReply*>(redisCommand(ctx, __VA_ARGS__))

struct RedisContextDeleter
{
  void operator()(redisContext * ctx) const
  {
    redisFree(ctx);
  }
};

const std::string K3_MASTER_NAME("k3_peers");
const std::string K3_PEER_MAP("k3_peer_map");
const std::string K3_UPDATE_CHANEL("k3_peer_updates");
const std::string LOCALHOST("127.0.0.1");
const std::string HELLO("HELLO");
const std::string GOODBYE("GOODBYE");

struct ExecException : public std::runtime_error
{
  ExecException(const std::string& program_name)
    : std::runtime_error(std::string("Error exec-ing ") + program_name)
  { }
};

static inline std::string getAddress(const std::string& addr_and_port)
{
  return std::string(addr_and_port.begin(), std::find(addr_and_port.begin(), addr_and_port.end(), ' '));
}

const std::string redisServerCommand = "redis-server";
K3::RedisResolver::RedisResolver(K3::SendFunctionPtr sendFn, bool master, const std::string& address)
  : Resolver(sendFn), LogMT("RedisResolver"),
    subscription_listener(), subscription_started(false),
    download_completed(false), cv(), directory()
{
  if (master)
    startMaster(address);
  else
    startSlave(address);

}

K3::RedisResolver::~RedisResolver()
{
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

static bool checkHSetReply(redisReply * reply)
{
  int answer = parseRedisResponse<int>(reply);
  return answer == 1;
}

static inline int checkHDeleteReply(redisReply * reply)
{
  return parseRedisResponse<int>(reply);
}

class RedisConnection
{
  private:
  std::unique_ptr<redisContext, RedisContextDeleter> ctx;

  public:
  RedisConnection(const std::string& address, int port)
    : ctx()
  {
    ctx = decltype(ctx)(redisConnect(address.c_str(), port));

    if (!ctx)
    {
      throw std::runtime_error("Error connecting to Redis");
    }
    // implicit ctx != nullptr
    else if (ctx->err)
    {
      throw std::runtime_error("Error connecting to Redis (" + std::to_string(ctx->err) + ") " + ctx->errstr);
    }
  }

  template<typename... T>
  redisReply * command(const char * cmd, T... args)
  {
    return redisCommand(ctx.get(), cmd, args...);
  }

  void HSET(const std::string& map, const std::string& key, const std::string& value)
  {
    redisReply * reply = command("HSET %s %s %s", map.c_str(), key.c_str(), value.c_str());
    checkHSetReply(reply);
    freeReplyObject(reply);
  }

  void HDELETE(const std::string& map, const std::string& key)
  {
    redisReply * reply = command("HDEL %s %s", map.c_str(), key.c_str());
    checkHDeleteReply(reply);
    freeReplyObject(reply);
  }

  void PUBLISH(const std::string& channel, const std::string& value)
  {
    redisReply * reply = command("PUBLISH %s %s", channel.c_str(), value.c_str());
    parseRedisResponse<int>(reply);
    freeReplyObject(reply);
  }

  void SUBSCRIBE(const std::string& channel)
  {
    redisReply * reply = command("SUBSCRIBE %s", channel.c_str());
    freeReplyObject(reply);
  }

  std::vector<std::string> GETALL(const std::string& key)
  {
    redisReply * reply = command("HGETALL %s", key.c_str());
    auto result = parseRedisResponse<std::vector<std::string>>(reply);
    freeReplyObject(reply);
    return result;
  }

  int getReply(redisReply ** reply)
  {
    return redisGetReply(ctx.get(), reinterpret_cast<void**>(reply));
  }
};

static std::vector<std::string> redisGetMasterAddress(const std::string& address)
{
  RedisConnection sentinelContext(address, SENTINEL_PORT);

  redisReply * reply = sentinelContext.command("SENTINEL get-master-addr-by-name %s", K3_MASTER_NAME.c_str());
  auto result = parseRedisResponse<std::vector<std::string>>(reply);

  freeReplyObject(reply);

  return result;
}

template<typename... T>
static inline void startRedis(const char* prog, T... params)
{
  if (fork() == 0)
  {
    // child
    execlp(prog, prog, params...);
    throw ExecException(prog);
  }
}

void K3::RedisResolver::startMaster(const std::string&)
{
  startRedis("redis-server", nullptr);
  sleep(2);

  // Redis-sentinel doesn't need the global IP address
  // the sentinels will figure out the IP of this server from each
  // other, so localhost is fine here.
  writeSentinelConfiguration(LOCALHOST, std::to_string(REDIS_PORT));
  startRedis("redis-server", "./sentinel.conf", "--sentinel", NULL);
  sleep(2); // need to know when redis starts up
}

void K3::RedisResolver::startSlave(const std::string& upstream)
{
  startRedis("redis-server", "--slaveof", upstream.c_str(), NULL);
  sleep(2);

  // Get the location of the master
  std::string upstream_address = getAddress(upstream);
  std::vector<std::string> master_address = redisGetMasterAddress(upstream_address);

  writeSentinelConfiguration(master_address);
  startRedis("redis-server", "./sentinel.conf", "--sentinel", NULL);
  sleep(2);
}

void K3::RedisResolver::sayHello(PeerId myID, Address me)
{
  const std::string my_address = addressHost(me) + " " + std::to_string(addressPort(me));

  std::vector<std::string> master_location = redisGetMasterAddress(LOCALHOST);
  std::string& master_address = master_location.at(0);
  int master_port = std::stoi(master_location.at(1));

  {
    RedisConnection writeContext(master_address, master_port);
    writeContext.HSET(K3_PEER_MAP, myID, my_address);

    writeContext.PUBLISH(K3_UPDATE_CHANEL.c_str(), (HELLO + " " + myID + " " + my_address).c_str());
  }

  subscription_listener = boost::thread([=] { this->subscriptionWork(); });
  {
    // Make sure that we're registered for updates before downloading the initial
    // directory, so that no updates get dropped in between.
    boost::unique_lock<boost::mutex> lock(mutex);
    while (!subscription_started)
    {
      cv.wait(lock);
    }
  }

  RedisConnection directoryCon(LOCALHOST, REDIS_PORT);
  std::vector<std::string> directory_download = directoryCon.GETALL(K3_PEER_MAP);
  // TODO fill in peers with the directory

  Collection<PeerRecord> name_list;
  for (size_t idx = 0; idx < directory_download.size(); idx += 2)
  {
    const std::string& name = directory_download.at(0);
    const std::string& address = directory_download.at(idx + 1);
    auto first_space = std::find(address.cbegin(), address.cend(), ' ');
    int port = std::stoi(std::string(first_space + 1, address.cend()));
    Address addr = make_address(std::string(address.cbegin(), first_space), port);

    directory.insert(std::make_pair(name, addr));
    name_list.insert(name);
  }

  publishHello(name_list);

  // Emit event notification for initial download
  {
    boost::unique_lock<boost::mutex> lock(mutex);
    download_completed = true;
  }
  cv.notify_all();
}

void K3::RedisResolver::sayGoodbye(PeerId myID)
{
  std::vector<std::string> master_location = redisGetMasterAddress(LOCALHOST);
  std::string& master_address = master_location.at(0);
  int master_port = std::stoi(master_location.at(1));

  RedisConnection writeContext(master_address, master_port);
  writeContext.HDELETE(K3_PEER_MAP, myID);
  writeContext.PUBLISH(K3_UPDATE_CHANEL, GOODBYE + " " + myID);

  // Make sure that the worker thread is done
  subscription_listener.join();
}


void K3::RedisResolver::subscriptionWork()
{
  RedisConnection readContext(LOCALHOST.c_str(), REDIS_PORT);
  {
    boost::unique_lock<boost::mutex> lock(mutex);
    readContext.SUBSCRIBE(K3_UPDATE_CHANEL);
    subscription_started = true;
  }
  cv.notify_all();

  // Wait for the parent thread to send the initial batch of peers
  {
    boost::unique_lock<boost::mutex> lock(mutex);
    while (!download_completed) {
      cv.wait(lock);
    }
  }

  redisReply * reply = nullptr;
  while(readContext.getReply(&reply) == REDIS_OK)
  {
    // Possible commands are:
    // HELLO peer_id ip_address port
    // GOODBYE peer_id
    std::vector<std::string> components = parseRedisResponse<std::vector<std::string>>(reply);
    if (components.at(0) != "message")
    {
      logAt(boost::log::trivial::warning,
          std::string("Directory service received an unexpected messsage: ") + components.at(0));
      continue;
    }
    if (components.at(1) != K3_UPDATE_CHANEL)
    {
      logAt(boost::log::trivial::warning,
          std::string("Directory service received update from unexpected channel \"") + components.at(1) + "\", expected \"" + K3_UPDATE_CHANEL + "\".");
    }
    const std::string& message = components.at(2);
    auto first_space = std::find(message.cbegin(), message.cend(), ' ');
    std::string command(message.cbegin(), first_space);

    // If there is no space, std::find returns message.cend()
    auto name_end = std::find(first_space + 1, message.cend(), ' ');
    PeerId peer(first_space + 1, name_end);

    if (command == HELLO)
    {
      // Also need to retrieve the ip address and port
      auto address_end = std::find(name_end + 1, message.cend(), ' ');
      std::string address(name_end + 1, address_end);
      int port = std::stoi(std::string(address_end + 1, message.cend()));
      K3::Address addr = make_address(address, port);

      {
        boost::unique_lock<boost::mutex> lock(mutex);
        directory.insert(std::make_pair(peer, addr));
      }

      publishHello(peer);
    }
    else if (command == GOODBYE)
    {
      {
        boost::unique_lock<boost::mutex> lock(mutex);
        directory.erase(peer);
      }
      publishGoodbye(peer);
    }
    else
    {
      exit(EXIT_FAILURE);
    }
  }
}

void K3::RedisResolver::publishHello(const PeerId& peer)
{
  Collection<PeerRecord> col;
  col.insert(PeerRecord(peer));

  publishHello(col);
}

void K3::RedisResolver::publishHello(const Collection<PeerRecord>& col)
{
  std::tuple<bool, Collection<PeerRecord>> args(true, col);
  std::shared_ptr<Value> value = std::make_shared<Value>(BoostSerializer::pack(args));

  bindings.notifyEvent(EndpointNotification::PeerChange, value);
}

void K3::RedisResolver::publishGoodbye(const PeerId& peer)
{
  Collection<PeerRecord> col;
  col.insert(PeerRecord(peer));

  std::tuple<bool, Collection<PeerRecord>> args(false, col);
  std::shared_ptr<Value> value = std::make_shared<Value>(BoostSerializer::pack(col));

  bindings.notifyEvent(EndpointNotification::PeerChange, value);
}

K3::Address K3::RedisResolver::lookup(const PeerId& id)
{
  boost::unique_lock<boost::mutex> lock(mutex);
  return directory.at(id);
}
// vim: set sw=2 ts=2 sts=2:
