#include "Resolver.hpp"
using namespace K3;

int main()
{
    RedisResolver resolver([](const Address&, const TriggerId, shared_ptr<Value>) { },true, "127.0.0.1 6379");
    resolver.sayHello("MY_ID");
    resolver.sayGoodbye();
    return 0;
}
