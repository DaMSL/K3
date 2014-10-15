#include "Resolver.hpp"
using namespace K3;

int main()
{
    RedisResolver resolver([](const Address&, const TriggerId, shared_ptr<Value>) { }, true, "");
    resolver.sayHello("MY_ID", K3::make_address("127.0.0.1", 40000));
    resolver.sayGoodbye();
    return 0;
}
