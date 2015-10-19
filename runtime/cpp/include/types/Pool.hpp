#ifndef K3_POOL
#define K3_POOL

#include <memory>
#include <utility>
#include <queue>

#include <concurrentqueue/blockingconcurrentqueue.h>
#include <bdlma_concurrentmultipoolallocator.h>

#include "types/Dispatcher.hpp"

namespace K3 {
class Pool {
 public:
  // Singleton instance
  static inline Pool& getInstance() {
    static Pool instance;
    return instance;
  }

  // Pool manages unique_ptrs with deleters.
  template <class T>
  using unique_ptr = std::unique_ptr<T, std::function<void(T*)>>;

  // std::make_unique equivalent for the Pool
  template <class T, class... Args>
  unique_ptr<T> make_unique(Args&&... args) {
    T* p = new(pool_) T(std::forward<Args>(args)...);
    return unique_ptr<T>(p, [this] (T* t) { pool_.deleteObject(t); });
  }

  // subclass variant of make_unique.
  // Allocates/Initializes a Derived object but returns a Base pointer
  template <class Base, class Derived, class... Args>
  unique_ptr<Base> make_unique_subclass(Args&&... args) {
    Derived *p = new(pool_) Derived(std::forward<Args>(args)...);
    return unique_ptr<Base>(p, [this] (Base* b) { pool_.deleteObject(b); });
  }

 protected:
  Pool() : pool_(5) { }
  Pool(const Pool& other) = delete;
  Pool& operator=(const Pool& other) = delete;
  BloombergLP::bdlma::ConcurrentMultipoolAllocator pool_;
};

}  // namespace K3

#endif
