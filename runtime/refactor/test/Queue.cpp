// Test that our lockfree Queue can hold Packed, Native, and Sentinel Values
// Ensure that multithreading works, including interrupting using a Sentinel

#include <atomic>
#include <memory>
#include <thread>

#include "gtest/gtest.h"

#include "Value.hpp"
#include "ProgramContext.hpp"
#include "Queue.hpp"

using std::unique_ptr;
using std::make_unique;

// Ensure we can enqueue a single int and dequeue it correctly
TEST(Queue, SimpleEnDeQueue) {
  int i = 100;
  unique_ptr<Value> v = make_unique<TNativeValue<int>>(i);
  Address me = make_address("127.0.0.1", 30000);
  unique_ptr<Message> m1 = make_unique<Message>(me, me, 1, std::move(v));

  Queue q;
  q.enqueue(std::move(m1));
  m1 = q.dequeue();
  NativeValue* nv = m1->value();
  ASSERT_EQ(i, *nv->as<int>());
}

// Enqueue 100 messages on thread 1 and dequeue on thread 2
TEST(Queue, ProducerConsumer) {
  Queue q;
  Address me = make_address("127.0.0.1", 30000);

  std::thread producer([&]() {
    for (int i = 0; i != 100; ++i) {
      unique_ptr<Value> v = make_unique<TNativeValue<int>>(i);
      unique_ptr<Message> m1 = make_unique<Message>(me, me, 1, std::move(v));
      q.enqueue(std::move(m1));
    }
  });

  int count = 0;
  std::thread consumer([&]() {
    for (int i = 0; i != 100; ++i) {
      unique_ptr<Message> m1 = q.dequeue();
      NativeValue* nv = m1->value();
      ASSERT_EQ(i, *nv->as<int>());
      count++;
    }
  });

  producer.join();
  consumer.join();
  ASSERT_EQ(100, count);
}

// Producer/Consumer where the consumer reads until a sentinel is found
TEST(Queue, Sentinel) {
  Queue q;
  shared_ptr<DummyState> s1 = make_shared<DummyState>();
  DummyContext context(s1);
  Address me = make_address("127.0.0.1", 30000);

  std::thread producer([&]() {
    for (int i = 0; i != 100; ++i) {
      unique_ptr<Value> v = make_unique<TNativeValue<int>>(i);
      unique_ptr<Message> m1 = make_unique<Message>(me, me, 1, std::move(v));
      q.enqueue(std::move(m1));
    }
  });

  int count = 0;
  std::thread consumer([&]() {
    try {
      while (true) {
        context.dispatch(q.dequeue());
        count++;
      }
    } catch(EndOfProgramException e) {
      // Pass
    }
  });

  producer.join();
  unique_ptr<Value> v = make_unique<SentinelValue>();
  unique_ptr<Message> m1 = make_unique<Message>(me, me, 1, std::move(v));
  q.enqueue(std::move(m1));
  consumer.join();
}
