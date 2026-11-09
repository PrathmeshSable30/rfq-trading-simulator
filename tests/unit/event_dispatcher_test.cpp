#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <vector>

#include "messaging/event_dispatcher.hpp"

using namespace rfq;

namespace {

class RecordingConsumer : public EventConsumer {
 public:
  void on_event(const Event& e) override {
    std::lock_guard lock(mu);
    seqs.push_back(e.seq);
  }
  std::mutex mu;
  std::vector<std::uint64_t> seqs;
};

class ThrowingConsumer : public EventConsumer {
 public:
  void on_event(const Event&) override { throw std::runtime_error("boom"); }
};

Event make(std::uint64_t seq) { return Event{seq, 0, RfqExpired{seq}}; }

}  // namespace

TEST(EventDispatcherTest, DeliversAllEventsInOrderBeforeDestructionReturns) {
  auto consumer = std::make_shared<RecordingConsumer>();
  {
    EventDispatcher dispatcher({consumer});
    for (std::uint64_t i = 1; i <= 1000; ++i) ASSERT_TRUE(dispatcher.push(make(i)));
  }

  ASSERT_EQ(consumer->seqs.size(), 1000u);
  for (std::uint64_t i = 0; i < 1000; ++i) EXPECT_EQ(consumer->seqs[i], i + 1);
}

TEST(EventDispatcherTest, FansOutToEveryConsumer) {
  auto a = std::make_shared<RecordingConsumer>();
  auto b = std::make_shared<RecordingConsumer>();
  {
    EventDispatcher dispatcher({a, b});
    dispatcher.push(make(1));
    dispatcher.push(make(2));
  }

  EXPECT_EQ(a->seqs.size(), 2u);
  EXPECT_EQ(b->seqs.size(), 2u);
}

TEST(EventDispatcherTest, ThrowingConsumerDoesNotStarveOthers) {
  auto good = std::make_shared<RecordingConsumer>();
  {
    EventDispatcher dispatcher({std::make_shared<ThrowingConsumer>(), good});
    dispatcher.push(make(1));
    dispatcher.push(make(2));
  }
  EXPECT_EQ(good->seqs.size(), 2u);
}

TEST(EventDispatcherTest, CountsConsumerErrors) {
  auto dispatcher = std::make_unique<EventDispatcher>(
      std::vector<std::shared_ptr<EventConsumer>>{std::make_shared<ThrowingConsumer>()});
  dispatcher->push(make(1));

  // Poll until the worker has processed the event.
  for (int i = 0; i < 1000 && dispatcher->consumer_errors() == 0; ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(2));
  }
  EXPECT_EQ(dispatcher->consumer_errors(), 1u);
}

TEST(EventDispatcherTest, DropsWhenQueueIsFull) {
  struct Gate : EventConsumer {
    std::mutex m;
    void on_event(const Event&) override { std::lock_guard lock(m); }
  };
  auto gate = std::make_shared<Gate>();
  std::unique_lock hold(gate->m);

  EventDispatcher dispatcher({gate}, 2);
  int rejected = 0;
  for (std::uint64_t i = 1; i <= 50; ++i) {
    if (!dispatcher.push(make(i))) ++rejected;
  }
  // The worker may have already taken a batch, but it cannot free more than a few slots.
  EXPECT_GT(rejected, 0);
  EXPECT_EQ(dispatcher.dropped(), static_cast<std::uint64_t>(rejected));
  hold.unlock();
}
