#pragma once

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include "messaging/event_consumer.hpp"

namespace rfq {

// Moves slow I/O (Kafka, Redis) off the engine thread: push() only enqueues.
class EventDispatcher {
 public:
  explicit EventDispatcher(std::vector<std::shared_ptr<EventConsumer>> consumers,
                           std::size_t capacity = 65'536);
  ~EventDispatcher();

  EventDispatcher(const EventDispatcher&) = delete;
  EventDispatcher& operator=(const EventDispatcher&) = delete;

  // Returns false and counts a drop when the queue is full.
  bool push(Event event);

  std::uint64_t dropped() const noexcept { return dropped_.load(); }
  std::uint64_t consumer_errors() const noexcept { return consumer_errors_.load(); }

 private:
  void run();

  std::vector<std::shared_ptr<EventConsumer>> consumers_;
  std::size_t capacity_;
  std::mutex mu_;
  std::condition_variable cv_;
  std::deque<Event> queue_;
  bool stopping_{false};
  std::atomic<std::uint64_t> dropped_{0};
  std::atomic<std::uint64_t> consumer_errors_{0};
  std::thread worker_;
};

}  // namespace rfq
