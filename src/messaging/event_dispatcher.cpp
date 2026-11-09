#include "messaging/event_dispatcher.hpp"

#include <exception>
#include <utility>

namespace rfq {

EventDispatcher::EventDispatcher(std::vector<std::shared_ptr<EventConsumer>> consumers,
                                 std::size_t capacity)
    : consumers_(std::move(consumers)), capacity_(capacity) {
  worker_ = std::thread([this] { run(); });
}

EventDispatcher::~EventDispatcher() {
  {
    std::lock_guard lock(mu_);
    stopping_ = true;
  }
  cv_.notify_one();
  worker_.join();
}

bool EventDispatcher::push(Event event) {
  {
    std::lock_guard lock(mu_);
    if (queue_.size() >= capacity_) {
      dropped_.fetch_add(1, std::memory_order_relaxed);
      return false;
    }
    queue_.push_back(std::move(event));
  }
  cv_.notify_one();
  return true;
}

void EventDispatcher::run() {
  std::deque<Event> batch;
  for (;;) {
    {
      std::unique_lock lock(mu_);
      cv_.wait(lock, [this] { return stopping_ || !queue_.empty(); });
      if (queue_.empty()) return;  // stopping and fully drained
      batch.swap(queue_);
    }
    for (const Event& event : batch) {
      for (const auto& consumer : consumers_) {
        try {
          consumer->on_event(event);
        } catch (const std::exception&) {
          consumer_errors_.fetch_add(1, std::memory_order_relaxed);
        }
      }
    }
    batch.clear();
  }
}

}  // namespace rfq
