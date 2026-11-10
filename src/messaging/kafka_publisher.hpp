#pragma once

#include <cstdint>
#include <memory>
#include <string>

#include "messaging/event_consumer.hpp"

namespace rfq {

// Publishes each event as JSON, keyed by rfq_id so one RFQ stays ordered within a partition.
class KafkaEventPublisher : public EventConsumer {
 public:
  KafkaEventPublisher(const std::string& brokers, std::string topic);
  ~KafkaEventPublisher() override;

  void on_event(const Event& event) override;
  void flush(int timeout_ms);

  // Serves delivery reports. produce() only queues a message; whether it reached the broker is
  // known later, so call this periodically even when no events are flowing. Thread-safe.
  void poll(int timeout_ms = 0);

  // Messages that were queued but could not be delivered (broker down past the retry timeout).
  std::uint64_t delivery_failures() const noexcept;

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace rfq
