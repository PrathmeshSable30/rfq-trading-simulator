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

  std::uint64_t delivery_failures() const noexcept;

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace rfq
