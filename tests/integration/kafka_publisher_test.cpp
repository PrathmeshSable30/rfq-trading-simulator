#include <gtest/gtest.h>

#include <cstdlib>
#include <string>

#include "messaging/kafka_publisher.hpp"

using namespace rfq;

TEST(KafkaPublisherIntegration, PublishesWithoutDeliveryFailures) {
  const char* brokers = std::getenv("RFQ_KAFKA_BROKERS");
  if (brokers == nullptr) GTEST_SKIP() << "RFQ_KAFKA_BROKERS not set";

  KafkaEventPublisher publisher(brokers, "rfq.events");
  for (std::uint64_t i = 1; i <= 100; ++i) {
    publisher.on_event({i, 0, RfqExpired{i}});
  }
  publisher.flush(10'000);

  EXPECT_EQ(publisher.delivery_failures(), 0u);
}
