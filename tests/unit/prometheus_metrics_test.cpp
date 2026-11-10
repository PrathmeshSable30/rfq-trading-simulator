#include <gtest/gtest.h>

#include <memory>
#include <string>

#include <prometheus/registry.h>
#include <prometheus/text_serializer.h>

#include "metrics/prometheus_metrics.hpp"

using namespace rfq;

namespace {

class PrometheusMetricsTest : public ::testing::Test {
 protected:
  std::shared_ptr<prometheus::Registry> registry = std::make_shared<prometheus::Registry>();
  PrometheusMetrics metrics{registry};

  std::string scrape() const { return prometheus::TextSerializer().Serialize(registry->Collect()); }

  bool has(const std::string& line) const { return scrape().find(line) != std::string::npos; }
};

TEST_F(PrometheusMetricsTest, CountersTrackLifecycleEvents) {
  metrics.on_rfq_created();
  metrics.on_rfq_created();
  metrics.on_quote_relayed();
  metrics.on_expired();
  metrics.on_cancelled();
  metrics.on_executed(0.004);

  EXPECT_TRUE(has("rfq_requests_total 2\n"));
  EXPECT_TRUE(has("rfq_quotes_total 1\n"));
  EXPECT_TRUE(has("rfq_expired_total 1\n"));
  EXPECT_TRUE(has("rfq_cancelled_total 1\n"));
  EXPECT_TRUE(has("rfq_executions_total 1\n"));
}

TEST_F(PrometheusMetricsTest, ExecutionLatencyLandsInTheRightBucket) {
  metrics.on_executed(0.004);

  EXPECT_TRUE(has("rfq_time_to_execution_seconds_count 1\n"));
  EXPECT_TRUE(has("rfq_time_to_execution_seconds_bucket{le=\"0.002\"} 0\n"));
  EXPECT_TRUE(has("rfq_time_to_execution_seconds_bucket{le=\"0.005\"} 1\n"));
}

TEST_F(PrometheusMetricsTest, ProcessingTimeIsLabelledByMessageType) {
  metrics.on_message("R", 2e-6);
  metrics.on_message("R", 3e-6);
  metrics.on_message("AJ", 1e-5);

  EXPECT_TRUE(has("rfq_message_processing_seconds_count{msg_type=\"R\"} 2\n"));
  EXPECT_TRUE(has("rfq_message_processing_seconds_count{msg_type=\"AJ\"} 1\n"));
}

TEST_F(PrometheusMetricsTest, RejectsAreLabelledByReason) {
  metrics.on_reject("no dealers online");
  metrics.on_reject("no dealers online");
  metrics.on_reject("unknown QuoteID");

  EXPECT_TRUE(has("rfq_rejects_total{reason=\"no dealers online\"} 2\n"));
  EXPECT_TRUE(has("rfq_rejects_total{reason=\"unknown QuoteID\"} 1\n"));
}

TEST_F(PrometheusMetricsTest, GaugesReflectLatestValues) {
  metrics.on_gauges(7, 3);
  metrics.on_gauges(5, 3);

  EXPECT_TRUE(has("rfq_open_rfqs 5\n"));
  EXPECT_TRUE(has("rfq_dealers_online 3\n"));
}

TEST_F(PrometheusMetricsTest, DispatcherTotalsAreExportedAsIncreases) {
  metrics.update_dispatcher(4, 1);
  metrics.update_dispatcher(10, 1);

  EXPECT_TRUE(has("rfq_dispatcher_dropped_events_total 10\n"));
  EXPECT_TRUE(has("rfq_dispatcher_consumer_errors_total 1\n"));
}

TEST_F(PrometheusMetricsTest, KafkaDeliveryFailuresAreExportedAsIncreases) {
  EXPECT_TRUE(has("rfq_kafka_delivery_failures_total 0\n"));
  metrics.update_kafka(3);
  metrics.update_kafka(3);
  metrics.update_kafka(5);
  EXPECT_TRUE(has("rfq_kafka_delivery_failures_total 5\n"));
}

}  // namespace
