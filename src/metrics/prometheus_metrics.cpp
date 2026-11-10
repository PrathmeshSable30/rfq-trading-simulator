#include "metrics/prometheus_metrics.hpp"

#include <string>
#include <vector>

namespace rfq {

namespace {

// 1 us .. 10 ms, with extra resolution around the 50 us - 2 ms range where FIX handling lands.
const std::vector<double> kProcessingBuckets{1e-6, 5e-6, 1e-5, 2.5e-5, 5e-5, 7.5e-5, 1e-4, 1.5e-4, 2e-4, 3e-4,
                                             4e-4, 5e-4, 7.5e-4, 1e-3, 1.5e-3, 2e-3, 3e-3, 5e-3, 1e-2};

// 0.5 ms .. 2.5 s: includes dealer response time.
const std::vector<double> kRoundTripBuckets{5e-4, 1e-3, 2e-3, 5e-3, 1e-2, 2e-2, 5e-2,
                                            1e-1, 2.5e-1, 5e-1, 1.0, 2.5};

}  // namespace

PrometheusMetrics::PrometheusMetrics(std::shared_ptr<prometheus::Registry> registry)
    : registry_(registry),
      processing_family_(prometheus::BuildHistogram()
                             .Name("rfq_message_processing_seconds")
                             .Help("Time the gateway spends handling one inbound FIX application message")
                             .Register(*registry)),
      rejects_family_(prometheus::BuildCounter()
                          .Name("rfq_rejects_total")
                          .Help("BusinessMessageRejects sent, by reason")
                          .Register(*registry)),
      requests_(prometheus::BuildCounter()
                    .Name("rfq_requests_total")
                    .Help("RFQs accepted from clients")
                    .Register(*registry)
                    .Add({})),
      quotes_(prometheus::BuildCounter()
                  .Name("rfq_quotes_total")
                  .Help("Dealer quotes accepted and relayed to clients")
                  .Register(*registry)
                  .Add({})),
      executions_(prometheus::BuildCounter()
                      .Name("rfq_executions_total")
                      .Help("RFQs executed")
                      .Register(*registry)
                      .Add({})),
      expired_(prometheus::BuildCounter()
                   .Name("rfq_expired_total")
                   .Help("RFQs that expired without execution")
                   .Register(*registry)
                   .Add({})),
      cancelled_(prometheus::BuildCounter()
                     .Name("rfq_cancelled_total")
                     .Help("RFQs cancelled by the client")
                     .Register(*registry)
                     .Add({})),
      first_quote_(prometheus::BuildHistogram()
                       .Name("rfq_time_to_first_quote_seconds")
                       .Help("RFQ received to first dealer quote relayed")
                       .Register(*registry)
                       .Add({}, kRoundTripBuckets)),
      execution_(prometheus::BuildHistogram()
                     .Name("rfq_time_to_execution_seconds")
                     .Help("RFQ received to execution")
                     .Register(*registry)
                     .Add({}, kRoundTripBuckets)),
      open_rfqs_(prometheus::BuildGauge()
                     .Name("rfq_open_rfqs")
                     .Help("RFQs currently open")
                     .Register(*registry)
                     .Add({})),
      dealers_online_(prometheus::BuildGauge()
                          .Name("rfq_dealers_online")
                          .Help("Dealer FIX sessions currently logged on")
                          .Register(*registry)
                          .Add({})),
      dispatcher_dropped_(prometheus::BuildCounter()
                              .Name("rfq_dispatcher_dropped_events_total")
                              .Help("Events dropped because the Kafka/Redis queue was full")
                              .Register(*registry)
                              .Add({})),
      dispatcher_errors_(prometheus::BuildCounter()
                             .Name("rfq_dispatcher_consumer_errors_total")
                             .Help("Exceptions raised by Kafka/Redis event consumers")
                             .Register(*registry)
                             .Add({})),
      kafka_delivery_failures_(prometheus::BuildCounter()
                                   .Name("rfq_kafka_delivery_failures_total")
                                   .Help("Events Kafka accepted locally but could not deliver to the broker")
                                   .Register(*registry)
                                   .Add({})) {}

void PrometheusMetrics::on_message(const char* fix_msg_type, double processing_seconds) {
  processing_family_.Add({{"msg_type", fix_msg_type}}, kProcessingBuckets).Observe(processing_seconds);
}

void PrometheusMetrics::on_executed(double seconds) {
  executions_.Increment();
  execution_.Observe(seconds);
}

void PrometheusMetrics::on_reject(const char* reason) {
  rejects_family_.Add({{"reason", reason}}).Increment();
}

void PrometheusMetrics::on_gauges(std::size_t open_rfqs, std::size_t dealers_online) {
  open_rfqs_.Set(static_cast<double>(open_rfqs));
  dealers_online_.Set(static_cast<double>(dealers_online));
}

void PrometheusMetrics::update_dispatcher(std::uint64_t dropped_total, std::uint64_t consumer_errors_total) {
  if (dropped_total > last_dropped_) dispatcher_dropped_.Increment(static_cast<double>(dropped_total - last_dropped_));
  if (consumer_errors_total > last_errors_) {
    dispatcher_errors_.Increment(static_cast<double>(consumer_errors_total - last_errors_));
  }
  last_dropped_ = dropped_total;
  last_errors_ = consumer_errors_total;
}

void PrometheusMetrics::update_kafka(std::uint64_t delivery_failures_total) {
  if (delivery_failures_total > last_kafka_failures_) {
    kafka_delivery_failures_.Increment(static_cast<double>(delivery_failures_total - last_kafka_failures_));
  }
  last_kafka_failures_ = delivery_failures_total;
}

}  // namespace rfq
