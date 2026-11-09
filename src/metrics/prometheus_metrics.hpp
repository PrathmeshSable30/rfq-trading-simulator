#pragma once

#include <cstdint>
#include <memory>

#include <prometheus/counter.h>
#include <prometheus/family.h>
#include <prometheus/gauge.h>
#include <prometheus/histogram.h>
#include <prometheus/registry.h>

#include "fix/gateway_observer.hpp"

namespace rfq {

// Exposes gateway measurements as Prometheus metrics (see deploy/grafana/dashboards/rfq.json).
class PrometheusMetrics : public GatewayObserver {
 public:
  explicit PrometheusMetrics(std::shared_ptr<prometheus::Registry> registry);

  void on_message(const char* fix_msg_type, double processing_seconds) override;
  void on_rfq_created() override { requests_.Increment(); }
  void on_quote_relayed() override { quotes_.Increment(); }
  void on_first_quote(double seconds) override { first_quote_.Observe(seconds); }
  void on_executed(double seconds) override;
  void on_expired() override { expired_.Increment(); }
  void on_cancelled() override { cancelled_.Increment(); }
  void on_reject(const char* reason) override;
  void on_gauges(std::size_t open_rfqs, std::size_t dealers_online) override;

  // Takes running totals from the event dispatcher and exports the increase since the last call.
  void update_dispatcher(std::uint64_t dropped_total, std::uint64_t consumer_errors_total);

 private:
  std::shared_ptr<prometheus::Registry> registry_;  // keeps the metrics below alive
  prometheus::Family<prometheus::Histogram>& processing_family_;
  prometheus::Family<prometheus::Counter>& rejects_family_;
  prometheus::Counter& requests_;
  prometheus::Counter& quotes_;
  prometheus::Counter& executions_;
  prometheus::Counter& expired_;
  prometheus::Counter& cancelled_;
  prometheus::Histogram& first_quote_;
  prometheus::Histogram& execution_;
  prometheus::Gauge& open_rfqs_;
  prometheus::Gauge& dealers_online_;
  prometheus::Counter& dispatcher_dropped_;
  prometheus::Counter& dispatcher_errors_;
  std::uint64_t last_dropped_{0};
  std::uint64_t last_errors_{0};
};

}  // namespace rfq
