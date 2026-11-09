#pragma once

#include <cstddef>

namespace rfq {

// Receives gateway measurements. on_message runs outside the gateway mutex, so implementations
// must be thread-safe; keep every call cheap.
class GatewayObserver {
 public:
  virtual ~GatewayObserver() = default;

  virtual void on_message(const char* /*fix_msg_type*/, double /*processing_seconds*/) {}
  virtual void on_rfq_created() {}
  virtual void on_quote_relayed() {}
  virtual void on_first_quote(double /*seconds_since_request*/) {}
  virtual void on_executed(double /*seconds_since_request*/) {}
  virtual void on_expired() {}
  virtual void on_cancelled() {}
  virtual void on_reject(const char* /*reason*/) {}
  virtual void on_gauges(std::size_t /*open_rfqs*/, std::size_t /*dealers_online*/) {}
};

}  // namespace rfq
