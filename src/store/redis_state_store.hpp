#pragma once

#include <string>
#include <vector>

#include "messaging/event_consumer.hpp"

struct redisContext;

namespace rfq {

// Projects events into Redis hashes:
//   rfq:{id}        state, client_id, instrument, side, qty, expires_at, executed_quote_id, ...
//   rfq:{id}:quotes set of quote ids
//   quote:{id}      rfq_id, dealer_id, price, qty, valid_until, status
// Keys of finished RFQs expire after closed_ttl_seconds.
class RedisStateStore : public EventConsumer {
 public:
  RedisStateStore(const std::string& host, int port, int closed_ttl_seconds = 3600);
  ~RedisStateStore() override;

  RedisStateStore(const RedisStateStore&) = delete;
  RedisStateStore& operator=(const RedisStateStore&) = delete;

  void on_event(const Event& event) override;

  // Returns the value of a hash field, or an empty string when absent.
  std::string hget(const std::string& key, const std::string& field);

 private:
  void command(const std::vector<std::string>& args);
  void expire_rfq(RfqId rfq_id);

  redisContext* ctx_{nullptr};
  int closed_ttl_seconds_;
};

}  // namespace rfq
