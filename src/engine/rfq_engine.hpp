#pragma once

#include <cstdint>
#include <functional>
#include <optional>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/events.hpp"
#include "common/types.hpp"

namespace rfq {

struct RfqRequest {
  std::string client_id;
  std::string instrument;
  Side side{Side::Buy};
  Qty qty{0};
  TimestampNs expires_at{0};
  std::vector<std::string> dealers;
};

struct QuoteInput {
  std::string dealer_id;
  Price price{0};
  Qty qty{0};
  TimestampNs valid_until{0};
};

struct Quote {
  QuoteId id{0};
  RfqId rfq_id{0};
  std::string dealer_id;
  Price price{0};
  Qty qty{0};
  TimestampNs valid_until{0};
  TimestampNs received_at{0};
  QuoteStatus status{QuoteStatus::Active};
};

struct Rfq {
  RfqId id{0};
  RfqRequest request;
  RfqState state{RfqState::Open};
  TimestampNs created_at{0};
  QuoteId executed_quote_id{0};
  std::vector<Quote> quotes;
};

struct Execution {
  RfqId rfq_id{0};
  QuoteId quote_id{0};
  std::string dealer_id;
  Price price{0};
  Qty qty{0};
};

// Single-threaded by design: the caller owns synchronisation and supplies the clock.
class RfqEngine {
 public:
  using EventSink = std::function<void(const Event&)>;

  // id_base offsets generated ids so they stay unique across restarts (ids start at id_base + 1).
  explicit RfqEngine(EventSink sink = {}, std::uint64_t id_base = 0)
      : sink_(std::move(sink)), next_rfq_id_(id_base + 1), next_quote_id_(id_base + 1) {}

  Result<RfqId> submit_rfq(RfqRequest request, TimestampNs now);
  Result<QuoteId> submit_quote(RfqId rfq_id, QuoteInput input, TimestampNs now);
  ErrorCode withdraw_quote(QuoteId quote_id, TimestampNs now);
  Result<Execution> execute(RfqId rfq_id, QuoteId quote_id, TimestampNs now);
  ErrorCode cancel_rfq(RfqId rfq_id, TimestampNs now);

  // Expires every open RFQ whose deadline is <= now and returns their ids.
  std::vector<RfqId> expire(TimestampNs now);

  const Rfq* find(RfqId rfq_id) const;
  std::optional<Quote> best_quote(RfqId rfq_id, TimestampNs now) const;

 private:
  void emit(TimestampNs ts, EventPayload payload);
  void close(Rfq& rfq, RfqState terminal_state);

  EventSink sink_;
  std::unordered_map<RfqId, Rfq> rfqs_;
  std::unordered_map<QuoteId, RfqId> quote_index_;
  std::set<std::pair<TimestampNs, RfqId>> expiry_;
  RfqId next_rfq_id_;
  QuoteId next_quote_id_;
  std::uint64_t next_seq_{1};
};

}  // namespace rfq
