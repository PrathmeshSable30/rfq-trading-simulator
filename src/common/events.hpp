#pragma once

#include <cstdint>
#include <string>
#include <variant>

#include "common/types.hpp"

namespace rfq {

struct RfqCreated {
  RfqId rfq_id{};
  std::string client_id;
  std::string instrument;
  Side side{};
  Qty qty{};
  TimestampNs expires_at{};
};

struct QuoteReceived {
  RfqId rfq_id{};
  QuoteId quote_id{};
  std::string dealer_id;
  Price price{};
  Qty qty{};
  TimestampNs valid_until{};
};

struct QuoteWithdrawn {
  RfqId rfq_id{};
  QuoteId quote_id{};
  std::string dealer_id;
};

// The dealer sent a new quote, which replaces this one. Kept apart from QuoteWithdrawn so the
// audit trail can tell "the dealer pulled its price" from "the dealer improved it".
struct QuoteSuperseded {
  RfqId rfq_id{};
  QuoteId quote_id{};
  std::string dealer_id;
  QuoteId replaced_by{};
};

struct RfqExecuted {
  RfqId rfq_id{};
  QuoteId quote_id{};
  std::string dealer_id;
  Price price{};
  Qty qty{};
};

struct RfqExpired {
  RfqId rfq_id{};
};

struct RfqCancelled {
  RfqId rfq_id{};
};

using EventPayload = std::variant<RfqCreated, QuoteReceived, QuoteWithdrawn, QuoteSuperseded,
                                  RfqExecuted, RfqExpired, RfqCancelled>;

struct Event {
  std::uint64_t seq{};
  TimestampNs ts{};
  EventPayload payload;
};

}  // namespace rfq
