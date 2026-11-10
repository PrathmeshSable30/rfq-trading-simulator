#include "messaging/event_codec.hpp"

#include <nlohmann/json.hpp>
#include <stdexcept>

namespace rfq {

namespace {

using nlohmann::json;

constexpr const char* kCreated = "RFQ_CREATED";
constexpr const char* kQuoteReceived = "QUOTE_RECEIVED";
constexpr const char* kQuoteWithdrawn = "QUOTE_WITHDRAWN";
constexpr const char* kQuoteSuperseded = "QUOTE_SUPERSEDED";
constexpr const char* kExecuted = "RFQ_EXECUTED";
constexpr const char* kExpired = "RFQ_EXPIRED";
constexpr const char* kCancelled = "RFQ_CANCELLED";

const char* side_name(Side s) { return s == Side::Buy ? "BUY" : "SELL"; }

void fill(json& j, const RfqCreated& p) {
  j["rfq_id"] = p.rfq_id;
  j["client_id"] = p.client_id;
  j["instrument"] = p.instrument;
  j["side"] = side_name(p.side);
  j["qty"] = p.qty;
  j["expires_at"] = p.expires_at;
}

void fill(json& j, const QuoteReceived& p) {
  j["rfq_id"] = p.rfq_id;
  j["quote_id"] = p.quote_id;
  j["dealer_id"] = p.dealer_id;
  j["price"] = p.price;
  j["qty"] = p.qty;
  j["valid_until"] = p.valid_until;
}

void fill(json& j, const QuoteWithdrawn& p) {
  j["rfq_id"] = p.rfq_id;
  j["quote_id"] = p.quote_id;
  j["dealer_id"] = p.dealer_id;
}

void fill(json& j, const QuoteSuperseded& p) {
  j["rfq_id"] = p.rfq_id;
  j["quote_id"] = p.quote_id;
  j["dealer_id"] = p.dealer_id;
  j["replaced_by"] = p.replaced_by;
}

void fill(json& j, const RfqExecuted& p) {
  j["rfq_id"] = p.rfq_id;
  j["quote_id"] = p.quote_id;
  j["dealer_id"] = p.dealer_id;
  j["price"] = p.price;
  j["qty"] = p.qty;
}

void fill(json& j, const RfqExpired& p) { j["rfq_id"] = p.rfq_id; }

void fill(json& j, const RfqCancelled& p) { j["rfq_id"] = p.rfq_id; }

EventPayload parse_payload(const std::string& type, const json& j) {
  if (type == kCreated) {
    return RfqCreated{j.at("rfq_id"), j.at("client_id"), j.at("instrument"),
                      j.at("side").get<std::string>() == "BUY" ? Side::Buy : Side::Sell,
                      j.at("qty"), j.at("expires_at")};
  }
  if (type == kQuoteReceived) {
    return QuoteReceived{j.at("rfq_id"), j.at("quote_id"), j.at("dealer_id"),
                         j.at("price"),  j.at("qty"),      j.at("valid_until")};
  }
  if (type == kQuoteWithdrawn) {
    return QuoteWithdrawn{j.at("rfq_id"), j.at("quote_id"), j.at("dealer_id")};
  }
  if (type == kQuoteSuperseded) {
    return QuoteSuperseded{j.at("rfq_id"), j.at("quote_id"), j.at("dealer_id"), j.at("replaced_by")};
  }
  if (type == kExecuted) {
    return RfqExecuted{j.at("rfq_id"), j.at("quote_id"), j.at("dealer_id"), j.at("price"),
                       j.at("qty")};
  }
  if (type == kExpired) return RfqExpired{j.at("rfq_id")};
  if (type == kCancelled) return RfqCancelled{j.at("rfq_id")};
  throw std::invalid_argument("unknown event type");
}

}  // namespace

const char* event_type(const EventPayload& payload) {
  struct Namer {
    const char* operator()(const RfqCreated&) const { return kCreated; }
    const char* operator()(const QuoteReceived&) const { return kQuoteReceived; }
    const char* operator()(const QuoteWithdrawn&) const { return kQuoteWithdrawn; }
    const char* operator()(const QuoteSuperseded&) const { return kQuoteSuperseded; }
    const char* operator()(const RfqExecuted&) const { return kExecuted; }
    const char* operator()(const RfqExpired&) const { return kExpired; }
    const char* operator()(const RfqCancelled&) const { return kCancelled; }
  };
  return std::visit(Namer{}, payload);
}

RfqId rfq_id_of(const EventPayload& payload) {
  return std::visit([](const auto& p) { return p.rfq_id; }, payload);
}

std::string encode(const Event& event) {
  json j;
  j["seq"] = event.seq;
  j["ts"] = event.ts;
  j["type"] = event_type(event.payload);
  std::visit([&j](const auto& p) { fill(j, p); }, event.payload);
  return j.dump();
}

std::optional<Event> decode(std::string_view text) {
  try {
    const json j = json::parse(text);
    Event e;
    e.seq = j.at("seq");
    e.ts = j.at("ts");
    e.payload = parse_payload(j.at("type").get<std::string>(), j);
    return e;
  } catch (const std::exception&) {
    return std::nullopt;
  }
}

}  // namespace rfq
