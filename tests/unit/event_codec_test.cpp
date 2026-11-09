#include <gtest/gtest.h>

#include "messaging/event_codec.hpp"

using namespace rfq;

namespace {

Event make(EventPayload p) { return Event{7, 123'456, std::move(p)}; }

template <typename T>
const T& as(const Event& e) {
  return std::get<T>(e.payload);
}

}  // namespace

TEST(EventCodecTest, RoundTripsRfqCreated) {
  const auto decoded =
      decode(encode(make(RfqCreated{1, "CLIENT", "US912828ZT58", Side::Sell, 5'000'000, 999})));

  ASSERT_TRUE(decoded.has_value());
  EXPECT_EQ(decoded->seq, 7u);
  EXPECT_EQ(decoded->ts, 123'456);
  const auto& p = as<RfqCreated>(*decoded);
  EXPECT_EQ(p.rfq_id, 1u);
  EXPECT_EQ(p.client_id, "CLIENT");
  EXPECT_EQ(p.instrument, "US912828ZT58");
  EXPECT_EQ(p.side, Side::Sell);
  EXPECT_EQ(p.qty, 5'000'000);
  EXPECT_EQ(p.expires_at, 999);
}

TEST(EventCodecTest, RoundTripsQuoteReceived) {
  const auto decoded = decode(encode(make(QuoteReceived{2, 11, "DEALER_A", 99'500'000, 1000, 555})));

  ASSERT_TRUE(decoded.has_value());
  const auto& p = as<QuoteReceived>(*decoded);
  EXPECT_EQ(p.rfq_id, 2u);
  EXPECT_EQ(p.quote_id, 11u);
  EXPECT_EQ(p.dealer_id, "DEALER_A");
  EXPECT_EQ(p.price, 99'500'000);
  EXPECT_EQ(p.valid_until, 555);
}

TEST(EventCodecTest, RoundTripsRemainingTypes) {
  const auto withdrawn = decode(encode(make(QuoteWithdrawn{3, 12, "B"})));
  ASSERT_TRUE(withdrawn.has_value());
  EXPECT_EQ(as<QuoteWithdrawn>(*withdrawn).quote_id, 12u);

  const auto executed = decode(encode(make(RfqExecuted{3, 12, "B", 100, 50})));
  ASSERT_TRUE(executed.has_value());
  EXPECT_EQ(as<RfqExecuted>(*executed).dealer_id, "B");
  EXPECT_EQ(as<RfqExecuted>(*executed).qty, 50);

  const auto expired = decode(encode(make(RfqExpired{4})));
  ASSERT_TRUE(expired.has_value());
  EXPECT_EQ(as<RfqExpired>(*expired).rfq_id, 4u);

  const auto cancelled = decode(encode(make(RfqCancelled{5})));
  ASSERT_TRUE(cancelled.has_value());
  EXPECT_EQ(as<RfqCancelled>(*cancelled).rfq_id, 5u);
}

TEST(EventCodecTest, RejectsMalformedInput) {
  EXPECT_FALSE(decode("not json").has_value());
  EXPECT_FALSE(decode("{}").has_value());
  EXPECT_FALSE(decode(R"({"seq":1,"ts":1,"type":"NOPE","rfq_id":1})").has_value());
  EXPECT_FALSE(decode(R"({"seq":1,"ts":1,"type":"RFQ_EXPIRED"})").has_value());
}

TEST(EventCodecTest, EventTypeAndRfqIdHelpers) {
  const EventPayload p = RfqExecuted{9, 1, "A", 1, 1};
  EXPECT_STREQ(event_type(p), "RFQ_EXECUTED");
  EXPECT_EQ(rfq_id_of(p), 9u);
}
