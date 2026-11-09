#include <gtest/gtest.h>

#include <vector>

#include "engine/rfq_engine.hpp"

using namespace rfq;

namespace {

constexpr TimestampNs kT0 = 1'000;
constexpr TimestampNs kExpiry = 100'000;
constexpr Qty kQty = 1'000'000;

class RfqEngineTest : public ::testing::Test {
 protected:
  std::vector<Event> events;
  RfqEngine engine{[this](const Event& e) { events.push_back(e); }};

  static RfqRequest make_request(Side side = Side::Buy) {
    RfqRequest r;
    r.client_id = "CLIENT1";
    r.instrument = "US912828ZT58";
    r.side = side;
    r.qty = kQty;
    r.expires_at = kExpiry;
    r.dealers = {"A", "B", "C"};
    return r;
  }

  static QuoteInput make_quote(std::string dealer, Price price, TimestampNs valid_until = kExpiry) {
    return QuoteInput{std::move(dealer), price, kQty, valid_until};
  }

  RfqId open_rfq(Side side = Side::Buy) {
    const auto r = engine.submit_rfq(make_request(side), kT0);
    EXPECT_TRUE(r.ok());
    return r.value;
  }
};

TEST_F(RfqEngineTest, RejectsInvalidRequests) {
  auto bad_qty = make_request();
  bad_qty.qty = 0;
  EXPECT_EQ(engine.submit_rfq(bad_qty, kT0).error, ErrorCode::InvalidRequest);

  auto no_dealers = make_request();
  no_dealers.dealers.clear();
  EXPECT_EQ(engine.submit_rfq(no_dealers, kT0).error, ErrorCode::InvalidRequest);

  auto already_expired = make_request();
  already_expired.expires_at = kT0;
  EXPECT_EQ(engine.submit_rfq(already_expired, kT0).error, ErrorCode::InvalidRequest);
  EXPECT_TRUE(events.empty());
}

TEST_F(RfqEngineTest, SubmitRfqEmitsCreatedEvent) {
  const RfqId id = open_rfq();

  ASSERT_EQ(events.size(), 1u);
  const auto* created = std::get_if<RfqCreated>(&events[0].payload);
  ASSERT_NE(created, nullptr);
  EXPECT_EQ(created->rfq_id, id);
  EXPECT_EQ(engine.find(id)->state, RfqState::Open);
}

TEST_F(RfqEngineTest, RejectsQuoteFromUninvitedDealer) {
  const RfqId id = open_rfq();
  EXPECT_EQ(engine.submit_quote(id, make_quote("Z", 100), kT0 + 1).error,
            ErrorCode::DealerNotInvited);
}

TEST_F(RfqEngineTest, RejectsQuoteBelowRequestedSize) {
  const RfqId id = open_rfq();
  QuoteInput q = make_quote("A", 100);
  q.qty = kQty - 1;
  EXPECT_EQ(engine.submit_quote(id, q, kT0 + 1).error, ErrorCode::InvalidQuote);
}

TEST_F(RfqEngineTest, BuyPicksLowestPrice) {
  const RfqId id = open_rfq(Side::Buy);
  engine.submit_quote(id, make_quote("A", 101), kT0 + 1);
  engine.submit_quote(id, make_quote("B", 99), kT0 + 2);
  engine.submit_quote(id, make_quote("C", 100), kT0 + 3);

  const auto best = engine.best_quote(id, kT0 + 4);
  ASSERT_TRUE(best.has_value());
  EXPECT_EQ(best->dealer_id, "B");
}

TEST_F(RfqEngineTest, SellPicksHighestPrice) {
  const RfqId id = open_rfq(Side::Sell);
  engine.submit_quote(id, make_quote("A", 101), kT0 + 1);
  engine.submit_quote(id, make_quote("B", 99), kT0 + 2);

  const auto best = engine.best_quote(id, kT0 + 3);
  ASSERT_TRUE(best.has_value());
  EXPECT_EQ(best->dealer_id, "A");
}

TEST_F(RfqEngineTest, EqualPricesKeepEarliestQuote) {
  const RfqId id = open_rfq();
  engine.submit_quote(id, make_quote("A", 100), kT0 + 1);
  engine.submit_quote(id, make_quote("B", 100), kT0 + 2);

  EXPECT_EQ(engine.best_quote(id, kT0 + 3)->dealer_id, "A");
}

TEST_F(RfqEngineTest, NewQuoteSupersedesDealersPreviousQuote) {
  const RfqId id = open_rfq();
  const auto first = engine.submit_quote(id, make_quote("A", 100), kT0 + 1);
  const auto second = engine.submit_quote(id, make_quote("A", 105), kT0 + 2);
  ASSERT_TRUE(first.ok() && second.ok());

  EXPECT_EQ(engine.best_quote(id, kT0 + 3)->id, second.value);
  EXPECT_EQ(engine.execute(id, first.value, kT0 + 3).error, ErrorCode::QuoteNotActive);
}

TEST_F(RfqEngineTest, WithdrawnQuoteIsNotBestAndCannotBeExecuted) {
  const RfqId id = open_rfq();
  const auto a = engine.submit_quote(id, make_quote("A", 99), kT0 + 1);
  engine.submit_quote(id, make_quote("B", 100), kT0 + 2);

  EXPECT_EQ(engine.withdraw_quote(a.value, kT0 + 3), ErrorCode::Ok);
  EXPECT_EQ(engine.best_quote(id, kT0 + 4)->dealer_id, "B");
  EXPECT_EQ(engine.execute(id, a.value, kT0 + 4).error, ErrorCode::QuoteNotActive);
  EXPECT_EQ(engine.withdraw_quote(a.value, kT0 + 4), ErrorCode::QuoteNotActive);
}

TEST_F(RfqEngineTest, ExecuteClosesRfqAndEmitsEvent) {
  const RfqId id = open_rfq();
  const auto q = engine.submit_quote(id, make_quote("A", 100), kT0 + 1);

  const auto exec = engine.execute(id, q.value, kT0 + 2);
  ASSERT_TRUE(exec.ok());
  EXPECT_EQ(exec.value.dealer_id, "A");
  EXPECT_EQ(exec.value.price, 100);
  EXPECT_EQ(exec.value.qty, kQty);
  EXPECT_EQ(engine.find(id)->state, RfqState::Executed);

  const auto* ev = std::get_if<RfqExecuted>(&events.back().payload);
  ASSERT_NE(ev, nullptr);
  EXPECT_EQ(ev->quote_id, q.value);

  EXPECT_EQ(engine.submit_quote(id, make_quote("B", 90), kT0 + 3).error, ErrorCode::RfqClosed);
  EXPECT_EQ(engine.execute(id, q.value, kT0 + 3).error, ErrorCode::RfqClosed);
}

TEST_F(RfqEngineTest, CannotExecuteQuoteAfterItsValidity) {
  const RfqId id = open_rfq();
  const auto q = engine.submit_quote(id, make_quote("A", 100, kT0 + 10), kT0 + 1);

  EXPECT_FALSE(engine.best_quote(id, kT0 + 10).has_value());
  EXPECT_EQ(engine.execute(id, q.value, kT0 + 10).error, ErrorCode::QuoteExpired);
  EXPECT_EQ(engine.find(id)->state, RfqState::Open);
}

TEST_F(RfqEngineTest, ExpireClosesOverdueRfqsOnly) {
  const RfqId early = open_rfq();
  auto later_req = make_request();
  later_req.expires_at = kExpiry * 2;
  const RfqId later = engine.submit_rfq(later_req, kT0).value;

  EXPECT_TRUE(engine.expire(kExpiry - 1).empty());
  const auto expired = engine.expire(kExpiry);

  ASSERT_EQ(expired.size(), 1u);
  EXPECT_EQ(expired[0], early);
  EXPECT_EQ(engine.find(early)->state, RfqState::Expired);
  EXPECT_EQ(engine.find(later)->state, RfqState::Open);
  EXPECT_NE(std::get_if<RfqExpired>(&events.back().payload), nullptr);
  EXPECT_TRUE(engine.expire(kExpiry).empty());
}

TEST_F(RfqEngineTest, RejectsActionsAfterDeadlineEvenBeforeExpireSweep) {
  const RfqId id = open_rfq();
  const auto q = engine.submit_quote(id, make_quote("A", 100), kT0 + 1);

  EXPECT_EQ(engine.submit_quote(id, make_quote("B", 99), kExpiry).error, ErrorCode::RfqExpired);
  EXPECT_EQ(engine.execute(id, q.value, kExpiry).error, ErrorCode::RfqExpired);
}

TEST_F(RfqEngineTest, CancelClosesRfqAndPreventsExpiry) {
  const RfqId id = open_rfq();

  EXPECT_EQ(engine.cancel_rfq(id, kT0 + 1), ErrorCode::Ok);
  EXPECT_EQ(engine.find(id)->state, RfqState::Cancelled);
  EXPECT_EQ(engine.cancel_rfq(id, kT0 + 2), ErrorCode::RfqClosed);
  EXPECT_TRUE(engine.expire(kExpiry).empty());
}

TEST_F(RfqEngineTest, UnknownIdsAreReported) {
  EXPECT_EQ(engine.cancel_rfq(42, kT0), ErrorCode::UnknownRfq);
  EXPECT_EQ(engine.withdraw_quote(42, kT0), ErrorCode::UnknownQuote);
  EXPECT_EQ(engine.execute(42, 1, kT0).error, ErrorCode::UnknownRfq);
}

TEST_F(RfqEngineTest, EventSequenceNumbersAreStrictlyIncreasing) {
  const RfqId id = open_rfq();
  const auto q = engine.submit_quote(id, make_quote("A", 100), kT0 + 1);
  engine.execute(id, q.value, kT0 + 2);

  ASSERT_EQ(events.size(), 3u);
  for (size_t i = 1; i < events.size(); ++i) {
    EXPECT_GT(events[i].seq, events[i - 1].seq);
  }
}

TEST_F(RfqEngineTest, IdBaseOffsetsGeneratedIds) {
  RfqEngine based({}, 1'000);
  const auto rfq = based.submit_rfq(make_request(), kT0);
  ASSERT_TRUE(rfq.ok());
  EXPECT_EQ(rfq.value, 1'001u);

  const auto quote = based.submit_quote(rfq.value, make_quote("A", 100), kT0 + 1);
  EXPECT_EQ(quote.value, 1'001u);
}

}  // namespace
