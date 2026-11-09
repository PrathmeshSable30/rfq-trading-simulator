#include <gtest/gtest.h>

#include <sstream>
#include <utility>
#include <vector>

#include <quickfix/fix44/ExecutionReport.h>
#include <quickfix/fix44/BusinessMessageReject.h>
#include <quickfix/fix44/Quote.h>
#include <quickfix/fix44/QuoteCancel.h>
#include <quickfix/fix44/QuoteRequest.h>
#include <quickfix/fix44/QuoteResponse.h>

#include "fix/fix_gateway.hpp"

using namespace rfq;

namespace {

constexpr TimestampNs kSec = 1'000'000'000;

const char* kConfig = R"(
[DEFAULT]
ConnectionType=acceptor
BeginString=FIX.4.4
SenderCompID=GATEWAY
StartTime=00:00:00
EndTime=00:00:00
[SESSION]
TargetCompID=CLIENT1
Role=CLIENT
[SESSION]
TargetCompID=CLIENT2
Role=CLIENT
[SESSION]
TargetCompID=DEALER1
Role=DEALER
[SESSION]
TargetCompID=DEALER2
Role=DEALER
)";

FIX::SessionID sid(const char* target) { return FIX::SessionID("FIX.4.4", "GATEWAY", target); }

struct Sent {
  FIX::Message message;
  std::string to;
};

class FixGatewayTest : public ::testing::Test {
 protected:
  FixGatewayTest() {
    std::istringstream in(kConfig);
    FIX::SessionSettings settings(in);
    gateway = std::make_unique<FixGateway>(
        settings, [this](const Event& e) { events.push_back(e); },
        [this](FIX::Message& m, const FIX::SessionID& to) { sent.push_back({m, to.getTargetCompID().getString()}); },
        [this] { return now; });
    gateway->onLogon(sid("DEALER1"));
    gateway->onLogon(sid("DEALER2"));
  }

  void request(const char* client, const char* req_id, char side = '1', double qty = 1'000'000) {
    FIX44::QuoteRequest r{FIX::QuoteReqID(req_id)};
    FIX44::QuoteRequest::NoRelatedSym g;
    g.set(FIX::Symbol("BOND1"));
    g.set(FIX::Side(side));
    g.set(FIX::OrderQty(qty));
    r.addGroup(g);
    gateway->fromApp(r, sid(client));
  }

  void quote(const char* dealer, const std::string& req_id, const char* dealer_quote_id, double px,
             bool offer = true, double qty = 1'000'000) {
    FIX44::Quote q{FIX::QuoteID(dealer_quote_id)};
    q.set(FIX::QuoteReqID(req_id));
    q.set(FIX::Symbol("BOND1"));
    if (offer) {
      q.set(FIX::OfferPx(px));
      q.set(FIX::OfferSize(qty));
    } else {
      q.set(FIX::BidPx(px));
      q.set(FIX::BidSize(qty));
    }
    gateway->fromApp(q, sid(dealer));
  }

  void hit(const char* client, const std::string& quote_id) {
    FIX44::QuoteResponse r{FIX::QuoteRespID("R1"), FIX::QuoteRespType(FIX::QuoteRespType_HIT)};
    r.set(FIX::Symbol("BOND1"));
    r.set(FIX::QuoteID(quote_id));
    gateway->fromApp(r, sid(client));
  }

  std::vector<const Sent*> to(const std::string& who, const char* msg_type) const {
    std::vector<const Sent*> out;
    for (const Sent& s : sent) {
      FIX::MsgType t;
      s.message.getHeader().getField(t);
      if (s.to == who && t.getString() == msg_type) out.push_back(&s);
    }
    return out;
  }

  static std::string field(const FIX::Message& m, int tag) {
    return m.isSetField(tag) ? m.getField(tag) : "";
  }

  std::unique_ptr<FixGateway> gateway;
  std::vector<Event> events;
  std::vector<Sent> sent;
  TimestampNs now = 1'000 * kSec;
};

TEST_F(FixGatewayTest, RequestIsForwardedToEveryOnlineDealerWithEngineId) {
  request("CLIENT1", "REQ-1");

  const auto d1 = to("DEALER1", "R");
  const auto d2 = to("DEALER2", "R");
  ASSERT_EQ(d1.size(), 1u);
  ASSERT_EQ(d2.size(), 1u);
  EXPECT_EQ(field(d1[0]->message, FIX::FIELD::QuoteReqID), "1");
  EXPECT_TRUE(to("CLIENT1", "S").empty());
  EXPECT_EQ(gateway->open_rfqs(), 1u);
}

TEST_F(FixGatewayTest, RequestWithoutDealersIsRejected) {
  gateway->onLogout(sid("DEALER1"));
  gateway->onLogout(sid("DEALER2"));
  request("CLIENT1", "REQ-1");

  const auto rejects = to("CLIENT1", "j");
  ASSERT_EQ(rejects.size(), 1u);
  EXPECT_EQ(field(rejects[0]->message, FIX::FIELD::Text), "no dealers online");
  EXPECT_EQ(gateway->open_rfqs(), 0u);
}

TEST_F(FixGatewayTest, DuplicateRequestIdIsRejected) {
  request("CLIENT1", "REQ-1");
  request("CLIENT1", "REQ-1");
  EXPECT_EQ(to("CLIENT1", "j").size(), 1u);
  EXPECT_EQ(gateway->open_rfqs(), 1u);
}

TEST_F(FixGatewayTest, DealerQuoteIsRelayedToClientUnderClientRequestId) {
  request("CLIENT1", "REQ-1");
  quote("DEALER1", "1", "DQ-1", 99.5);

  const auto quotes = to("CLIENT1", "S");
  ASSERT_EQ(quotes.size(), 1u);
  EXPECT_EQ(field(quotes[0]->message, FIX::FIELD::QuoteReqID), "REQ-1");
  EXPECT_EQ(field(quotes[0]->message, FIX::FIELD::QuoteID), "1");
  EXPECT_DOUBLE_EQ(std::stod(field(quotes[0]->message, FIX::FIELD::OfferPx)), 99.5);
}

TEST_F(FixGatewayTest, SellerReceivesBidPrices) {
  request("CLIENT1", "REQ-1", '2');
  quote("DEALER1", "1", "DQ-1", 99.0, /*offer=*/false);

  const auto quotes = to("CLIENT1", "S");
  ASSERT_EQ(quotes.size(), 1u);
  EXPECT_DOUBLE_EQ(std::stod(field(quotes[0]->message, FIX::FIELD::BidPx)), 99.0);
}

TEST_F(FixGatewayTest, QuoteWithWrongPriceSideIsRejected) {
  request("CLIENT1", "REQ-1", '1');
  quote("DEALER1", "1", "DQ-1", 99.0, /*offer=*/false);
  EXPECT_EQ(to("DEALER1", "j").size(), 1u);
  EXPECT_TRUE(to("CLIENT1", "S").empty());
}

TEST_F(FixGatewayTest, QuoteForUnknownRfqIsRejected) {
  quote("DEALER1", "42", "DQ-1", 99.0);
  EXPECT_EQ(to("DEALER1", "j").size(), 1u);
}

TEST_F(FixGatewayTest, HittingBestQuoteExecutesAndNotifiesBothSides) {
  request("CLIENT1", "REQ-1");
  quote("DEALER1", "1", "DQ-1", 99.6);
  quote("DEALER2", "1", "DQ-2", 99.4);
  hit("CLIENT1", "2");  // DEALER2's quote got engine id 2

  const auto client = to("CLIENT1", "8");
  ASSERT_EQ(client.size(), 1u);
  EXPECT_EQ(field(client[0]->message, FIX::FIELD::ExecType), "F");
  EXPECT_EQ(field(client[0]->message, FIX::FIELD::ClOrdID), "REQ-1");
  EXPECT_DOUBLE_EQ(std::stod(field(client[0]->message, FIX::FIELD::LastPx)), 99.4);
  EXPECT_EQ(field(client[0]->message, FIX::FIELD::Text), "dealer=DEALER2");

  const auto dealer = to("DEALER2", "8");
  ASSERT_EQ(dealer.size(), 1u);
  EXPECT_EQ(field(dealer[0]->message, FIX::FIELD::Side), "2");  // dealer sells to a buying client
  EXPECT_TRUE(to("DEALER1", "8").empty());
  EXPECT_EQ(gateway->open_rfqs(), 0u);

  hit("CLIENT1", "2");
  EXPECT_EQ(to("CLIENT1", "j").size(), 1u);
}

TEST_F(FixGatewayTest, ClientCannotHitAnotherClientsQuote) {
  request("CLIENT1", "REQ-1");
  quote("DEALER1", "1", "DQ-1", 99.5);
  hit("CLIENT2", "1");

  EXPECT_EQ(to("CLIENT2", "j").size(), 1u);
  EXPECT_TRUE(to("CLIENT1", "8").empty());
  EXPECT_EQ(gateway->open_rfqs(), 1u);
}

TEST_F(FixGatewayTest, WithdrawnQuoteCannotBeHit) {
  request("CLIENT1", "REQ-1");
  quote("DEALER1", "1", "DQ-1", 99.5);

  FIX44::QuoteCancel c{FIX::QuoteID("DQ-1"), FIX::QuoteCancelType(FIX::QuoteCancelType_CANCEL_FOR_ONE_OR_MORE_SECURITIES)};
  gateway->fromApp(c, sid("DEALER1"));
  hit("CLIENT1", "1");

  EXPECT_EQ(to("CLIENT1", "j").size(), 1u);
  EXPECT_TRUE(to("CLIENT1", "8").empty());
}

TEST_F(FixGatewayTest, ClientCancelSendsCancelledReport) {
  request("CLIENT1", "REQ-1");

  FIX44::QuoteCancel c{FIX::QuoteID("*"), FIX::QuoteCancelType(FIX::QuoteCancelType_CANCEL_ALL_QUOTES)};
  c.set(FIX::QuoteReqID("REQ-1"));
  gateway->fromApp(c, sid("CLIENT1"));

  const auto reports = to("CLIENT1", "8");
  ASSERT_EQ(reports.size(), 1u);
  EXPECT_EQ(field(reports[0]->message, FIX::FIELD::ExecType), "4");
  EXPECT_EQ(gateway->open_rfqs(), 0u);
}

TEST_F(FixGatewayTest, TickExpiresOverdueRfqs) {
  request("CLIENT1", "REQ-1");

  gateway->tick();
  EXPECT_TRUE(to("CLIENT1", "8").empty());

  now += 11 * kSec;  // default lifetime is 10 s
  gateway->tick();
  const auto reports = to("CLIENT1", "8");
  ASSERT_EQ(reports.size(), 1u);
  EXPECT_EQ(field(reports[0]->message, FIX::FIELD::ExecType), "C");
  EXPECT_EQ(gateway->open_rfqs(), 0u);
}

TEST_F(FixGatewayTest, DealerCannotRequestAndClientCannotQuote) {
  request("DEALER1", "REQ-1");
  EXPECT_EQ(to("DEALER1", "j").size(), 1u);

  request("CLIENT1", "REQ-2");
  quote("CLIENT1", "1", "X", 99.0);
  EXPECT_EQ(to("CLIENT1", "j").size(), 1u);
}

TEST_F(FixGatewayTest, EngineEventsAreEmitted) {
  request("CLIENT1", "REQ-1");
  quote("DEALER1", "1", "DQ-1", 99.5);
  hit("CLIENT1", "1");

  ASSERT_EQ(events.size(), 3u);
  EXPECT_NE(std::get_if<RfqCreated>(&events[0].payload), nullptr);
  EXPECT_NE(std::get_if<QuoteReceived>(&events[1].payload), nullptr);
  EXPECT_NE(std::get_if<RfqExecuted>(&events[2].payload), nullptr);
}

struct RecordingObserver : GatewayObserver {
  std::vector<std::string> messages;
  int created = 0, relayed = 0, first_quotes = 0, executed = 0, expired = 0, cancelled = 0;
  std::vector<std::string> rejects;
  std::size_t open = 0, dealers = 0;
  double last_execution_seconds = -1;

  void on_message(const char* type, double seconds) override {
    EXPECT_GE(seconds, 0.0);
    messages.push_back(type);
  }
  void on_rfq_created() override { ++created; }
  void on_quote_relayed() override { ++relayed; }
  void on_first_quote(double) override { ++first_quotes; }
  void on_executed(double seconds) override { ++executed, last_execution_seconds = seconds; }
  void on_expired() override { ++expired; }
  void on_cancelled() override { ++cancelled; }
  void on_reject(const char* reason) override { rejects.emplace_back(reason); }
  void on_gauges(std::size_t o, std::size_t d) override { open = o, dealers = d; }
};

TEST_F(FixGatewayTest, ObserverSeesTheWholeLifecycle) {
  auto observer = std::make_shared<RecordingObserver>();
  gateway->set_observer(observer);

  request("CLIENT1", "REQ-1");
  quote("DEALER1", "1", "DQ-1", 99.5);
  quote("DEALER2", "1", "DQ-2", 99.6);
  EXPECT_EQ(observer->open, 1u);
  EXPECT_EQ(observer->dealers, 2u);

  hit("CLIENT1", "1");

  EXPECT_EQ(observer->created, 1);
  EXPECT_EQ(observer->relayed, 2);
  EXPECT_EQ(observer->first_quotes, 1);  // only the first quote of an RFQ counts
  EXPECT_EQ(observer->executed, 1);
  EXPECT_GE(observer->last_execution_seconds, 0.0);
  EXPECT_EQ(observer->open, 0u);
  EXPECT_EQ(observer->messages, (std::vector<std::string>{"R", "S", "S", "AJ"}));
}

TEST_F(FixGatewayTest, ObserverSeesRejectsCancelsAndExpiry) {
  auto observer = std::make_shared<RecordingObserver>();
  gateway->set_observer(observer);

  quote("DEALER1", "42", "DQ-1", 99.0);
  ASSERT_EQ(observer->rejects.size(), 1u);
  EXPECT_EQ(observer->rejects[0], "unknown QuoteReqID");

  request("CLIENT1", "REQ-1");
  FIX44::QuoteCancel c{FIX::QuoteID("*"), FIX::QuoteCancelType(FIX::QuoteCancelType_CANCEL_ALL_QUOTES)};
  c.set(FIX::QuoteReqID("REQ-1"));
  gateway->fromApp(c, sid("CLIENT1"));
  EXPECT_EQ(observer->cancelled, 1);

  request("CLIENT1", "REQ-2");
  now += 11 * kSec;
  gateway->tick();
  EXPECT_EQ(observer->expired, 1);
  EXPECT_EQ(observer->open, 0u);
}

TEST_F(FixGatewayTest, MessageTimingIsReportedEvenWhenTheHandlerRejects) {
  auto observer = std::make_shared<RecordingObserver>();
  gateway->set_observer(observer);

  request("DEALER1", "REQ-1");  // wrong role -> reject
  EXPECT_EQ(observer->messages, (std::vector<std::string>{"R"}));
}

TEST(FixGatewayConfigTest, SessionWithoutRoleIsRejected) {
  std::istringstream in(R"(
[DEFAULT]
ConnectionType=acceptor
BeginString=FIX.4.4
SenderCompID=GATEWAY
[SESSION]
TargetCompID=CLIENT1
)");
  FIX::SessionSettings settings(in);
  EXPECT_ANY_THROW(FixGateway(settings, nullptr));
}

}  // namespace
