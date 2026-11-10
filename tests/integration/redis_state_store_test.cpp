#include <gtest/gtest.h>

#include <cstdlib>
#include <memory>
#include <string>

#include <hiredis/hiredis.h>

#include "store/redis_state_store.hpp"

using namespace rfq;

namespace {

bool redis_endpoint(std::string& host, int& port) {
  const char* h = std::getenv("RFQ_REDIS_HOST");
  if (h == nullptr) return false;
  host = h;
  const char* p = std::getenv("RFQ_REDIS_PORT");
  port = p != nullptr ? std::atoi(p) : 6379;
  return true;
}

}  // namespace

TEST(RedisStateStoreIntegration, ProjectsRfqLifecycle) {
  std::string host;
  int port = 0;
  if (!redis_endpoint(host, port)) GTEST_SKIP() << "RFQ_REDIS_HOST not set";

  RedisStateStore store(host, port, 60);
  // High ids avoid clashing with leftovers from earlier runs.
  const RfqId rfq = 9'000'000 + static_cast<RfqId>(std::rand() % 1'000'000);
  const QuoteId quote = rfq;

  store.on_event({1, 0, RfqCreated{rfq, "CLIENT", "BOND", Side::Buy, 1000, 5000}});
  EXPECT_EQ(store.hget("rfq:" + std::to_string(rfq), "state"), "OPEN");

  store.on_event({2, 0, QuoteReceived{rfq, quote, "DEALER", 100, 1000, 4000}});
  EXPECT_EQ(store.hget("quote:" + std::to_string(quote), "status"), "ACTIVE");
  EXPECT_EQ(store.hget("quote:" + std::to_string(quote), "dealer_id"), "DEALER");

  store.on_event({3, 0, RfqExecuted{rfq, quote, "DEALER", 100, 1000}});
  EXPECT_EQ(store.hget("rfq:" + std::to_string(rfq), "state"), "EXECUTED");
  EXPECT_EQ(store.hget("quote:" + std::to_string(quote), "status"), "EXECUTED");
  EXPECT_EQ(store.hget("rfq:" + std::to_string(rfq), "executed_price"), "100");
}

TEST(RedisStateStoreIntegration, SurvivesADroppedConnection) {
  std::string host;
  int port = 0;
  if (!redis_endpoint(host, port)) GTEST_SKIP() << "RFQ_REDIS_HOST not set";

  RedisStateStore store(host, port, 60);
  const RfqId rfq = 9'000'000 + static_cast<RfqId>(std::rand() % 1'000'000);
  store.on_event({1, 0, RfqCreated{rfq, "CLIENT", "BOND", Side::Buy, 1000, 5000}});

  // Kill every other client connection, as a Redis restart would.
  std::unique_ptr<redisContext, decltype(&redisFree)> admin(redisConnect(host.c_str(), port), &redisFree);
  ASSERT_TRUE(admin && admin->err == 0);
  freeReplyObject(redisCommand(admin.get(), "CLIENT KILL TYPE normal SKIPME yes"));

  store.on_event({2, 0, QuoteReceived{rfq, rfq, "DEALER", 100, 1000, 4000}});
  store.on_event({3, 0, QuoteSuperseded{rfq, rfq, "DEALER", rfq + 1}});
  EXPECT_EQ(store.hget("quote:" + std::to_string(rfq), "status"), "SUPERSEDED");
  EXPECT_EQ(store.hget("quote:" + std::to_string(rfq), "replaced_by"), std::to_string(rfq + 1));
}
