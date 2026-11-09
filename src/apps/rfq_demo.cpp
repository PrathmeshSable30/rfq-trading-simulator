#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <string>
#include <vector>

#include "engine/rfq_engine.hpp"
#include "messaging/event_dispatcher.hpp"
#include "messaging/kafka_publisher.hpp"
#include "store/redis_state_store.hpp"

using namespace rfq;

// Drives the engine with simulated RFQs and streams the events to Kafka and Redis.
int main(int argc, char** argv) {
  const std::string brokers = argc > 1 ? argv[1] : "localhost:9092";
  const std::string redis_host = argc > 2 ? argv[2] : "127.0.0.1";
  const int count = argc > 3 ? std::atoi(argv[3]) : 1000;
  const std::string topic = argc > 4 ? argv[4] : "rfq.events";
  if (count <= 0) {
    std::fprintf(stderr, "usage: rfq_demo [brokers] [redis-host|none] [rfq-count] [topic]\n");
    return 2;
  }

  std::shared_ptr<KafkaEventPublisher> kafka;
  std::vector<std::shared_ptr<EventConsumer>> consumers;
  try {
    kafka = std::make_shared<KafkaEventPublisher>(brokers, topic);
    consumers.push_back(kafka);
    if (redis_host != "none") consumers.push_back(std::make_shared<RedisStateStore>(redis_host, 6379));
  } catch (const std::exception& e) {
    std::fprintf(stderr, "startup failed: %s\n", e.what());
    return 1;
  }

  std::uint64_t dropped = 0;
  {
    EventDispatcher dispatcher(consumers, 1 << 20);

    const std::vector<std::string> dealers{"D1", "D2", "D3"};
    constexpr TimestampNs kMs = 1'000'000;
    constexpr TimestampNs kSec = 1'000 * kMs;
    TimestampNs now = std::chrono::duration_cast<std::chrono::nanoseconds>(
                          std::chrono::system_clock::now().time_since_epoch())
                          .count();
    RfqEngine engine([&](const Event& e) { dispatcher.push(e); },
                     static_cast<std::uint64_t>(now / kMs) << 22);  // unique ids per run

    for (int i = 1; i <= count; ++i) {
      now += kMs;
      RfqRequest req{"CLIENT", "US912828ZT58", i % 2 ? Side::Buy : Side::Sell, 1'000'000,
                     now + 5 * kSec, dealers};
      const RfqId rfq = engine.submit_rfq(std::move(req), now).value;

      QuoteId first_quote = 0;
      for (int d = 0; d < 3; ++d) {
        const Price price = 99 * kPriceScale + ((i * 7 + d * 13) % 100) * 1000;
        const auto q = engine.submit_quote(rfq, {dealers[d], price, 1'000'000, now + 4 * kSec},
                                           now + (d + 1) * 100'000);
        if (d == 0) first_quote = q.value;
      }

      if (i % 3 == 0) engine.withdraw_quote(first_quote, now + 500'000);

      if (i % 10 == 0) {
        engine.cancel_rfq(rfq, now + 600'000);
      } else if (i % 5 != 0) {  // every 5th RFQ is left to expire
        if (const auto best = engine.best_quote(rfq, now + 600'000)) {
          engine.execute(rfq, best->id, now + 700'000);
        }
      }
    }
    engine.expire(now + 10 * kSec);
    dropped = dispatcher.dropped();
  }

  kafka->flush(10'000);
  std::printf("published %d RFQs, dropped=%llu, kafka delivery failures=%llu\n", count,
              static_cast<unsigned long long>(dropped),
              static_cast<unsigned long long>(kafka->delivery_failures()));
  return kafka->delivery_failures() == 0 && dropped == 0 ? 0 : 1;
}
