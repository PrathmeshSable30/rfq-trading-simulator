#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <quickfix/FileLog.h>
#include <quickfix/FileStore.h>
#include <quickfix/Log.h>
#include <quickfix/MessageStore.h>
#include <quickfix/SocketAcceptor.h>

#include <prometheus/exposer.h>
#include <prometheus/registry.h>

#include "fix/fix_gateway.hpp"
#include "messaging/event_dispatcher.hpp"
#include "messaging/kafka_publisher.hpp"
#include "metrics/prometheus_metrics.hpp"
#include "store/redis_state_store.hpp"

namespace {

std::atomic<bool> g_stop{false};
void on_signal(int) { g_stop = true; }

// QuickFIX ships NullLog but no factory for it.
class NullLogFactory : public FIX::LogFactory {
 public:
  FIX::Log* create() override { return new FIX::NullLog(); }
  FIX::Log* create(const FIX::SessionID&) override { return new FIX::NullLog(); }
  void destroy(FIX::Log* log) override { delete log; }
};

}  // namespace

// rfq_gateway <fix-config> [kafka-brokers|none] [redis-host|none] [metrics-port|0]
int main(int argc, char** argv) {
  if (argc < 2) {
    std::fprintf(stderr,
                 "usage: rfq_gateway <fix-config> [kafka-brokers|none] [redis-host|none] [metrics-port|0]\n");
    return 2;
  }
  const std::string brokers = argc > 2 ? argv[2] : "localhost:9092";
  const std::string redis_host = argc > 3 ? argv[3] : "127.0.0.1";
  const int metrics_port = argc > 4 ? std::atoi(argv[4]) : 9100;

  std::signal(SIGINT, on_signal);
  std::signal(SIGTERM, on_signal);

  try {
    std::vector<std::shared_ptr<rfq::EventConsumer>> consumers;
    if (brokers != "none") consumers.push_back(std::make_shared<rfq::KafkaEventPublisher>(brokers, "rfq.events"));
    if (redis_host != "none") consumers.push_back(std::make_shared<rfq::RedisStateStore>(redis_host, 6379));

    rfq::EventDispatcher dispatcher(consumers, 1 << 20);

    FIX::SessionSettings settings(argv[1]);
    // Optional custom settings: StoreType=file|memory and LogType=file|none (default file).
    const FIX::Dictionary& defaults = settings.get();
    std::unique_ptr<FIX::MessageStoreFactory> store_factory;
    if (defaults.has("StoreType") && defaults.getString("StoreType") == "memory") {
      store_factory = std::make_unique<FIX::MemoryStoreFactory>();
    } else {
      store_factory = std::make_unique<FIX::FileStoreFactory>(settings);
    }
    std::unique_ptr<FIX::LogFactory> log_factory;
    if (defaults.has("LogType") && defaults.getString("LogType") == "none") {
      log_factory = std::make_unique<NullLogFactory>();
    } else {
      log_factory = std::make_unique<FIX::FileLogFactory>(settings);
    }
    // Start-time based id space (ms << 22) stays unique across restarts and fits a BIGINT.
    const auto start_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                              std::chrono::system_clock::now().time_since_epoch())
                              .count();
    rfq::FixGateway gateway(
        settings, [&dispatcher](const rfq::Event& e) { dispatcher.push(e); }, {}, {},
        static_cast<std::uint64_t>(start_ms) << 22);

    FIX::SocketAcceptor acceptor(gateway, *store_factory, settings, *log_factory);

    std::shared_ptr<rfq::PrometheusMetrics> metrics;
    std::unique_ptr<prometheus::Exposer> exposer;
    if (metrics_port > 0) {
      auto registry = std::make_shared<prometheus::Registry>();
      metrics = std::make_shared<rfq::PrometheusMetrics>(registry);
      gateway.set_observer(metrics);
      // Bound on all interfaces so Prometheus in Docker can scrape it; do not expose publicly.
      exposer = std::make_unique<prometheus::Exposer>("0.0.0.0:" + std::to_string(metrics_port));
      exposer->RegisterCollectable(registry);
    }

    acceptor.start();
    std::printf("rfq_gateway listening (kafka=%s redis=%s metrics=%s); Ctrl+C to stop\n", brokers.c_str(),
                redis_host.c_str(), metrics_port > 0 ? std::to_string(metrics_port).c_str() : "off");
    std::fflush(stdout);

    auto next_stats = std::chrono::steady_clock::now();
    while (!g_stop) {
      std::this_thread::sleep_for(std::chrono::milliseconds(20));
      gateway.tick();
      if (metrics && std::chrono::steady_clock::now() >= next_stats) {
        metrics->update_dispatcher(dispatcher.dropped(), dispatcher.consumer_errors());
        next_stats += std::chrono::seconds(1);
      }
    }
    acceptor.stop();
    std::printf("stopped; events dropped=%llu\n", static_cast<unsigned long long>(dispatcher.dropped()));
  } catch (const std::exception& e) {
    std::fprintf(stderr, "fatal: %s\n", e.what());
    return 1;
  }
  return 0;
}
