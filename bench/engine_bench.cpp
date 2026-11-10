#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <string>
#include <vector>

#include "engine/rfq_engine.hpp"
#include "messaging/event_codec.hpp"
#include "messaging/event_dispatcher.hpp"

using namespace rfq;
using Clock = std::chrono::steady_clock;

namespace {

constexpr int kRfqs = 200'000;
constexpr int kDealers = 4;

struct NoopConsumer : EventConsumer {
  void on_event(const Event&) override {}
};

struct Samples {
  std::vector<std::int64_t> ns;

  void add(std::int64_t v) { ns.push_back(v); }

  void report(const char* name) {
    std::sort(ns.begin(), ns.end());
    auto pct = [&](double p) { return ns[static_cast<std::size_t>(p * (ns.size() - 1))]; };
    std::printf("%-28s n=%-8zu p50=%6lld ns  p99=%7lld ns  p99.9=%8lld ns  max=%9lld ns\n", name,
                ns.size(), static_cast<long long>(pct(0.50)), static_cast<long long>(pct(0.99)),
                static_cast<long long>(pct(0.999)), static_cast<long long>(ns.back()));
  }
};

std::int64_t since(Clock::time_point t0) {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - t0).count();
}

void run_engine(bool with_dispatcher) {
  std::unique_ptr<EventDispatcher> dispatcher;
  RfqEngine::EventSink sink;
  if (with_dispatcher) {
    dispatcher = std::make_unique<EventDispatcher>(
        std::vector<std::shared_ptr<EventConsumer>>{std::make_shared<NoopConsumer>()}, 1 << 22);
    sink = [&](const Event& e) { dispatcher->push(e); };
  }
  RfqEngine engine(sink);

  Samples submit, quote, execute;
  submit.ns.reserve(kRfqs);
  quote.ns.reserve(kRfqs * kDealers);
  execute.ns.reserve(kRfqs);

  const std::vector<std::string> dealers{"D1", "D2", "D3", "D4"};
  const auto start = Clock::now();
  TimestampNs now = 1;

  for (int i = 0; i < kRfqs; ++i) {
    RfqRequest req{"CLIENT", "US912828ZT58", Side::Buy, 1'000'000, now + 1'000'000, dealers};
    auto t = Clock::now();
    const auto rfq = engine.submit_rfq(std::move(req), now);
    submit.add(since(t));

    QuoteId best = 0;
    Price best_price = INT64_MAX;
    for (int d = 0; d < kDealers; ++d) {
      const Price price = 100 * kPriceScale + (i * 7 + d * 13) % 50;
      t = Clock::now();
      const auto q = engine.submit_quote(rfq.value, {dealers[d], price, 1'000'000, now + 500'000}, now + 1);
      quote.add(since(t));
      if (price < best_price) {
        best_price = price;
        best = q.value;
      }
    }

    t = Clock::now();
    engine.execute(rfq.value, best, now + 2);
    execute.add(since(t));
    now += 10;
  }

  const double secs = std::chrono::duration<double>(Clock::now() - start).count();
  std::printf("\n== engine %s ==\n", with_dispatcher ? "+ dispatcher queue" : "alone");
  submit.report("submit_rfq");
  quote.report("submit_quote");
  execute.report("execute");
  std::printf("throughput: %.0f RFQs/sec (each = 1 request + %d quotes + 1 execution)\n",
              kRfqs / secs, kDealers);
}

void run_codec() {
  Samples s;
  const Event e{1, 1, QuoteReceived{1, 1, "DEALER_1", 100'000'000, 1'000'000, 5'000'000}};
  for (int i = 0; i < 100'000; ++i) {
    const auto t = Clock::now();
    const auto text = encode(e);
    s.add(since(t));
    if (text.empty()) std::puts("unexpected");
  }
  std::puts("\n== JSON encode (runs on the dispatcher thread, not the engine thread) ==");
  s.report("encode(QuoteReceived)");
}

}  // namespace

int main() {
  run_engine(false);
  run_engine(true);
  run_codec();
  return 0;
}
