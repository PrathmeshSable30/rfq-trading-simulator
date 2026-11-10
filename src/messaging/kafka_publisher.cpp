#include "messaging/kafka_publisher.hpp"

#include <librdkafka/rdkafkacpp.h>

#include <atomic>
#include <stdexcept>

#include "messaging/event_codec.hpp"

namespace rfq {

namespace {

class DeliveryReport : public RdKafka::DeliveryReportCb {
 public:
  void dr_cb(RdKafka::Message& message) override {
    if (message.err() != RdKafka::ERR_NO_ERROR) failures.fetch_add(1, std::memory_order_relaxed);
  }
  std::atomic<std::uint64_t> failures{0};
};

void set_or_throw(RdKafka::Conf& conf, const std::string& key, const std::string& value) {
  std::string err;
  if (conf.set(key, value, err) != RdKafka::Conf::CONF_OK) {
    throw std::runtime_error("kafka config " + key + ": " + err);
  }
}

}  // namespace

struct KafkaEventPublisher::Impl {
  std::string topic;
  DeliveryReport report;
  std::unique_ptr<RdKafka::Producer> producer;
};

KafkaEventPublisher::KafkaEventPublisher(const std::string& brokers, std::string topic)
    : impl_(std::make_unique<Impl>()) {
  impl_->topic = std::move(topic);

  std::unique_ptr<RdKafka::Conf> conf(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
  set_or_throw(*conf, "bootstrap.servers", brokers);
  set_or_throw(*conf, "enable.idempotence", "true");
  set_or_throw(*conf, "linger.ms", "1");

  std::string err;
  if (conf->set("dr_cb", &impl_->report, err) != RdKafka::Conf::CONF_OK) {
    throw std::runtime_error("kafka dr_cb: " + err);
  }

  impl_->producer.reset(RdKafka::Producer::create(conf.get(), err));
  if (!impl_->producer) throw std::runtime_error("kafka producer: " + err);
}

KafkaEventPublisher::~KafkaEventPublisher() {
  if (impl_ && impl_->producer) impl_->producer->flush(5000);
}

void KafkaEventPublisher::on_event(const Event& event) {
  const std::string payload = encode(event);
  const std::string key = std::to_string(rfq_id_of(event.payload));

  for (int attempt = 0; attempt < 2; ++attempt) {
    const RdKafka::ErrorCode rc = impl_->producer->produce(
        impl_->topic, RdKafka::Topic::PARTITION_UA, RdKafka::Producer::RK_MSG_COPY,
        const_cast<char*>(payload.data()), payload.size(), key.data(), key.size(), 0, nullptr);

    if (rc == RdKafka::ERR_NO_ERROR) break;
    if (rc != RdKafka::ERR__QUEUE_FULL || attempt == 1) {
      throw std::runtime_error("kafka produce: " + RdKafka::err2str(rc));
    }
    impl_->producer->poll(100);
  }
  impl_->producer->poll(0);
}

void KafkaEventPublisher::flush(int timeout_ms) { impl_->producer->flush(timeout_ms); }

void KafkaEventPublisher::poll(int timeout_ms) { impl_->producer->poll(timeout_ms); }

std::uint64_t KafkaEventPublisher::delivery_failures() const noexcept {
  return impl_->report.failures.load();
}

}  // namespace rfq
