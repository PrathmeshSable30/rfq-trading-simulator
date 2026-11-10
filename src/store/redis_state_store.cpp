#include "store/redis_state_store.hpp"

#include <hiredis/hiredis.h>

#include <memory>
#include <stdexcept>

namespace rfq {

namespace {

struct ReplyDeleter {
  void operator()(redisReply* r) const { freeReplyObject(r); }
};
using Reply = std::unique_ptr<redisReply, ReplyDeleter>;

std::string rfq_key(RfqId id) { return "rfq:" + std::to_string(id); }
std::string quotes_key(RfqId id) { return rfq_key(id) + ":quotes"; }
std::string quote_key(QuoteId id) { return "quote:" + std::to_string(id); }

}  // namespace

RedisStateStore::RedisStateStore(const std::string& host, int port, int closed_ttl_seconds)
    : host_(host), port_(port), closed_ttl_seconds_(closed_ttl_seconds) {
  connect();
}

void RedisStateStore::connect() {
  if (ctx_ != nullptr) {
    redisFree(ctx_);
    ctx_ = nullptr;
  }
  redisContext* ctx = redisConnect(host_.c_str(), port_);
  if (ctx == nullptr) throw std::runtime_error("redis: out of memory");
  if (ctx->err != 0) {
    const std::string msg = ctx->errstr;
    redisFree(ctx);
    throw std::runtime_error("redis connect: " + msg);
  }
  ctx_ = ctx;
}

RedisStateStore::~RedisStateStore() {
  if (ctx_ != nullptr) redisFree(ctx_);
}

void* RedisStateStore::run(const std::vector<std::string>& args) {
  std::vector<const char*> argv;
  std::vector<size_t> lens;
  argv.reserve(args.size());
  lens.reserve(args.size());
  for (const auto& a : args) {
    argv.push_back(a.data());
    lens.push_back(a.size());
  }

  // hiredis leaves a context unusable after an I/O error, so reconnect and retry once.
  for (int attempt = 0; attempt < 2; ++attempt) {
    if (ctx_ == nullptr || ctx_->err != 0) connect();
    void* reply = redisCommandArgv(ctx_, static_cast<int>(args.size()), argv.data(), lens.data());
    if (reply != nullptr) return reply;
  }
  throw std::runtime_error(std::string("redis command: ") + (ctx_ != nullptr ? ctx_->errstr : "no connection"));
}

void RedisStateStore::command(const std::vector<std::string>& args) {
  Reply reply(static_cast<redisReply*>(run(args)));
  if (reply->type == REDIS_REPLY_ERROR) {
    throw std::runtime_error(std::string("redis error: ") + reply->str);
  }
}

std::string RedisStateStore::hget(const std::string& key, const std::string& field) {
  Reply reply(static_cast<redisReply*>(run({"HGET", key, field})));
  if (reply->type == REDIS_REPLY_STRING) return std::string(reply->str, reply->len);
  return {};
}

void RedisStateStore::expire_rfq(RfqId rfq_id) {
  const std::string ttl = std::to_string(closed_ttl_seconds_);

  Reply members(static_cast<redisReply*>(run({"SMEMBERS", quotes_key(rfq_id)})));
  if (members->type == REDIS_REPLY_ARRAY) {
    for (size_t i = 0; i < members->elements; ++i) {
      const redisReply* m = members->element[i];
      command({"EXPIRE", "quote:" + std::string(m->str, m->len), ttl});
    }
  }
  command({"EXPIRE", quotes_key(rfq_id), ttl});
  command({"EXPIRE", rfq_key(rfq_id), ttl});
}

void RedisStateStore::on_event(const Event& event) {
  struct Visitor {
    RedisStateStore& self;

    void operator()(const RfqCreated& p) const {
      self.command({"HSET", rfq_key(p.rfq_id), "state", "OPEN", "client_id", p.client_id,
                    "instrument", p.instrument, "side", p.side == Side::Buy ? "BUY" : "SELL",
                    "qty", std::to_string(p.qty), "expires_at", std::to_string(p.expires_at)});
    }

    void operator()(const QuoteReceived& p) const {
      self.command({"HSET", quote_key(p.quote_id), "rfq_id", std::to_string(p.rfq_id),
                    "dealer_id", p.dealer_id, "price", std::to_string(p.price), "qty",
                    std::to_string(p.qty), "valid_until", std::to_string(p.valid_until),
                    "status", "ACTIVE"});
      self.command({"SADD", quotes_key(p.rfq_id), std::to_string(p.quote_id)});
    }

    void operator()(const QuoteWithdrawn& p) const {
      self.command({"HSET", quote_key(p.quote_id), "status", "WITHDRAWN"});
    }

    void operator()(const QuoteSuperseded& p) const {
      self.command({"HSET", quote_key(p.quote_id), "status", "SUPERSEDED", "replaced_by",
                    std::to_string(p.replaced_by)});
    }

    void operator()(const RfqExecuted& p) const {
      self.command({"HSET", quote_key(p.quote_id), "status", "EXECUTED"});
      self.command({"HSET", rfq_key(p.rfq_id), "state", "EXECUTED", "executed_quote_id",
                    std::to_string(p.quote_id), "executed_dealer_id", p.dealer_id,
                    "executed_price", std::to_string(p.price)});
      self.expire_rfq(p.rfq_id);
    }

    void operator()(const RfqExpired& p) const {
      self.command({"HSET", rfq_key(p.rfq_id), "state", "EXPIRED"});
      self.expire_rfq(p.rfq_id);
    }

    void operator()(const RfqCancelled& p) const {
      self.command({"HSET", rfq_key(p.rfq_id), "state", "CANCELLED"});
      self.expire_rfq(p.rfq_id);
    }
  };

  std::visit(Visitor{*this}, event.payload);
}

}  // namespace rfq
