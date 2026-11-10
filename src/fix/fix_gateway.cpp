#include "fix/fix_gateway.hpp"

#include <charconv>
#include <chrono>
#include <cmath>
#include <limits>
#include <optional>

#include <quickfix/Session.h>
#include <quickfix/fix44/BusinessMessageReject.h>
#include <quickfix/fix44/ExecutionReport.h>
#include <quickfix/fix44/Quote.h>
#include <quickfix/fix44/QuoteCancel.h>
#include <quickfix/fix44/QuoteRequest.h>
#include <quickfix/fix44/QuoteResponse.h>
#include <quickfix/fix44/QuoteStatusReport.h>

namespace rfq {

namespace {

constexpr TimestampNs kDefaultRfqLifetimeNs = 10'000'000'000;
// Longest ExpireTime a client may ask for. Without a cap an RFQ could stay open indefinitely.
constexpr TimestampNs kMaxRfqLifetimeNs = 300'000'000'000;

// QuoteStatus (297) values used in QuoteStatusReport.
constexpr int kQuoteStatusRemovedFromMarket = 6;
constexpr int kQuoteStatusExpired = 7;

TimestampNs system_now_ns() {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

std::optional<std::uint64_t> parse_id(const std::string& s) {
  std::uint64_t v = 0;
  const auto [ptr, ec] = std::from_chars(s.data(), s.data() + s.size(), v);
  if (ec != std::errc{} || ptr != s.data() + s.size()) return std::nullopt;
  return v;
}

Price to_price(double px) { return static_cast<Price>(std::llround(px * kPriceScale)); }

// FIX carries quantities as decimals, but the engine trades whole units. Rounding would let a
// dealer quoting 99.6 pass a full-size check for 100, so anything that is not a whole number is
// refused instead.
std::optional<Qty> whole_qty(double v) {
  constexpr double kMaxQty = 1e15;  // far above any real size, well inside int64
  if (!std::isfinite(v) || v <= 0 || v > kMaxQty || v != std::floor(v)) return std::nullopt;
  return static_cast<Qty>(v);
}

bool valid_price(double px) { return std::isfinite(px) && px > 0 && px < 1e12; }
double from_price(Price p) { return static_cast<double>(p) / kPriceScale; }

TimestampNs to_ns(const FIX::UtcTimeStamp& t) {
  return static_cast<TimestampNs>(t.getTimeT()) * 1'000'000'000 + t.getFraction(3) * 1'000'000;
}

FIX::UtcTimeStamp from_ns(TimestampNs ns) {
  const auto secs = static_cast<std::time_t>(ns / 1'000'000'000);
  const int millis = static_cast<int>((ns % 1'000'000'000) / 1'000'000);
  return FIX::UtcTimeStamp(secs, millis, 3);
}

char fix_side(Side s) { return s == Side::Buy ? '1' : '2'; }
char opposite_fix_side(Side s) { return s == Side::Buy ? '2' : '1'; }

}  // namespace

FixGateway::FixGateway(const FIX::SessionSettings& settings, RfqEngine::EventSink sink, Sender sender,
                       Clock clock, std::uint64_t id_base)
    : engine_(std::move(sink), id_base), sender_(std::move(sender)), clock_(std::move(clock)) {
  if (!clock_) clock_ = system_now_ns;
  if (!sender_) {
    sender_ = [](FIX::Message& m, const FIX::SessionID& sid) { FIX::Session::sendToTarget(m, sid); };
  }
  for (const FIX::SessionID& sid : settings.getSessions()) {
    const std::string role = settings.get(sid).getString("Role");
    if (role == "CLIENT") {
      roles_[sid.toString()] = Role::Client;
    } else if (role == "DEALER") {
      roles_[sid.toString()] = Role::Dealer;
    } else {
      throw FIX::ConfigError("session " + sid.toString() + ": Role must be CLIENT or DEALER");
    }
  }
}

bool FixGateway::has_role(const FIX::SessionID& sid, Role role) const {
  const auto it = roles_.find(sid.toString());
  return it != roles_.end() && it->second == role;
}

void FixGateway::send(FIX::Message& m, const FIX::SessionID& sid) { sender_(m, sid); }

void FixGateway::onLogon(const FIX::SessionID& sid) {
  std::lock_guard lock(mu_);
  if (has_role(sid, Role::Dealer)) dealers_online_[sid.getTargetCompID().getString()] = sid;
  publish_gauges();
}

void FixGateway::onLogout(const FIX::SessionID& sid) {
  std::lock_guard lock(mu_);
  const std::string dealer = sid.getTargetCompID().getString();
  const auto it = dealers_online_.find(dealer);
  if (it != dealers_online_.end() && it->second == sid) dealers_online_.erase(it);

  if (has_role(sid, Role::Dealer)) {
    // A disconnected dealer cannot confirm a trade, so its live quotes must not stay hittable.
    const TimestampNs now = clock_();
    for (const auto& [qid, info] : quotes_) {
      if (info.dealer != dealer || engine_.withdraw_quote(qid, now) != ErrorCode::Ok) continue;
      const auto rfq = rfqs_.find(info.rfq_id);
      if (rfq != rfqs_.end()) quote_removed(rfq->second, qid, "dealer disconnected");
    }
  }
  publish_gauges();
}

void FixGateway::publish_gauges() {
  if (observer_) observer_->on_gauges(rfqs_.size(), dealers_online_.size());
}

void FixGateway::fromApp(const FIX::Message& message, const FIX::SessionID& sid)
    EXCEPT(FIX::FieldNotFound, FIX::IncorrectDataFormat, FIX::IncorrectTagValue,
           FIX::UnsupportedMessageType) {
  // Declared before the lock so the duration covers the whole handler and is reported after unlock.
  struct Timing {
    GatewayObserver* observer;
    std::string type;
    std::chrono::steady_clock::time_point start = std::chrono::steady_clock::now();
    ~Timing() {
      if (observer == nullptr) return;
      const std::chrono::duration<double> elapsed = std::chrono::steady_clock::now() - start;
      observer->on_message(type.c_str(), elapsed.count());
    }
  } timing{observer_.get(), message.getHeader().getField(FIX::FIELD::MsgType)};

  std::lock_guard lock(mu_);
  crack(message, sid);
  publish_gauges();
}

std::size_t FixGateway::open_rfqs() const {
  std::lock_guard lock(mu_);
  return rfqs_.size();
}

void FixGateway::reject(const FIX::SessionID& sid, const char* ref_msg_type, const std::string& reason) {
  FIX44::BusinessMessageReject r{FIX::RefMsgType(ref_msg_type),
                                 FIX::BusinessRejectReason(FIX::BusinessRejectReason_OTHER)};
  r.set(FIX::Text(reason));
  send(r, sid);
  if (observer_) observer_->on_reject(reason.c_str());
}

void FixGateway::exec_report(const FIX::SessionID& to, const std::string& order_id,
                             const std::string& cl_ord_id, char exec_type, char ord_status, char side,
                             const std::string& symbol, Price price, Qty qty, const std::string& text) {
  const bool traded = exec_type == 'F';
  FIX44::ExecutionReport er(FIX::OrderID(order_id), FIX::ExecID(std::to_string(++exec_seq_)),
                            FIX::ExecType(exec_type), FIX::OrdStatus(ord_status), FIX::Side(side),
                            FIX::LeavesQty(0), FIX::CumQty(traded ? static_cast<double>(qty) : 0.0),
                            FIX::AvgPx(traded ? from_price(price) : 0.0));
  er.set(FIX::Symbol(symbol));
  if (!cl_ord_id.empty()) er.set(FIX::ClOrdID(cl_ord_id));
  if (traded) {
    er.set(FIX::LastPx(from_price(price)));
    er.set(FIX::LastQty(static_cast<double>(qty)));
  }
  if (!text.empty()) er.set(FIX::Text(text));
  send(er, to);
}

void FixGateway::quote_removed(const RfqInfo& rfq, QuoteId quote_id, const std::string& reason) {
  FIX44::QuoteStatusReport r;
  r.setField(FIX::QuoteReqID(rfq.client_req_id));
  r.setField(FIX::QuoteID(std::to_string(quote_id)));
  r.setField(FIX::QuoteStatus(kQuoteStatusRemovedFromMarket));
  r.setField(FIX::Symbol(rfq.symbol));
  r.setField(FIX::Text(reason));
  send(r, rfq.client);
}

void FixGateway::rfq_closed(RfqId rfq_id, const RfqInfo& rfq, int quote_status, const std::string& reason,
                            const std::string& except_dealer) {
  for (const std::string& dealer : rfq.dealers) {
    if (dealer == except_dealer) continue;
    const auto online = dealers_online_.find(dealer);
    if (online == dealers_online_.end()) continue;

    FIX44::QuoteStatusReport r;
    r.setField(FIX::QuoteReqID(std::to_string(rfq_id)));
    // Name the dealer's own latest quote, if it sent one.
    for (auto q = rfq.quote_ids.rbegin(); q != rfq.quote_ids.rend(); ++q) {
      const auto info = quotes_.find(*q);
      if (info != quotes_.end() && info->second.dealer == dealer) {
        r.setField(FIX::QuoteID(info->second.dealer_quote_id));
        break;
      }
    }
    r.setField(FIX::QuoteStatus(quote_status));
    r.setField(FIX::Symbol(rfq.symbol));
    r.setField(FIX::Text(reason));
    send(r, online->second);
  }
}

void FixGateway::forget(RfqId rfq_id) {
  engine_.erase(rfq_id);
  const auto it = rfqs_.find(rfq_id);
  if (it == rfqs_.end()) return;

  for (const QuoteId qid : it->second.quote_ids) {
    const auto q = quotes_.find(qid);
    if (q == quotes_.end()) continue;
    dealer_quote_index_.erase({q->second.dealer, q->second.dealer_quote_id});
    quotes_.erase(q);
  }
  client_req_index_.erase({it->second.client.toString(), it->second.client_req_id});
  rfqs_.erase(it);
}

void FixGateway::onMessage(const FIX44::QuoteRequest& m, const FIX::SessionID& sid) {
  if (!has_role(sid, Role::Client)) return reject(sid, "R", "only clients may request quotes");

  FIX::QuoteReqID req_id;
  m.get(req_id);
  if (client_req_index_.count({sid.toString(), req_id.getString()}) != 0) {
    return reject(sid, "R", "duplicate QuoteReqID");
  }

  FIX::NoRelatedSym count;
  m.get(count);
  if (count.getValue() < 1) return reject(sid, "R", "no instrument in request");

  FIX44::QuoteRequest::NoRelatedSym group;
  m.getGroup(1, group);
  FIX::Symbol symbol;
  FIX::Side side;
  FIX::OrderQty qty;
  group.get(symbol);
  group.get(side);
  group.get(qty);
  if (side != FIX::Side_BUY && side != FIX::Side_SELL) return reject(sid, "R", "Side must be 1 or 2");

  const TimestampNs now = clock_();
  TimestampNs expires = now + kDefaultRfqLifetimeNs;
  FIX::ExpireTime expire_time;
  if (group.getFieldIfSet(expire_time)) expires = to_ns(expire_time.getValue());
  if (expires - now > kMaxRfqLifetimeNs) return reject(sid, "R", "ExpireTime is more than 300 s away");

  std::vector<std::string> dealer_ids;
  for (const auto& [dealer, session] : dealers_online_) dealer_ids.push_back(dealer);
  if (dealer_ids.empty()) return reject(sid, "R", "no dealers online");

  const auto rfq_qty = whole_qty(qty.getValue());
  if (!rfq_qty) return reject(sid, "R", "OrderQty must be a positive whole number");

  const Side rfq_side = side == FIX::Side_BUY ? Side::Buy : Side::Sell;
  const auto created = engine_.submit_rfq(
      RfqRequest{sid.getTargetCompID().getString(), symbol.getString(), rfq_side, *rfq_qty, expires, dealer_ids},
      now);
  if (!created.ok()) return reject(sid, "R", to_string(created.error));

  const RfqId rfq_id = created.value;
  rfqs_[rfq_id] = RfqInfo{sid, req_id.getString(), symbol.getString(), rfq_side, dealer_ids, {},
                          std::chrono::steady_clock::now(), false};
  client_req_index_[{sid.toString(), req_id.getString()}] = rfq_id;
  if (observer_) observer_->on_rfq_created();

  for (const auto& [dealer, session] : dealers_online_) {
    FIX44::QuoteRequest forward{FIX::QuoteReqID(std::to_string(rfq_id))};
    FIX44::QuoteRequest::NoRelatedSym g;
    g.set(symbol);
    g.set(side);
    g.set(qty);
    g.set(FIX::ExpireTime(from_ns(expires)));
    forward.addGroup(g);
    send(forward, session);
  }
}

void FixGateway::onMessage(const FIX44::Quote& m, const FIX::SessionID& sid) {
  if (!has_role(sid, Role::Dealer)) return reject(sid, "S", "only dealers may quote");

  FIX::QuoteReqID req_id;
  FIX::QuoteID dealer_quote_id;
  m.get(req_id);
  m.get(dealer_quote_id);

  const auto rfq_id = parse_id(req_id.getString());
  const auto rfq = rfq_id ? rfqs_.find(*rfq_id) : rfqs_.end();
  if (rfq == rfqs_.end()) return reject(sid, "S", "unknown QuoteReqID");

  FIX::OfferPx offer_px;
  FIX::OfferSize offer_size;
  FIX::BidPx bid_px;
  FIX::BidSize bid_size;
  const bool buy = rfq->second.side == Side::Buy;  // client buys -> dealer offers
  const bool complete = buy ? (m.getFieldIfSet(offer_px) && m.getFieldIfSet(offer_size))
                            : (m.getFieldIfSet(bid_px) && m.getFieldIfSet(bid_size));
  if (!complete) return reject(sid, "S", buy ? "OfferPx and OfferSize required" : "BidPx and BidSize required");

  const double px = buy ? offer_px.getValue() : bid_px.getValue();
  const double size = buy ? offer_size.getValue() : bid_size.getValue();
  const auto quote_qty = whole_qty(size);
  if (!quote_qty) return reject(sid, "S", "quote size must be a positive whole number");
  if (!valid_price(px)) return reject(sid, "S", "quote price must be a positive number");

  TimestampNs valid_until = std::numeric_limits<TimestampNs>::max();
  FIX::ValidUntilTime valid;
  if (m.getFieldIfSet(valid)) valid_until = to_ns(valid.getValue());

  const std::string dealer = sid.getTargetCompID().getString();
  const TimestampNs now = clock_();

  // A new quote replaces the dealer's live one; remember which, to tell the client after.
  std::vector<QuoteId> replaced;
  if (const Rfq* before = engine_.find(*rfq_id)) {
    for (const Quote& q : before->quotes) {
      if (q.dealer_id == dealer && q.status == QuoteStatus::Active) replaced.push_back(q.id);
    }
  }

  const auto quoted = engine_.submit_quote(
      *rfq_id, QuoteInput{dealer, to_price(px), *quote_qty, valid_until}, now);
  if (!quoted.ok()) return reject(sid, "S", to_string(quoted.error));

  const QuoteId qid = quoted.value;
  quotes_[qid] = QuoteInfo{*rfq_id, dealer, dealer_quote_id.getString(), sid};
  dealer_quote_index_[{dealer, dealer_quote_id.getString()}] = qid;
  rfq->second.quote_ids.push_back(qid);

  for (const QuoteId old : replaced) quote_removed(rfq->second, old, "replaced by quote " + std::to_string(qid));

  // Relay the price as stored (fixed-point), so the client sees exactly the price it would trade at.
  const Quote& stored = engine_.find(*rfq_id)->quotes.back();
  FIX44::Quote out{FIX::QuoteID(std::to_string(qid))};
  out.set(FIX::QuoteReqID(rfq->second.client_req_id));
  out.set(FIX::Symbol(rfq->second.symbol));
  if (buy) {
    out.set(FIX::OfferPx(from_price(stored.price)));
    out.set(FIX::OfferSize(static_cast<double>(stored.qty)));
  } else {
    out.set(FIX::BidPx(from_price(stored.price)));
    out.set(FIX::BidSize(static_cast<double>(stored.qty)));
  }
  out.set(FIX::ValidUntilTime(from_ns(stored.valid_until)));
  send(out, rfq->second.client);

  if (observer_) {
    observer_->on_quote_relayed();
    if (!rfq->second.quoted) {
      rfq->second.quoted = true;
      const std::chrono::duration<double> since = std::chrono::steady_clock::now() - rfq->second.received_at;
      observer_->on_first_quote(since.count());
    }
  }
}

void FixGateway::onMessage(const FIX44::QuoteResponse& m, const FIX::SessionID& sid) {
  if (!has_role(sid, Role::Client)) return reject(sid, "AJ", "only clients may respond to quotes");

  FIX::QuoteRespType type;
  m.get(type);
  if (type != FIX::QuoteRespType_HIT) return reject(sid, "AJ", "only QuoteRespType=1 (hit/lift) is supported");

  FIX::QuoteID quote_id;
  m.get(quote_id);
  const auto qid = parse_id(quote_id.getString());
  const auto quote = qid ? quotes_.find(*qid) : quotes_.end();
  if (quote == quotes_.end()) return reject(sid, "AJ", "unknown QuoteID");

  const RfqId rfq_id = quote->second.rfq_id;
  const RfqInfo& info = rfqs_.at(rfq_id);
  if (!(info.client == sid)) return reject(sid, "AJ", "quote belongs to another client");

  const auto exec = engine_.execute(rfq_id, *qid, clock_());
  if (!exec.ok()) return reject(sid, "AJ", to_string(exec.error));

  const Execution& e = exec.value;
  const std::string dealer_text = "dealer=" + e.dealer_id;
  exec_report(sid, std::to_string(rfq_id), info.client_req_id, 'F', '2', fix_side(info.side), info.symbol,
              e.price, e.qty, dealer_text);
  exec_report(quote->second.dealer_session, std::to_string(*qid), quote->second.dealer_quote_id, 'F', '2',
              opposite_fix_side(info.side), info.symbol, e.price, e.qty, "");
  rfq_closed(rfq_id, info, kQuoteStatusRemovedFromMarket, "traded away", e.dealer_id);
  if (observer_) {
    const std::chrono::duration<double> since = std::chrono::steady_clock::now() - info.received_at;
    observer_->on_executed(since.count());
  }
  forget(rfq_id);
}

void FixGateway::onMessage(const FIX44::QuoteCancel& m, const FIX::SessionID& sid) {
  FIX::QuoteID quote_id;
  m.get(quote_id);

  if (has_role(sid, Role::Dealer)) {
    const std::string dealer = sid.getTargetCompID().getString();
    const auto it = dealer_quote_index_.find({dealer, quote_id.getString()});
    if (it == dealer_quote_index_.end()) return reject(sid, "Z", "unknown QuoteID");
    const QuoteId qid = it->second;
    const ErrorCode rc = engine_.withdraw_quote(qid, clock_());
    if (rc != ErrorCode::Ok) return reject(sid, "Z", to_string(rc));
    const auto rfq = rfqs_.find(quotes_.at(qid).rfq_id);
    if (rfq != rfqs_.end()) quote_removed(rfq->second, qid, "withdrawn by dealer");
    return;
  }

  FIX::QuoteReqID req_id;
  if (!m.getFieldIfSet(req_id)) return reject(sid, "Z", "QuoteReqID required");
  const auto it = client_req_index_.find({sid.toString(), req_id.getString()});
  if (it == client_req_index_.end()) return reject(sid, "Z", "unknown QuoteReqID");

  const RfqId rfq_id = it->second;
  const RfqInfo info = rfqs_.at(rfq_id);
  const ErrorCode rc = engine_.cancel_rfq(rfq_id, clock_());
  if (rc != ErrorCode::Ok) return reject(sid, "Z", to_string(rc));

  exec_report(sid, std::to_string(rfq_id), info.client_req_id, '4', '4', fix_side(info.side), info.symbol, 0, 0, "");
  rfq_closed(rfq_id, info, kQuoteStatusRemovedFromMarket, "RFQ cancelled by client");
  if (observer_) observer_->on_cancelled();
  forget(rfq_id);
}

void FixGateway::tick() {
  std::lock_guard lock(mu_);
  for (const RfqId rfq_id : engine_.expire(clock_())) {
    const auto it = rfqs_.find(rfq_id);
    if (it == rfqs_.end()) continue;
    const RfqInfo& info = it->second;
    exec_report(info.client, std::to_string(rfq_id), info.client_req_id, 'C', 'C', fix_side(info.side),
                info.symbol, 0, 0, "RFQ expired");
    rfq_closed(rfq_id, info, kQuoteStatusExpired, "RFQ expired");
    if (observer_) observer_->on_expired();
    forget(rfq_id);
  }
  publish_gauges();
}

}  // namespace rfq
