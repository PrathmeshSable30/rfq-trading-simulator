#include "engine/rfq_engine.hpp"

#include <algorithm>

namespace rfq {

namespace {

bool is_better(const Quote& candidate, const Quote& current, Side side) {
  // Strict comparison keeps the earlier quote on equal prices.
  return side == Side::Buy ? candidate.price < current.price : candidate.price > current.price;
}

bool is_live(const Quote& q, TimestampNs now) {
  return q.status == QuoteStatus::Active && now < q.valid_until;
}

}  // namespace

void RfqEngine::emit(TimestampNs ts, EventPayload payload) {
  const std::uint64_t seq = next_seq_++;
  if (sink_) {
    sink_(Event{seq, ts, std::move(payload)});
  }
}

void RfqEngine::close(Rfq& rfq, RfqState terminal_state) {
  rfq.state = terminal_state;
  expiry_.erase({rfq.request.expires_at, rfq.id});
}

Result<RfqId> RfqEngine::submit_rfq(RfqRequest request, TimestampNs now) {
  if (request.qty <= 0 || request.expires_at <= now || request.dealers.empty() ||
      request.client_id.empty() || request.instrument.empty()) {
    return {ErrorCode::InvalidRequest, 0};
  }

  const RfqId id = next_rfq_id_++;
  Rfq rfq;
  rfq.id = id;
  rfq.created_at = now;
  rfq.request = std::move(request);

  const auto [it, inserted] = rfqs_.emplace(id, std::move(rfq));
  (void)inserted;
  const RfqRequest& req = it->second.request;
  expiry_.emplace(req.expires_at, id);

  emit(now, RfqCreated{id, req.client_id, req.instrument, req.side, req.qty, req.expires_at});
  return {ErrorCode::Ok, id};
}

Result<QuoteId> RfqEngine::submit_quote(RfqId rfq_id, QuoteInput input, TimestampNs now) {
  const auto it = rfqs_.find(rfq_id);
  if (it == rfqs_.end()) return {ErrorCode::UnknownRfq, 0};

  Rfq& rfq = it->second;
  if (is_terminal(rfq.state)) return {ErrorCode::RfqClosed, 0};
  if (now >= rfq.request.expires_at) return {ErrorCode::RfqExpired, 0};

  const auto& dealers = rfq.request.dealers;
  if (std::find(dealers.begin(), dealers.end(), input.dealer_id) == dealers.end()) {
    return {ErrorCode::DealerNotInvited, 0};
  }
  // Partial fills are not supported, so a quote must cover the full size.
  if (input.price <= 0 || input.qty < rfq.request.qty || input.valid_until <= now) {
    return {ErrorCode::InvalidQuote, 0};
  }

  for (Quote& previous : rfq.quotes) {
    if (previous.dealer_id == input.dealer_id && previous.status == QuoteStatus::Active) {
      previous.status = QuoteStatus::Superseded;
      emit(now, QuoteWithdrawn{rfq_id, previous.id, previous.dealer_id});
    }
  }

  Quote quote;
  quote.id = next_quote_id_++;
  quote.rfq_id = rfq_id;
  quote.dealer_id = std::move(input.dealer_id);
  quote.price = input.price;
  quote.qty = input.qty;
  quote.valid_until = std::min(input.valid_until, rfq.request.expires_at);
  quote.received_at = now;

  quote_index_.emplace(quote.id, rfq_id);
  emit(now, QuoteReceived{rfq_id, quote.id, quote.dealer_id, quote.price, quote.qty,
                          quote.valid_until});
  const QuoteId id = quote.id;
  rfq.quotes.push_back(std::move(quote));
  return {ErrorCode::Ok, id};
}

ErrorCode RfqEngine::withdraw_quote(QuoteId quote_id, TimestampNs now) {
  const auto idx = quote_index_.find(quote_id);
  if (idx == quote_index_.end()) return ErrorCode::UnknownQuote;

  Rfq& rfq = rfqs_.at(idx->second);
  if (is_terminal(rfq.state)) return ErrorCode::RfqClosed;

  const auto q = std::find_if(rfq.quotes.begin(), rfq.quotes.end(),
                              [&](const Quote& x) { return x.id == quote_id; });
  if (q == rfq.quotes.end() || q->status != QuoteStatus::Active) return ErrorCode::QuoteNotActive;

  q->status = QuoteStatus::Withdrawn;
  emit(now, QuoteWithdrawn{rfq.id, q->id, q->dealer_id});
  return ErrorCode::Ok;
}

Result<Execution> RfqEngine::execute(RfqId rfq_id, QuoteId quote_id, TimestampNs now) {
  const auto it = rfqs_.find(rfq_id);
  if (it == rfqs_.end()) return {ErrorCode::UnknownRfq, {}};

  Rfq& rfq = it->second;
  if (is_terminal(rfq.state)) return {ErrorCode::RfqClosed, {}};
  if (now >= rfq.request.expires_at) return {ErrorCode::RfqExpired, {}};

  const auto q = std::find_if(rfq.quotes.begin(), rfq.quotes.end(),
                              [&](const Quote& x) { return x.id == quote_id; });
  if (q == rfq.quotes.end()) return {ErrorCode::UnknownQuote, {}};
  if (q->status != QuoteStatus::Active) return {ErrorCode::QuoteNotActive, {}};
  if (now >= q->valid_until) return {ErrorCode::QuoteExpired, {}};

  q->status = QuoteStatus::Executed;
  rfq.executed_quote_id = q->id;
  close(rfq, RfqState::Executed);

  Execution exec{rfq.id, q->id, q->dealer_id, q->price, rfq.request.qty};
  emit(now, RfqExecuted{exec.rfq_id, exec.quote_id, exec.dealer_id, exec.price, exec.qty});
  return {ErrorCode::Ok, std::move(exec)};
}

ErrorCode RfqEngine::cancel_rfq(RfqId rfq_id, TimestampNs now) {
  const auto it = rfqs_.find(rfq_id);
  if (it == rfqs_.end()) return ErrorCode::UnknownRfq;
  if (is_terminal(it->second.state)) return ErrorCode::RfqClosed;

  close(it->second, RfqState::Cancelled);
  emit(now, RfqCancelled{rfq_id});
  return ErrorCode::Ok;
}

std::vector<RfqId> RfqEngine::expire(TimestampNs now) {
  std::vector<RfqId> expired;
  while (!expiry_.empty() && expiry_.begin()->first <= now) {
    const RfqId id = expiry_.begin()->second;
    close(rfqs_.at(id), RfqState::Expired);
    emit(now, RfqExpired{id});
    expired.push_back(id);
  }
  return expired;
}

bool RfqEngine::erase(RfqId rfq_id) {
  const auto it = rfqs_.find(rfq_id);
  if (it == rfqs_.end() || !is_terminal(it->second.state)) return false;

  for (const Quote& q : it->second.quotes) quote_index_.erase(q.id);
  rfqs_.erase(it);
  return true;
}

const Rfq* RfqEngine::find(RfqId rfq_id) const {
  const auto it = rfqs_.find(rfq_id);
  return it == rfqs_.end() ? nullptr : &it->second;
}

std::optional<Quote> RfqEngine::best_quote(RfqId rfq_id, TimestampNs now) const {
  const Rfq* rfq = find(rfq_id);
  if (rfq == nullptr || is_terminal(rfq->state) || now >= rfq->request.expires_at) {
    return std::nullopt;
  }

  const Quote* best = nullptr;
  for (const Quote& q : rfq->quotes) {
    if (!is_live(q, now)) continue;
    if (best == nullptr || is_better(q, *best, rfq->request.side)) best = &q;
  }
  if (best == nullptr) return std::nullopt;
  return *best;
}

}  // namespace rfq
