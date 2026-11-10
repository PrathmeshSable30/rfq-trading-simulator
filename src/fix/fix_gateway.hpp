#pragma once

#include <cstdint>
#include <chrono>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include <quickfix/Application.h>
#include <quickfix/MessageCracker.h>
#include <quickfix/SessionSettings.h>

#include "engine/rfq_engine.hpp"
#include "fix/gateway_observer.hpp"

namespace rfq {

// Bridges FIX 4.4 sessions to the RFQ engine.
//   client  -> QuoteRequest (R)          gateway -> dealers: QuoteRequest (R)
//   dealer  -> Quote (S)                 gateway -> client:  Quote (S)
//   client  -> QuoteResponse (AJ) = hit  gateway -> client + winning dealer: ExecutionReport (8)
//   client  -> QuoteCancel (Z)           cancels the RFQ; dealer QuoteCancel withdraws one quote
// Status notifications use QuoteStatusReport (AI):
//   to the client when a quote it was shown is withdrawn, replaced, or its dealer disconnects;
//   to every other invited dealer when the RFQ trades away, is cancelled, or expires.
// A dealer that logs out has its live quotes withdrawn, so they can no longer be hit.
// Session roles come from the custom "Role=CLIENT|DEALER" setting of each session.
// All entry points are serialised by one mutex, so the engine is never used concurrently.
class FixGateway : public FIX::Application, public FIX::MessageCracker {
 public:
  using Sender = std::function<void(FIX::Message&, const FIX::SessionID&)>;
  using Clock = std::function<TimestampNs()>;

  FixGateway(const FIX::SessionSettings& settings, RfqEngine::EventSink sink, Sender sender = {},
             Clock clock = {}, std::uint64_t id_base = 0);

  // Expires overdue RFQs and notifies their clients; call periodically.
  void tick();

  // Call before the acceptor starts; the observer is not guarded against concurrent replacement.
  void set_observer(std::shared_ptr<GatewayObserver> observer) { observer_ = std::move(observer); }

  std::size_t open_rfqs() const;

  void onCreate(const FIX::SessionID&) override {}
  void onLogon(const FIX::SessionID& sid) override;
  void onLogout(const FIX::SessionID& sid) override;
  void toAdmin(FIX::Message&, const FIX::SessionID&) override {}
  void toApp(FIX::Message&, const FIX::SessionID&) EXCEPT(FIX::DoNotSend) override {}
  void fromAdmin(const FIX::Message&, const FIX::SessionID&)
      EXCEPT(FIX::FieldNotFound, FIX::IncorrectDataFormat, FIX::IncorrectTagValue,
             FIX::RejectLogon) override {}
  void fromApp(const FIX::Message& message, const FIX::SessionID& sid)
      EXCEPT(FIX::FieldNotFound, FIX::IncorrectDataFormat, FIX::IncorrectTagValue,
             FIX::UnsupportedMessageType) override;

  void onMessage(const FIX44::QuoteRequest& m, const FIX::SessionID& sid) override;
  void onMessage(const FIX44::Quote& m, const FIX::SessionID& sid) override;
  void onMessage(const FIX44::QuoteResponse& m, const FIX::SessionID& sid) override;
  void onMessage(const FIX44::QuoteCancel& m, const FIX::SessionID& sid) override;

 private:
  enum class Role { Client, Dealer };

  struct RfqInfo {
    FIX::SessionID client;
    std::string client_req_id;
    std::string symbol;
    Side side{};
    std::vector<std::string> dealers;  // invited dealers (their comp ids)
    std::vector<QuoteId> quote_ids;
    std::chrono::steady_clock::time_point received_at;
    bool quoted{false};
  };

  struct QuoteInfo {
    RfqId rfq_id{};
    std::string dealer;
    std::string dealer_quote_id;
    FIX::SessionID dealer_session;
  };

  bool has_role(const FIX::SessionID& sid, Role role) const;
  void send(FIX::Message& m, const FIX::SessionID& sid);
  void reject(const FIX::SessionID& sid, const char* ref_msg_type, const std::string& reason);
  void exec_report(const FIX::SessionID& to, const std::string& order_id, const std::string& cl_ord_id,
                   char exec_type, char ord_status, char side, const std::string& symbol, Price price,
                   Qty qty, const std::string& text);
  void quote_removed(const RfqInfo& rfq, QuoteId quote_id, const std::string& reason);
  void rfq_closed(RfqId rfq_id, const RfqInfo& rfq, int quote_status, const std::string& reason,
                  const std::string& except_dealer = {});
  void forget(RfqId rfq_id);
  void publish_gauges();

  RfqEngine engine_;
  Sender sender_;
  Clock clock_;
  std::shared_ptr<GatewayObserver> observer_;
  std::map<std::string, Role> roles_;

  mutable std::mutex mu_;
  std::map<std::string, FIX::SessionID> dealers_online_;
  std::unordered_map<RfqId, RfqInfo> rfqs_;
  std::unordered_map<QuoteId, QuoteInfo> quotes_;
  std::map<std::pair<std::string, std::string>, QuoteId> dealer_quote_index_;
  std::map<std::pair<std::string, std::string>, RfqId> client_req_index_;
  std::uint64_t exec_seq_{0};
};

}  // namespace rfq
