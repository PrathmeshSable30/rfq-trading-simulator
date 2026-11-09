#pragma once

#include <cstdint>

namespace rfq {

using RfqId = std::uint64_t;
using QuoteId = std::uint64_t;
using TimestampNs = std::int64_t;
using Price = std::int64_t;  // fixed-point, 1e-6 units
using Qty = std::int64_t;

inline constexpr Price kPriceScale = 1'000'000;

enum class Side : std::uint8_t { Buy, Sell };

enum class RfqState : std::uint8_t { Open, Executed, Expired, Cancelled };

enum class QuoteStatus : std::uint8_t { Active, Withdrawn, Superseded, Executed };

enum class ErrorCode : std::uint8_t {
  Ok,
  InvalidRequest,
  InvalidQuote,
  UnknownRfq,
  UnknownQuote,
  RfqClosed,
  RfqExpired,
  DealerNotInvited,
  QuoteNotActive,
  QuoteExpired,
};

constexpr bool is_terminal(RfqState s) noexcept { return s != RfqState::Open; }

constexpr const char* to_string(ErrorCode e) noexcept {
  switch (e) {
    case ErrorCode::Ok: return "Ok";
    case ErrorCode::InvalidRequest: return "InvalidRequest";
    case ErrorCode::InvalidQuote: return "InvalidQuote";
    case ErrorCode::UnknownRfq: return "UnknownRfq";
    case ErrorCode::UnknownQuote: return "UnknownQuote";
    case ErrorCode::RfqClosed: return "RfqClosed";
    case ErrorCode::RfqExpired: return "RfqExpired";
    case ErrorCode::DealerNotInvited: return "DealerNotInvited";
    case ErrorCode::QuoteNotActive: return "QuoteNotActive";
    case ErrorCode::QuoteExpired: return "QuoteExpired";
  }
  return "Unknown";
}

template <typename T>
struct Result {
  ErrorCode error{ErrorCode::Ok};
  T value{};

  bool ok() const noexcept { return error == ErrorCode::Ok; }
};

}  // namespace rfq
