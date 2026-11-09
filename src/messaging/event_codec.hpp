#pragma once

#include <optional>
#include <string>
#include <string_view>

#include "common/events.hpp"

namespace rfq {

std::string encode(const Event& event);

// Returns nullopt for malformed input or an unknown event type.
std::optional<Event> decode(std::string_view json);

RfqId rfq_id_of(const EventPayload& payload);

const char* event_type(const EventPayload& payload);

}  // namespace rfq
