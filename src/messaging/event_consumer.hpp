#pragma once

#include "common/events.hpp"

namespace rfq {

class EventConsumer {
 public:
  virtual ~EventConsumer() = default;
  virtual void on_event(const Event& event) = 0;
};

}  // namespace rfq
