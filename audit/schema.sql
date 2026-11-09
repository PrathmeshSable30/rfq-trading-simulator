-- Append-only audit trail. (partition_key, event_id) is the Kafka "topic:partition" and
-- offset, which makes redelivered messages idempotent.
CREATE TABLE IF NOT EXISTS rfq_events (
    event_id    BIGINT       NOT NULL,
    partition_key TEXT       NOT NULL,
    rfq_id      BIGINT       NOT NULL,
    event_type  TEXT         NOT NULL,
    event_ts_ns BIGINT       NOT NULL,
    payload     JSONB        NOT NULL,
    recorded_at TIMESTAMPTZ  NOT NULL DEFAULT now(),
    PRIMARY KEY (partition_key, event_id)
);

CREATE INDEX IF NOT EXISTS idx_rfq_events_rfq ON rfq_events (rfq_id, event_id);

CREATE TABLE IF NOT EXISTS rfqs (
    rfq_id      BIGINT PRIMARY KEY,
    client_id   TEXT        NOT NULL,
    instrument  TEXT        NOT NULL,
    side        TEXT        NOT NULL CHECK (side IN ('BUY', 'SELL')),
    qty         BIGINT      NOT NULL,
    state       TEXT        NOT NULL,
    created_ns  BIGINT      NOT NULL,
    expires_ns  BIGINT      NOT NULL
);

CREATE TABLE IF NOT EXISTS quotes (
    quote_id    BIGINT PRIMARY KEY,
    rfq_id      BIGINT      NOT NULL REFERENCES rfqs (rfq_id),
    dealer_id   TEXT        NOT NULL,
    price       BIGINT      NOT NULL,
    qty         BIGINT      NOT NULL,
    status      TEXT        NOT NULL,
    received_ns BIGINT      NOT NULL,
    valid_until_ns BIGINT   NOT NULL
);

CREATE TABLE IF NOT EXISTS executions (
    rfq_id      BIGINT PRIMARY KEY REFERENCES rfqs (rfq_id),
    quote_id    BIGINT      NOT NULL REFERENCES quotes (quote_id),
    dealer_id   TEXT        NOT NULL,
    price       BIGINT      NOT NULL,
    qty         BIGINT      NOT NULL,
    executed_ns BIGINT      NOT NULL
);

-- Messages that could not be parsed or applied; kept so nothing is silently lost.
CREATE TABLE IF NOT EXISTS rfq_events_dead (
    event_id      BIGINT      NOT NULL,
    partition_key TEXT        NOT NULL,
    raw           TEXT        NOT NULL,
    error         TEXT        NOT NULL,
    recorded_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (partition_key, event_id)
);
