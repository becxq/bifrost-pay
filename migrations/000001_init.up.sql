CREATE TYPE order_status AS ENUM ('pending', 'success', 'failed');

CREATE TABLE idempotency_key(
    key VARCHAR(255) PRIMARY KEY,
    status order_status NOT NULL DEFAULT 'pending',
    status_code INT DEFAULT NULL,
    status_body TEXT,
    locked_till TIMESTAMPTZ,
    created_time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_time TIMESTAMPTZ NOT NULL DEFAULT NOW()
);