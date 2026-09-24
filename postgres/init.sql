-- Marketplace OLTP schema (source of CDC) + serving tables for the stream/batch layers.

CREATE TABLE sellers (
    seller_id   serial PRIMARY KEY,
    name        text NOT NULL,
    phone       text NOT NULL,
    seller_type text NOT NULL CHECK (seller_type IN ('individual', 'agency', 'dealer')),
    created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE listings (
    listing_id  serial PRIMARY KEY,
    seller_id   int NOT NULL REFERENCES sellers,
    category    text NOT NULL CHECK (category IN ('property', 'car', 'classified')),
    subcategory text NOT NULL,
    location    text NOT NULL,
    title       text NOT NULL,
    description text NOT NULL,
    price       numeric(12, 2) NOT NULL CHECK (price > 0),
    image_phash bigint,
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now()
);
-- full "before" image on UPDATE/DELETE so CDC events carry old price
ALTER TABLE listings REPLICA IDENTITY FULL;

CREATE TABLE listing_interactions (
    interaction_id   bigserial PRIMARY KEY,
    listing_id       int NOT NULL,  -- no FK: interactions outlive deleted listings
    interaction_type text NOT NULL CHECK (interaction_type IN ('view', 'favorite', 'lead')),
    created_at       timestamptz NOT NULL DEFAULT now()
);

-- Price benchmark (stand-in for the Project 1 property warehouse export)
CREATE TABLE benchmark_prices (
    category     text NOT NULL,
    location     text NOT NULL,
    median_price numeric(12, 2) NOT NULL,
    PRIMARY KEY (category, location)
);
COPY benchmark_prices FROM '/docker-entrypoint-initdb.d/benchmark_prices.csv' CSV HEADER;

-- Written by the stream processor
CREATE TABLE risk_events (
    risk_event_id    bigserial PRIMARY KEY,
    listing_id       int NOT NULL,
    seller_id        int NOT NULL,
    risk_score       numeric(4, 3) NOT NULL,
    flags            text[] NOT NULL,
    status           text NOT NULL,
    source_partition int NOT NULL,
    source_offset    bigint NOT NULL,
    event_ts         timestamptz NOT NULL,
    processed_at     timestamptz NOT NULL DEFAULT now(),
    latency_ms       int NOT NULL,
    UNIQUE (source_partition, source_offset)  -- replay-safe
);
CREATE INDEX ON risk_events (listing_id, processed_at DESC);
CREATE INDEX ON risk_events (processed_at);

CREATE TABLE pipeline_metrics (
    ts             timestamptz NOT NULL DEFAULT now(),
    service        text NOT NULL,
    events         int NOT NULL,
    events_per_sec real NOT NULL,
    lag            bigint,
    latency_p50_ms real,
    latency_p95_ms real,
    errors         int NOT NULL DEFAULT 0
);
CREATE INDEX ON pipeline_metrics (ts);

CREATE TABLE recon_results (
    run_at           timestamptz NOT NULL DEFAULT now(),
    topic            text NOT NULL,
    kafka_events     bigint NOT NULL,
    s3_events        bigint NOT NULL,
    s3_duplicates    bigint NOT NULL,
    warehouse_events bigint NOT NULL,
    pending_events   bigint NOT NULL,
    mismatch         bigint NOT NULL,
    status           text NOT NULL
);

-- dbt marts are published here; Ask-Your-Data runs as a read-only role
CREATE SCHEMA analytics;
CREATE ROLE analyst_ro LOGIN PASSWORD 'analyst_ro';
GRANT USAGE ON SCHEMA analytics TO analyst_ro;
ALTER DEFAULT PRIVILEGES IN SCHEMA analytics GRANT SELECT ON TABLES TO analyst_ro;
ALTER ROLE analyst_ro SET default_transaction_read_only = on;
ALTER ROLE analyst_ro SET statement_timeout = '5s';
