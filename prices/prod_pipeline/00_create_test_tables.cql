-- ============================================================================
-- Prod Pipeline Test Schema (Astra CQL Editor)
-- ----------------------------------------------------------------------------
-- 1) Select your TEST keyspace in the Astra CQL editor, then run this file.
-- 2) These tables are isolated from your existing gecko_* production tables.
-- 3) Table names use pp_* prefix (prod-pipeline test).
-- ============================================================================

-- Optional (run once if you want a dedicated keyspace):
-- CREATE KEYSPACE IF NOT EXISTS blockviz_prod_pipeline_test
-- WITH replication = {'class': 'NetworkTopologyStrategy', 'Cassandra': 3};
-- USE blockviz_prod_pipeline_test;

CREATE TABLE IF NOT EXISTS pp_prices_live (
    id text PRIMARY KEY,
    symbol text,
    name text,
    category text,
    market_cap_rank int,
    price_usd double,
    market_cap double,
    volume_24h double,
    last_updated timestamp,
    last_fetched timestamp,
    ath_price double,
    ath_date timestamp,
    circulating_supply double,
    total_supply double,
    max_supply double,
    vs_currency text
);

CREATE TABLE IF NOT EXISTS pp_prices_live_ranked (
    bucket text,
    market_cap_rank int,
    id text,
    symbol text,
    name text,
    category text,
    price_usd double,
    market_cap double,
    volume_24h double,
    circulating_supply double,
    total_supply double,
    last_updated timestamp,
    PRIMARY KEY ((bucket), market_cap_rank, id)
) WITH CLUSTERING ORDER BY (market_cap_rank ASC, id ASC);

CREATE TABLE IF NOT EXISTS pp_prices_live_rolling (
    id text,
    last_updated timestamp,
    symbol text,
    name text,
    category text,
    market_cap_rank int,
    price_usd double,
    market_cap double,
    volume_24h double,
    last_fetched timestamp,
    ath_price double,
    ath_date timestamp,
    circulating_supply double,
    total_supply double,
    max_supply double,
    vs_currency text,
    PRIMARY KEY ((id), last_updated)
) WITH CLUSTERING ORDER BY (last_updated DESC);

CREATE TABLE IF NOT EXISTS pp_prices_10m_7d (
    id text,
    ts timestamp,
    symbol text,
    name text,
    open double,
    high double,
    low double,
    close double,
    price_usd double,
    market_cap double,
    volume_24h double,
    market_cap_rank int,
    circulating_supply double,
    total_supply double,
    last_updated timestamp,
    candle_source text,
    point_count int,
    PRIMARY KEY ((id), ts)
) WITH CLUSTERING ORDER BY (ts ASC);

CREATE TABLE IF NOT EXISTS pp_candles_hourly_30d (
    id text,
    ts timestamp,
    symbol text,
    name text,
    open double,
    high double,
    low double,
    close double,
    price_usd double,
    market_cap double,
    volume_24h double,
    market_cap_rank int,
    circulating_supply double,
    total_supply double,
    candle_source text,
    point_count int,
    last_updated timestamp,
    PRIMARY KEY ((id), ts)
) WITH CLUSTERING ORDER BY (ts ASC);

CREATE TABLE IF NOT EXISTS pp_candles_daily_contin (
    id text,
    date date,
    symbol text,
    name text,
    open double,
    high double,
    low double,
    close double,
    price_usd double,
    market_cap double,
    volume_24h double,
    market_cap_rank int,
    circulating_supply double,
    total_supply double,
    candle_source text,
    point_count int,
    last_updated timestamp,
    PRIMARY KEY ((id), date)
) WITH CLUSTERING ORDER BY (date ASC);

CREATE TABLE IF NOT EXISTS pp_candles_monthly (
    id text,
    year_month text,
    symbol text,
    name text,
    open double,
    high double,
    low double,
    close double,
    volume double,
    market_cap double,
    market_cap_rank int,
    circulating_supply double,
    total_supply double,
    candle_source text,
    last_updated timestamp,
    PRIMARY KEY ((id), year_month)
) WITH CLUSTERING ORDER BY (year_month ASC);

CREATE TABLE IF NOT EXISTS pp_market_cap_live (
    category text PRIMARY KEY,
    last_updated timestamp,
    market_cap double,
    market_cap_rank int,
    volume_24h double
);

CREATE TABLE IF NOT EXISTS pp_market_cap_10m_7d (
    category text,
    ts timestamp,
    last_updated timestamp,
    market_cap double,
    market_cap_rank int,
    volume_24h double,
    PRIMARY KEY ((category), ts)
) WITH CLUSTERING ORDER BY (ts DESC);

CREATE TABLE IF NOT EXISTS pp_market_cap_hourly_30d (
    category text,
    ts timestamp,
    last_updated timestamp,
    market_cap double,
    market_cap_rank int,
    volume_24h double,
    PRIMARY KEY ((category), ts)
) WITH CLUSTERING ORDER BY (ts DESC);

CREATE TABLE IF NOT EXISTS pp_market_cap_daily_contin (
    category text,
    date date,
    last_updated timestamp,
    market_cap double,
    market_cap_rank int,
    volume_24h double,
    PRIMARY KEY ((category), date)
) WITH CLUSTERING ORDER BY (date DESC);

CREATE TABLE IF NOT EXISTS pp_asset_categories (
    id text PRIMARY KEY,
    symbol text,
    category text,
    updated_at timestamp,
    source text
);

CREATE TABLE IF NOT EXISTS pp_coin_daily_coverage_ranges (
    id text,
    start_date date,
    end_date date,
    detected_at timestamp,
    PRIMARY KEY ((id), start_date)
) WITH CLUSTERING ORDER BY (start_date ASC);

CREATE TABLE IF NOT EXISTS pp_coin_daily_availability (
    id text PRIMARY KEY,
    first_day date,
    last_day date,
    expected_days int,
    have_any int,
    missing_any int,
    have_price int,
    missing_price int,
    have_volume int,
    missing_volume int,
    have_mcap int,
    missing_mcap int,
    symbol text,
    name text,
    updated_at timestamp,
    open_range_start date
);

CREATE TABLE IF NOT EXISTS pp_coin_intraday_coverage (
    id text,
    granularity tinyint,
    day date,
    bitmap blob,
    set_count int,
    first_seen timestamp,
    last_seen timestamp,
    PRIMARY KEY ((id, granularity), day)
) WITH CLUSTERING ORDER BY (day DESC);

CREATE TABLE IF NOT EXISTS pp_job_locks (
    job text,
    bucket text,
    created_at timestamp,
    PRIMARY KEY ((job), bucket)
);
