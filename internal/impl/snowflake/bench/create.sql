-- Snowflake benchmark table setup.
-- Run once before executing any benchmark.
-- Substitute ${DB} and ${SCHEMA} with actual values, or run with:
--   snowsql -a $SNOWFLAKE_ACCOUNT -u $SNOWFLAKE_USER -d $SNOWFLAKE_DB -s $SNOWFLAKE_SCHEMA -f create.sql

-- ── write-streaming ──────────────────────────────────────────────────────────
-- Typed table consumed by the snowflake_streaming (Snowpipe Streaming) benchmark.
CREATE TABLE IF NOT EXISTS BENCH_EVENTS (
    ID         NUMBER,
    USER_ID    NUMBER,
    EVENT_TYPE VARCHAR,
    VALUE      FLOAT,
    INFO       VARCHAR,
    TS         TIMESTAMP_NTZ
);

-- ── write-bulk ───────────────────────────────────────────────────────────────
-- VARIANT table consumed by the snowflake_put (staged file + Snowpipe) benchmark.
CREATE TABLE IF NOT EXISTS BENCH_EVENTS_JSON (
    RECORD VARIANT
);

-- Internal stage for staged uploads.
CREATE STAGE IF NOT EXISTS BENCH_STAGE;

-- Pipe that loads each JSON array element as one row.
-- Matches the json_array archive processor in write-bulk/benchmark_config.yaml.
CREATE PIPE IF NOT EXISTS BENCH_PIPE AUTO_INGEST = FALSE AS
    COPY INTO BENCH_EVENTS_JSON (RECORD)
    FROM (SELECT $1 FROM @BENCH_STAGE)
    FILE_FORMAT = (TYPE = JSON STRIP_OUTER_ARRAY = TRUE COMPRESSION = AUTO);
