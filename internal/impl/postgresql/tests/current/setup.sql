-- Multi-schema CDC test setup
-- Tests: schema glob (tenant_*), exclude_schemas (tenant_c), database_schema
-- metadata, commit_ts_ms, before (update/delete)

-- ── Tenant schemas ────────────────────────────────────────────────────────────
-- tenant_a and tenant_b are replicated; tenant_c matches the tenant_* glob in
-- test_config.yaml but is carved out via exclude_schemas — its rows must
-- never appear in the pipeline output.

CREATE SCHEMA IF NOT EXISTS tenant_a;
CREATE SCHEMA IF NOT EXISTS tenant_b;
CREATE SCHEMA IF NOT EXISTS tenant_c;

-- ── Events table (same shape in each schema) ──────────────────────────────────

CREATE TABLE IF NOT EXISTS tenant_a.events (
    id         SERIAL PRIMARY KEY,
    name       TEXT NOT NULL,
    status     TEXT NOT NULL DEFAULT 'active',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS tenant_b.events (
    id         SERIAL PRIMARY KEY,
    name       TEXT NOT NULL,
    status     TEXT NOT NULL DEFAULT 'active',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS tenant_c.events (
    id         SERIAL PRIMARY KEY,
    name       TEXT NOT NULL,
    status     TEXT NOT NULL DEFAULT 'active',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- REPLICA IDENTITY FULL so update/delete messages carry the full before-row.
ALTER TABLE tenant_a.events REPLICA IDENTITY FULL;
ALTER TABLE tenant_b.events REPLICA IDENTITY FULL;
ALTER TABLE tenant_c.events REPLICA IDENTITY FULL;

-- ── Seed snapshot rows ────────────────────────────────────────────────────────
-- These are visible during the initial snapshot (stream_snapshot: true).
-- tenant_c's row (mallory) must NOT appear in the output — see exclude_schemas
-- in test_config.yaml.

INSERT INTO tenant_a.events (name) VALUES ('alice'), ('bob');
INSERT INTO tenant_b.events (name) VALUES ('carol');
INSERT INTO tenant_c.events (name) VALUES ('mallory');
