-- Multi-schema CDC test setup
-- Tests: schema glob (tenant_*), pg_schema metadata, commit_ts_ms, before (update/delete)

-- ── Tenant schemas ────────────────────────────────────────────────────────────

CREATE SCHEMA IF NOT EXISTS tenant_a;
CREATE SCHEMA IF NOT EXISTS tenant_b;

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

-- REPLICA IDENTITY FULL so update/delete messages carry the full before-row.
ALTER TABLE tenant_a.events REPLICA IDENTITY FULL;
ALTER TABLE tenant_b.events REPLICA IDENTITY FULL;

-- ── Seed snapshot rows ────────────────────────────────────────────────────────
-- These are visible during the initial snapshot (stream_snapshot: true).

INSERT INTO tenant_a.events (name) VALUES ('alice'), ('bob');
INSERT INTO tenant_b.events (name) VALUES ('carol');
