# Converter Web Playground — Design

**Date:** 2026-06-23
**Branch:** `kafka_converter`
**Extends:** the `internal/connect_converter` engine + `convert` CLI.

## Purpose

A simple local web playground for the Kafka Connect → Redpanda Connect converter:
the user pastes a Kafka Connect connector JSON in the left pane and the equivalent
Redpanda Connect (RPCN) YAML appears in the right pane, updating live. Mirrors the
benthos `blobl server` Bloblang editor pattern.

## Architecture

A new subcommand of the existing `convert` command, served by a small Go HTTP
server that reuses the existing engine — no conversion logic is duplicated.

```bash
rpk connect convert server                  # http://localhost:4196 (default)
rpk connect convert server --http :8080     # custom bind address
```

Layout (mirrors benthos `internal/cli/blobl/`):

```
internal/cli/convertserver/
  server.go            # convertServerCli(), mux, handlers
  server_test.go       # httptest-based handler tests
  resources/page.html  # //go:embed-ed single-page UI

internal/cli/convert.go  # register `server` as a subcommand of `convert`
```

Routes on a single `http.NewServeMux`:

- `GET /` → serves the embedded HTML page (`text/html`).
- `POST /convert` → request body is the raw Kafka Connect JSON; the handler calls
  `connectconverter.Convert(body)` and responds with JSON:
  ```json
  { "yaml": "...", "warnings": [{"field": "...", "message": "..."}], "error": "" }
  ```
  On a hard engine error (malformed JSON / missing `connector.class`), `yaml` is
  empty and `error` carries the message; HTTP status stays 200 (the page renders
  the error in the output pane). Genuine request errors (wrong method, unreadable
  body) return 4xx/5xx.

The server binds to localhost by default and serves a self-contained page (no
external assets), so it works offline. It is a thin presentation layer over the
engine; all mapping behavior and validation live in `connectconverter`.

## The page (two panes)

A single self-contained HTML file — vanilla JS, inline CSS/JS, no build step and
no external CDN (matches benthos's embedded editor page).

- **Left pane:** a `<textarea>` for the Kafka Connect JSON, pre-filled with a
  sample S3-sink config so the page is useful on first load.
- **Right pane:** a read-only monospace area showing the converted RPCN YAML. The
  `# TODO` comments are already part of the YAML text the engine returns.
- **Live conversion:** on input, debounced ~300 ms, the page POSTs the left-pane
  text to `/convert` and renders `yaml` (or `error`) in the right pane.
- **Warnings strip:** a small bar near the output showing
  `N warning(s) — review # TODO markers` derived from the `warnings` array (count;
  hovering/expanding can list `field: message`, but the count is the minimum).
- **Copy button:** copies the YAML to the clipboard.

Deliberately minimal — no connector picker, no file upload, no history. Just
paste-and-see.

## Data flow

```
textarea input
  → debounce 300ms → POST /convert (body = KC JSON)
  → server: connectconverter.Convert(body)
  → JSON {yaml, warnings, error}
  → page renders YAML (or error) in right pane + warning count
```

## Error handling

- Malformed JSON / missing `connector.class` → engine returns an error → response
  `{yaml:"", warnings:[], error:"<msg>"}` → page shows the error text in the output
  pane (not a blank pane). HTTP 200.
- `POST /convert` with the wrong method → 405; unreadable body → 400.
- The engine never panics on unknown connectors/fields (it emits TODO stubs), so
  the normal path always yields YAML.

## Testing

- `server_test.go` uses `httptest` against the mux:
  - `POST /convert` with a known S3-sink config → asserts the response `yaml`
    contains `aws_s3:` and the expected non-empty `warnings`/TODO behavior.
  - `POST /convert` with malformed JSON → asserts non-empty `error` and empty
    `yaml`.
  - `GET /` → asserts 200 and that the body contains the page markers (e.g. the
    two-pane element ids), proving the embed is wired.
- No browser/JS automation — the page is static vanilla JS.
- CLI wiring: a small test that `convertServerCli()` builds and is named `server`
  (consistent with the existing `convert` command tests).

## Constraints

- Module `github.com/redpanda-data/connect/v4`; new package `convertserver` in
  `internal/cli/`; package name `convertserver`.
- No new third-party dependencies — stdlib `net/http` + `embed` only.
- RCL Enterprise 2026 license header on every new `.go` file (matches `internal/cli`).
- Reuse `connectconverter.Convert`; no conversion logic in the server.

## Out of scope

- Authentication, TLS, public hosting (localhost dev tool).
- Pulling configs from a live Connect REST API in the UI (paste/curl remains the
  path).
- A connector catalog / examples gallery beyond the single seeded sample.
- Persisting or sharing conversions.
