# Converter Web Playground Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `convert server` subcommand that serves a two-pane web playground — paste Kafka Connect JSON on the left, see live Redpanda Connect YAML on the right — backed by a thin HTTP endpoint reusing the `connectconverter` engine.

**Architecture:** A new `internal/cli/convertserver` package holds an `http.ServeMux` with `GET /` (serves an embedded single-page UI) and `POST /convert` (calls `connectconverter.Convert` and returns JSON). The page is vanilla JS, debounced-live. The `server` subcommand is registered under the existing `convert` command. No conversion logic lives in the server.

**Tech Stack:** Go stdlib `net/http` + `embed`, `github.com/urfave/cli/v2`, the existing `connectconverter` engine. Vanilla HTML/CSS/JS (no build step, no CDN).

## Global Constraints

- Module path: `github.com/redpanda-data/connect/v4`.
- New package: `internal/cli/convertserver/` — package name `convertserver`.
- No new third-party dependencies (stdlib `net/http`, `embed`, `encoding/json`, `io` only; plus existing `urfave/cli/v2` and `connectconverter`).
- RCL Enterprise 2026 license header verbatim on every new `.go` file:
  ```
  // Copyright 2026 Redpanda Data, Inc.
  //
  // Licensed as a Redpanda Enterprise file under the Redpanda Community
  // License (the "License"); you may not use this file except in compliance with
  // the License. You may obtain a copy of the License at
  //
  // https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md
  ```
- Reuse `connectconverter.Convert(input []byte) (*connectconverter.Result, error)`; `Result{YAML []byte; Warnings []connectconverter.Warning}`, `Warning{Field, Message string}`. No conversion logic in the server.
- Use an explicit `&http.Server{ReadHeaderTimeout: ...}` rather than `http.ListenAndServe` (avoids the gosec G114 lint finding).
- Default bind address: `localhost:4196`.
- Run `task fmt` before each commit; `./bin/golangci-lint run ./internal/cli/...` must be 0 issues by the end.

## File Structure

```
internal/cli/convertserver/
  server.go            # newMux(), handleIndex, handleConvert, Command()
  resources/page.html  # //go:embed-ed two-pane UI (vanilla JS)
  server_test.go       # httptest tests for both routes
internal/cli/convert.go  # add Subcommands: []*cli.Command{convertserver.Command()}
```

---

### Task 1: The `convertserver` package (HTTP server, page, handlers)

**Files:**
- Create: `internal/cli/convertserver/resources/page.html`
- Create: `internal/cli/convertserver/server.go`
- Test: `internal/cli/convertserver/server_test.go`

**Interfaces:**
- Consumes: `connectconverter.Convert`, `connectconverter.Warning`, `github.com/urfave/cli/v2`.
- Produces:
  - `func newMux() *http.ServeMux` — internal, testable router.
  - `func Command() *cli.Command` — the `server` subcommand (exported; called from `convert.go` in Task 2).
  - `GET /` → 200 `text/html`, the embedded page. Non-`/` paths → 404.
  - `POST /convert` → 200 `application/json` `{"yaml":string,"warnings":[{"field","message"}],"error":string}`. Wrong method → 405. Unreadable body → 400.

- [ ] **Step 1: Create the embedded page `resources/page.html`**

Create the directory and file with this exact content:

```html
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Kafka Connect → Redpanda Connect</title>
<style>
  * { box-sizing: border-box; }
  body { margin: 0; font-family: -apple-system, system-ui, sans-serif; background: #1e1e1e; color: #ddd; }
  header { padding: 10px 16px; background: #252526; border-bottom: 1px solid #333; font-size: 14px; }
  header b { color: #fff; }
  .panes { display: flex; height: calc(100vh - 86px); }
  .pane { flex: 1; display: flex; flex-direction: column; min-width: 0; }
  .pane + .pane { border-left: 1px solid #333; }
  .pane h2 { margin: 0; padding: 6px 12px; font-size: 12px; font-weight: 600; color: #aaa; background: #252526; text-transform: uppercase; letter-spacing: .05em; }
  textarea, pre { flex: 1; margin: 0; border: 0; padding: 12px; font-family: "SFMono-Regular", Consolas, monospace; font-size: 13px; line-height: 1.5; background: #1e1e1e; color: #ddd; resize: none; overflow: auto; white-space: pre; tab-size: 2; }
  textarea:focus { outline: none; }
  #warnings { padding: 6px 12px; font-size: 12px; background: #2d2d2d; border-top: 1px solid #333; color: #c8a; min-height: 16px; }
  #warnings.ok { color: #6a6; }
  .bar { display: flex; align-items: center; justify-content: space-between; padding: 4px 12px; background: #252526; border-top: 1px solid #333; }
  button { font-size: 12px; background: #0e639c; color: #fff; border: 0; padding: 4px 10px; border-radius: 3px; cursor: pointer; }
  button:hover { background: #1177bb; }
</style>
</head>
<body>
<header>Kafka Connect → <b>Redpanda Connect</b> converter — paste a connector config on the left, the pipeline appears on the right.</header>
<div class="panes">
  <div class="pane">
    <h2>Kafka Connect JSON</h2>
    <textarea id="input" spellcheck="false"></textarea>
  </div>
  <div class="pane">
    <h2>Redpanda Connect YAML</h2>
    <pre id="output"></pre>
    <div class="bar"><span id="warnings">&nbsp;</span><button id="copy">Copy</button></div>
  </div>
</div>
<script>
const input = document.getElementById("input");
const output = document.getElementById("output");
const warnings = document.getElementById("warnings");
const copyBtn = document.getElementById("copy");

input.value = JSON.stringify({
  name: "s3-sink",
  config: {
    "connector.class": "io.confluent.connect.s3.S3SinkConnector",
    "s3.bucket.name": "my-bucket",
    "s3.region": "us-east-1",
    "flush.size": "1000",
    "format.class": "io.confluent.connect.s3.format.avro.AvroFormat",
    "topics": "orders"
  }
}, null, 2);

let timer = null;
function scheduleConvert() {
  clearTimeout(timer);
  timer = setTimeout(convert, 300);
}

async function convert() {
  try {
    const resp = await fetch("/convert", { method: "POST", body: input.value });
    const data = await resp.json();
    if (data.error) {
      output.textContent = "# error: " + data.error;
      warnings.textContent = "invalid input";
      warnings.className = "";
      return;
    }
    output.textContent = data.yaml;
    const n = (data.warnings || []).length;
    warnings.textContent = n === 0 ? "no warnings" : n + " warning(s) — review # TODO markers";
    warnings.className = n === 0 ? "ok" : "";
  } catch (e) {
    output.textContent = "# request failed: " + e;
    warnings.textContent = "";
    warnings.className = "";
  }
}

input.addEventListener("input", scheduleConvert);
copyBtn.addEventListener("click", () => navigator.clipboard.writeText(output.textContent));
convert();
</script>
</body>
</html>
```

- [ ] **Step 2: Write the failing tests `server_test.go`**

```go
// <PASTE LICENSE HEADER>

package convertserver

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConvertEndpointHappy(t *testing.T) {
	srv := httptest.NewServer(newMux())
	defer srv.Close()

	body := `{"name":"s3","config":{"connector.class":"io.confluent.connect.s3.S3SinkConnector","s3.bucket.name":"b","topics":"orders"}}`
	resp, err := http.Post(srv.URL+"/convert", "application/json", strings.NewReader(body))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var out struct {
		YAML     string `json:"yaml"`
		Warnings []struct {
			Field   string `json:"field"`
			Message string `json:"message"`
		} `json:"warnings"`
		Error string `json:"error"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	assert.Empty(t, out.Error)
	assert.Contains(t, out.YAML, "aws_s3:")
	assert.Contains(t, out.YAML, "bucket: b")
}

func TestConvertEndpointError(t *testing.T) {
	srv := httptest.NewServer(newMux())
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/convert", "application/json", strings.NewReader(`{not json`))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var out struct {
		YAML  string `json:"yaml"`
		Error string `json:"error"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	assert.NotEmpty(t, out.Error)
	assert.Empty(t, out.YAML)
}

func TestConvertEndpointWrongMethod(t *testing.T) {
	srv := httptest.NewServer(newMux())
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/convert")
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode)
}

func TestIndexServesPage(t *testing.T) {
	srv := httptest.NewServer(newMux())
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	b, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	page := string(b)
	assert.Contains(t, page, `id="input"`)
	assert.Contains(t, page, `id="output"`)
}

func TestCommandBuilds(t *testing.T) {
	cmd := Command()
	require.NotNil(t, cmd)
	assert.Equal(t, "server", cmd.Name)
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `go test ./internal/cli/convertserver/ -v`
Expected: FAIL — `undefined: newMux` / `undefined: Command`.

- [ ] **Step 4: Implement `server.go`**

```go
// <PASTE LICENSE HEADER>

package convertserver

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/urfave/cli/v2"

	connectconverter "github.com/redpanda-data/connect/v4/internal/connect_converter"
)

//go:embed resources/page.html
var pageHTML []byte

type warningJSON struct {
	Field   string `json:"field"`
	Message string `json:"message"`
}

type convertResponse struct {
	YAML     string        `json:"yaml"`
	Warnings []warningJSON `json:"warnings"`
	Error    string        `json:"error"`
}

func handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(pageHTML)
}

func handleConvert(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "read body: "+err.Error(), http.StatusBadRequest)
		return
	}

	resp := convertResponse{Warnings: []warningJSON{}}
	res, err := connectconverter.Convert(body)
	if err != nil {
		resp.Error = err.Error()
	} else {
		resp.YAML = string(res.YAML)
		for _, wn := range res.Warnings {
			resp.Warnings = append(resp.Warnings, warningJSON{Field: wn.Field, Message: wn.Message})
		}
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func newMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/convert", handleConvert)
	mux.HandleFunc("/", handleIndex)
	return mux
}

// Command returns the `server` subcommand that serves the converter playground.
func Command() *cli.Command {
	return &cli.Command{
		Name:  "server",
		Usage: "Serve a web playground for converting Kafka Connect configs",
		Description: `
Starts a local web server with a two-pane playground: paste a Kafka Connect
connector config on the left and see the equivalent Redpanda Connect pipeline on
the right.

  {{.BinaryName}} convert server --http localhost:4196`[1:],
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "http",
				Value: "localhost:4196",
				Usage: "The address to bind the playground server to.",
			},
		},
		Action: func(c *cli.Context) error {
			addr := c.String("http")
			server := &http.Server{
				Addr:              addr,
				Handler:           newMux(),
				ReadHeaderTimeout: 5 * time.Second,
			}
			fmt.Fprintf(c.App.Writer, "Running converter playground at http://%s\n", addr)
			return server.ListenAndServe()
		},
	}
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `go test ./internal/cli/convertserver/ -v`
Expected: PASS (all five).

- [ ] **Step 6: Lint, format, commit**

```bash
./bin/golangci-lint run ./internal/cli/convertserver/...
task fmt
git add internal/cli/convertserver/
git commit -m "feat(cli): converter web playground server package"
```
Expected: lint 0 issues.

---

### Task 2: Wire the `server` subcommand into `convert`

**Files:**
- Modify: `internal/cli/convert.go` (add the import + `Subcommands`)
- Test: `internal/cli/convert_test.go` (add a subcommand-presence assertion)

**Interfaces:**
- Consumes: `convertserver.Command()` (Task 1).
- Produces: `convert server` reachable from the built binary.

- [ ] **Step 1: Add a failing test to `convert_test.go`**

Append this test to the existing `internal/cli/convert_test.go`:

```go
func TestConvertCliHasServerSubcommand(t *testing.T) {
	cmd := convertCli()
	var found bool
	for _, sub := range cmd.Subcommands {
		if sub.Name == "server" {
			found = true
		}
	}
	assert.True(t, found, "convert should have a 'server' subcommand")
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test ./internal/cli/ -run TestConvertCliHasServerSubcommand -v`
Expected: FAIL — `cmd.Subcommands` is empty (no `server`).

- [ ] **Step 3: Register the subcommand in `convert.go`**

Add the import to the import block:

```go
	"github.com/redpanda-data/connect/v4/internal/cli/convertserver"
```

In `convertCli()`'s returned `&cli.Command{...}`, add a `Subcommands` field (place it after `Flags`):

```go
		Subcommands: []*cli.Command{convertserver.Command()},
```

(The existing `Action` still handles `convert <file>` / stdin; urfave/cli dispatches to the `server` subcommand only when the first arg is `server`.)

- [ ] **Step 4: Run the test + build to verify**

Run: `go test ./internal/cli/ -run TestConvertCliHasServerSubcommand -v`
Expected: PASS.

Run: `go build ./cmd/redpanda-connect/`
Expected: builds clean.

- [ ] **Step 5: Manual smoke test**

```bash
go build -o /tmp/rpcn ./cmd/redpanda-connect
/tmp/rpcn convert server --http localhost:4196 &
sleep 1
curl -s -XPOST localhost:4196/convert --data '{"name":"s","config":{"connector.class":"io.confluent.connect.s3.S3SinkConnector","s3.bucket.name":"b","topics":"o"}}'
curl -s localhost:4196/ | grep -o 'id="input"'
kill %1
rm -f /tmp/rpcn
```
Expected: the POST returns JSON with `"yaml"` containing `aws_s3:`; the GET returns the page (grep prints `id="input"`).

- [ ] **Step 6: Lint, format, commit**

```bash
./bin/golangci-lint run ./internal/cli/...
task fmt
git add internal/cli/convert.go internal/cli/convert_test.go
git commit -m "feat(cli): register convert server subcommand"
```
Expected: lint 0 issues.

---

## Self-Review

**Spec coverage:**
- `convert server` subcommand + `--http` default `localhost:4196` → Task 1 (`Command()`), Task 2 (wiring). ✓
- `internal/cli/convertserver/` package, name `convertserver` → Task 1. ✓
- `GET /` serves embedded page; `POST /convert` returns `{yaml,warnings,error}` JSON → Task 1 (`handleIndex`/`handleConvert`). ✓
- Two-pane live (debounced 300ms) page, sample S3 config, warnings strip, copy button → `page.html` (Step 1). ✓
- Error handling: malformed/missing-class → `error` field, HTTP 200; wrong method → 405; unreadable body → 400 → `handleConvert`. ✓
- Thin over engine, reuses `connectconverter.Convert`, no new deps → Task 1. ✓
- gosec-safe `&http.Server{ReadHeaderTimeout}` instead of `http.ListenAndServe` → `Command()`. ✓
- Tests: httptest for both routes (happy/error/method/index) + CLI subcommand presence → `server_test.go`, `convert_test.go`. ✓

**Placeholder scan:** No "TBD"/"add error handling"/"similar to" steps. `<PASTE LICENSE HEADER>` markers are concrete instructions to paste the verbatim RCL block from Global Constraints.

**Type consistency:** `newMux() *http.ServeMux`, `Command() *cli.Command`, `convertResponse{YAML,Warnings,Error}` with `warningJSON{Field,Message}` (json tags `yaml`/`warnings`/`error`/`field`/`message`) are consistent between `server.go` and the test's decode struct. `connectconverter.Convert`/`Result.YAML`/`Result.Warnings`/`Warning.Field`/`Warning.Message` match the engine's real API. Page element ids (`input`, `output`, `warnings`, `copy`) match the test's `id="input"`/`id="output"` assertions and the JS `getElementById` calls.
