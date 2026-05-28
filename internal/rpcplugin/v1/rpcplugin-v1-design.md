# v1 Plugin Runtime — Engineering Design

**Status:** Draft for discussion
**Authors:** Connect engineering (Ash Jeffs)
**Tracking:** [ENG-1201](https://redpandadata.atlassian.net/browse/ENG-1201) (RFC delivery), [CON-145](https://redpandadata.atlassian.net/browse/CON-145) (host-side socket-only registration mode)
**Audience:** Connect engineering
**Scope:** Engineering design of the v1 plugin runtime as it lives in `internal/rpcplugin/v1/` and the v1 SDKs at `public/plugin/{go,python}/`. Cloud deployment topology, container packaging, Kubernetes sidecar orchestration, and BYOC operator integration are deliberately out of scope — see the cloud-deployment design in the `redpanda-data/redpanda-operator` repository at [`docs/connect-plugin-sidecars.md`](https://github.com/redpanda-data/redpanda-operator/blob/rpcn-plugin-rfc/docs/connect-plugin-sidecars.md) for those concerns.

---

## Summary

A new v1 plugin runtime added alongside the existing v0 runtime
(`internal/rpcplugin/`). v1 closes the developer-experience gaps in
today's `rpcplugin`:

- **In-code config specs.** No `plugin.yaml` — schema is built in code
  via a mirror of the benthos config-spec builder and serialised to
  the host at startup.
- **Multi-component per binary.** One plugin process registers any
  mix of inputs / processors / outputs; the host multiplexes
  instances onto a single gRPC connection.
- **Plugin interfaces byte-identical to `public/service`.**
  `BatchProcessor`, `BatchInput`, `BatchOutput`, `Message`,
  `ParsedConfig`, `Resources` etc. are type aliases — plugin code
  reads like in-tree benthos extension code modulo the import path
  and the final `rpcn.Serve(env)` call.
- **Socket-only host.** Connect dials a peer-owned Unix socket; it
  does not exec plugin binaries and does not own plugin process
  lifecycle. The peer is started and managed by an external process
  manager (e.g. the developer, or a container orchestrator).

The v1 runtime ships **additively** in the Connect v4.x release
stream — no major version bump, no breaking changes to v0. A working
proof-of-concept is checked in at `internal/rpcplugin/v1/`.

---

## Motivation

The current `internal/rpcplugin` shape has three load-bearing
properties that block "rpcplugin as canonical":

1. **One subprocess equals one plugin instance.** Two `catshout`
   processors in a user pipeline spawn two subprocesses, gRPC sockets,
   stdin/stdout pumps. At scale this is wasteful and operationally
   complex.
2. **The config schema lives in `plugin.yaml`, separate from the code
   that consumes it.** Plugin authors maintain field declarations in
   two places (YAML and a Go config struct with `json` tags); typos
   between them produce silent miscoercion at runtime.
3. **The author-facing API (`rpcn.ProcessorMain[T]`) diverges from
   `./public/service`.** A plugin author who learned the in-tree
   benthos `service.NewConfigSpec` / `RegisterBatchProcessor` style
   has to translate that mental model into the rpcn-specific
   `ProcessorMain` + plugin.yaml shape. The brief was "almost
   indistinguishable"; today it isn't.

This proposal addresses all three with a single architectural change:
**plugins build a `*service.Environment` of their own, register
multiple components in code, and serve them over a multiplexed
gRPC connection. The plugin.yaml goes away. The author writes code
that's identical to in-tree benthos extension code modulo the final
`rpcn.Serve(env)` call.**

---

## Author surface — what plugin code looks like

### Go

**Today (rpcn):**

```go
package main

import (
    "github.com/redpanda-data/benthos/v4/public/service"
    "github.com/redpanda-data/connect/v4/public/plugin/go/rpcn"
)

type cfg struct { Suffix string `json:"suffix"` }

func main() {
    rpcn.ProcessorMain(func(c cfg) (service.BatchProcessor, error) {
        return &catshout{suffix: c.Suffix}, nil
    })
}
```

Plus a side-car `plugin.yaml` (illustrative; real example at
`internal/rpcplugin/testdata/catshout/plugin.yaml`):

```yaml
name: catshout
type: processor
command: ["go", "run", "."]
fields:
  - { name: suffix, type: string, kind: scalar, default: " *purr*" }
```

**Proposed:**

```go
package main

import (
    "log"

    rpcn "github.com/redpanda-data/connect/v4/public/plugin/go/rpcn/v1"
)

func main() {
    env := rpcn.NewEnvironment()

    err := env.RegisterBatchProcessor(
        "catshout",
        rpcn.NewConfigSpec().
            Summary("Append a configurable suffix to each message, MEOW-style.").
            Field(rpcn.NewStringField("suffix").Default(" *purr*")),
        func(conf *rpcn.ParsedConfig, _ *rpcn.Resources) (rpcn.BatchProcessor, error) {
            suffix, err := conf.FieldString("suffix")
            if err != nil {
                return nil, err
            }
            return &catshout{suffix: []byte(suffix)}, nil
        },
    )
    if err != nil {
        log.Fatal(err)
    }

    // A single binary can register multiple components.
    if err := env.RegisterBatchInput("spamcounter", spamcounterSpec(), spamcounterCtor); err != nil {
        log.Fatal(err)
    }

    if err := rpcn.Serve(env); err != nil {
        log.Fatal(err)
    }
}
```

Plugin code is structurally identical to in-tree benthos extension
code modulo three mechanical differences:

- **Import path swap.** Customers import the SDK at
  `.../public/plugin/go/rpcn/v1` instead of benthos's
  `.../benthos/v4/public/service`. Constructor and method names are
  identical (`NewConfigSpec`, `NewStringField`, `ProcessBatch`,
  `AsBytes`, etc.); plugin interfaces (`BatchProcessor`,
  `BatchInput`, `BatchOutput`, `Message`, `ParsedConfig`,
  `Resources`, etc.) are type-aliased through to the benthos types
  (D1), so method signatures match byte-for-byte.
- **Explicit `*Environment` value** vs benthos's package-level
  registrar. An in-tree benthos extension calls
  `service.RegisterBatchProcessor(...)` from `init()` against a
  global env; a plugin calls `env.RegisterBatchProcessor(...)` on
  an `*rpcn.Environment` it constructs in `main()`. Same builder
  surface, different receiver.
- **`rpcn.Serve(env)` entrypoint** that drives the v1 plugin
  runtime's gRPC protocol for the host. In-tree benthos extensions
  compile into a benthos binary and don't have this.

The config spec is in code, the constructor signature matches
`./public/service` exactly, and one binary can register N
components.

No `plugin.yaml`. The schema is communicated to the host at startup
time via the gRPC protocol described below.

### Python

Equivalent shape in the Python SDK — exact decorator/class form TBD;
the principle is identical:

```python
from redpanda_connect_v1 import Environment, ProcessorSpec, ParsedConfig, Resources
import asyncio

env = Environment()

@env.batch_processor("catshout", ProcessorSpec().field("suffix", default=" *purr*"))
def make_catshout(conf: ParsedConfig, res: Resources):
    suffix = conf.field_string("suffix")
    return MyCatshout(suffix)

@env.batch_input("spamcounter", spamcounter_spec)
def make_spamcounter(conf: ParsedConfig, res: Resources):
    return Spamcounter(...)

asyncio.run(env.serve())
```

Same shape, same registration pattern, no `plugin.yaml`.

---

## Plugin distribution

The v1 Go SDK ships at the Go module path
`github.com/redpanda-data/connect/v4/public/plugin/go/rpcn/v1`.
Plugin authors `go get` it like any other Go dependency.

The v1 Python SDK ships as a PyPI package `redpanda-connect-v1`
(source at `public/plugin/python/v1/`). Plugin authors install it
via `pip` / `uv` / `poetry` as they would any other Python package.

A single plugin process can register N components in code (see
§"Author surface" and D4 for the multiplexing model). The plugin
author ships exactly one binary per plugin project regardless of how
many processors / inputs / outputs that project provides.

How plugin binaries are *deployed* — as standalone executables, as
container images in a Kubernetes sidecar, or something else — is a
deployment concern and is out of scope for this document. The
runtime contract the v1 host expects is a peer process listening on
a Unix socket; everything beyond that is the deployer's concern. See
the cloud-deployment design (linked in Scope above) for the BYOC
sidecar packaging convention and the local-development orchestration
story.

---

## Host integration — how plugins land in the user's pipeline

### Discovery

The host CLI accepts socket paths — either explicitly or by
directory scan — pointing at already-listening plugin processes:

```bash
redpanda-connect run \
  --plugin-socket catshout=/var/run/connect-plugins/catshout.sock \
  --plugin-socket-dir /var/run/connect-plugins/ \
  config/pipeline.yaml
```

For each socket:

1. Host dials the Unix socket using gRPC. If the socket is not yet
   present, it retries with exponential backoff (~30 s default
   budget) — handles the case where Connect starts before the
   plugin process is ready.
2. Host calls `Handshake` (D6) to negotiate protocol version and
   collect SDK metadata.
3. Host calls `ListComponents`. The plugin returns every component
   the author registered, including each component's serialised
   `*service.ConfigSpec`.
4. Host walks the returned list and calls `env.RegisterBatchProcessor`
   / `RegisterBatchInput` / `RegisterBatchOutput` on its own
   `*service.Environment` with reconstituted specs and proxy
   constructors.
5. From here on the user's pipeline YAML is linted, validated, and
   constructed exactly as if the plugin were compiled in-tree.

The host does not spawn the plugin process and does not own its
lifecycle. See D8.

### Per-instance proxy

When the user's pipeline references e.g. `catshout: { suffix: "!" }`,
benthos calls the constructor we registered. The constructor:

1. Issues `OpenInstance(component="catshout", config=<parsed config>)`
   on the gRPC connection.
2. Receives an `instance_id` from the plugin process.
3. Returns a `service.BatchProcessor` whose methods are gRPC proxies
   carrying `instance_id` on every call.

Multiple references to the same component name produce multiple
`OpenInstance` calls and therefore multiple distinct instances —
all multiplexed over the single gRPC connection to the single
plugin process.

### Lifecycle

- One gRPC connection per plugin socket, persistent for the host's
  lifetime.
- N component types per plugin (declared at `Register*` time on the
  plugin side).
- N instances per component type (created on demand by
  `OpenInstance`).
- **Connection loss** (plugin process restart, peer process exit,
  etc.): host marks all instances as failed (each Read / Write /
  Process call returns `ErrNotConnected`), reconnects with backoff
  when the socket reappears, re-issues `OpenInstance` for each live
  instance, framework retries. **The host does not respawn the
  plugin process.**
- Pipeline shutdown: host closes all live instances, then closes
  the gRPC connection. The plugin process exits when its Unix
  socket peer closes, on its own timer, or when its process manager
  terminates it.

---

## Wire protocol

Single gRPC service replaces the current three (`BatchInputService`,
`BatchProcessorService`, `BatchOutputService`). Lifecycle methods
take an `instance_id` and a discriminating component type internally
known from `OpenInstance`.

```protobuf
service PluginRuntime {
    // Discovery
    rpc ListComponents(ListComponentsRequest) returns (ListComponentsResponse);

    // Instance management
    rpc OpenInstance(OpenInstanceRequest) returns (OpenInstanceResponse);
    rpc CloseInstance(CloseInstanceRequest) returns (CloseInstanceResponse);

    // Lifecycle (all carry instance_id)
    rpc Connect(InstanceConnectRequest) returns (InstanceConnectResponse);
    rpc ReadBatch(InstanceReadRequest) returns (InstanceReadResponse);
    rpc Ack(InstanceAckRequest) returns (InstanceAckResponse);
    rpc ProcessBatch(InstanceProcessRequest) returns (InstanceProcessResponse);
    rpc SendBatch(InstanceSendRequest) returns (InstanceSendResponse);
}

message ListComponentsResponse {
    repeated ComponentDescriptor components = 1;
}

message ComponentDescriptor {
    string name = 1;
    ComponentKind kind = 2;  // INPUT | PROCESSOR | OUTPUT
    ConfigSpec spec = 3;
}

enum ComponentKind { COMPONENT_INPUT = 0; COMPONENT_PROCESSOR = 1; COMPONENT_OUTPUT = 2; }

message OpenInstanceRequest {
    string component_name = 1;
    Value config = 2;          // parsed user-supplied config as Value (already on the wire today)
}

message OpenInstanceResponse {
    uint64 instance_id = 1;
    // For outputs:
    int32 max_in_flight = 2;
    BatchPolicy batch_policy = 3;
    // For inputs:
    bool auto_replay_nacks = 4;
    Error error = 5;
}

message InstanceReadRequest  { uint64 instance_id = 1; }
message InstanceReadResponse { uint64 batch_id = 1; MessageBatch batch = 2; Error error = 3; }
message InstanceAckRequest   { uint64 instance_id = 1; uint64 batch_id = 2; Error error = 3; }
// ...etc.
```

The host correlates `(instance_id, batch_id)` pairs for ack callbacks
locally; the plugin process doesn't need a table — it just gets `Ack`
and dispatches to the right component's stored ackFn.

Concurrency: each `instance_id` has its own goroutine on the plugin
side; the gRPC handler routes by `instance_id` and serialises calls
per benthos's per-instance contract (single-reader inputs, serial
processor calls unless wrapped, parallel `Send` up to `max_in_flight`
on outputs).

The proto file lives at `proto/redpanda/runtime/v2/plugin_runtime.proto`.
The path's `/v2/` suffix follows proto API version conventions (the
next stable major after `v1alpha1`) and is independent of the v0 / v1
plugin-API labelling used elsewhere in this document.

---

## Config-spec serialisation — the awkward bit

`service.NewConfigSpec()` is a fluent builder. The internal tree of
field definitions is not currently exposed publicly. To round-trip a
spec to the host we need access to that tree. Three options, picked
in roughly increasing order of effort:

1. **Benthos adds a public `Unwrap` / `Describe()`** that returns a
   serialisable representation. Cleanest path. The method doesn't
   exist today; Redpanda maintains benthos, so the addition is
   in-house — a small benthos PR co-landed with the v1 SDK.
2. **Have benthos add `FormatYAML()` / `FormatJSON()`** on
   `*service.ConfigSpec`. Today only `*service.ConfigView` (a
   parsed-config view) exposes `FormatJSON`; the spec itself
   doesn't. Adding format methods to the spec is a benthos PR
   roughly as scoped as option (1), so this isn't a "free" path.
   Plugin would emit YAML/JSON, host re-parses. Round-trips through
   a well-understood format but loses lint-only metadata.
3. **Plugins use a wrapper builder we control.** `rpcn.NewConfigSpec()`
   instead of `service.NewConfigSpec()`. Same fluent API; under the
   hood it both builds a `*service.ConfigSpec` *and* captures a
   serialisable descriptor. Slightly violates the "indistinguishable
   from public/service" goal — author has to remember to use the rpcn
   import — but works without any benthos changes.

**Recommendation:** option (1) for the production version; option
(3) as an acceptable fallback if the benthos change isn't ready in
time. Option (2) carries the same benthos-change cost as (1)
without (1)'s lint-metadata fidelity and is therefore not
preferred.

For the proto representation:

```protobuf
message ConfigSpec {
    string summary = 1;
    string description = 2;
    string version = 3;
    repeated string categories = 4;
    repeated ConfigField fields = 5;
}

message ConfigField {
    string name = 1;
    string description = 2;
    FieldType type = 3;         // scalar/list/map/object
    FieldScalarKind scalar = 4; // string/int/float/bool/duration/...
    Value default = 5;
    bool optional = 6;
    bool advanced = 7;
    bool deprecated = 8;
    repeated ConfigField children = 9;   // for object fields
    repeated Value examples = 10;
    string lint_expression = 11;         // bloblang expression for custom lint
}
```

The proto must cover the **whole shape** of `*service.ConfigSpec`,
including: linter expressions, examples, optionality, deprecation,
defaults at every nesting level, list/map element type metadata. This
is non-trivial; it's the bulk of the work for the spec-serialisation
piece.

---

## Implementation phases

Sized in roughly the implementation order. "Session" below denotes
a discrete dev block (roughly half a day of focused engineering);
the units are nominal and serve only to indicate relative size.

### Phase A — protocol scaffolding (1-2 sessions)

- Define the new gRPC service in `proto/redpanda/runtime/v2/`.
- Generate `runtimepb` Go bindings.
- Stub host-side `PluginRuntime` client (dial / handshake /
  `ListComponents`).
- Legacy `v1alpha1` protos remain in-tree and default-built — the
  v0 plugin runtime is fully supported alongside v1 (D7). No build
  tag, no deprecation.

### Phase B — spec serialisation (2-3 sessions on the Connect side)

- Decide between options 1/2/3 above for ConfigSpec round-trip. If we
  go with (1), upstream a benthos PR adding `Describe()`.
- Implement the conversion: `*service.ConfigSpec → ConfigSpec proto`
  on the plugin side, `ConfigSpec proto → *service.ConfigSpec` on the
  host side. Round-trip test against every field type benthos
  supports.
- Validation matrix: ensure the host's linter / config-construction
  paths work identically against a spec round-tripped through proto
  vs the same spec built directly.

**External timeline dependency:** if we pursue option (1), the
benthos `Describe()` PR has its own review cycle that this estimate
does not include. Option (3) is the fallback if the benthos change
isn't ready in time — it adds no Connect-side rework over option (1),
so the 2-3 session estimate holds either way. The "what's the
biggest unknown" question for this phase is *benthos PR timing*, not
the Connect work itself.

### Phase C — Go SDK redesign (1-2 sessions)

- New `rpcn.Serve(env *rpcn.Environment) error` entry point — the
  Environment is the SDK's own type that captures registrations into
  serialisable descriptors; the host reconstructs a real
  `*service.Environment` from those descriptors during ListComponents
  handling.
- Add `Serve` alongside `rpcn.ProcessorMain` — both entry points
  remain reachable in default builds (D7). v0 plugin authors are
  not asked to migrate; new authors can pick either.
- Per-instance plumbing on the plugin side: spawn a goroutine per
  `instance_id`, route gRPC calls to the right component's stored
  ackFn + per-instance state.

### Phase D — host-side discovery + per-instance proxy (1-2 sessions)

- Plugin discovery for v1 via `--plugin-socket name=path` and
  `--plugin-socket-dir path` CLI flags (D5). v1 host is socket-only
  — Connect does not spawn v1 plugin processes (D8, CON-145). The
  existing `--rpc-plugins` flag for v0 plugins keeps working
  unchanged in the same binary.
- Per-RPC timeout: `--plugin-rpc-timeout` (default 30 s), passed to
  the host adapter's gRPC client call options.
- `OpenInstance` → `remoteBatch{Input,Processor,Output}` proxies.
- Connection-loss recovery: backoff-and-reconnect when the socket
  disappears, re-issue `OpenInstance` for each previously-live
  instance, never respawn the process.
- urfave/cli `altsrc` wiring so the new flags lift from the
  existing Connect config file (D5).

### Phase E — Python SDK production-shape (2 sessions)

- Finalise the v1 Python SDK at
  `public/plugin/python/v1/src/redpanda_connect_v1/` (PoC form
  already checked in). Decorator-based registration matching the
  Go shape.
- The v0 Python SDK at `public/plugin/python/src/redpanda_connect/`
  remains untouched — v1 is a separate, additive package on PyPI
  (per D2). v0 plugin authors stay on the v0 SDK; new authors pick
  v1.
- Production work over the PoC: full field-type matrix in the
  config-spec mirror, decorator surface stabilisation, packaging
  metadata (PyPI release pipeline).

### Phase F — testdata + integration tests (1 session)

- Port `catshout` to the new shape (in-code spec, single binary,
  registered via `env.RegisterBatchProcessor`).
- Add a "multi-component plugin" testdata example that registers an
  input + processor + output in one binary, exercises all of them
  from a single user pipeline.
- Conformance tests: multiple concurrent instances of the same
  component; per-instance state isolation; connection-loss recovery
  (plugin process disappears and comes back, host reconnects).

### Phase G — Scaffolding templates and local-dev story (1-2 sessions)

The v1 plugin runtime's host is socket-only (D5, D8). Plugin authors
need two things from Phase G:

1. **Starter project scaffolding** — templates emitted into a fresh
   directory containing a working `main.go` / `main.py`, a sane
   default config spec, and an example pipeline YAML for testing.
   Templates live in this repo at:
   - `public/plugin/go/rpcn/v1/templates/`
   - `public/plugin/python/templates/`
2. **A minimal manual local-dev path** — start the plugin in one
   terminal listening on a known socket; point Connect at it:

   ```bash
   # Terminal 1
   REDPANDA_CONNECT_PLUGIN_ADDRESS=unix:///tmp/myplugin.sock ./catshout

   # Terminal 2
   redpanda-connect run \
     --plugin-socket catshout=/tmp/myplugin.sock pipeline.yaml
   ```

   Connect treats this identically to any other peer — dials the
   socket, handshakes, lists components, runs the pipeline. There is
   no "Connect-runs-the-plugin" path anywhere in the codebase.

Higher-level orchestration that builds plugin images, runs them in
containers, and launches Connect with the right socket flags is
detailed in the cloud-deployment design (linked in Scope above).
That tooling composes the primitives described above; it does not
embed any plugin runtime logic.

### Phase H — docs + migration guide (1 session)

- Author-facing docs walking through writing a Go and a Python plugin
  end-to-end.
- Optional migration guide: how to convert an existing v0
  rpcplugin to v1 (`plugin.yaml` deletion; `ProcessorMain[T]` →
  `env.RegisterBatchProcessor`; `rpcn.Serve(env)`). Migration is
  opt-in.
- Update v0 scaffolding templates emitted by
  `internal/rpcplugin/init.go` to reference current CLI flag
  names (today's templates still emit a pre-v4 `--rpcplugin=` flag
  that no longer exists; the v0 flag is `--rpc-plugins`, and the
  v1 path uses `--plugin-socket`).

**Total estimate: 10-15 sessions** for a production-shaped
redesign (sum of the per-phase ranges above; Phase B's external
benthos-PR dependency is on top of this if option (1) is pursued).
Phase B is the highest-risk; everything else is mechanical once the
protocol and spec serialisation are nailed down.

---

## Decisions locked in

### D1. Spec construction — mirror builder in `rpcn`, alias the interfaces

`rpcn.NewConfigSpec()`, `rpcn.NewStringField()`, etc. are our own
types that mirror the public surface of `service.NewConfigSpec` /
`service.NewXxxField`. Plugin authors get a builder API that's
shape-identical to in-tree benthos code; the wrapper is the
import-path swap that makes plugin code "almost indistinguishable".

Plugin interfaces (`BatchProcessor`, `BatchInput`, `BatchOutput`,
`Message`, `MessageBatch`, `AckFunc`, `ParsedConfig`, `Resources`,
`BatchPolicy`) are exposed via type aliases — `type BatchProcessor =
service.BatchProcessor` — so the plugin author's `ProcessBatch`,
`Connect`, `Close` etc. method signatures are byte-identical to in-tree
benthos extensions. The only translation step is `s/service\./rpcn\./g`
on imports and `service.NewConfigSpec()` → `rpcn.NewConfigSpec()`.

~300 lines of mechanical mirroring in our repo, plus round-trip
tests.

**D1 commits to the author-facing surface, not the serialisation
mechanism.** The path the host uses to reconstruct a `*service.ConfigSpec`
from what the plugin sends on the wire is a separate concern — see
§"Config-spec serialisation — the awkward bit" for the three
candidate paths and the recommendation (option 1 as the production
target, option 3 as fallback). The wrapper builder works under any
of them.

### D2. Plugin distribution — Go module + PyPI package

The v1 SDKs ship as ordinary language-ecosystem dependencies:

- Go: `github.com/redpanda-data/connect/v4/public/plugin/go/rpcn/v1`,
  resolvable via `go get`.
- Python: `redpanda-connect-v1` on PyPI, installable via `pip` / `uv`
  / `poetry` / whatever the plugin author prefers.

The plugin author builds a binary (Go) or runnable Python entrypoint
the way they would build any other program in those ecosystems.
There is no manifest format, no managed dependency catalogue, no
bespoke packaging requirement imposed by the v1 runtime.

**Deployment-flavoured packaging — what runs in production cloud
deployments, where container images come from, how base images are
versioned — is out of scope here.** See the cloud-deployment design
(linked in Scope above) for the container-image packaging convention
used for BYOC sidecar deployment.

### D3. Resources proxying — telemetry tier ships in v1, the rest defers

Plugin's `*service.Resources` exposes six things: Logger, Metrics,
OtelTracer, FS, AccessCache, AccessRateLimit. The first three are
**Tier 1** (production observability — non-negotiable); the last
three are **Tier 2** (resource sharing — useful, deferrable).

**V1 ships Tier 1 only**, via a single client-streaming gRPC method:

```protobuf
rpc Telemetry(stream TelemetryEvent) returns (TelemetrySummary);

message TelemetryEvent {
    uint64 instance_id = 1;  // 0 for process-level events
    oneof event {
        LogRecord    log    = 10;
        MetricUpdate metric = 11;
        TraceSpan    span   = 12;
    }
}
```

Plugin's `res.Logger()` returns a `*service.Logger` built via
`service.NewLoggerFromSlog(slog.New(handler))`, where `handler` is
an `slog.Handler` implementation that emits onto the Telemetry
stream. Plugin's
`res.Metrics()` returns a `*service.Metrics` whose counter/gauge/timer
updates batch onto the stream. Host receives, tags every event with
`{plugin, component, instance}` labels automatically, dispatches into
the host's `service.Logger` / `service.Metrics` / OTel tracer.

Delivery is **batched and asynchronous**: plugin call sites enqueue
events and return immediately; a background goroutine drains the
queue onto the stream. Observability shouldn't gate plugin
throughput.

**Tier 2 (FS, AccessCache, AccessRateLimit) is explicitly deferred.**
For one-customer-per-deploy, plugin and host share a filesystem
mount; no FS proxying needed. Cache and rate-limit sharing is niche
enough that "first customer to ask" is the right trigger. Benthos
exposes both via callback-style APIs (`AccessCache(ctx, name, fn
func(Cache))`, `AccessRateLimit(ctx, name, fn func(RateLimit))`),
so the proxy shape is a short-lived session RPC rather than a
handle-returning getter:

```protobuf
rpc OpenCacheSession(OpenCacheSessionRequest)
    returns (OpenCacheSessionResponse);  // returns session_id
rpc CacheOp(CacheOpRequest) returns (CacheOpResponse);
rpc CloseCacheSession(CloseCacheSessionRequest)
    returns (CloseCacheSessionResponse);
```

(and a mirror set for RateLimit). The exact shape is hashed out
when the first customer needs it.

### D4. Plugin instance multiplexing — one connection per plugin, persistent, eager discovery

**Model:** one gRPC connection per plugin (per socket), alive for
the host's lifetime. All instances of all components from that
plugin are multiplexed onto the single connection, distinguished by
`instance_id` on every lifecycle call. **The host does not own the
plugin's process lifecycle** — see D8.

**Startup:** eager. Every registered plugin socket is dialled and
`ListComponents`'d before the user's pipeline YAML is parsed. The
host's `*service.Environment` is fully populated by the time
linting and config validation run, so plugin components are
first-class to the linter. If a plugin's socket is not yet ready
when the host starts, the host retries with exponential backoff
(see "Reconnection" below).

**Reconnection** (covers both initial startup race and mid-pipeline
disconnects): 3 retries with exponential backoff (500 ms, 2 s, 8 s),
extended to ~120 s under `CI=1`. While the connection is down,
every instance returns `service.ErrNotConnected`; benthos's
framework retries on `Connect`. After the socket comes back, the
host re-issues `OpenInstance` for each previously-live instance
with its original config. **In-process state does not recover** —
`Connect` is the plugin author's hook for re-establishing state,
same as in-tree components.

**Process respawn is not the host's concern.** When the plugin
process dies, its process manager decides whether and when to
restart it. From Connect's point of view this is the same as any
transient network disconnect — wait, reconnect, re-open instances.

**Clean shutdown:** host walks live instances calling
`CloseInstance`, then closes the gRPC connection. The plugin
process exits on its own when its peer hangs up, or its process
manager terminates it.

**Component name collisions:** first registration wins; subsequent
collisions log a warning. Plugin authors are expected to namespace
component names (suggested `customer:component`). Strict refusal
would break rolling-upgrade scenarios.

**Explicitly not in v1:** live reload, warm-spare plugin
connections, per-instance memory isolation inside the plugin
process. Horizontal scale is the answer to "I need more CPU" —
multiple Connect hosts each with their own plugin processes.

### D5. Plugin discovery — socket flags, no manifest format

Three CLI flags. The first two are complementary and can be set
together; the union is the plugin set. The third is a global
behaviour knob.

- `--plugin-socket name=path` (repeatable) — explicit registration
  of a plugin by name + Unix-socket path.
- `--plugin-socket-dir /path/to/dir` (repeatable) — directory scan.
  The host registers every `*.sock` file in the directory as a
  plugin, using the filename (sans extension) as the plugin name.
- `--plugin-rpc-timeout <duration>` — per-RPC timeout applied to
  every plugin call (Handshake, ListComponents, OpenInstance,
  ProcessBatch, etc.). Default 30 s. Applies to all plugins; no
  per-plugin override in v1.

All three flags should also be readable from the existing Connect
config file. Connect's current CLI does not auto-lift flags from
YAML — that requires opting into urfave/cli's `altsrc` package and
wiring `app.Before` to load the YAML source. Phase D adds this
wiring for the new flags (and ideally retrofits it for the wider
flag set; existing flags only support env-var fallback today):

```yaml
plugin-socket:
  - catshout=/var/run/connect-plugins/catshout.sock
plugin-socket-dir:
  - /var/run/connect-plugins/
```

is equivalent to `--plugin-socket catshout=... --plugin-socket-dir
/var/run/connect-plugins/` once the altsrc wiring is in place. No
new manifest format invented.

**There is no `--plugin /path/to/binary` form.** Connect does not
spawn plugin processes. The peer process is always owned by a
separate process manager. See D8 for the rationale.

### D6. Wire protocol versioning — explicit handshake replaces env var

Replace today's `REDPANDA_CONNECT_PLUGIN_VERSION=1` env var with a `Handshake` RPC the host calls first, before anything else:

```protobuf
service PluginRuntime {
    rpc Handshake(HandshakeRequest) returns (HandshakeResponse);
    // ... lifecycle methods ...
}

message HandshakeRequest {
    repeated uint32 supported_protocol_versions = 1;  // host's set
    string host_version = 2;                          // for the plugin's logs
}

message HandshakeResponse {
    uint32 selected_protocol_version = 1;   // must be one host advertised
    string sdk_name      = 2;               // "rpcn-go" / "rpcn-python" / ...
    string sdk_version   = 3;               // semver of the SDK package
    string language      = 4;               // "go" / "python" / ...
    repeated string capabilities = 5;       // forward-compat feature flags
    Error error          = 6;               // populated if plugin can't agree
}
```

Flow:
1. Host dials the plugin's Unix socket.
2. Host calls `Handshake` with its supported versions (initially `[1]`).
3. Plugin returns the highest version it supports that's also in the host's set, plus SDK metadata.
4. If selection fails (no overlap), host closes the connection and treats the plugin as failed. Restart is the process manager's concern, not Connect's.

The `capabilities` field is for **forward-compatible feature adds** that don't justify a major-version bump — e.g. "supports Tier 2 resource proxying" as the SDK ships it later. Host queries the capability list before invoking optional features.

Initial protocol version: `1` ships with the first Connect v4.x
minor release containing the v1 plugin runtime. The pre-existing
`internal/rpcplugin/` runtime is referred to throughout this
document as **v0**; v0 and v1 are independent wire protocols
served by independent host adapters in the same Connect binary.

### D7. Release vehicle and coexistence with the legacy v0 runtime

The v1 plugin runtime ships **additively** in the Connect v4.x
release stream. No major version bump. No breaking changes to v0.
Both runtimes are present, default-built, and fully supported in
the same Connect binary.

**Why additive instead of a v5 cut.** Plugin-side, v0 and v1 are
disjoint surfaces — v0 plugin authors call `rpcn.ProcessorMain[T]`
with a `plugin.yaml`; v1 plugin authors call
`env.RegisterBatchProcessor(...)` + `rpcn.Serve(env)` with no
YAML. Host-side, v0 dials a subprocess Connect spawned and speaks
the legacy three-service protocol; v1 dials a peer-owned socket
and speaks the multiplexed PluginRuntime protocol. The two paths
do not share runtime state, do not share gRPC services, and do not
share CLI flags. They coexist cleanly in one binary. Forcing v0
users to migrate would gain nothing.

**Namespace separation.** v1 code lives in
`internal/rpcplugin/v1/`. The existing v0 host
(`internal/rpcplugin/{config,input,output,processor}.go`,
`runtimepb/`, and `subprocess/`) stays where it is — physically
untouched by v1 work. The v0 subprocess plumbing is used only by
the v0 host; v1 does not depend on it.

**Default-built, no build tag.** Both runtimes compile in every
build configuration of the Connect binary. There is no
`legacy_rpcplugin` build tag. There is no dormant-legacy posture.
Both surfaces are first-class and supported.

**Scaffolding.** The existing `redpanda-connect plugin init`
subcommand (powered by `internal/rpcplugin/init.go` and the
`golangtemplate/` / `pythontemplate/` directories) keeps emitting
v0 scaffolding for anyone who still wants it; no changes there.
The v1 scaffolding templates live at
`public/plugin/go/rpcn/v1/templates/` and
`public/plugin/python/templates/`. Plugin authors choose by which
subcommand they invoke, not by a flag.

**CLI flag coexistence.** `--rpc-plugins` (v0) and the new
`--plugin-socket` / `--plugin-socket-dir` / `--plugin-rpc-timeout`
(v1, per D5) flags all live in the same binary. The host
registers v0 plugins via the v0 path (spawn subprocess, speak
three-service protocol) and v1 plugins via the v1 path (dial
socket, speak PluginRuntime protocol). Components from both
sources merge into the same `*service.Environment` with the
existing first-wins collision policy.

**Lifecycle ownership differs by path.** v0 plugins are spawned
and managed by Connect (current behaviour, unchanged). v1 plugins
are owned by their process manager — Connect is a pure gRPC
client. Plugin authors choose the path that fits.

**Future deprecation policy.** Not addressed by this document. If
a future proposal recommends deprecating v0, it will go through
its own process with its own migration plan. This design commits
only to landing v1 alongside v0; the v0 deprecation question is
independent and out of scope.

### D8. Socket-only host model for v1 — Connect never spawns v1 plugins (CON-145)

**The v1 plugin runtime's host has exactly one plugin registration
model: dial a Unix socket.** For v1 plugins, Connect does not exec
plugin binaries, does not own plugin processes, and does not
respawn dead plugins. The peer process is always owned by a
separate process manager.

The CLI surface for v1 follows from this: `--plugin-socket
name=path` and `--plugin-socket-dir path` (D5). The v1 surface has
no `--plugin /path/to/binary` form. **The existing v0
`--rpc-plugins` flag is unaffected** — v0 keeps spawning
subprocesses as it does today (D7).

**Why this is the only mode:**

- **One mental model.** Connect dials a socket. Always.
  Documentation, debugging, and code-review burden drop
  dramatically vs a subprocess-or-socket-depending-on-flag world.
- **Security posture.** "Connect never executes customer binaries"
  is true everywhere. Removes the entire exec-related threat
  surface.
- **Smaller host code.** No subprocess crash recovery, no exec
  ownership, no SIGKILL grace. The host adapter is purely a gRPC
  client.
- **Deployment parity.** A plugin binary started in a developer's
  terminal and a plugin binary started by an external orchestrator
  look the same to Connect — same wire protocol, same lifecycle
  semantics.

**No forced migration.** v1's socket-only host model applies only
to v1 plugins. v0 plugins continue to run as Connect-spawned
subprocesses unchanged (D7).

**Codebase impact.** The v1 host adapter at
`internal/rpcplugin/v1/host/` exposes a single entry point:
`RegisterPluginSocket(env, name, socketPath, opts...)`. The PoC-era
`RegisterPluginBinary` is removed. The v1 host does not depend on
`internal/rpcplugin/subprocess/` — that package stays in the tree
and is used by the v0 host (default-built, per D7).

**What this is not.** Socket-only mode is *not* a remote-endpoint
mechanism — the socket is always local to whatever environment
Connect is running in. Cross-host or cross-network gRPC
connectivity is a separate problem this design does not address.

### Open items (acknowledged, deferred from v1)

These came up during design review and are explicitly out of scope
for v1. Each gets its own follow-up effort when the forcing
function arrives.

- **Hot reload.** Plugin binary changes on disk → graceful instance
  swap without host restart. Useful, non-trivial, not blocking.
- **Plugin versioning.** `ListComponents` could return per-component
  semver. Useful for "is this customer's pipeline pinned to v1.2 of
  the plugin?" tracking. Currently no version field.
- **Health checks beyond connection loss.** A wedged-but-alive
  plugin (stuck goroutine, blocked I/O, socket still open) isn't
  currently detectable. Add a heartbeat eventually.
- **Tier 2 resource proxying.** `AccessCache`, `AccessRateLimit`,
  FS. Deferred per D3.
- **Cross-language SDK conformance suite.** Shared test fixtures
  every SDK (Go, Python, eventual Rust) runs to prove it speaks the
  protocol identically. Needed before the second SDK ships.

---

## Package layout summary

What lives where once the v1 runtime ships. Both v0 and v1 are
default-built; no build tag separates them.

| Path | Role |
|---|---|
| `internal/rpcplugin/subprocess/` | v0 host subprocess management |
| `internal/rpcplugin/{config,input,output,processor,init}.go` | v0 host (manifest-driven, subprocess-spawning) |
| `internal/rpcplugin/runtimepb/` | v0 protos (the three-services protocol) |
| `internal/rpcplugin/{golang,python}template/` | v0 plugin scaffolding |
| `internal/rpcplugin/v1/` | v1 host code (handshake, discovery, instance proxies, telemetry) |
| `internal/rpcplugin/v1/runtimepb/` | v1 protos (`PluginRuntime` service) |
| `public/plugin/go/rpcn/` | v0 Go SDK (`ProcessorMain[T]` etc.) |
| `public/plugin/go/rpcn/v1/` | v1 Go SDK (`NewEnvironment`, `Register*`, `Serve`) |
| `public/plugin/go/rpcn/v1/templates/` | v1 Go plugin scaffolding |
| `public/plugin/python/src/redpanda_connect/` | v0 Python SDK |
| `public/plugin/python/v1/src/redpanda_connect_v1/` | v1 Python SDK |
| `public/plugin/python/templates/` | v1 Python plugin scaffolding |

CI matrix:
- Default build (`go build ./...`) compiles both v0 and v1 host
  adapters and both SDK surfaces.
- No build-tag variant — there is no "v1-only" configuration to
  test separately.

## What this unblocks

- **Plugin code that's almost indistinguishable from in-tree benthos
  components.** The brief's original "almost exactly matches" target.
- **One plugin process per binary**, regardless of how many
  components or instances it serves. Dramatic resource win at any
  reasonable plugin density.
- **Host-side linting that knows about plugin schemas before any
  instance is constructed.** Removes a whole class of "deployed and
  failed in production" surprises.
- **Multi-component plugin binaries** — a single Python package can
  ship an input + processor + output that share helper code, share
  config, share a connection to the upstream service. Today each is
  a separate v0 subprocess.
- **A uniform plugin authoring story across deployment modes.** The
  same plugin binary runs locally during development and inside
  whatever production deployment topology the operator chooses.

## Performance characteristics and the batching dependency

The PoC benchmarks (see `internal/rpcplugin/v1/host/bench_test.go`)
compare the same transform implemented natively, as a Go plugin, and
as a Python plugin. Numbers on an M3 Pro, batched at varying sizes:

| Batch | Native (ns/msg) | Plugin Go (ns/msg) | Plugin Python (ns/msg) |
|---|---|---|---|
| 1 | 178 | 44,900 | 163,000 |
| 10 | 142 | 5,900 | 18,700 |
| 100 | 134 | 1,600 | 4,000 |

**Read:** the per-batch gRPC round-trip is ~45µs of fixed cost
(HTTP/2 framing, codec setup, ~95 allocations on the host side,
plus the equivalent on the plugin side). At batch=100 that's
amortised to ~1.5µs of overhead per message; at batch=1 it
dominates native by a factor of ~250×. The Python curve has the
same shape shifted up ~3× — interpreter and pure-Python protobuf
decode, not anything fixable on the wire.

Numbers will drift as the SDK and host adapter evolve; the table
above is intended as a current floor, not a contract. Re-run
`go test -bench=. -benchmem -run='^$' -benchtime=3s
./internal/rpcplugin/v1/host/...` to refresh.

The cheap quick-wins are already applied (lazy-allocated metadata,
preallocated batch slices in `runtimepb/convert.go`). What's left on
the table:

- **Streaming RPC.** Replace per-call unary `ProcessBatch` with a
  single bidirectional stream per instance carrying request /
  response pairs tagged by sequence ID. Amortises HTTP/2 framing
  across every batch the instance ever processes. Likely a ~50%
  win at batch=1, smaller at batch=100. Not architecturally cheap
  — needs in-flight-call multiplexing on both sides and a new proto
  shape. Worth doing in v1 only if benchmarks against a realistic
  customer pipeline show small-batch latency biting; otherwise queue
  for a v1.1.

- **Engine-side batching (cross-team dependency).** Batching is
  currently a knob plugin authors and customers tune via pipeline
  YAML (`batch_policy`, `batching` blocks) rather than something
  the engine arranges automatically based on downstream component
  characteristics. A separate Connect engine rework is on the
  roadmap to invert this — let the engine decide batch boundaries
  based on what each component can absorb. That effort is
  independent of this design; its scope and target release belong
  to the engine team. **That's the lever that actually
  matters for plugin-based pipelines:** the table above shows a
  ~28× throughput multiplier (Go) / ~41× (Python) between batch=1
  and batch=100, and customers can't reliably reach for that today
  without understanding the cost model. Once the engine batches
  intelligently across plugin boundaries, the practical "is the
  plugin runtime fast enough?" question shifts from "what's the
  round-trip per call?" to "what's the steady-state per-message
  cost at the engine's chosen batch size?" — which is the regime
  this design is already good at.

- **Python decode cost.** PluginPython is ~2.5–3.6× Plugin Go
  depending on batch size (3.6× at batch=1, 3.2× at batch=10, 2.5×
  at batch=100; ~3× on average) — the per-batch gap narrows as
  larger batches amortise the per-call decode overhead. The
  host-side allocation count is identical across the two, so the
  gap is entirely in the plugin process — Python's pure-Python
  protobuf runtime and our `_value.py` per-message attribute walk.
  Migrating to `protobuf` C++ generated types (or a faster
  alternative like `betterproto`) closes most of it. Out of scope
  for v1.

The streaming-RPC and Python-decode items are queued as follow-up
work, and engine-side batching is a hard prerequisite for any
customer pipeline that benchmarks small-batch-dominated against
the numbers above.

## Proof-of-concept artefacts

A minimal end-to-end PoC of the design is checked in alongside this
doc. It exercises the four headline ideas of this design — in-code
spec, multi-component per binary, byte-identical plugin interfaces,
and host-side discovery + remote proxy — across both languages:

| Layer | Go | Python |
|---|---|---|
| Wire protocol | `proto/redpanda/runtime/v2/plugin_runtime.proto` | (shared) |
| Generated bindings | `internal/rpcplugin/v1/runtimepb/` | `public/plugin/python/v1/src/redpanda_connect_v1/_proto/` |
| Plugin SDK | `public/plugin/go/rpcn/v1/` | `public/plugin/python/v1/src/redpanda_connect_v1/` |
| Host adapter | `internal/rpcplugin/v1/host/` | (host adapter is Go) |
| Example plugin | `internal/rpcplugin/v1/testdata/catshout/` | `internal/rpcplugin/v1/testdata/pycatshout/` |
| End-to-end test | `internal/rpcplugin/v1/host/host_test.go::TestGoCatshoutEndToEnd` | `internal/rpcplugin/v1/host/host_test.go::TestPythonCatshoutEndToEnd` |

Both PoC plugins register two processors (`catshout` + `reverser` and
`pycatshout` + `pyreverser`) from a single plugin process, validating
the multi-component-per-binary property. The end-to-end tests build a
real benthos `*service.Environment`, register the plugin components
on it, and run messages through a chained pipeline.

**Note on PoC vs final design.** The PoC's host adapter currently
exposes `RegisterPluginBinary(env, cmd, ...)`, which spawns the
plugin as a subprocess. This was a PoC-stage shortcut for fast
iteration. Per D8 the production host model is socket-only —
`RegisterPluginBinary` will be removed and `RegisterPluginSocket(env,
name, socketPath, ...)` becomes the sole entry point. The PoC
plugins and benchmarks need a corresponding update (start the
plugin binary separately, pass the socket path to the host). Scoped
as part of Phase D.

PoC scope is intentionally narrow — only the processor lifecycle is
exercised, single-instance multiplexing, no resources proxying, no
CLI integration. The proto + SDK + host surfaces are all built with
the full design in mind so the remaining lifecycle methods slot in
without re-architecting.
