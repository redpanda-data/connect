// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package salesforce

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Jeffail/checkpoint"
	"github.com/Jeffail/shutdown"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/httpclient"
	"github.com/redpanda-data/connect/v4/internal/impl/salesforce/salesforcegrpc"
	"github.com/redpanda-data/connect/v4/internal/impl/salesforce/salesforcehttp"
	"github.com/redpanda-data/connect/v4/internal/license"
)

const (
	sfciFieldTopics         = "topics"
	sfciFieldStreamSnapshot = "stream_snapshot"
	sfciFieldReplayPreset   = "replay_preset"

	sfciFieldSnapshotMaxBatchSize    = "snapshot_max_batch_size"
	sfciFieldStreamBatchSize         = "stream_batch_size"
	sfciFieldMaxParallelSnapshotObjs = "max_parallel_snapshot_objects"

	sfciReplayLatest   = "latest"
	sfciReplayEarliest = "earliest"

	cdcDataPrefix     = "/data/"
	cdcFirehoseTopic  = "/data/ChangeEvents"
	cdcChangeEventSfx = "ChangeEvent"
	peEventPrefix     = "/event/"
)

func init() {
	service.MustRegisterBatchInput("salesforce_cdc", salesforceCDCInputConfigSpec(), newSalesforceCDCInput)
}

func salesforceCDCInputConfigSpec() *service.ConfigSpec {
	spec := service.NewConfigSpec().
		Categories("Services").
		Summary("Streams Salesforce Change Data Capture (CDC) and Platform Events from the Pub/Sub gRPC API, optionally preceded by a REST snapshot of the configured sObjects.").
		Description(`Subscribes to one or more Salesforce Pub/Sub topics in parallel and emits a message per event. Topics may be:

- ` + "`/data/<sObject>ChangeEvent`" + ` — per-sObject CDC channel.
- ` + "`/data/ChangeEvents`" + ` — CDC firehose (every CDC-enabled sObject).
- ` + "`/event/<EventName>__e`" + ` — custom Platform Event.
- ` + "`/event/<StandardEventName>`" + ` — standard Platform Event (e.g. ` + "`LoginEventStream`" + `).
- A bare sObject name (e.g. ` + "`Account`" + `) is shorthand for ` + "`/data/AccountChangeEvent`" + `.

Optionally runs a REST snapshot for the CDC sObjects before opening the streaming subscriptions, so the pipeline sees the current state plus continuous changes. Per-topic replay state persists in a cache resource so each subscription resumes across restarts independently.

== When to use this input

Use ` + "`salesforce_cdc`" + ` for:

- Continuous ingestion with both historical state (snapshot) and live changes (CDC).
- Real-time custom or standard Platform Events.
- Mixed CDC + Platform Event pipelines under a single component.

Use a different Salesforce input instead if:

- You only need a one-off extract or periodic SOQL query — use xref:components:inputs/salesforce.adoc[` + "`salesforce`" + `].
- You need a GraphQL query (cross-object in one request) — use xref:components:inputs/salesforce_graphql.adoc[` + "`salesforce_graphql`" + `].

== Metadata

Every emitted message has:

- ` + "`topic`" + `: The full Pub/Sub topic path (e.g. "/event/Order__e").
- ` + "`replay_id`" + `: The Pub/Sub replay ID in hex (streaming events only).

CDC events also carry:

- ` + "`operation`" + `: "read" for snapshot rows; "create", "update", "delete", or "undelete" for CDC events.
- ` + "`sobject`" + `: The sObject API name (e.g. "Account").
- ` + "`record_ids`" + `: Comma-separated record IDs affected by the event (when present).

Platform Events also carry:

- ` + "`event_uuid`" + `: The Salesforce ` + "`EventUuid`" + ` extracted from the payload (the canonical dedup key), when present.

== Authentication

Uses the Salesforce OAuth 2.0 Client Credentials flow. Create a Connected App in Salesforce, enable OAuth settings and the Client Credentials Flow, ensure Change Data Capture is enabled for the target sObjects (Setup → Change Data Capture), then supply the Consumer Key as ` + "`client_id`" + ` and Consumer Secret as ` + "`client_secret`" + `.

== Prerequisites

Each ` + "`/data/...`" + ` topic requires Change Data Capture to be enabled for the corresponding sObject (Setup → Change Data Capture). Each ` + "`/event/...`" + ` topic requires the corresponding Platform Event to exist in Salesforce; standard events may require Event Monitoring licenses or specific permissions.
`)

	spec = spec.Fields(authFieldSpecs()...).
		Field(service.NewStringListField(sfciFieldTopics).
			Description("Pub/Sub topics to subscribe to. Each entry is one of: a bare sObject name (`Account` → `/data/AccountChangeEvent`), an explicit CDC channel (`/data/AccountChangeEvent`), the CDC firehose (`/data/ChangeEvents`), or a Platform Event topic (`/event/Order__e`, `/event/LoginEventStream`). Each topic gets its own gRPC subscription with an independent replay cursor.").
			Example([]string{"Account", "Contact"}).
			Example([]string{"/data/ChangeEvents"}).
			Example([]string{"Account", "/event/Order__e"}).
			Example([]string{"Opportunity", "MyCustom__c", "/event/Sync_Requested__e"})).
		Field(service.NewBoolField(sfciFieldStreamSnapshot).
			Description("When true (default), paginate a full REST snapshot of every CDC sObject in `topics` before opening any streaming subscription. When false, skip the snapshot and start streaming immediately. Platform Event topics (`/event/...`) are always skipped — they have no REST equivalent.").
			Default(true)).
		Field(service.NewStringEnumField(sfciFieldReplayPreset, sfciReplayLatest, sfciReplayEarliest).
			Description("Initial replay position used per topic only on first run (when no checkpoint exists in the cache); ignored once a topic's replay ID has been written.\n\n- `latest`: Start from new events only; any changes between prior run and Connect are skipped.\n- `earliest`: Replay from the retention start (24h standard, 72h with enhanced retention). Use to recover missed events after outages.").
			Default(sfciReplayLatest)).
		Field(service.NewIntField(sfciFieldSnapshotMaxBatchSize).
			Description("Page size for the REST snapshot query — records per `/query` response. Must be between 200 and 2000 per Salesforce REST API limits. Larger pages reduce HTTP round trips; smaller pages reduce peak memory per fetch.").
			Default(2000).
			Example(2000).
			Example(500)).
		Field(service.NewIntField(sfciFieldStreamBatchSize).
			Description("Number of events requested per gRPC `Fetch` call, per topic. Higher values improve throughput at the cost of peak batch memory; lower values give steadier latency under load.").
			Default(100).
			Example(100).
			Example(500)).
		Field(service.NewIntField(sfciFieldMaxParallelSnapshotObjs).
			Description("Number of sObjects snapshotted concurrently during the REST snapshot phase. Each in-flight snapshot consumes one HTTP connection and Salesforce API call quota. Default 1 serializes the work — raise when snapshotting many sObjects and your API limits permit.").
			Default(1)).
		Field(service.NewStringField(sfFieldCheckpointCache).
			Description("Name of the cache resource used to persist snapshot cursor and per-topic replay IDs across restarts. The cache must be declared under the top-level `cache_resources` block. Choose a durable cache (Redis, Postgres, DynamoDB) for production; in-memory caches lose checkpoints on restart.").
			Example("persistent_cache")).
		Field(service.NewStringField(sfFieldCheckpointCacheKey).
			Description("Key inside the checkpoint cache where this input's state is stored. Change when running multiple `salesforce_cdc` inputs against the same cache resource to avoid collisions.").
			Default("salesforce_cdc")).
		Field(service.NewIntField(sfFieldCheckpointLimit).
			Description("Maximum number of unacknowledged batches in flight (per topic) before that topic pauses reading. Prevents unbounded memory growth when downstream components stall. Higher values increase throughput in steady state; lower values bound memory under backpressure.").
			Default(1024)).
		Field(service.NewAutoRetryNacksToggleField()).
		Field(grpcFieldSpec()).
		Field(httpFieldSpec()).
		Field(service.NewBatchPolicyField(sfFieldBatching))

	spec = spec.Example("Snapshot + CDC for core objects",
		"Snapshot Accounts, Contacts, and Opportunities, then stream their CDC events:",
		`
input:
  salesforce_cdc:
    org_url: https://acme.my.salesforce.com
    client_id: ${SALESFORCE_CLIENT_ID}
    client_secret: ${SALESFORCE_CLIENT_SECRET}
    topics: [ Account, Contact, Opportunity ]
    stream_snapshot: true
    replay_preset: latest
    checkpoint_cache: persistent_cache

cache_resources:
  - label: persistent_cache
    redis:
      url: redis://localhost:6379
`,
	)

	spec = spec.Example("CDC firehose without snapshot",
		"Subscribe to every CDC-enabled sObject via the firehose, no historical snapshot:",
		`
input:
  salesforce_cdc:
    org_url: https://acme.my.salesforce.com
    client_id: ${SALESFORCE_CLIENT_ID}
    client_secret: ${SALESFORCE_CLIENT_SECRET}
    topics: [ /data/ChangeEvents ]
    stream_snapshot: false
    replay_preset: latest
    checkpoint_cache: persistent_cache
`,
	)

	spec = spec.Example("Mixed CDC + Platform Events",
		"Combine Account CDC with a custom Platform Event in one pipeline:",
		`
input:
  salesforce_cdc:
    org_url: https://acme.my.salesforce.com
    client_id: ${SALESFORCE_CLIENT_ID}
    client_secret: ${SALESFORCE_CLIENT_SECRET}
    topics:
      - Account
      - /event/Order_Created__e
    stream_snapshot: true
    replay_preset: latest
    checkpoint_cache: persistent_cache
`,
	)

	spec = spec.Example("Platform Events only",
		"Stream a custom event and a standard login stream — snapshot is skipped automatically:",
		`
input:
  salesforce_cdc:
    org_url: https://acme.my.salesforce.com
    client_id: ${SALESFORCE_CLIENT_ID}
    client_secret: ${SALESFORCE_CLIENT_SECRET}
    topics:
      - /event/Order__e
      - /event/LoginEventStream
    replay_preset: latest
    checkpoint_cache: persistent_cache
`,
	)

	return spec
}

// cdcTopicKind classifies a parsed topic entry.
type cdcTopicKind int

const (
	cdcTopicKindCDC cdcTopicKind = iota
	cdcTopicKindCDCFirehose
	cdcTopicKindPE
)

// cdcTopicSpec is one parsed entry from the `topics` list.
type cdcTopicSpec struct {
	Path    string // full Pub/Sub topic path, e.g. /data/AccountChangeEvent
	SObject string // populated for cdcTopicKindCDC; empty otherwise
	Kind    cdcTopicKind
}

// parseCDCTopic resolves a user-supplied topic entry to a cdcTopicSpec.
//
// Accepted shapes:
//   - "Account"                       → /data/AccountChangeEvent (CDC)
//   - "/data/AccountChangeEvent"      → CDC, sObject extracted
//   - "/data/ChangeEvents"            → CDC firehose
//   - "/event/Order__e"               → Platform Event
func parseCDCTopic(entry string) (cdcTopicSpec, error) {
	entry = strings.TrimSpace(entry)
	if entry == "" {
		return cdcTopicSpec{}, errors.New("topic must not be empty")
	}
	if entry == cdcFirehoseTopic {
		return cdcTopicSpec{Path: entry, Kind: cdcTopicKindCDCFirehose}, nil
	}
	if strings.HasPrefix(entry, peEventPrefix) {
		return cdcTopicSpec{Path: entry, Kind: cdcTopicKindPE}, nil
	}
	if body, ok := strings.CutPrefix(entry, cdcDataPrefix); ok {
		if !strings.HasSuffix(body, cdcChangeEventSfx) {
			return cdcTopicSpec{}, fmt.Errorf("invalid CDC topic %q (expected /data/<sObject>ChangeEvent or /data/ChangeEvents)", entry)
		}
		sobj := strings.TrimSuffix(body, cdcChangeEventSfx)
		if sobj == "" {
			return cdcTopicSpec{}, fmt.Errorf("invalid CDC topic %q (missing sObject)", entry)
		}
		return cdcTopicSpec{Path: entry, SObject: sobj, Kind: cdcTopicKindCDC}, nil
	}
	if strings.Contains(entry, "/") {
		return cdcTopicSpec{}, fmt.Errorf("topic %q has unsupported prefix (must be /data/... or /event/...)", entry)
	}
	// Plain sObject shorthand → CDC topic.
	return cdcTopicSpec{
		Path:    cdcDataPrefix + entry + cdcChangeEventSfx,
		SObject: entry,
		Kind:    cdcTopicKindCDC,
	}, nil
}

// CDCInputConfig holds the parsed configuration for the salesforce_cdc input.
type CDCInputConfig struct {
	Auth       AuthConfig
	Checkpoint CheckpointConfig
	GRPC       GRPCConfig
	HTTP       httpclient.Config

	Topics                  []string
	StreamSnapshot          bool
	ReplayPreset            salesforcegrpc.ReplayPreset
	SnapshotMaxBatchSize    int
	StreamBatchSize         int32
	MaxParallelSnapshotObjs int
}

// NewCDCInputConfigFromParsed parses a CDCInputConfig from a benthos parsed config.
func NewCDCInputConfigFromParsed(pConf *service.ParsedConfig) (CDCInputConfig, error) {
	var cfg CDCInputConfig
	var err error

	if cfg.Auth, err = NewAuthConfigFromParsed(pConf); err != nil {
		return cfg, err
	}
	if cfg.Checkpoint, err = NewCheckpointConfigFromParsed(pConf); err != nil {
		return cfg, err
	}
	if cfg.GRPC, err = NewGRPCConfigFromParsed(pConf.Namespace(sfFieldGRPC)); err != nil {
		return cfg, err
	}
	if cfg.HTTP, err = newHTTPConfigFromParsed(cfg.Auth.OrgURL, pConf); err != nil {
		return cfg, err
	}

	if cfg.Topics, err = pConf.FieldStringList(sfciFieldTopics); err != nil {
		return cfg, err
	}
	if len(cfg.Topics) == 0 {
		return cfg, fmt.Errorf("%s must contain at least one topic", sfciFieldTopics)
	}
	seen := make(map[string]struct{}, len(cfg.Topics))
	for _, t := range cfg.Topics {
		if _, dup := seen[t]; dup {
			return cfg, fmt.Errorf("%s contains duplicate entry %q", sfciFieldTopics, t)
		}
		seen[t] = struct{}{}
		if _, err := parseCDCTopic(t); err != nil {
			return cfg, err
		}
	}

	if cfg.StreamSnapshot, err = pConf.FieldBool(sfciFieldStreamSnapshot); err != nil {
		return cfg, err
	}

	rawReplay, err := pConf.FieldString(sfciFieldReplayPreset)
	if err != nil {
		return cfg, err
	}
	switch rawReplay {
	case sfciReplayLatest:
		cfg.ReplayPreset = salesforcegrpc.ReplayPreset_LATEST
	case sfciReplayEarliest:
		cfg.ReplayPreset = salesforcegrpc.ReplayPreset_EARLIEST
	default:
		return cfg, fmt.Errorf("invalid %s %q", sfciFieldReplayPreset, rawReplay)
	}

	if cfg.SnapshotMaxBatchSize, err = pConf.FieldInt(sfciFieldSnapshotMaxBatchSize); err != nil {
		return cfg, err
	}
	streamBatch, err := pConf.FieldInt(sfciFieldStreamBatchSize)
	if err != nil {
		return cfg, err
	}
	cfg.StreamBatchSize = int32(streamBatch)

	if cfg.MaxParallelSnapshotObjs, err = pConf.FieldInt(sfciFieldMaxParallelSnapshotObjs); err != nil {
		return cfg, err
	}
	if cfg.MaxParallelSnapshotObjs < 1 {
		cfg.MaxParallelSnapshotObjs = 1
	}

	return cfg, nil
}

// asyncMessage pairs a batch with its ack function for channel delivery.
type asyncMessage struct {
	msg   service.MessageBatch
	ackFn service.AckFunc
}

// TopicReplays maps each subscribed Pub/Sub topic to its latest replay ID.
type TopicReplays map[string][]byte

// executorState is the JSON shape written to the checkpoint cache.
type executorState struct {
	SnapshotComplete bool                  `json:"snapshot_complete"`
	RestCursor       salesforcehttp.Cursor `json:"rest_cursor,omitzero"`
	Topics           TopicReplays          `json:"topics,omitempty"`
}

// salesforceCDCInput is the registered benthos input. It owns immutable
// configuration and parsed topic specs; per-Connect runtime state lives on
// salesforceCDCInputExecutor (built fresh each Connect).
type salesforceCDCInput struct {
	conf       CDCInputConfig
	topicSpecs []cdcTopicSpec
	batching   service.BatchPolicy
	mgr        *service.Resources
	logger     *service.Logger

	executor atomic.Pointer[salesforceCDCInputExecutor]
}

// salesforceCDCInputExecutor hosts everything Connect builds: the gRPC and
// HTTP clients, the run-loop signaller, the message channel, and the durable
// checkpoint state. Methods on the executor never need their clients passed
// in — they read them from the embedded fields.
type salesforceCDCInputExecutor struct {
	*salesforceCDCInput

	httpClient *salesforcehttp.Client
	grpcClient *salesforcegrpc.Client

	msgChan chan asyncMessage
	stopSig *shutdown.Signaller

	stateMu sync.Mutex
	state   executorState
}

func newSalesforceCDCInput(pConf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
	if err := license.CheckRunningEnterprise(mgr); err != nil {
		return nil, err
	}

	conf, err := NewCDCInputConfigFromParsed(pConf)
	if err != nil {
		return nil, err
	}

	specs := make([]cdcTopicSpec, 0, len(conf.Topics))
	for _, t := range conf.Topics {
		spec, err := parseCDCTopic(t)
		if err != nil {
			return nil, err
		}
		specs = append(specs, spec)
	}

	batching, err := pConf.FieldBatchPolicy(sfFieldBatching)
	if err != nil {
		return nil, err
	}
	if batching.IsNoop() {
		batching.Count = 1
	}

	in := &salesforceCDCInput{
		conf:       conf,
		topicSpecs: specs,
		batching:   batching,
		mgr:        mgr,
		logger:     mgr.Logger(),
	}
	return service.AutoRetryNacksBatchedToggled(pConf, in)
}

// Connect builds the HTTP and gRPC clients, loads any persisted checkpoint,
// and launches the run goroutine. The run goroutine performs the REST snapshot
// (when applicable) and then opens one Pub/Sub subscription per topic on the
// shared gRPC client.
func (s *salesforceCDCInput) Connect(ctx context.Context) error {
	httpDoer, err := httpclient.NewClient(s.conf.HTTP, s.mgr)
	if err != nil {
		return fmt.Errorf("build http client: %w", err)
	}
	httpClient, err := salesforcehttp.NewClient(salesforcehttp.ClientConfig{
		OrgURL:         s.conf.Auth.OrgURL,
		ClientID:       s.conf.Auth.ClientID,
		ClientSecret:   s.conf.Auth.ClientSecret,
		APIVersion:     s.conf.Auth.APIVersion,
		QueryBatchSize: s.conf.SnapshotMaxBatchSize,
		HTTPClient:     httpDoer,
		Logger:         s.logger,
	})
	if err != nil {
		return fmt.Errorf("build salesforce http client: %w", err)
	}
	if err := httpClient.RefreshToken(ctx); err != nil {
		return fmt.Errorf("salesforce auth: %w", err)
	}

	snapObjects, firehose := snapshotSObjects(s.topicSpecs)
	if !firehose && len(snapObjects) > 0 {
		httpClient.SetRestObjects(snapObjects)
	}

	grpcClient, err := salesforcegrpc.NewClient(
		s.logger,
		httpClient.InstanceURL(),
		httpClient.TenantID(),
		httpClient.BearerToken(),
		salesforcegrpc.Config{
			BaseBackoff:  s.conf.GRPC.ReconnectBaseDelay,
			MaxBackoff:   s.conf.GRPC.ReconnectMaxDelay,
			MaxReconnect: s.conf.GRPC.ReconnectMaxAttempts,
			Metrics:      s.mgr.Metrics(),
			OnAuthRefresh: func(refreshCtx context.Context) (string, string, string, error) {
				if err := httpClient.RefreshToken(refreshCtx); err != nil {
					return "", "", "", err
				}
				return httpClient.BearerToken(), httpClient.InstanceURL(), httpClient.TenantID(), nil
			},
		},
	)
	if err != nil {
		return fmt.Errorf("build grpc client: %w", err)
	}

	e := &salesforceCDCInputExecutor{
		salesforceCDCInput: s,
		httpClient:         httpClient,
		grpcClient:         grpcClient,
		msgChan:            make(chan asyncMessage),
		stopSig:            shutdown.NewSignaller(),
	}
	if err := e.loadState(ctx); err != nil {
		_ = grpcClient.Close()
		return fmt.Errorf("load checkpoint state: %w", err)
	}

	prev := s.executor.Swap(e)
	if prev != nil {
		if err := prev.Close(ctx); err != nil {
			s.logger.Errorf("close prior executor: %v", err)
		}
	}

	go e.run()
	return nil
}

// snapshotSObjects derives the sObject list to REST-snapshot from the parsed
// topic specs. A firehose entry returns nil, signalling "snapshot every
// queryable sObject" (no filter). Otherwise the deduplicated list of CDC
// sObjects is returned. If only Platform Event topics are configured, the
// returned list is empty and `firehose` is false — caller should skip
// snapshot entirely.
func snapshotSObjects(specs []cdcTopicSpec) (objects []string, firehose bool) {
	seen := make(map[string]struct{})
	for _, s := range specs {
		switch s.Kind {
		case cdcTopicKindCDCFirehose:
			return nil, true
		case cdcTopicKindCDC:
			if _, ok := seen[s.SObject]; !ok {
				seen[s.SObject] = struct{}{}
				objects = append(objects, s.SObject)
			}
		case cdcTopicKindPE:
			// Platform Events have no REST equivalent — skip in snapshot.
		}
	}
	return objects, false
}

// run executes the snapshot phase (when applicable) followed by parallel
// per-topic streaming subscriptions on the shared gRPC client.
func (e *salesforceCDCInputExecutor) run() {
	defer e.stopSig.TriggerHasStopped()

	ctx, cancel := e.stopSig.SoftStopCtx(context.Background())
	defer cancel()

	cdcCount, peCount, firehose := classifyTopics(e.topicSpecs)
	e.logger.Infof(
		"salesforce_cdc starting (topics=%d cdc=%d platform_events=%d firehose=%t snapshot=%t replay_preset=%v)",
		len(e.topicSpecs), cdcCount, peCount, firehose, e.conf.StreamSnapshot, e.conf.ReplayPreset,
	)

	if err := e.runSnapshotIfNeeded(ctx); err != nil {
		if ctx.Err() == nil {
			e.logger.Errorf("snapshot phase failed: %v", err)
		}
		return
	}

	e.logger.Infof("salesforce_cdc streaming %d topic(s)", len(e.topicSpecs))

	// Streaming phase — one goroutine per topic, all sharing the gRPC client.
	var wg sync.WaitGroup
	wg.Add(len(e.topicSpecs))
	for _, spec := range e.topicSpecs {
		go func(spec cdcTopicSpec) {
			defer wg.Done()
			if err := e.runTopic(ctx, spec); err != nil {
				if ctx.Err() == nil {
					e.logger.Errorf("topic %s: %v", spec.Path, err)
				}
			}
		}(spec)
	}
	wg.Wait()
	e.logger.Info("salesforce_cdc run loop exited")
}

// classifyTopics summarises a topic-spec slice for log output.
func classifyTopics(specs []cdcTopicSpec) (cdc, pe int, firehose bool) {
	for _, s := range specs {
		switch s.Kind {
		case cdcTopicKindCDC:
			cdc++
		case cdcTopicKindCDCFirehose:
			firehose = true
		case cdcTopicKindPE:
			pe++
		}
	}
	return cdc, pe, firehose
}

// runSnapshotIfNeeded runs the REST snapshot phase when configured and not
// already complete. It marks the snapshot complete in the persisted state once
// done so subsequent restarts skip straight to streaming.
//
// Snapshot is meaningful only when at least one CDC topic (or the firehose) is
// in the topic list. Platform-Event-only configs skip it implicitly.
func (e *salesforceCDCInputExecutor) runSnapshotIfNeeded(ctx context.Context) error {
	e.stateMu.Lock()
	ok := e.state.SnapshotComplete
	e.stateMu.Unlock()
	if ok {
		return nil
	}

	_, firehose := snapshotSObjects(e.topicSpecs)
	hasCDCObjects := false
	for _, spec := range e.topicSpecs {
		if spec.Kind == cdcTopicKindCDC {
			hasCDCObjects = true
			break
		}
	}
	if !e.conf.StreamSnapshot || (!firehose && !hasCDCObjects) {
		e.stateMu.Lock()
		e.state.SnapshotComplete = true
		err := e.saveStateLocked(ctx)
		e.stateMu.Unlock()

		if err != nil {
			return fmt.Errorf("persist snapshot completion: %w", err)
		}
		return nil
	}

	batcher, err := e.batching.NewBatcher(e.mgr)
	if err != nil {
		return fmt.Errorf("build snapshot batcher: %w", err)
	}
	defer func() {
		hardCtx, hardCancel := e.stopSig.HardStopCtx(context.Background())
		defer hardCancel()
		if err := batcher.Close(hardCtx); err != nil {
			e.logger.Errorf("close snapshot batcher: %v", err)
		}
	}()
	cp := checkpoint.NewCapped[*executorState](int64(e.conf.Checkpoint.Limit))
	return e.runSnapshot(ctx, batcher, cp)
}

// runSnapshot paginates GetNextBatchParallel until done. Each page is emitted
// as a batch through the snapshot batcher; acks advance the durable cursor in
// the cache.
func (e *salesforceCDCInputExecutor) runSnapshot(
	ctx context.Context,
	batcher *service.Batcher,
	cp *checkpoint.Capped[*executorState],
) error {
	e.stateMu.Lock()
	cursor := e.state.RestCursor
	e.stateMu.Unlock()

	for {
		if e.stopSig.IsSoftStopSignalled() {
			return nil
		}

		rows, nextCursor, done, err := e.httpClient.GetNextBatchParallel(ctx, cursor, e.conf.MaxParallelSnapshotObjs)
		if err != nil {
			return fmt.Errorf("rest snapshot fetch: %w", err)
		}

		for _, msg := range rows {
			msg.MetaSet("operation", "read")
			if batcher.Add(msg) {
				if err := e.flushSnapshot(ctx, batcher, cp, nextCursor, false); err != nil {
					return err
				}
			}
		}
		cursor = nextCursor

		if done {
			return e.flushSnapshot(ctx, batcher, cp, salesforcehttp.Cursor{}, true)
		}
	}
}

// flushSnapshot flushes the snapshot batcher and emits the resulting batch.
func (e *salesforceCDCInputExecutor) flushSnapshot(
	ctx context.Context,
	batcher *service.Batcher,
	cp *checkpoint.Capped[*executorState],
	nextCursor salesforcehttp.Cursor,
	force bool,
) error {
	batch, err := batcher.Flush(ctx)
	if err != nil {
		return fmt.Errorf("flush snapshot batch: %w", err)
	}
	if len(batch) == 0 && !force {
		return nil
	}

	return e.emitSnapshot(ctx, cp, batch, &executorState{
		SnapshotComplete: force,
		RestCursor:       nextCursor,
	})
}

// emitSnapshot tracks a snapshot batch under the Capped checkpoint and forwards
// it on msgChan with an ack that persists the resolved state.
func (e *salesforceCDCInputExecutor) emitSnapshot(
	ctx context.Context,
	cp *checkpoint.Capped[*executorState],
	batch service.MessageBatch,
	snap *executorState,
) error {
	resolveFn, err := cp.Track(ctx, snap, int64(len(batch)))
	if err != nil {
		return fmt.Errorf("track snapshot checkpoint: %w", err)
	}

	ackFn := func(ackCtx context.Context, _ error) error {
		resolved := resolveFn()
		if resolved == nil {
			return nil
		}
		acked := *resolved
		if acked == nil {
			return nil
		}

		e.stateMu.Lock()
		e.state.SnapshotComplete = acked.SnapshotComplete
		e.state.RestCursor = acked.RestCursor
		err := e.saveStateLocked(ackCtx)
		e.stateMu.Unlock()

		if err != nil {
			return fmt.Errorf("persist snapshot checkpoint: %w", err)
		}
		return nil
	}

	if len(batch) == 0 {
		// Force-flush of an empty batch: ack immediately so SnapshotComplete is
		// persisted even when the snapshot finished on a 0-row page.
		return ackFn(ctx, nil)
	}

	select {
	case e.msgChan <- asyncMessage{msg: batch, ackFn: ackFn}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// runTopic owns one Pub/Sub subscription. It opens the stream on the shared
// gRPC client, batches incoming events through its own Batcher, and emits
// batches with topic-scoped ack functions that update only this topic's
// replay-ID slot in the persisted state.
func (e *salesforceCDCInputExecutor) runTopic(ctx context.Context, spec cdcTopicSpec) error {
	batcher, err := e.batching.NewBatcher(e.mgr)
	if err != nil {
		return fmt.Errorf("build batcher: %w", err)
	}
	defer func() {
		hardCtx, hardCancel := e.stopSig.HardStopCtx(context.Background())
		defer hardCancel()
		if err := batcher.Close(hardCtx); err != nil {
			e.logger.Errorf("close batcher for topic %s: %v", spec.Path, err)
		}
	}()

	cp := checkpoint.NewCapped[[]byte](int64(e.conf.Checkpoint.Limit))

	for {
		if e.stopSig.IsSoftStopSignalled() {
			return nil
		}
		if err := e.subscribeAndPump(ctx, spec, batcher, cp); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		// nil return from subscribeAndPump on stale-replay reset → loop and
		// resubscribe with the cleared replay (configured preset takes over).
	}
}

// subscribeAndPump opens a subscription for one topic and pumps events through
// the per-topic batcher. Returns nil for stale-replay reset (caller should
// retry); a non-nil error indicates an unrecoverable failure.
func (e *salesforceCDCInputExecutor) subscribeAndPump(
	ctx context.Context,
	spec cdcTopicSpec,
	batcher *service.Batcher,
	cp *checkpoint.Capped[[]byte],
) error {
	e.stateMu.Lock()
	replayID := e.state.Topics[spec.Path]
	e.stateMu.Unlock()

	sub, err := e.grpcClient.Subscribe(ctx, salesforcegrpc.SubscriptionConfig{
		TopicName:  spec.Path,
		BatchSize:  e.conf.StreamBatchSize,
		BufferSize: e.conf.GRPC.BufferSize,
	}, e.conf.ReplayPreset, replayID)
	if err != nil {
		return fmt.Errorf("subscribe %s: %w", spec.Path, err)
	}
	defer func() { _ = sub.Close() }()

	if err := sub.WaitReady(ctx); err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return fmt.Errorf("subscription %s not ready: %w", spec.Path, err)
	}

	events := sub.Events()
	var nextTimed <-chan time.Time
	var pendingReplayID []byte

	healthTick := time.NewTicker(time.Second)
	defer healthTick.Stop()

	for {
		select {
		case <-e.stopSig.SoftStopChan():
			return nil
		case <-ctx.Done():
			return nil
		case ev := <-events:
			if ev == nil {
				continue
			}
			msg, err := eventToMessage(ev)
			if err != nil {
				e.logger.Errorf("marshal event: %v", err)
				continue
			}
			if len(ev.ReplayID) > 0 {
				pendingReplayID = ev.ReplayID
			}
			if batcher.Add(msg) {
				if err := e.flushTopic(ctx, spec.Path, batcher, cp, pendingReplayID); err != nil {
					return err
				}
				pendingReplayID = nil
				nextTimed = nil
			} else if d, ok := batcher.UntilNext(); ok {
				nextTimed = time.After(d)
			}
		case <-nextTimed:
			nextTimed = nil
			if err := e.flushTopic(ctx, spec.Path, batcher, cp, pendingReplayID); err != nil {
				return err
			}
			pendingReplayID = nil
		case <-healthTick.C:
			if err := sub.StreamErr(); err != nil {
				if e.handleStreamErr(ctx, spec.Path, err) {
					return nil
				}
				return fmt.Errorf("topic %s stream: %w", spec.Path, err)
			}
		}
	}
}

// flushTopic flushes a topic's batcher and emits the batch with an ack
// function that records the latest replay ID for this topic.
func (e *salesforceCDCInputExecutor) flushTopic(
	ctx context.Context,
	topic string,
	batcher *service.Batcher,
	cp *checkpoint.Capped[[]byte],
	latestReplay []byte,
) error {
	batch, err := batcher.Flush(ctx)
	if err != nil {
		return fmt.Errorf("flush batch for %s: %w", topic, err)
	}
	if len(batch) == 0 {
		return nil
	}

	resolveFn, err := cp.Track(ctx, latestReplay, int64(len(batch)))
	if err != nil {
		return fmt.Errorf("track checkpoint for %s: %w", topic, err)
	}

	ackFn := func(ackCtx context.Context, _ error) error {
		resolved := resolveFn()
		if resolved == nil {
			return nil
		}
		acked := *resolved
		if len(acked) == 0 {
			return nil
		}

		e.stateMu.Lock()
		e.state.Topics[topic] = acked
		err := e.saveStateLocked(ackCtx)
		e.stateMu.Unlock()

		if err != nil {
			return fmt.Errorf("persist checkpoint: %w", err)
		}
		return nil
	}

	select {
	case e.msgChan <- asyncMessage{msg: batch, ackFn: ackFn}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// eventToMessage marshals a Pub/Sub event to a Benthos message and attaches
// metadata. CDC and Platform Event metadata fields are both set when their
// underlying values are present.
func eventToMessage(ev *salesforcegrpc.PubSubEvent) (*service.Message, error) {
	payload, err := json.Marshal(ev.RawPayload)
	if err != nil {
		return nil, err
	}
	msg := service.NewMessage(payload)
	setMetaIfNonEmpty(msg, "topic", ev.TopicName)
	if len(ev.ReplayID) > 0 {
		msg.MetaSet("replay_id", hex.EncodeToString(ev.ReplayID))
	}
	setMetaIfNonEmpty(msg, "operation", strings.ToLower(ev.ChangeType))
	setMetaIfNonEmpty(msg, "sobject", ev.EntityName)
	if len(ev.RecordIDs) > 0 {
		msg.MetaSet("record_ids", strings.Join(ev.RecordIDs, ","))
	}
	setMetaIfNonEmpty(msg, "event_uuid", ev.EventUUID)
	return msg, nil
}

func setMetaIfNonEmpty(msg *service.Message, key, value string) {
	if value == "" {
		return
	}
	msg.MetaSet(key, value)
}

// handleStreamErr handles a per-topic permanent stream failure. A stale
// replay_id (codes.InvalidArgument) is recoverable: clear it, persist, and
// return true so the caller resubscribes via the configured preset.
func (e *salesforceCDCInputExecutor) handleStreamErr(ctx context.Context, topic string, err error) bool {
	if grpcErr, ok := status.FromError(err); ok && grpcErr.Code() == codes.InvalidArgument {
		e.logger.Warnf("topic %s replay_id rejected (%s); clearing and reconnecting via configured preset", topic, grpcErr.Message())

		e.stateMu.Lock()
		delete(e.state.Topics, topic)
		saveErr := e.saveStateLocked(ctx)
		e.stateMu.Unlock()

		if saveErr != nil {
			e.logger.Errorf("clear stale replay_id for %s: %v", topic, saveErr)
		}
		return true
	}
	return false
}

// loadState reads the persisted checkpoint from the cache. A missing key is
// not an error — it means first run.
func (e *salesforceCDCInputExecutor) loadState(ctx context.Context) error {
	var (
		raw    []byte
		getErr error
	)
	if err := e.mgr.AccessCache(ctx, e.conf.Checkpoint.Cache, func(cache service.Cache) {
		raw, getErr = cache.Get(ctx, e.conf.Checkpoint.CacheKey)
	}); err != nil {
		return fmt.Errorf("access cache %q: %w", e.conf.Checkpoint.Cache, err)
	}

	e.stateMu.Lock()
	defer e.stateMu.Unlock()
	e.state.Topics = TopicReplays{}

	if getErr != nil {
		if errors.Is(getErr, service.ErrKeyNotFound) {
			return nil
		}
		return fmt.Errorf("read cache key %q: %w", e.conf.Checkpoint.CacheKey, getErr)
	}
	if len(raw) == 0 {
		return nil
	}

	return json.Unmarshal(raw, &e.state)
}

// saveStateLocked writes the in-memory checkpoint state to the cache. When the
// serialized bytes are unchanged it skips the cache round-trip. Callers must
// hold e.mu.
func (e *salesforceCDCInputExecutor) saveStateLocked(ctx context.Context) error {
	b, err := json.Marshal(e.state)
	if err != nil {
		return fmt.Errorf("marshal checkpoint state: %w", err)
	}

	var setErr error
	if err := e.mgr.AccessCache(ctx, e.conf.Checkpoint.Cache, func(cache service.Cache) {
		setErr = cache.Set(ctx, e.conf.Checkpoint.CacheKey, b, nil)
	}); err != nil {
		return fmt.Errorf("access cache %q: %w", e.conf.Checkpoint.Cache, err)
	}
	if setErr != nil {
		return fmt.Errorf("set cache key %q: %w", e.conf.Checkpoint.CacheKey, setErr)
	}

	return nil
}

// ReadBatch delivers the next batch produced by the executor's run goroutine.
// Returns ErrNotConnected when no executor is currently installed (Connect
// has not been called or the input has been closed).
func (s *salesforceCDCInput) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	e := s.executor.Load()
	if e == nil {
		return nil, nil, service.ErrNotConnected
	}
	select {
	case m := <-e.msgChan:
		return m.msg, m.ackFn, nil
	case <-e.stopSig.HasStoppedChan():
		return nil, nil, service.ErrNotConnected
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	}
}

// Close detaches the active executor and shuts it down.
func (s *salesforceCDCInput) Close(ctx context.Context) error {
	e := s.executor.Swap(nil)
	if e == nil {
		return nil
	}
	return e.Close(ctx)
}

// Close triggers a soft stop on the run goroutine, waits for it to drain
// (or ctx to fire), then closes the gRPC client which tears down every
// hosted subscription.
func (e *salesforceCDCInputExecutor) Close(ctx context.Context) error {
	e.stopSig.TriggerSoftStop()
	select {
	case <-ctx.Done():
	case <-e.stopSig.HasStoppedChan():
	}
	if err := e.grpcClient.Close(); err != nil {
		e.logger.Errorf("close grpc client: %v", err)
	}
	return nil
}
