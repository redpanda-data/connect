// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package bigquery

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"cloud.google.com/go/bigquery/storage/managedwriter"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/impersonate"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/license"
)

const (
	bqwaFieldProject             = "project"
	bqwaFieldDataset             = "dataset"
	bqwaFieldTable               = "table"
	bqwaFieldMessageFormat       = "message_format"
	bqwaFieldBatching            = "batching"
	bqwaFieldCredentialsJSON     = "credentials_json"
	bqwaFieldTargetPrincipal     = "target_principal"
	bqwaFieldDelegates           = "delegates"
	bqwaFieldStreamIdleTimeout   = "stream_idle_timeout"
	bqwaFieldStreamSweepInterval = "stream_sweep_interval"
	bqwaFieldEndpoint            = "endpoint"
	bqwaepFieldHTTP              = "http"
	bqwaepFieldGRPC              = "grpc"
)

func init() {
	service.MustRegisterBatchOutput("gcp_bigquery_write_api", bigQueryWriteAPISpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (
			out service.BatchOutput, batchPolicy service.BatchPolicy, maxInFlight int, err error,
		) {
			if maxInFlight, err = conf.FieldMaxInFlight(); err != nil {
				return
			}
			if batchPolicy, err = conf.FieldBatchPolicy(bqwaFieldBatching); err != nil {
				return
			}
			out, err = bigQueryWriteAPIOutputFromConfig(conf, mgr)
			return
		})
}

func bigQueryWriteAPISpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Version("4.90.0").
		Categories("GCP", "Services").
		Summary("Streams data into BigQuery using the Storage Write API.").
		Description(`
Writes messages to a BigQuery table using the Storage Write API.
This provides higher throughput and lower latency than the legacy streaming API or load jobs.

Messages can be formatted as JSON (default) or raw protobuf bytes.
When using JSON format the component automatically fetches the table schema and converts each message to the corresponding proto representation.

WARNING: protojson encodes int64 and uint64 values as strings, bytes as base64-encoded strings, and timestamps as RFC 3339 strings.
JSON messages must follow these conventions (e.g. `+"`"+`"age": "30"`+"`"+`, `+"`"+`"data": "aGVsbG8="`+"`"+`, `+"`"+`"created_at": "2026-01-02T15:04:05Z"`+"`"+`); otherwise the write will fail with an unmarshalling error.

When batching is enabled the table name is resolved from the first message in each batch.
All messages in the same batch are written to that table.

The interpolated table name is sanitized for BigQuery: dots, hyphens, slashes and whitespace are replaced with underscores, non-ASCII-alphanumeric characters are stripped, leading digits are prefixed with `+"`_`"+`, and the result is truncated to 1024 characters.
A name that sanitizes to the empty string is rejected as a permanent error.
`).
		Fields(
			service.NewStringField(bqwaFieldProject).
				Description("The GCP project ID."+
					" If empty, the project is auto-detected from the environment.").
				Default(""),
			service.NewStringField(bqwaFieldDataset).
				Description("The BigQuery dataset ID."),
			service.NewInterpolatedStringField(bqwaFieldTable).
				Description("The BigQuery table ID."+
					" Supports interpolation functions."+
					" When batching, resolved from the first message in each batch."),
			service.NewStringEnumField(bqwaFieldMessageFormat, "json", "protobuf").
				Description("The format of input messages."+
					" Use 'json' to have the component convert JSON to proto automatically."+
					" Use 'protobuf' to supply raw proto-encoded bytes.").
				Default("json"),
			service.NewOutputMaxInFlightField().Default(4),
			service.NewBatchPolicyField(bqwaFieldBatching),
			service.NewStringField(bqwaFieldCredentialsJSON).
				Description("An optional JSON string containing GCP credentials."+
					" If empty, credentials are loaded from the environment.").
				Secret().
				Default(""),
			service.NewStringField(bqwaFieldTargetPrincipal).
				Description("Service account email to impersonate."+
					" When set, the output obtains tokens acting as this service account."+
					" Requires the caller to have roles/iam.serviceAccountTokenCreator on the target.").
				Advanced().
				Default(""),
			service.NewStringListField(bqwaFieldDelegates).
				Description("Optional delegation chain for chained service account impersonation."+
					" Each service account must be granted roles/iam.serviceAccountTokenCreator on the next in the chain.").
				Advanced().
				Default([]any{}),
			service.NewDurationField(bqwaFieldStreamIdleTimeout).
				Description("How long a cached stream can remain unused before being closed."+
					" Relevant when the table field uses interpolation to route to many tables.").
				Advanced().
				Default("5m"),
			service.NewDurationField(bqwaFieldStreamSweepInterval).
				Description("How often to check for idle streams to close.").
				Advanced().
				Default("1m"),
			service.NewObjectField(bqwaFieldEndpoint,
				service.NewStringField(bqwaepFieldHTTP).
					Description("Override the BigQuery HTTP endpoint."+
						" Useful for local emulators.").
					Default(""),
				service.NewStringField(bqwaepFieldGRPC).
					Description("Override the BigQuery Storage gRPC endpoint."+
						" Useful for local emulators.").
					Default(""),
			).
				Description("Optional endpoint overrides for the BigQuery and Storage Write API clients.").
				Advanced(),
		)
}

type bigQueryWriteAPIConfig struct {
	ProjectID           string
	DatasetID           string
	MessageFormat       string
	CredentialsJSON     string
	TargetPrincipal     string
	Delegates           []string
	StreamIdleTimeout   time.Duration
	StreamSweepInterval time.Duration
	EndpointHTTP        string
	EndpointGRPC        string
}

func bigQueryWriteAPIConfigFromParsed(pConf *service.ParsedConfig) (conf bigQueryWriteAPIConfig, err error) {
	if conf.ProjectID, err = pConf.FieldString(bqwaFieldProject); err != nil {
		return
	}
	if conf.ProjectID == "" {
		conf.ProjectID = bigquery.DetectProjectID
	}
	if conf.DatasetID, err = pConf.FieldString(bqwaFieldDataset); err != nil {
		return
	}
	if conf.MessageFormat, err = pConf.FieldString(bqwaFieldMessageFormat); err != nil {
		return
	}
	if conf.CredentialsJSON, err = pConf.FieldString(bqwaFieldCredentialsJSON); err != nil {
		return
	}
	if conf.TargetPrincipal, err = pConf.FieldString(bqwaFieldTargetPrincipal); err != nil {
		return
	}
	if conf.Delegates, err = pConf.FieldStringList(bqwaFieldDelegates); err != nil {
		return
	}
	if len(conf.Delegates) > 0 && conf.TargetPrincipal == "" {
		err = fmt.Errorf("%s requires %s to be set", bqwaFieldDelegates, bqwaFieldTargetPrincipal)
		return
	}
	if conf.StreamIdleTimeout, err = pConf.FieldDuration(bqwaFieldStreamIdleTimeout); err != nil {
		return
	}
	if conf.StreamIdleTimeout <= 0 {
		err = fmt.Errorf("%s must be greater than zero", bqwaFieldStreamIdleTimeout)
		return
	}
	if conf.StreamSweepInterval, err = pConf.FieldDuration(bqwaFieldStreamSweepInterval); err != nil {
		return
	}
	if conf.StreamSweepInterval <= 0 {
		err = fmt.Errorf("%s must be greater than zero", bqwaFieldStreamSweepInterval)
		return
	}
	epConf := pConf.Namespace(bqwaFieldEndpoint)
	if conf.EndpointHTTP, err = epConf.FieldString(bqwaepFieldHTTP); err != nil {
		return
	}
	if conf.EndpointGRPC, err = epConf.FieldString(bqwaepFieldGRPC); err != nil {
		return
	}
	return
}

type bqwaMetrics struct {
	rowsSent                *service.MetricCounter
	rowsFailed              *service.MetricCounter
	batchesSent             *service.MetricCounter
	batchLatency            *service.MetricTimer
	retries                 *service.MetricCounter
	schemaEvolutions        *service.MetricCounter
	schemaEvolutionFailures *service.MetricCounter
}

func newBQWAMetrics(m *service.Metrics) *bqwaMetrics {
	return &bqwaMetrics{
		rowsSent:                m.NewCounter("bigquery_write_api_rows_sent_total"),
		rowsFailed:              m.NewCounter("bigquery_write_api_rows_failed_total"),
		batchesSent:             m.NewCounter("bigquery_write_api_batches_sent_total"),
		batchLatency:            m.NewTimer("bigquery_write_api_batch_latency_ns"),
		retries:                 m.NewCounter("bigquery_write_api_retries_total"),
		schemaEvolutions:        m.NewCounter("bigquery_write_api_schema_evolutions_total"),
		schemaEvolutionFailures: m.NewCounter("bigquery_write_api_schema_evolutions_failures_total"),
	}
}

type bqErrorKind int

const (
	bqErrorTransient bqErrorKind = iota
	bqErrorPermanent
	bqErrorSchemaMismatch
)

// bqError wraps an error from the BigQuery REST or Storage Write APIs together
// with its retryability classification. Callers query intent via IsRetryable /
// IsPermanent / IsSchemaMismatch rather than re-inspecting the underlying
// gRPC status or googleapi.Error.
//
// SCHEMA_MISMATCH_EXTRA_FIELDS is kept distinct from generic permanent errors
// because it can be resolved by adding the missing columns and retrying; every
// other permanent classification routes straight to the DLQ.
type bqError struct {
	kind bqErrorKind
	err  error
}

func (e bqError) Error() string { return e.err.Error() }
func (e bqError) Unwrap() error { return e.err }

func (e bqError) IsRetryable() bool      { return e.kind == bqErrorTransient }
func (e bqError) IsPermanent() bool      { return e.kind == bqErrorPermanent }
func (e bqError) IsSchemaMismatch() bool { return e.kind == bqErrorSchemaMismatch }

// classifyBQError inspects err and returns a bqError carrying its retry
// classification. gRPC permanent codes (InvalidArgument, NotFound,
// PermissionDenied, AlreadyExists, FailedPrecondition, Unimplemented,
// Unauthenticated) and REST 4xx codes (except 408 timeout and 429 throttling)
// are treated as permanent; SCHEMA_MISMATCH_EXTRA_FIELDS surfaces separately;
// everything else falls back to transient so the benthos retry loop gets a
// chance.
func classifyBQError(err error) bqError {
	if st, ok := grpcstatus.FromError(err); ok {
		for _, detail := range st.Details() {
			if storageErr, ok := detail.(*storagepb.StorageError); ok {
				if storageErr.GetCode() == storagepb.StorageError_SCHEMA_MISMATCH_EXTRA_FIELDS {
					return bqError{kind: bqErrorSchemaMismatch, err: err}
				}
			}
		}
		switch st.Code() {
		case codes.InvalidArgument,
			codes.NotFound,
			codes.PermissionDenied,
			codes.AlreadyExists,
			codes.FailedPrecondition,
			codes.Unimplemented,
			codes.Unauthenticated:
			return bqError{kind: bqErrorPermanent, err: err}
		}
		return bqError{kind: bqErrorTransient, err: err}
	}

	var apiErr *googleapi.Error
	if errors.As(err, &apiErr) &&
		apiErr.Code >= 400 && apiErr.Code < 500 &&
		apiErr.Code != http.StatusRequestTimeout &&
		apiErr.Code != http.StatusTooManyRequests {
		return bqError{kind: bqErrorPermanent, err: err}
	}

	return bqError{kind: bqErrorTransient, err: err}
}

type streamWithDescriptor struct {
	stream     *managedwriter.ManagedStream
	descriptor protoreflect.MessageDescriptor
	lastUsed   atomic.Int64 // UnixNano timestamp, safe for concurrent access
}

type bigQueryWriteAPIOutput struct {
	conf        bigQueryWriteAPIConfig
	tableInterp *service.InterpolatedString
	log         *service.Logger
	metrics     *bqwaMetrics
	resolver    *schemaResolver
	evolver     *schemaEvolver

	connMu            sync.RWMutex
	client            *bigquery.Client
	storageClient     *managedwriter.Client
	resolvedProjectID string

	// Lock ordering: connMu must always be acquired before streamsMu to
	// prevent deadlocks. Close() acquires connMu then streamsMu;
	// getOrCreateStream/createStream acquire them independently but never
	// hold streamsMu while calling createStream.
	streamsMu sync.RWMutex
	streams   map[string]*streamWithDescriptor
	stopSweep chan struct{}
	sweepWg   sync.WaitGroup

	// closeWg tracks background ManagedStream.Close goroutines spawned by
	// evictStream and the sweep loop, so Close can wait for them to finish
	// before tearing down the underlying clients.
	closeWg sync.WaitGroup
}

var _ service.BatchOutput = (*bigQueryWriteAPIOutput)(nil)

func bigQueryWriteAPIOutputFromConfig(conf *service.ParsedConfig, mgr *service.Resources) (*bigQueryWriteAPIOutput, error) {
	if err := license.CheckRunningEnterprise(mgr); err != nil {
		return nil, err
	}
	cfg, err := bigQueryWriteAPIConfigFromParsed(conf)
	if err != nil {
		return nil, err
	}
	tableInterp, err := conf.FieldInterpolatedString(bqwaFieldTable)
	if err != nil {
		return nil, err
	}

	return &bigQueryWriteAPIOutput{
		conf:        cfg,
		tableInterp: tableInterp,
		log:         mgr.Logger(),
		metrics:     newBQWAMetrics(mgr.Metrics()),
		streams:     make(map[string]*streamWithDescriptor),
		resolver:    &schemaResolver{log: mgr.Logger()},
		evolver:     &schemaEvolver{log: mgr.Logger()},
	}, nil
}

func (o *bigQueryWriteAPIOutput) Connect(ctx context.Context) error {
	o.connMu.Lock()
	defer o.connMu.Unlock()

	if o.client != nil {
		return nil
	}

	bqOpts, err := o.buildAuthOpts(ctx, o.conf.EndpointHTTP, false)
	if err != nil {
		return err
	}

	storageOpts, err := o.buildAuthOpts(ctx, o.conf.EndpointGRPC, true)
	if err != nil {
		return err
	}

	bqClient, err := bigquery.NewClient(ctx, o.conf.ProjectID, bqOpts...)
	if err != nil {
		return fmt.Errorf("creating bigquery client: %w", err)
	}

	// Resolve the real project ID if auto-detection was used, so that
	// tableCacheKey produces valid resource paths for the Storage Write API.
	resolvedProject := bqClient.Project()
	if resolvedProject == "" {
		_ = bqClient.Close()
		return errors.New("could not determine GCP project ID; set the 'project' field explicitly")
	}

	storageClient, err := managedwriter.NewClient(ctx, resolvedProject, storageOpts...)
	if err != nil {
		_ = bqClient.Close()
		return fmt.Errorf("creating storage write client: %w", err)
	}

	o.resolvedProjectID = resolvedProject
	o.client = bqClient
	o.storageClient = storageClient
	o.stopSweep = make(chan struct{})
	o.sweepWg.Add(1)
	go o.sweepIdleStreams(o.stopSweep)
	return nil
}

func (o *bigQueryWriteAPIOutput) WriteBatch(ctx context.Context, batch service.MessageBatch) error {
	if len(batch) == 0 {
		return nil
	}

	// Snapshot client + resolvedProjectID together under one RLock so a
	// concurrent Connect-after-Close (which rewrites both) cannot tear them.
	o.connMu.RLock()
	client := o.client
	projectID := o.resolvedProjectID
	o.connMu.RUnlock()

	if client == nil {
		return service.ErrNotConnected
	}

	start := time.Now()

	rawTableID, err := batch.TryInterpolatedString(0, o.tableInterp)
	if err != nil {
		return fmt.Errorf("interpolating table name: %w", err)
	}
	tableID := sanitizeTableName(rawTableID)
	if tableID == "" {
		// Permanent: the interpolation result has no usable characters.
		return permanentBatchError(batch, fmt.Errorf("interpolated table name %q is empty after sanitization", rawTableID))
	}

	swd, cacheKey, err := o.getOrCreateStream(ctx, client, projectID, tableID)
	if err != nil {
		if classifyBQError(err).IsPermanent() {
			return permanentBatchError(batch, fmt.Errorf("getting stream for table %q: %w", tableID, err))
		}
		return fmt.Errorf("getting stream for table %q: %w", tableID, err)
	}

	rows := make([][]byte, 0, len(batch))
	for i, msg := range batch {
		msgBytes, err := msg.AsBytes()
		if err != nil {
			return fmt.Errorf("reading message %d: %w", i, err)
		}

		var protoBytes []byte
		switch o.conf.MessageFormat {
		case "json":
			protoBytes, err = jsonToProtoBytes(msgBytes, swd.descriptor)
			if err != nil {
				return fmt.Errorf("converting message %d from JSON to proto: %w", i, err)
			}
		case "protobuf":
			protoBytes = msgBytes
		default:
			// Safety net: the config spec's enum validation rejects any value
			// other than "json" or "protobuf" before WriteBatch is called.
			return fmt.Errorf("unsupported message format: %s", o.conf.MessageFormat)
		}
		rows = append(rows, protoBytes)
	}

	result, err := swd.stream.AppendRows(ctx, rows)
	if err != nil {
		o.evictStream(cacheKey)
		return o.handleWriteError(ctx, client, err, batch, tableID, swd.descriptor, "appending rows")
	}

	resp, err := result.FullResponse(ctx)
	if err != nil {
		o.evictStream(cacheKey)
		return o.handleWriteError(ctx, client, err, batch, tableID, swd.descriptor, "waiting for append result")
	}

	o.metrics.batchLatency.Timing(time.Since(start).Nanoseconds())

	if resp.GetUpdatedSchema() != nil {
		o.log.Infof("BigQuery reported schema update for table %q, evicting cached stream", tableID)
		o.evictStream(cacheKey)
		o.resolver.Evict(tableID)
	}

	if rowErrs := resp.GetRowErrors(); len(rowErrs) > 0 {
		o.metrics.rowsFailed.Incr(int64(len(rowErrs)))
		o.metrics.rowsSent.Incr(int64(len(batch) - len(rowErrs)))
		batchErr := service.NewBatchError(batch, errors.New("row errors from BigQuery"))
		for _, re := range rowErrs {
			idx := int(re.GetIndex())
			if idx < len(batch) {
				batchErr = batchErr.Failed(idx, fmt.Errorf("row %d: code %d: %s", idx, re.GetCode(), re.GetMessage()))
			}
		}
		return batchErr
	}

	o.metrics.batchesSent.Incr(1)
	o.metrics.rowsSent.Incr(int64(len(batch)))
	return nil
}

// permanentBatchError builds a service.BatchError that fails every message in
// the batch with the same root cause. Use this for errors that benthos must
// not retry.
func permanentBatchError(batch service.MessageBatch, err error) error {
	batchErr := service.NewBatchError(batch, err)
	for i := range batch {
		batchErr = batchErr.Failed(i, err)
	}
	return batchErr
}

// handleWriteError classifies a gRPC error from an append or response and
// returns the appropriate error for benthos: a BatchError (permanent, no retry)
// or a plain error (transient, retry).
func (o *bigQueryWriteAPIOutput) handleWriteError(
	ctx context.Context, client *bigquery.Client, err error, batch service.MessageBatch,
	tableID string, descriptor protoreflect.MessageDescriptor, phase string,
) error {
	bqErr := classifyBQError(err)
	switch {
	case bqErr.IsSchemaMismatch():
		// Evolve returns (true, nil) for every outcome that warrants a retry —
		// columns we added, columns another writer added, or columns already
		// present when we read metadata. Any (false, ...) result carries a
		// non-nil error.
		if _, evolveErr := o.evolver.Evolve(ctx, client, o.conf.DatasetID, tableID, descriptor); evolveErr != nil {
			o.metrics.schemaEvolutionFailures.Incr(1)
			o.log.Warnf("Schema evolution failed for table %q: %v", tableID, evolveErr)
			return permanentBatchError(batch, fmt.Errorf("schema evolution failed for table %q: %w", tableID, evolveErr))
		}
		o.metrics.schemaEvolutions.Incr(1)
		o.resolver.Evict(tableID)
		o.metrics.retries.Incr(1)
		return fmt.Errorf("schema mismatch (evolution attempted): %w", err)
	case bqErr.IsPermanent():
		return permanentBatchError(batch, fmt.Errorf("permanent error %s: %w", phase, err))
	default:
		o.metrics.retries.Incr(1)
		return fmt.Errorf("%s: %w", phase, err)
	}
}

func (o *bigQueryWriteAPIOutput) Close(ctx context.Context) error {
	o.connMu.Lock()
	defer o.connMu.Unlock()

	if o.stopSweep != nil {
		close(o.stopSweep)
		o.stopSweep = nil
	}

	// Wait for the sweep goroutine to finish before closing streams/clients
	// so it does not access shared state after shutdown.
	o.sweepWg.Wait()

	// Wait for any in-flight async stream closes (from evictStream or
	// sweepIdleStreams) so they don't race with client shutdown.
	o.closeWg.Wait()

	o.streamsMu.Lock()
	streams := o.streams
	o.streams = make(map[string]*streamWithDescriptor)
	o.streamsMu.Unlock()

	// Close every stream and client unconditionally: the underlying gRPC
	// teardowns are non-blocking and skipping them leaks connections and
	// goroutines. ctx.Err() is appended afterwards so a caller-side deadline
	// is still surfaced, but it never gates the cleanup itself.
	var errs []error
	for _, swd := range streams {
		if err := swd.stream.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	if o.storageClient != nil {
		if err := o.storageClient.Close(); err != nil {
			errs = append(errs, err)
		}
		o.storageClient = nil
	}

	if o.client != nil {
		if err := o.client.Close(); err != nil {
			errs = append(errs, err)
		}
		o.client = nil
	}

	if err := ctx.Err(); err != nil {
		errs = append(errs, err)
	}

	// Drop the resolver's schema cache so a subsequent Connect doesn't reuse
	// state tied to the now-closed client.
	o.resolver.cache.Clear()

	return errors.Join(errs...)
}

// buildAuthOpts returns client options for authentication based on the config.
// If an endpoint override is provided, authentication is skipped (emulator mode).
func (o *bigQueryWriteAPIOutput) buildAuthOpts(ctx context.Context, endpointOverride string, isGRPC bool) ([]option.ClientOption, error) {
	if endpointOverride != "" {
		o.log.Warnf("endpoint override %q is set; authentication is disabled — do not use this against production BigQuery", endpointOverride)
		if o.conf.TargetPrincipal != "" {
			o.log.Warnf("endpoint override is set; ignoring target_principal %q", o.conf.TargetPrincipal)
		}
		opts := []option.ClientOption{
			option.WithoutAuthentication(),
			option.WithEndpoint(endpointOverride),
		}
		if isGRPC {
			opts = append(opts, option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())))
		}
		return opts, nil
	}

	var opts []option.ClientOption

	if o.conf.TargetPrincipal != "" {
		var baseOpts []option.ClientOption
		if o.conf.CredentialsJSON != "" {
			baseOpts = append(baseOpts, option.WithCredentialsJSON([]byte(o.conf.CredentialsJSON)))
		}

		// Detach from ctx: the token source outlives Connect (it refreshes
		// tokens on demand for the lifetime of the client). If we passed
		// Connect's ctx and that ctx was later cancelled, every refresh
		// would fail.
		ts, err := impersonate.CredentialsTokenSource(context.WithoutCancel(ctx), impersonate.CredentialsConfig{
			TargetPrincipal: o.conf.TargetPrincipal,
			Scopes:          []string{bigquery.Scope},
			Delegates:       o.conf.Delegates,
		}, baseOpts...)
		if err != nil {
			return nil, fmt.Errorf("creating impersonated credentials for %q: %w", o.conf.TargetPrincipal, err)
		}
		opts = append(opts, option.WithTokenSource(ts))
		return opts, nil
	}

	if o.conf.CredentialsJSON != "" {
		opts = append(opts, option.WithCredentialsJSON([]byte(o.conf.CredentialsJSON)))
	}

	return opts, nil
}

// tableCacheKey builds the BigQuery resource path for the given table. The
// projectID argument is captured by callers under connMu.RLock and passed
// through, so this function must not read o.resolvedProjectID directly.
func (o *bigQueryWriteAPIOutput) tableCacheKey(projectID, tableID string) string {
	return fmt.Sprintf("projects/%s/datasets/%s/tables/%s", projectID, o.conf.DatasetID, tableID)
}

// closeStreamAsync schedules a stream close on a background goroutine tracked
// by closeWg, so Close() can wait for it to finish before tearing down the
// underlying clients.
func (o *bigQueryWriteAPIOutput) closeStreamAsync(s *managedwriter.ManagedStream) {
	if s == nil {
		return
	}
	o.closeWg.Go(func() { _ = s.Close() })
}

func (o *bigQueryWriteAPIOutput) getOrCreateStream(ctx context.Context, client *bigquery.Client, projectID, tableID string) (*streamWithDescriptor, string, error) {
	cacheKey := o.tableCacheKey(projectID, tableID)

	now := time.Now()

	// Fast path: check cache under read lock.
	o.streamsMu.RLock()
	if cached, exists := o.streams[cacheKey]; exists {
		cached.lastUsed.Store(now.UnixNano())
		o.streamsMu.RUnlock()
		return cached, cacheKey, nil
	}
	o.streamsMu.RUnlock()

	// Slow path: create stream without holding the lock (network I/O).
	swd, err := o.createStream(ctx, client, cacheKey, tableID)
	if err != nil {
		return nil, cacheKey, err
	}

	// Store in cache, but another goroutine may have raced us.
	o.streamsMu.Lock()
	if cached, exists := o.streams[cacheKey]; exists {
		o.streamsMu.Unlock()
		o.closeStreamAsync(swd.stream)
		return cached, cacheKey, nil
	}
	swd.lastUsed.Store(now.UnixNano())
	o.streams[cacheKey] = swd
	o.streamsMu.Unlock()
	return swd, cacheKey, nil
}

// evictStream removes a stream from the cache and closes it on a tracked
// background goroutine. Concurrent WriteBatch goroutines that already hold a
// reference to the evicted stream will see errors from the closed stream and
// retry, which will create a fresh stream via getOrCreateStream.
func (o *bigQueryWriteAPIOutput) evictStream(cacheKey string) {
	o.streamsMu.Lock()
	swd, exists := o.streams[cacheKey]
	delete(o.streams, cacheKey)
	o.streamsMu.Unlock()

	if exists {
		o.closeStreamAsync(swd.stream)
	}
}

// sweepIdleStreams periodically evicts streams that haven't been used within
// the configured idle timeout. This prevents unbounded growth of the stream cache when
// the table field uses interpolation and routes to many distinct tables.
func (o *bigQueryWriteAPIOutput) sweepIdleStreams(stop <-chan struct{}) {
	defer o.sweepWg.Done()

	ticker := time.NewTicker(o.conf.StreamSweepInterval)
	defer ticker.Stop()

	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
		}

		now := time.Now()
		type evicted struct {
			key string
			swd *streamWithDescriptor
		}
		var toClose []evicted

		o.streamsMu.Lock()
		for key, swd := range o.streams {
			lastUsed := time.Unix(0, swd.lastUsed.Load())
			if now.Sub(lastUsed) > o.conf.StreamIdleTimeout {
				toClose = append(toClose, evicted{key, swd})
				delete(o.streams, key)
			}
		}
		o.streamsMu.Unlock()

		for _, e := range toClose {
			o.log.Debugf("Closing idle BigQuery stream for %s", e.key)
			o.closeStreamAsync(e.swd.stream)
		}
	}
}

func (o *bigQueryWriteAPIOutput) createStream(ctx context.Context, client *bigquery.Client, cacheKey, tableID string) (*streamWithDescriptor, error) {
	o.connMu.RLock()
	storageClient := o.storageClient
	o.connMu.RUnlock()

	if storageClient == nil {
		return nil, service.ErrNotConnected
	}

	rs, err := o.resolver.Resolve(ctx, client, o.conf.DatasetID, tableID)
	if err != nil {
		return nil, err
	}

	// Detach from the per-batch ctx: the cached stream outlives this WriteBatch
	// and is reused by every subsequent batch routing to the same table. If the
	// stream were bound to this ctx, cancellation of the first batch (per-message
	// deadline, source shutdown, ack timeout) would block all later AppendRows
	// against the cached stream until the idle sweeper evicted it.
	ms, err := storageClient.NewManagedStream(context.WithoutCancel(ctx),
		managedwriter.WithDestinationTable(cacheKey),
		managedwriter.WithType(managedwriter.DefaultStream),
		managedwriter.WithSchemaDescriptor(rs.descriptorProto),
	)
	if err != nil {
		return nil, fmt.Errorf("creating managed stream for %q: %w", cacheKey, err)
	}

	return &streamWithDescriptor{
		stream:     ms,
		descriptor: rs.messageDescriptor,
	}, nil
}

// descriptorProtoToMessageDescriptor converts a *descriptorpb.DescriptorProto
// into a protoreflect.MessageDescriptor by wrapping it in a synthetic
// FileDescriptorProto and resolving it via protodesc.
func descriptorProtoToMessageDescriptor(dp *descriptorpb.DescriptorProto) (protoreflect.MessageDescriptor, error) {
	fdp := &descriptorpb.FileDescriptorProto{
		Name:        new("synthetic.proto"),
		Syntax:      new("proto2"),
		MessageType: []*descriptorpb.DescriptorProto{dp},
	}
	fd, err := protodesc.NewFile(fdp, nil)
	if err != nil {
		return nil, fmt.Errorf("creating file descriptor from normalized proto: %w", err)
	}
	if fd.Messages().Len() == 0 {
		return nil, errors.New("normalized descriptor produced no messages")
	}
	return fd.Messages().Get(0), nil
}

func jsonToProtoBytes(jsonData []byte, descriptor protoreflect.MessageDescriptor) ([]byte, error) {
	msg := dynamicpb.NewMessage(descriptor)
	if err := protojson.Unmarshal(jsonData, msg); err != nil {
		return nil, fmt.Errorf("unmarshalling JSON into proto message: %w", err)
	}
	return proto.Marshal(msg)
}
