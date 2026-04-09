// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package enterprise

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/bigquery/storage/managedwriter"
	"cloud.google.com/go/bigquery/storage/managedwriter/adapt"
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
	bqwaFieldCredentialsJSON     = "credentials_json"
	bqwaFieldEndpoint            = "endpoint"
	bqwaFieldEndpointHTTP        = "http"
	bqwaFieldEndpointGRPC        = "grpc"
	bqwaFieldBatching            = "batching"
	bqwaFieldTargetPrincipal     = "target_principal"
	bqwaFieldDelegates           = "delegates"
	bqwaFieldStreamIdleTimeout   = "stream_idle_timeout"
	bqwaFieldStreamSweepInterval = "stream_sweep_interval"
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
		Version("4.87.0").
		Categories("GCP", "Services").
		Summary("Streams data into BigQuery using the Storage Write API.").
		Description(`
Writes messages to a BigQuery table using the Storage Write API.
This provides higher throughput and lower latency than the legacy streaming API or load jobs.

Messages can be formatted as JSON (default) or raw protobuf bytes.
When using JSON format the component automatically fetches the table schema and converts each message to the corresponding proto representation.

WARNING: The proto3 JSON mapping encodes int64 and uint64 values as strings.
JSON messages with integer fields must use string values (e.g. `+"`"+`"age": "30"`+"`"+` not `+"`"+`"age": 30`+"`"+`).
Otherwise the write will fail with an unmarshalling error.

When batching is enabled the table name is resolved from the first message in each batch.
All messages in the same batch are written to that table.
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
			service.NewOutputMaxInFlightField().Default(64),
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
			service.NewStringField(bqwaFieldStreamIdleTimeout).
				Description("How long a cached stream can remain unused before being closed."+
					" Relevant when the table field uses interpolation to route to many tables.").
				Advanced().
				Default("5m"),
			service.NewStringField(bqwaFieldStreamSweepInterval).
				Description("How often to check for idle streams to close.").
				Advanced().
				Default("1m"),
			service.NewObjectField(bqwaFieldEndpoint,
				service.NewStringField(bqwaFieldEndpointHTTP).
					Description("Override the BigQuery HTTP endpoint."+
						" Useful for local emulators.").
					Default(""),
				service.NewStringField(bqwaFieldEndpointGRPC).
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
	var idleTimeoutStr, sweepIntervalStr string
	if idleTimeoutStr, err = pConf.FieldString(bqwaFieldStreamIdleTimeout); err != nil {
		return
	}
	if conf.StreamIdleTimeout, err = time.ParseDuration(idleTimeoutStr); err != nil {
		err = fmt.Errorf("invalid %s: %w", bqwaFieldStreamIdleTimeout, err)
		return
	}
	if sweepIntervalStr, err = pConf.FieldString(bqwaFieldStreamSweepInterval); err != nil {
		return
	}
	if conf.StreamSweepInterval, err = time.ParseDuration(sweepIntervalStr); err != nil {
		err = fmt.Errorf("invalid %s: %w", bqwaFieldStreamSweepInterval, err)
		return
	}
	epConf := pConf.Namespace(bqwaFieldEndpoint)
	if conf.EndpointHTTP, err = epConf.FieldString(bqwaFieldEndpointHTTP); err != nil {
		return
	}
	if conf.EndpointGRPC, err = epConf.FieldString(bqwaFieldEndpointGRPC); err != nil {
		return
	}
	return
}

type bqwaMetrics struct {
	rowsSent     *service.MetricCounter
	rowsFailed   *service.MetricCounter
	batchesSent  *service.MetricCounter
	batchLatency *service.MetricTimer
	retries      *service.MetricCounter
}

func newBQWAMetrics(m *service.Metrics) *bqwaMetrics {
	return &bqwaMetrics{
		rowsSent:     m.NewCounter("bigquery_write_api_rows_sent"),
		rowsFailed:   m.NewCounter("bigquery_write_api_rows_failed"),
		batchesSent:  m.NewCounter("bigquery_write_api_batches_sent"),
		batchLatency: m.NewTimer("bigquery_write_api_batch_latency_ns"),
		retries:      m.NewCounter("bigquery_write_api_retries"),
	}
}

type grpcErrorKind int

const (
	grpcTransient grpcErrorKind = iota
	grpcPermanent
)

// classifyGRPCError inspects a gRPC status code to determine if the error is
// transient (worth retrying) or permanent (will never succeed).
// Non-gRPC errors are treated as transient to be safe.
func classifyGRPCError(err error) grpcErrorKind {
	st, ok := grpcstatus.FromError(err)
	if !ok {
		// Try unwrapping, since errors are often wrapped with fmt.Errorf.
		unwrapped := err
		for unwrapped != nil {
			if st, ok = grpcstatus.FromError(unwrapped); ok {
				break
			}
			unwrapped = errors.Unwrap(unwrapped)
		}
		if !ok {
			return grpcTransient
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
		return grpcPermanent
	default:
		return grpcTransient
	}
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

	connMu        sync.RWMutex
	client        *bigquery.Client
	storageClient *managedwriter.Client

	// Lock ordering: connMu must always be acquired before streamsMu to
	// prevent deadlocks. Close() acquires connMu then streamsMu;
	// getOrCreateStream/createStream acquire them independently but never
	// hold streamsMu while calling createStream.
	streamsMu sync.RWMutex
	streams   map[string]*streamWithDescriptor
	stopSweep chan struct{}
	sweepWg   sync.WaitGroup
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

	o.conf.ProjectID = resolvedProject
	o.client = bqClient
	o.storageClient = storageClient
	o.stopSweep = make(chan struct{})
	o.sweepWg.Add(1)
	go o.sweepIdleStreams()
	return nil
}

func (o *bigQueryWriteAPIOutput) WriteBatch(ctx context.Context, batch service.MessageBatch) error {
	if len(batch) == 0 {
		return nil
	}

	o.connMu.RLock()
	client := o.client
	o.connMu.RUnlock()

	if client == nil {
		return service.ErrNotConnected
	}

	start := time.Now()

	tableID, err := batch.TryInterpolatedString(0, o.tableInterp)
	if err != nil {
		return fmt.Errorf("interpolating table name: %w", err)
	}

	swd, cacheKey, err := o.getOrCreateStream(ctx, tableID)
	if err != nil {
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
		if classifyGRPCError(err) == grpcPermanent {
			return fmt.Errorf("permanent error appending rows (will not retry): %w", err)
		}
		o.metrics.retries.Incr(1)
		return fmt.Errorf("appending rows: %w", err)
	}

	resp, err := result.FullResponse(ctx)
	if err != nil {
		o.evictStream(cacheKey)
		if classifyGRPCError(err) == grpcPermanent {
			return fmt.Errorf("permanent error waiting for append result (will not retry): %w", err)
		}
		o.metrics.retries.Incr(1)
		return fmt.Errorf("waiting for append result: %w", err)
	}

	o.metrics.batchLatency.Timing(time.Since(start).Nanoseconds())

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

func (o *bigQueryWriteAPIOutput) Close(_ context.Context) error {
	o.connMu.Lock()
	defer o.connMu.Unlock()

	if o.stopSweep != nil {
		close(o.stopSweep)
		o.stopSweep = nil
	}

	// Wait for the sweep goroutine to finish before closing streams/clients
	// so it does not access shared state after shutdown.
	o.sweepWg.Wait()

	o.streamsMu.Lock()
	streams := o.streams
	o.streams = make(map[string]*streamWithDescriptor)
	o.streamsMu.Unlock()

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

	return errors.Join(errs...)
}

// buildAuthOpts returns client options for authentication based on the config.
// If an endpoint override is provided, authentication is skipped (emulator mode).
func (o *bigQueryWriteAPIOutput) buildAuthOpts(ctx context.Context, endpointOverride string, isGRPC bool) ([]option.ClientOption, error) {
	if endpointOverride != "" {
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

		ts, err := impersonate.CredentialsTokenSource(ctx, impersonate.CredentialsConfig{
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

func (o *bigQueryWriteAPIOutput) tableCacheKey(tableID string) string {
	return fmt.Sprintf("projects/%s/datasets/%s/tables/%s", o.conf.ProjectID, o.conf.DatasetID, tableID)
}

func (o *bigQueryWriteAPIOutput) getOrCreateStream(ctx context.Context, tableID string) (*streamWithDescriptor, string, error) {
	cacheKey := o.tableCacheKey(tableID)

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
	swd, err := o.createStream(ctx, cacheKey, tableID)
	if err != nil {
		return nil, cacheKey, err
	}

	// Store in cache, but another goroutine may have raced us.
	o.streamsMu.Lock()
	if cached, exists := o.streams[cacheKey]; exists {
		o.streamsMu.Unlock()
		go func() { _ = swd.stream.Close() }()
		return cached, cacheKey, nil
	}
	swd.lastUsed.Store(now.UnixNano())
	o.streams[cacheKey] = swd
	o.streamsMu.Unlock()
	return swd, cacheKey, nil
}

// evictStream removes a stream from the cache and closes it asynchronously.
// Concurrent WriteBatch goroutines that already hold a reference to the evicted
// stream will see errors from the closed stream and retry, which will create a
// fresh stream via getOrCreateStream.
func (o *bigQueryWriteAPIOutput) evictStream(cacheKey string) {
	o.streamsMu.Lock()
	swd, exists := o.streams[cacheKey]
	delete(o.streams, cacheKey)
	o.streamsMu.Unlock()

	if exists {
		go func() { _ = swd.stream.Close() }()
	}
}

// sweepIdleStreams periodically evicts streams that haven't been used within
// the configured idle timeout. This prevents unbounded growth of the stream cache when
// the table field uses interpolation and routes to many distinct tables.
func (o *bigQueryWriteAPIOutput) sweepIdleStreams() {
	defer o.sweepWg.Done()

	ticker := time.NewTicker(o.conf.StreamSweepInterval)
	defer ticker.Stop()

	for {
		select {
		case <-o.stopSweep:
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
			go func() { _ = e.swd.stream.Close() }()
		}
	}
}

func (o *bigQueryWriteAPIOutput) createStream(ctx context.Context, cacheKey, tableID string) (*streamWithDescriptor, error) {
	o.connMu.RLock()
	client := o.client
	o.connMu.RUnlock()

	if client == nil {
		return nil, service.ErrNotConnected
	}

	tableMeta, err := client.Dataset(o.conf.DatasetID).Table(tableID).Metadata(ctx)
	if err != nil {
		return nil, fmt.Errorf("fetching metadata for table %q: %w", tableID, err)
	}

	tableSchema, err := adapt.BQSchemaToStorageTableSchema(tableMeta.Schema)
	if err != nil {
		return nil, fmt.Errorf("converting BQ schema to storage schema: %w", err)
	}

	descriptor, err := adapt.StorageSchemaToProto2Descriptor(tableSchema, "root")
	if err != nil {
		return nil, fmt.Errorf("converting storage schema to proto descriptor: %w", err)
	}

	messageDescriptor, ok := descriptor.(protoreflect.MessageDescriptor)
	if !ok {
		return nil, errors.New("schema descriptor is not a MessageDescriptor")
	}

	normalizedDescriptor, err := adapt.NormalizeDescriptor(messageDescriptor)
	if err != nil {
		return nil, fmt.Errorf("normalizing proto descriptor: %w", err)
	}

	// Convert the normalized DescriptorProto back to a protoreflect.MessageDescriptor
	// so that jsonToProtoBytes uses the same schema the stream was configured with.
	normalizedMsgDesc, err := descriptorProtoToMessageDescriptor(normalizedDescriptor)
	if err != nil {
		return nil, fmt.Errorf("resolving normalized descriptor: %w", err)
	}

	o.connMu.RLock()
	storageClient := o.storageClient
	o.connMu.RUnlock()

	if storageClient == nil {
		return nil, service.ErrNotConnected
	}

	stream, err := storageClient.NewManagedStream(ctx,
		managedwriter.WithDestinationTable(cacheKey),
		managedwriter.WithType(managedwriter.DefaultStream),
		managedwriter.WithSchemaDescriptor(normalizedDescriptor),
	)
	if err != nil {
		return nil, fmt.Errorf("creating managed stream for %q: %w", cacheKey, err)
	}

	return &streamWithDescriptor{
		stream:     stream,
		descriptor: normalizedMsgDesc,
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
