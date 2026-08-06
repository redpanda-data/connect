// Copyright 2025 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package enterprise

import (
	"context"
	"encoding/base64"
	"fmt"
	"sync"
	"time"

	"github.com/Jeffail/shutdown"
	"google.golang.org/api/option"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/ack"

	"github.com/redpanda-data/connect/v4/internal/impl/gcp/enterprise/changestreams"
	"github.com/redpanda-data/connect/v4/internal/license"
)

// Spanner Input Fields
const (
	siFieldCredentialsJSON      = "credentials_json"
	siFieldProjectID            = "project_id"
	siFieldInstanceID           = "instance_id"
	siFieldDatabaseID           = "database_id"
	siFieldStreamID             = "stream_id"
	siFieldStartTimestamp       = "start_timestamp"
	siFieldEndTimestamp         = "end_timestamp"
	siFieldHeartbeatInterval    = "heartbeat_interval"
	siFieldMetadataTable        = "metadata_table"
	siFieldMinWatermarkCacheTTL = "min_watermark_cache_ttl"
	siFieldAllowedModTypes      = "allowed_mod_types"
	siFieldBatchPolicy          = "batching"
)

// Default values
const (
	defaultMetadataTableFormat = "cdc_metadata_%s"
	shutdownTimeout            = 5 * time.Second
)

type spannerCDCInputConfig struct {
	changestreams.Config
}

func parseRFC3339Nano(pConf *service.ParsedConfig, key string) (time.Time, error) {
	s, err := pConf.FieldString(key)
	if err != nil {
		return time.Time{}, err
	}
	if s == "" {
		return time.Time{}, nil
	}

	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Time{}, fmt.Errorf("parsing %v as RFC3339Nano: %w", key, err)
	}
	return t, nil
}

func spannerCDCInputConfigFromParsed(pConf *service.ParsedConfig) (conf spannerCDCInputConfig, err error) {
	credentialsJSON, err := pConf.FieldString(siFieldCredentialsJSON)
	if err != nil {
		return
	}
	if credentialsJSON != "" {
		credBytes, err := base64.StdEncoding.DecodeString(credentialsJSON)
		if err != nil {
			return conf, fmt.Errorf("decode base64 credentials: %w", err)
		}
		conf.SpannerClientOptions = append(conf.SpannerClientOptions,
			option.WithCredentialsJSON(credBytes))
	}

	if conf.ProjectID, err = pConf.FieldString(siFieldProjectID); err != nil {
		return
	}
	if conf.InstanceID, err = pConf.FieldString(siFieldInstanceID); err != nil {
		return
	}
	if conf.DatabaseID, err = pConf.FieldString(siFieldDatabaseID); err != nil {
		return
	}
	if conf.StreamID, err = pConf.FieldString(siFieldStreamID); err != nil {
		return
	}
	if conf.StartTimestamp, err = parseRFC3339Nano(pConf, siFieldStartTimestamp); err != nil {
		return
	}
	if conf.EndTimestamp, err = parseRFC3339Nano(pConf, siFieldEndTimestamp); err != nil {
		return
	}
	if conf.HeartbeatInterval, err = pConf.FieldDuration(siFieldHeartbeatInterval); err != nil {
		return
	}
	if conf.MetadataTable, err = pConf.FieldString(siFieldMetadataTable); err != nil {
		return
	}
	if conf.MetadataTable == "" {
		conf.MetadataTable = fmt.Sprintf(defaultMetadataTableFormat, conf.StreamID)
	}
	if pConf.Contains(siFieldAllowedModTypes) {
		if conf.AllowedModTypes, err = pConf.FieldStringList(siFieldAllowedModTypes); err != nil {
			return
		}
	}
	if conf.MinWatermarkCacheTTL, err = pConf.FieldDuration(siFieldMinWatermarkCacheTTL); err != nil {
		return
	}

	return
}

func spannerCDCInputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Version("4.56.0").
		Categories("Services", "GCP").
		Summary("Creates an input that consumes from a spanner change stream.").
		Description(`
Consumes change records from a Google Cloud Spanner change stream. This input allows
you to track and process database changes in real-time, making it useful for data
replication, event-driven architectures, and maintaining derived data stores.

The input reads from a specified change stream within a Spanner database and converts
each change record into a message. The message payload contains the change records in
JSON format, and metadata is added with details about the Spanner instance, database,
and stream.

Change streams provide a way to track mutations to your Spanner database tables. For
more information about Spanner change streams, refer to the Google Cloud documentation:
https://cloud.google.com/spanner/docs/change-streams
`).
		Field(service.NewStringField(siFieldCredentialsJSON).Optional().Description("Base64 encoded GCP service account JSON credentials file for authentication. If not provided, Application Default Credentials (ADC) will be used.").
			ShortDescription("Base64 encoded GCP service account JSON credentials. Application Default Credentials are used if unset.").Default("")).
		Field(service.NewStringField(siFieldProjectID).Description("GCP project ID containing the Spanner instance")).
		Field(service.NewStringField(siFieldInstanceID).Description("Spanner instance ID")).
		Field(service.NewStringField(siFieldDatabaseID).Description("Spanner database ID")).
		Field(service.NewStringField(siFieldStreamID).Description("The name of the change stream to track, the stream must exist in the database. To create a change stream, see https://cloud.google.com/spanner/docs/change-streams/manage.").
			ShortDescription("The name of the change stream to track. The stream must already exist in the database.")).
		Field(service.NewStringField(siFieldStartTimestamp).Optional().Description("RFC3339 formatted inclusive timestamp to start reading from the change stream (default: current time)").Example("2022-01-01T00:00:00Z").Default("")).
		Field(service.NewStringField(siFieldEndTimestamp).Optional().Description("RFC3339 formatted exclusive timestamp to stop reading at (default: no end time)").Example("2022-01-01T00:00:00Z").Default("")).
		Field(service.NewStringField(siFieldHeartbeatInterval).Advanced().Description("Duration string for heartbeat interval").Default("10s")).
		Field(service.NewStringField(siFieldMetadataTable).Advanced().Optional().Description("The table to store metadata in (default: cdc_metadata_<stream_id>)").Default("")).
		Field(service.NewStringField(siFieldMinWatermarkCacheTTL).Advanced().Description("Duration string for frequency of querying Spanner for minimum watermark.").Default("5s")).
		Field(service.NewStringListField(siFieldAllowedModTypes).Advanced().Optional().Description("List of modification types to process. If not specified, all modification types are processed. Allowed values: INSERT, UPDATE, DELETE").
			ShortDescription("Modification types to process: INSERT, UPDATE, DELETE. All are processed if unset.").Example([]string{"INSERT", "UPDATE", "DELETE"})).
		Field(service.NewBatchPolicyField(siFieldBatchPolicy)).
		Field(service.NewAutoRetryNacksToggleField())
}

func init() {
	service.MustRegisterBatchInput("gcp_spanner_cdc", spannerCDCInputSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
			r, err := newSpannerCDCReaderFromParsed(conf, mgr)
			if err != nil {
				return nil, err
			}
			return service.AutoRetryNacksBatchedToggled(conf, r)
		})
}

//------------------------------------------------------------------------------

type asyncMessage struct {
	msg   service.MessageBatch
	ackFn service.AckFunc
}

type spannerCDCReader struct {
	conf    spannerCDCInputConfig
	log     *service.Logger
	metrics *changestreams.Metrics

	batching   service.BatchPolicy
	batcher    *spannerPartitionBatcherFactory
	res        *service.Resources
	resCh      chan asyncMessage
	subscriber *changestreams.Subscriber
	stopSig    *shutdown.Signaller

	// updateWatermark persists a partition watermark; set to the subscriber's
	// UpdatePartitionWatermark in Connect, overridable in tests.
	updateWatermark func(ctx context.Context, partitionToken string, ts time.Time) error
}

var _ service.BatchInput = (*spannerCDCReader)(nil)

func newSpannerCDCReaderFromParsed(pConf *service.ParsedConfig, mgr *service.Resources) (*spannerCDCReader, error) {
	if err := license.CheckRunningEnterprise(mgr); err != nil {
		return nil, err
	}

	conf, err := spannerCDCInputConfigFromParsed(pConf)
	if err != nil {
		return nil, err
	}

	batching, err := pConf.FieldBatchPolicy("batching")
	if err != nil {
		return nil, err
	} else if batching.IsNoop() {
		batching.Count = 1
	}

	return newSpannerCDCReader(conf, batching, mgr), nil
}

func newSpannerCDCReader(conf spannerCDCInputConfig, batching service.BatchPolicy, mgr *service.Resources) *spannerCDCReader {
	return &spannerCDCReader{
		conf:     conf,
		log:      mgr.Logger(),
		metrics:  changestreams.NewMetrics(mgr.Metrics(), conf.StreamID),
		batching: batching,
		batcher:  newSpannerPartitionBatcherFactory(batching, mgr),
		res:      mgr,
		resCh:    make(chan asyncMessage),
		stopSig:  shutdown.NewSignaller(),
	}
}

// resetPartitionBatchers discards all cached partition batchers. Called on
// (re)connect: stale batchers hold rows and ack state from the previous
// subscriber session, and the re-read from the persisted watermarks
// re-delivers those rows anyway.
func (r *spannerCDCReader) resetPartitionBatchers() {
	r.batcher = newSpannerPartitionBatcherFactory(r.batching, r.res)
}

func (r *spannerCDCReader) emit(
	ctx context.Context,
	batcher *spannerPartitionBatcher,
	partitionToken string,
	msg service.MessageBatch,
	commitTimestamp time.Time,
) (*ack.Once, error) {
	if len(msg) == 0 {
		return nil, nil
	}
	// A zero commitTimestamp means "mid-record, do not advance": substitute
	// the last known-safe watermark so an out-of-order resolve can never
	// regress or stall behind it. Per-partition callbacks are serialized, so
	// this needs no locking.
	if commitTimestamp.IsZero() {
		commitTimestamp = batcher.lastWatermark
	} else {
		batcher.lastWatermark = commitTimestamp
	}
	resolveFn, err := batcher.cp.Track(ctx, commitTimestamp, int64(len(msg)))
	if err != nil {
		return nil, fmt.Errorf("tracking watermark checkpoint: %w", err)
	}
	ackOnce := ack.NewOnce(func(ctx context.Context) error {
		// Only the resolved (contiguous-prefix) watermark is safe to persist:
		// a batch acked out of order must not advance the watermark past
		// still-unacked earlier batches, or a crash in that window would skip
		// their records on restart.
		resolved := resolveFn()
		if resolved == nil || resolved.IsZero() {
			return nil
		}
		// If we processed the message and failed to update the watermark, we
		// would try to update it on the next message, no need to return an error here.
		if err := r.updateWatermark(ctx, partitionToken, *resolved); err != nil {
			r.log.Errorf("%s: failed to update watermark: %v", partitionToken, err)
		}
		return nil
	})
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case r.resCh <- asyncMessage{msg: msg, ackFn: ackOnce.Ack}:
		return ackOnce, nil
	}
}

var forcePeriodicFlush = &changestreams.DataChangeRecord{
	ModType: "FORCE_PERIODIC_FLUSH", // This is fake mod type to indicate periodic flush
}

func (r *spannerCDCReader) onDataChangeRecord(ctx context.Context, partitionToken string, dcr *changestreams.DataChangeRecord) error {
	batcher, _, err := r.batcher.forPartition(partitionToken)
	if err != nil {
		return err
	}

	if err := batcher.AckError(); err != nil {
		return fmt.Errorf("ack error: %v", err)
	}

	// On partition end, flush the remaining messages and wait for all messages
	// to be acked before returning and marking the partition as finished.
	if dcr == nil {
		msg, ts, err := batcher.Flush(ctx)
		if err != nil {
			return err
		}
		ack, err := r.emit(ctx, batcher, partitionToken, msg, ts)
		if err != nil {
			return err
		}
		batcher.AddAck(ack)

		if err := batcher.WaitAcks(ctx); err != nil {
			return fmt.Errorf("ack error: %v", err)
		}
		if err := batcher.Close(ctx); err != nil {
			return err
		}

		return nil
	}

	if dcr == forcePeriodicFlush {
		msg, ts, err := batcher.Flush(ctx)
		if err != nil {
			return err
		}
		ack, err := r.emit(ctx, batcher, partitionToken, msg, ts)
		if err != nil {
			return err
		}
		batcher.AddAck(ack)

		return nil
	}

	iter := batcher.MaybeFlushWith(dcr)
	for mb, ts := range iter.Iter(ctx) {
		ack, err := r.emit(ctx, batcher, partitionToken, mb, ts)
		if err != nil {
			return err
		}
		batcher.AddAck(ack)
	}
	if err := iter.Err(); err != nil {
		return err
	}

	return nil
}

func (r *spannerCDCReader) Connect(ctx context.Context) error {
	r.log.Infof("Connecting to Spanner CDC stream: %s (project: %s, instance: %s, database: %s)",
		r.conf.StreamID, r.conf.ProjectID, r.conf.InstanceID, r.conf.DatabaseID)

	var cb changestreams.CallbackFunc = r.onDataChangeRecord
	if r.batching.Period != "" {
		r.log.Infof("Periodic flushing enabled: %s", r.batching.Period)
		p := periodicallyFlushingSpannerCDCReader{
			spannerCDCReader: r,
			reqCh:            make(map[string]chan callbackRequest),
		}
		cb = p.onDataChangeRecord
	}

	// Discard any partition batchers from a previous subscriber session:
	// their buffered rows were never acked and will be re-read from the
	// persisted watermarks; reusing them would duplicate rows into mixed
	// batches and misalign the ack tracker.
	r.resetPartitionBatchers()

	var err error
	r.subscriber, err = changestreams.NewSubscriber(ctx, r.conf.Config, cb, r.log, r.metrics)
	if err != nil {
		return fmt.Errorf("create Spanner change stream reader: %w", err)
	}
	r.updateWatermark = r.subscriber.UpdatePartitionWatermark

	if err := r.subscriber.Setup(ctx); err != nil {
		return fmt.Errorf("setup Spanner change stream reader: %w", err)
	}

	// Reset our stop signal
	r.stopSig = shutdown.NewSignaller()
	ctx, cancel := r.stopSig.SoftStopCtx(context.Background())

	go func() {
		defer cancel()
		if err := r.subscriber.Run(ctx); err != nil {
			r.log.Errorf("Spanner change stream reader error: %v", err)
		}
		r.subscriber.Close()
		r.stopSig.TriggerHasStopped()
	}()

	return nil
}

func (r *spannerCDCReader) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	select {
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	case <-r.stopSig.HasStoppedChan():
		return nil, nil, service.ErrNotConnected
	case am := <-r.resCh:
		return am.msg, am.ackFn, nil
	}
}

func (r *spannerCDCReader) Close(ctx context.Context) error {
	r.stopSig.TriggerSoftStop()
	select {
	case <-ctx.Done():
	case <-time.After(shutdownTimeout):
	case <-r.stopSig.HasStoppedChan():
	}
	r.stopSig.TriggerHardStop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(shutdownTimeout):
	case <-r.stopSig.HasStoppedChan():
	}
	return nil
}

type callbackRequest struct {
	partitionToken string
	dcr            *changestreams.DataChangeRecord
	errCh          chan error
}

// periodicallyFlushingSpannerCDCReader synchronizes callback invocations with
// periodic flushes to ensure ordering of messages. The flush period is
// governed by the spannerPartitionBatcher.period timer.
//
// When spannerPartitionBatcher.Close is called the timer is stopped and the
// go routine is terminated.
//
// All calls to spannerCDCReader.onDataChangeRecord use the same context as the
// first call to periodicallyFlushingSpannerCDCReader.onDataChangeRecord for
// a given partition.
type periodicallyFlushingSpannerCDCReader struct {
	*spannerCDCReader
	mu    sync.RWMutex
	reqCh map[string]chan callbackRequest
}

func (r *periodicallyFlushingSpannerCDCReader) onDataChangeRecord(ctx context.Context, partitionToken string, dcr *changestreams.DataChangeRecord) error {
	batcher, cached, err := r.batcher.forPartition(partitionToken)
	if err != nil {
		return err
	}

	if !cached {
		ch := make(chan callbackRequest)
		r.mu.Lock()
		r.reqCh[partitionToken] = ch
		r.mu.Unlock()

		softStopCh := r.stopSig.SoftStopChan()
		go func() {
			r.log.Debugf("%s: starting periodic flusher", partitionToken)
			defer func() {
				r.mu.Lock()
				delete(r.reqCh, partitionToken)
				r.mu.Unlock()
				r.log.Debugf("%s: periodic flusher stopped", partitionToken)
			}()

			for {
				select {
				case <-ctx.Done():
					return
				case <-softStopCh:
					return
				case _, ok := <-batcher.period.C:
					if !ok {
						return
					}

					err := r.spannerCDCReader.onDataChangeRecord(ctx, partitionToken, forcePeriodicFlush)
					if err != nil {
						r.log.Warnf("%s: periodic flush error: %v", partitionToken, err)
					}
				case cr := <-ch:
					cr.errCh <- r.spannerCDCReader.onDataChangeRecord(ctx, partitionToken, cr.dcr)
				}
			}
		}()
	}

	r.mu.RLock()
	ch := r.reqCh[partitionToken]
	r.mu.RUnlock()

	errCh := make(chan error)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case ch <- callbackRequest{
		partitionToken: partitionToken,
		dcr:            dcr,
		errCh:          errCh,
	}:
		// ok
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errCh:
		return err
	}
}
