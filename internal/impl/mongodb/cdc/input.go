// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package cdc

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/Jeffail/checkpoint"
	"github.com/Jeffail/shutdown"
	"github.com/Masterminds/semver"
	"github.com/dustin/go-humanize"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"

	"github.com/redpanda-data/benthos/v4/public/bloblang"
	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/asyncroutine"
	"github.com/redpanda-data/connect/v4/internal/impl/mongodb"
	"github.com/redpanda-data/connect/v4/internal/license"
)

const (
	fieldClientURL           = "url"
	fieldClientDatabase      = "database"
	fieldClientUsername      = "username"
	fieldClientPassword      = "password"
	fieldClientAppName       = "app_name"
	fieldCollections         = "collections"
	fieldStreamSnapshot      = "stream_snapshot"
	fieldSnapshotParallelism = "snapshot_parallelism"
	fieldBucketSharding      = "snapshot_auto_bucket_sharding"
	fieldCheckpointKey       = "checkpoint_key"
	fieldCheckpointCache     = "checkpoint_cache"
	fieldCheckpointInterval  = "checkpoint_interval"
	fieldCheckpointLimit     = "checkpoint_limit"
	fieldReadBatchSize       = "read_batch_size"
	fieldReadMaxWait         = "read_max_wait"
	fieldDocumentMode        = "document_mode"
	fieldJSONMarshalMode     = "json_marshal_mode"

	marshalModeCanonical string = "canonical"
	marshalModeRelaxed   string = "relaxed"
)

func spec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Summary(`Streams changes from a MongoDB replica set.`).
		Description(`Read from a MongoDB replica set using https://www.mongodb.com/docs/manual/changeStreams/[^Change Streams]. It's only possible to watch for changes when using a sharded MongoDB or a MongoDB cluster running as a replica set.

By default MongoDB does not propagate changes in all cases. In order to capture all changes (including deletes) in a MongoDB cluster one needs to enable pre and post image saving and the collection needs to also enable saving these pre and post images. For more information see https://www.mongodb.com/docs/manual/changeStreams/#change-streams-with-document-pre--and-post-images[^MongoDB documentation].

== Metadata

Each message emitted by this plugin has the following metadata:

- operation: either "insert", "replace", "delete" or "update" for changes streamed. Documents from the initial snapshot have the operation set to "read".
- collection: the collection the document was written to.
- operation_time: the oplog time for when this operation occurred.
- schema: the collection schema in benthos common schema format (set as immutable metadata). Extracted from the collection's `+"`$jsonSchema`"+` validator if available, otherwise inferred from the first document seen. Not present on messages where no schema could be determined (e.g. deletes without pre-images when no prior schema is cached).

== Schema Detection

Schema metadata is discovered using a two-tier strategy:

1. *$jsonSchema validators* are preferred and queried at startup for each watched collection. When a validator exists, the schema provides accurate type information and required/optional field classification.
2. When no validator exists, schema is *inferred from the first document* received per collection. All fields are marked optional.

*Change detection:* when a document's top-level field set differs from the cached schema, the schema is re-inferred from that document. This applies to both validator-sourced and inference-sourced schemas.

*Limitations:* type changes within existing fields and structural changes inside nested subdocuments are not detected automatically. Restart the input to force a full schema refresh.

*Fields with null values, unknown BSON types, or mixed-type arrays* are mapped to the `+"`Any`"+` schema type. The `+"`parquet_encode`"+` processor does not support `+"`Any`"+` and will error if it encounters one. Add an upstream processor (e.g. `+"`mapping`"+`) to convert or remove these fields before `+"`parquet_encode`"+`.

*Schema stability:* MongoDB collections may contain documents with varying field sets. When this occurs, the schema updates on each structural change, which can cause frequent schema version bumps in schema registries with compatibility modes. For schema registry targets, configuring a `+"`$jsonSchema`"+` validator on the collection is strongly recommended.
    `).
		Fields(
			service.NewStringField(fieldClientURL).
				Description("The URL of the target MongoDB server.").
				Example("mongodb://localhost:27017"),
			service.NewStringField(fieldClientDatabase).
				Description("The name of the target MongoDB database."),
			service.NewStringField(fieldClientUsername).
				Description("The username to connect to the database.").
				Default(""),
			service.NewStringField(fieldClientPassword).
				Description("The password to connect to the database.").
				Default("").
				Secret(),
			mongodb.AWSIAMAuthField(),
			service.NewStringListField(fieldCollections).
				Description("The collections to stream changes from."),
			service.NewStringField(fieldCheckpointKey).
				Description("Checkpoint cache key name.").
				Default("mongodb_cdc_checkpoint"),
			service.NewStringField(fieldCheckpointCache).
				Description("Checkpoint cache name."),
			service.NewDurationField(fieldCheckpointInterval).
				Description("The interval between writing checkpoints to the cache.").
				Default("5s"),
			service.NewIntField(fieldCheckpointLimit).
				Description("").
				Default(1000),
			service.NewIntField(fieldReadBatchSize).
				Description("The batch size of documents for MongoDB to return.").
				Default(1000),
			service.NewDurationField(fieldReadMaxWait).
				Description("The maximum time MongoDB waits to fulfill `read_batch_size` on the change stream before returning documents.").
				ShortDescription("Maximum time MongoDB waits to fill read_batch_size on the change stream before returning.").
				Default("1s"),
			service.NewBoolField(fieldStreamSnapshot).
				Description("If to read initial snapshot before streaming changes.").
				Default(false),
			service.NewIntField(fieldSnapshotParallelism).
				Description("Parallelism for snapshot phase.").
				Default(1).
				LintRule(`match {
  this < 1 => ["field snapshot_parallelism must be greater or equal to 1."],
}`),
			service.NewBoolField(fieldBucketSharding).
				Description("If true, determine parallel snapshot chunks using `$bucketAuto` instead of the `splitVector` command. This allows parallel collection reading in environments where privileged access to the MongoDB cluster is not allowed such as MongoDB Atlas.").
				ShortDescription("Use $bucketAuto rather than splitVector to determine parallel snapshot chunks, avoiding privileged access.").
				Default(false).
				Advanced(),
			service.NewStringAnnotatedEnumField(fieldDocumentMode, map[string]string{
				"update_lookup":       "In this mode insert, replace and update operations have the full document emitted and deletes only have the _id field populated. Documents updates lookup the full document. This corresponds to the updateLookup option, see the https://www.mongodb.com/docs/manual/changeStreams/#std-label-change-streams-updateLookup[^MongoDB documentation] for more information.",
				"pre_and_post_images": "Uses pre and post image collection to emit the full documents for update and delete operations. To use and configure this mode see the setup steps in the https://www.mongodb.com/docs/manual/changeStreams/#change-streams-with-document-pre--and-post-images[^MongoDB documentation].",
				"partial_update": `In this mode update operations only have a description of the update operation, which follows the following schema:
      {
        "_id": <document_id>,
        "operations": [
          # type == set means that the value was updated like so:
          # root.foo."bar.baz" = "world"
          {"path": ["foo", "bar.baz"], "type": "set", "value":"world"},
          # type == unset means that the value was deleted like so:
          # root.qux = deleted()
          {"path": ["qux"], "type": "unset", "value": null},
          # type == truncatedArray means that the array at that path was truncated to value number of elements
          # root.array = this.array.slice(2)
          {"path": ["array"], "type": "truncatedArray", "value": 2}
        ]
      }
      `,
			}).
				Description("The mode in which to emit documents, specifically updates and deletes.").
				Default("update_lookup").
				Advanced(),
			service.NewStringAnnotatedEnumField(fieldJSONMarshalMode, map[string]string{
				marshalModeCanonical: "A string format that emphasizes type preservation at the expense of readability and interoperability. " +
					"That is, conversion from canonical to BSON will generally preserve type information except in certain specific cases. ",
				marshalModeRelaxed: "A string format that emphasizes readability and interoperability at the expense of type preservation." +
					"That is, conversion from relaxed format to BSON can lose type information.",
			}).
				Description("The json_marshal_mode setting is optional and controls the format of the output message.").
				Default(marshalModeCanonical).
				Advanced(),
			service.NewStringField(fieldClientAppName).
				Description("The client application name.").
				Default("benthos").
				Advanced(),
			service.NewAutoRetryNacksToggleField(),
		)
}

func init() {
	service.MustRegisterBatchInput("mongodb_cdc", spec(), newMongoCDC)
}

func newMongoCDC(conf *service.ParsedConfig, res *service.Resources) (i service.BatchInput, err error) {
	if err := license.CheckRunningEnterprise(res); err != nil {
		return nil, err
	}
	cdc := &mongoCDC{
		readChan:          make(chan mongoBatch),
		errorChan:         make(chan error, 1),
		logger:            res.Logger(),
		collectionSchemas: make(map[string]*cachedSchema),
	}
	if cdc.cc, err = mongodb.ClientConfigFromParsed(conf, res.Logger()); err != nil {
		return
	}
	if cdc.collections, err = conf.FieldStringList(fieldCollections); err != nil {
		return
	}
	if len(cdc.collections) == 0 {
		return nil, errors.New("at least one collection must be specified")
	}
	var snapshotEnabled bool
	if snapshotEnabled, err = conf.FieldBool(fieldStreamSnapshot); err != nil {
		return
	}
	if snapshotEnabled {
		if cdc.snapshotParallelism, err = conf.FieldInt(fieldSnapshotParallelism); err != nil {
			return
		}
		cdc.snapshotSemaphore = semaphore.NewWeighted(int64(cdc.snapshotParallelism))
	}
	if cdc.useAutoBucketSnapshots, err = conf.FieldBool(fieldBucketSharding); err != nil {
		return
	}
	if cdc.readBatchSize, err = conf.FieldInt(fieldReadBatchSize); err != nil {
		return
	}
	if cdc.streamMaxWait, err = conf.FieldDuration(fieldReadMaxWait); err != nil {
		return
	}
	var documentMode string
	if documentMode, err = conf.FieldString(fieldDocumentMode); err != nil {
		return
	}
	switch documentMode {
	case "update_lookup":
		cdc.docMode = documentModeUpdateLookup
	case "pre_and_post_images":
		cdc.docMode = documentModePreAndPostImage
	case "partial_update":
		cdc.docMode = documentModePartialUpdate
	default:
		return nil, fmt.Errorf("unknown document_mode value: %s", documentMode)
	}
	marshalMode, err := conf.FieldString(fieldJSONMarshalMode)
	if err != nil {
		return nil, err
	}
	cdc.marshalCanonical = marshalMode == marshalModeCanonical
	var cacheKey, cacheName string
	var checkpointInterval time.Duration
	if cacheName, err = conf.FieldString(fieldCheckpointCache); err != nil {
		return
	}
	if !res.HasCache(cacheName) {
		return nil, fmt.Errorf("unknown `%s` %s", fieldCheckpointCache, cacheName)
	}
	if cacheKey, err = conf.FieldString(fieldCheckpointKey); err != nil {
		return
	}
	if checkpointInterval, err = conf.FieldDuration(fieldCheckpointInterval); err != nil {
		return
	}
	cdc.checkpoint = &checkpointCache{
		resources: res,
		cacheName: cacheName,
		cacheKey:  cacheKey,
	}
	if checkpointInterval.Seconds() > 0 {
		cdc.checkpointFlusher = asyncroutine.NewPeriodicWithContext(
			checkpointInterval,
			func() func(context.Context) {
				// Don't resave the resume token if it hasn't changed.
				var lastResumeToken bson.Raw
				return func(ctx context.Context) {
					cdc.resumeTokenMu.Lock()
					defer cdc.resumeTokenMu.Unlock()
					if cdc.resumeToken == nil || bytes.Equal(lastResumeToken, cdc.resumeToken) {
						return
					}
					if err := cdc.checkpoint.Store(ctx, cdc.resumeToken); err != nil {
						res.Logger().Warnf("unable to store checkpoints in cache: %v", err)
					} else {
						lastResumeToken = cdc.resumeToken
					}
				}
			}(),
		)
	}

	if cdc.checkpointLimit, err = conf.FieldInt(fieldCheckpointLimit); err != nil {
		return
	}

	return service.AutoRetryNacksBatchedToggled(conf, cdc)
}

type mongoBatch struct {
	documents service.MessageBatch
	ackFn     service.AckFunc
}

type documentMode int

const (
	documentModePreAndPostImage documentMode = iota
	documentModeUpdateLookup
	documentModePartialUpdate
)

type mongoCDC struct {
	cc *mongodb.ClientConfig
	// client and db are established lazily on Connect so that any credentials
	// are resolved fresh on every (re)connection attempt.
	client      *mongo.Client
	db          *mongo.Database
	collections []string
	logger      *service.Logger

	shutsig   *shutdown.Signaller
	readChan  chan mongoBatch
	errorChan chan error

	readBatchSize    int
	streamMaxWait    time.Duration
	docMode          documentMode
	marshalCanonical bool

	snapshotParallelism    int // if > 0 then enabled
	snapshotSemaphore      *semaphore.Weighted
	useAutoBucketSnapshots bool

	checkpoint        *checkpointCache
	checkpointFlusher *asyncroutine.Periodic
	checkpointLimit   int

	resumeToken   bson.Raw
	resumeTokenMu sync.Mutex

	collectionSchemas   map[string]*cachedSchema
	collectionSchemasMu sync.RWMutex
}

type cachedSchema struct {
	schema any      // serialised Common Schema (from ToAny())
	keys   []string // sorted top-level field names for key-set fingerprinting
}

// mongoClientBSONOptions configures BSON marshalling options applied to every
// client this input builds - both the initial connection and the post-snapshot
// credential refresh - so the two call sites cannot drift.
func mongoClientBSONOptions(o *options.ClientOptions) {
	o.SetBSONOptions(&options.BSONOptions{DefaultDocumentM: true})
}

func (m *mongoCDC) Connect(ctx context.Context) error {
	if m.shutsig != nil {
		m.shutsig.TriggerSoftStop()
		select {
		case <-m.shutsig.HasStoppedChan():
		case <-ctx.Done():
			return ctx.Err()
		}
		m.shutsig = nil
		select {
		case err := <-m.errorChan:
			// ReadBatch's select may have taken the goroutine-stopped case
			// instead of this error, so log it here: otherwise the reason the
			// previous connection died is lost.
			m.logger.Warnf("Reconnecting after stream failure: %v", err)
		default:
		}
	}
	// Reset schema cache on reconnect so stale schemas from a previous
	// connection don't persist if collections were changed in between.
	m.collectionSchemasMu.Lock()
	m.collectionSchemas = make(map[string]*cachedSchema)
	m.collectionSchemasMu.Unlock()
	if m.client != nil {
		if m.cc.AssumesRole() {
			_ = m.client.Disconnect(ctx)
			m.client = nil
		} else if err := m.client.Ping(ctx, nil); err != nil {
			_ = m.client.Disconnect(ctx)
			m.client = nil
		}
	}
	if m.client == nil {
		client, db, err := m.cc.Connect(ctx, mongoClientBSONOptions)
		if err != nil {
			return fmt.Errorf("unable to connect to mongo: %w", err)
		}
		m.client, m.db = client, db
	}
	if err := m.client.Ping(ctx, nil); err != nil {
		return fmt.Errorf("unable to ping mongodb: %w", err)
	}
	r := m.db.RunCommand(ctx, bson.M{"buildInfo": 1})
	if r.Err() != nil {
		return fmt.Errorf("failure to determine mongodb version: %w", r.Err())
	}
	var buildInfo bson.M
	if err := r.Decode(&buildInfo); err != nil {
		return fmt.Errorf("failure to decode mongodb version: %w", r.Err())
	}
	versionStr, ok := buildInfo["version"].(string)
	if !ok {
		return errors.New("unable to determine mongodb version")
	}
	version, err := semver.NewVersion(versionStr)
	if err != nil {
		return fmt.Errorf("unable to parse mongodb version: %w", err)
	}
	if version.Major() < 4 {
		return fmt.Errorf("`mongodc_cdc` requires MongoDB version 4 or higher - current version: %v", version.String())
	}
	m.resumeToken, err = m.checkpoint.Load(ctx)
	if err != nil {
		return fmt.Errorf("unable to load checkpoints from cache: %w", err)
	}
	// Set the stream start when starting fresh to be the current oplog end time.
	r = m.db.RunCommand(ctx, bson.M{"hello": 1})
	if r.Err() != nil {
		return fmt.Errorf("unable to determine replication info (is your mongodb instance running as a replication set?): %w", r.Err())
	}
	var helloReply bson.M
	if err := r.Decode(&helloReply); err != nil {
		return fmt.Errorf("unable to decode replication info: %w", err)
	}
	ts, hasTS := bsonGetPath(helloReply, "lastWrite", "majorityOpTime", "ts").(bson.Timestamp)
	// Capture the current change stream position as a resume token when a
	// snapshot is about to run, because only a token can be persisted to the
	// checkpoint cache once that snapshot completes - the oplog timestamp
	// cannot. Sharded clusters (mongos) report no oplog timestamp at all, so
	// there the token is fetched regardless as the only usable start position.
	// Otherwise the oplog timestamp positions the stream as before, and nothing
	// needs a token: without a snapshot there is nothing to checkpoint early,
	// and when a checkpoint already exists the stream resumes from it.
	var initialResumeToken bson.Raw = nil
	if m.resumeToken == nil && (m.snapshotParallelism > 0 || !hasTS) {
		token, err := m.getCurrentResumeToken(ctx)
		switch {
		case err == nil:
			initialResumeToken = token
		case !hasTS:
			return fmt.Errorf("unable to compute stream start position: %w (%s)", err, helloReply.String())
		default:
			// Replica sets before 4.0.7 may not report a post-batch resume
			// token. The oplog timestamp still positions this run, but there is
			// nothing storable, so the post-snapshot checkpoint is skipped and
			// a failure re-runs the snapshot as before.
			m.logger.Debugf("unable to capture a pre-snapshot resume token, a completed snapshot will not be checkpointed: %v", err)
		}
	}
	// Tier 1: pre-fetch $jsonSchema validators for all watched collections
	// during Connect() so the stream goroutine is not delayed.
	for _, coll := range m.collections {
		s, keys, err := fetchCollectionSchema(ctx, m.db, coll)
		if err != nil {
			m.logger.Warnf("Failed to fetch $jsonSchema for collection %s: %v", coll, err)
			continue
		}
		if s != nil {
			m.collectionSchemasMu.Lock()
			m.collectionSchemas[coll] = &cachedSchema{schema: s, keys: keys}
			m.collectionSchemasMu.Unlock()
		}
	}

	shutsig := shutdown.NewSignaller()
	m.shutsig = shutsig
	go func() {
		ctx, cancel := shutsig.SoftStopCtx(context.Background())
		if m.checkpointFlusher != nil {
			m.checkpointFlusher.Start()
			defer m.checkpointFlusher.Stop()
		}
		defer cancel()
		defer shutsig.TriggerHasStopped()

		opts := options.ChangeStream().
			SetBatchSize(int32(m.readBatchSize)).
			SetMaxAwaitTime(m.streamMaxWait)
		switch m.docMode {
		case documentModePreAndPostImage:
			opts = opts.SetFullDocument(options.Required)
			if version.Major() >= 6 {
				opts = opts.SetFullDocumentBeforeChange(options.Required)
			}
		case documentModeUpdateLookup:
			opts = opts.SetFullDocument(options.UpdateLookup)
		case documentModePartialUpdate:
			if version.Compare(semver.MustParse("6.1.0")) >= 0 {
				opts = opts.SetShowExpandedEvents(true)
			}
		}
		func() {
			m.resumeTokenMu.Lock()
			defer m.resumeTokenMu.Unlock()
			if m.resumeToken != nil {
				// TODO: Handle the resume token becoming invalid due to collection rename/drop
				opts = opts.SetResumeAfter(m.resumeToken)
			} else if initialResumeToken != nil {
				opts = opts.SetResumeAfter(initialResumeToken)
			} else {
				// If there are no writes between snapshot and streaming, we want to skip the last
				// document that will be read in the snapshot.
				nextTS := nextTimestamp(ts)
				opts = opts.SetStartAtOperationTime(&nextTS)
			}
		}()
		cp := checkpoint.NewCapped[bson.Raw](int64(m.checkpointLimit))
		if m.resumeToken == nil {
			g, gctx := errgroup.WithContext(ctx)
			for _, name := range m.collections {
				coll := m.db.Collection(name)
				g.Go(func() error { return m.readSnapshot(gctx, coll, ts, cp) })
			}
			if err := g.Wait(); err != nil {
				select {
				case m.errorChan <- fmt.Errorf("error reading MongoDB snapshot: %w", err):
				default:
				}
				return
			}
			// CONTRIBUTING §5.4.1: checkpoint as soon as the snapshot completes
			// rather than waiting for the first streamed message, so that a
			// restart resumes the stream instead of re-running the snapshot.
			if !m.storeSnapshotCheckpoint(ctx, cp, initialResumeToken, m.checkpoint.Store) {
				return
			}
			if m.snapshotParallelism > 0 && m.cc.AssumesRole() {
				// The snapshot can run for a large fraction (or all) of the STS
				// session duration, since progress within the snapshot is not
				// checkpointed. Rebuild the client here so the change-stream
				// phase starts with freshly resolved credentials rather than a
				// session that may have already expired or be about to.
				m.logger.Debugf("Snapshot complete, refreshing IAM credentials before streaming")
				_ = m.client.Disconnect(ctx)
				client, db, err := m.cc.Connect(ctx, mongoClientBSONOptions)
				if err != nil {
					select {
					case m.errorChan <- fmt.Errorf("error refreshing MongoDB credentials after snapshot: %w", err):
					default:
					}
					return
				}
				m.client, m.db = client, db
			}
		}
		if err := m.readFromStream(ctx, cp, opts); err != nil {
			select {
			case m.errorChan <- fmt.Errorf("error watching MongoDB change stream: %w", err):
			default:
			}
		}
		func() {
			// Save the resume token before the background fiber finishes.
			ctx, cancel := shutsig.HardStopCtx(context.Background())
			defer cancel()
			m.resumeTokenMu.Lock()
			defer m.resumeTokenMu.Unlock()
			if m.resumeToken == nil {
				return
			}
			if err := m.checkpoint.Store(ctx, m.resumeToken); err != nil {
				m.logger.Warnf("unable to store checkpoint before stopping `mongodb_cdc`: %v", err)
			}
		}()
	}()
	return nil
}

// snapshotCheckpointWriteTimeout bounds the post-snapshot checkpoint write on
// its detached context, mirroring the bounded terminal save: a shutdown that
// raced the final ack should not be extended indefinitely by a slow cache.
const snapshotCheckpointWriteTimeout = 10 * time.Second

// snapshotAckPollInterval is how often the post-snapshot checkpoint waits for
// the snapshot batches still in flight to be acknowledged.
const snapshotAckPollInterval = 100 * time.Millisecond

// pendingTracker reports how many tracked batches are yet to be resolved. It is
// satisfied by *checkpoint.Capped.
type pendingTracker interface {
	Pending() int64
}

// storeSnapshotCheckpoint persists the pre-snapshot stream position once every
// snapshot batch tracked by cp has been resolved downstream, and returns false
// only when ctx was cancelled while acks were still outstanding (the caller
// should then stop). A cancellation that arrives after the final ack does not
// discard the checkpoint: the snapshot completed, and persisting it is exactly
// what lets the next start resume the stream instead of re-running it.
//
// Contract: only call this after the snapshot errgroup returned nil, which
// guarantees every tracked batch was delivered to the framework (all
// resolve-without-delivery paths return errors).
//
// The ack gate is a correctness requirement, not an optimisation: persisting the
// pre-snapshot position while part of the snapshot is still in flight would let
// a restart load the checkpoint, skip the snapshot entirely, and silently lose
// the undelivered tail. Re-running the snapshot is the safe failure mode.
// Waiting here is safe because every snapshot batch has already been handed to
// the framework and their acks arrive independently of this goroutine; if they
// never arrive the pipeline is already wedged, and shutdown still exits through
// the ctx case.
//
// token is nil when no resume token could be captured for this run, in which
// case nothing is stored and behaviour degrades to re-running the snapshot after
// a failure - never to a checkpoint that cannot be resumed from.
func (m *mongoCDC) storeSnapshotCheckpoint(
	ctx context.Context,
	cp pendingTracker,
	token bson.Raw,
	store func(context.Context, bson.Raw) error,
) bool {
	if token == nil {
		return true
	}
	// Pending() == 0 genuinely means "every snapshot batch was acked" here:
	// this method only runs after g.Wait() returned nil, and every path that
	// resolves a slot without delivering its batch (the send select's
	// cancellation branches) returns a non-nil error, making the gate
	// unreachable. So a completed count may be trusted even when ctx has been
	// cancelled in the meantime; only outstanding acks at cancellation force
	// giving up, in which case the snapshot re-runs (the safe failure mode).
	for cp.Pending() > 0 {
		select {
		case <-ctx.Done():
			if cp.Pending() > 0 {
				return false
			}
		case <-time.After(snapshotAckPollInterval):
		}
	}
	// Keep the periodic flusher and the terminal save consistent with what was
	// written: both dedupe on the token they last saw. The write uses a
	// detached context so a shutdown arriving after the last ack still
	// persists the completed snapshot.
	m.resumeTokenMu.Lock()
	m.resumeToken = token
	m.resumeTokenMu.Unlock()
	storeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), snapshotCheckpointWriteTimeout)
	defer cancel()
	if err := store(storeCtx, token); err != nil {
		// A failed write only means the snapshot may run again, so warn and
		// carry on as every other checkpoint store in this file does.
		m.logger.Warnf("unable to store post-snapshot checkpoint in cache: %v", err)
	}
	return true
}

func (m *mongoCDC) readSnapshot(
	ctx context.Context,
	coll *mongo.Collection,
	snapshotTime bson.Timestamp,
	cp *checkpoint.Capped[bson.Raw],
) (err error) {
	if m.snapshotParallelism == 0 {
		return nil
	}
	if m.snapshotParallelism > 1 {
		return m.readParallelSnapshot(ctx, coll, snapshotTime, cp)
	} else {
		return m.readSnapshotRange(ctx, coll, snapshotTime, cp, bson.MinKey{}, bson.MaxKey{})
	}
}

func getCollectionSize(ctx context.Context, collection *mongo.Collection) (int64, error) {
	cmd := bson.M{"collStats": collection.Name()}
	var result bson.M
	if err := collection.Database().RunCommand(ctx, cmd).Decode(&result); err != nil {
		return 0, fmt.Errorf("error estimating collection size: %w", err)
	}
	size, err := bloblang.ValueAsInt64(result["size"])
	if err != nil {
		return 0, fmt.Errorf("unable to extract collection size: %w", err)
	}
	return size, nil
}

func (m *mongoCDC) getParallelRanges(ctx context.Context, coll *mongo.Collection) ([][2]any, error) {
	if m.useAutoBucketSnapshots {
		return m.autoBuckets(ctx, coll)
	}
	return m.computeSplitPoints(ctx, coll)
}

func (m *mongoCDC) computeSplitPoints(ctx context.Context, coll *mongo.Collection) ([][2]any, error) {
	size, err := getCollectionSize(ctx, coll)
	if err != nil {
		return nil, err
	}
	chunkSize := max(int(size)/m.snapshotParallelism, 16*humanize.MiByte)
	command := bson.D{
		{Key: "splitVector", Value: fmt.Sprintf("%s.%s", m.db.Name(), coll.Name())},
		{Key: "keyPattern", Value: bson.D{{Key: "_id", Value: 1}}},
		{Key: "min", Value: bson.D{{Key: "_id", Value: bson.MinKey{}}}},
		{Key: "max", Value: bson.D{{Key: "_id", Value: bson.MaxKey{}}}},
		{Key: "maxChunkSizeBytes", Value: chunkSize},
	}
	var result bson.M
	if err := m.db.RunCommand(ctx, command).Decode(&result); err != nil {
		return nil, err
	}
	splitKeys, ok := result["splitKeys"].(bson.A)
	if !ok {
		return nil, fmt.Errorf("unexpected splitVector result format: %s", result.String())
	}
	var prev any = bson.MinKey{}
	ranges := [][2]any{}
	for i := range splitKeys {
		v, ok := splitKeys[i].(bson.M)
		if !ok {
			return nil, fmt.Errorf("unexpected splitVector range result format: %s", result.String())
		}
		id := v["_id"]
		ranges = append(ranges, [2]any{prev, id})
		prev = id
	}
	ranges = append(ranges, [2]any{prev, bson.MaxKey{}})
	return ranges, nil
}

func (m *mongoCDC) autoBuckets(ctx context.Context, coll *mongo.Collection) ([][2]any, error) {
	pipeline := mongo.Pipeline{
		bson.D{{
			Key: "$bucketAuto",
			Value: bson.D{
				{Key: "groupBy", Value: "$_id"},
				{Key: "buckets", Value: m.snapshotParallelism},
			},
		}},
	}
	opts := options.Aggregate().SetAllowDiskUse(true)
	cursor, err := coll.Aggregate(ctx, pipeline, opts)
	if err != nil {
		return nil, fmt.Errorf("unable to compute buckets: %w", err)
	}
	ranges := [][2]any{}
	for cursor.Next(ctx) {
		var bucket bson.M
		if err := cursor.Decode(&bucket); err != nil {
			return nil, fmt.Errorf("unable to extract bucket: %w", err)
		}

		ranges = append(ranges, [2]any{
			bsonGetPath(bucket, "_id", "min"),
			bsonGetPath(bucket, "_id", "max"),
		})
	}
	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("unable to read buckets results: %w", err)
	}
	if len(ranges) == 0 {
		return [][2]any{{bson.MinKey{}, bson.MaxKey{}}}, nil
	}
	ranges[0][0] = bson.MinKey{}
	ranges[len(ranges)-1][1] = bson.MaxKey{}
	return ranges, nil
}

func (m *mongoCDC) readParallelSnapshot(
	ctx context.Context,
	coll *mongo.Collection,
	snapshotTime bson.Timestamp,
	cp *checkpoint.Capped[bson.Raw],
) error {
	begin := time.Now()
	ranges, err := m.getParallelRanges(ctx, coll)
	if err != nil {
		m.logger.Warnf("unable to determine split points for queries over %s, falling back to sequential scan due to: %v", coll.Name(), err)
		return m.readSnapshotRange(ctx, coll, snapshotTime, cp, bson.MinKey{}, bson.MaxKey{})
	}
	m.logger.Debugf("determined collection split points in %v", time.Since(begin))
	g, ctx := errgroup.WithContext(ctx)
	for _, r := range ranges {
		minKey := r[0]
		maxKey := r[1]
		g.Go(func() error {
			return m.readSnapshotRange(ctx, coll, snapshotTime, cp, minKey, maxKey)
		})
	}
	return g.Wait()
}

func (m *mongoCDC) readSnapshotRange(
	ctx context.Context,
	coll *mongo.Collection,
	snapshotTime bson.Timestamp,
	cp *checkpoint.Capped[bson.Raw],
	start, end any,
) error {
	if err := m.snapshotSemaphore.Acquire(ctx, 1); err != nil {
		return err
	}
	defer m.snapshotSemaphore.Release(1)
	cursor, err := coll.Find(ctx, bson.D{
		{
			Key: "_id",
			Value: bson.D{
				{Key: "$gte", Value: start},
				{Key: "$lt", Value: end},
			},
		},
	}, options.Find().SetBatchSize(int32(m.readBatchSize)))
	if err != nil {
		return fmt.Errorf("reading snapshot: %w", err)
	}
	cursor.SetBatchSize(int32(m.readBatchSize))
	defer cursor.Close(ctx)
	var mb service.MessageBatch
	for cursor.Next(ctx) {
		var doc bson.M
		if err := cursor.Decode(&doc); err != nil {
			return fmt.Errorf("unable to decode document: %w", err)
		}
		msg, err := m.newMongoDBCDCMessage(doc, "read", coll.Name(), snapshotTime, false)
		if err != nil {
			return fmt.Errorf("unable to create message from document: %w", err)
		}
		mb = append(mb, msg)
		if cursor.RemainingBatchLength() == 0 {
			resolve, err := cp.Track(ctx, nil, int64(len(mb)))
			if err != nil {
				return fmt.Errorf("unable to create batch: %w", err)
			}
			// The error argument is ignored on purpose: with
			// `auto_replay_nacks: false` a nacked snapshot batch resolves its
			// slot by design (the documented opt-in to dropping failed
			// batches), which also lets the post-snapshot checkpoint proceed.
			// The drop scope is that batch, per the framework contract.
			b := mongoBatch{mb, func(context.Context, error) error {
				resumeToken := resolve()
				if resumeToken != nil && *resumeToken != nil {
					return fmt.Errorf("unexpected resume token for snapshot batch: %s", resumeToken.String())
				}
				return nil
			}}
			select {
			case m.readChan <- b:
			case <-ctx.Done():
				// Fail the snapshot rather than resolving the slot of a batch
				// nobody received: a resolved slot would let the post-snapshot
				// checkpoint be written for a snapshot that was never fully
				// delivered, and a restart would then skip it.
				return ctx.Err()
			case <-m.shutsig.SoftStopChan():
				_ = b.ackFn(ctx, nil)
				return context.Canceled
			}
			mb = nil
		}
	}
	if err := cursor.Err(); err != nil {
		return fmt.Errorf("reading snapshot: %w", err)
	}
	return nil
}

// getCurrentResumeToken opens a short-lived change stream over the watched
// collections and returns its initial post-batch resume token, i.e. the current
// end of the stream. This works on any deployment that supports change streams:
// sharded clusters, where it is the only available start position, and replica
// sets, where servers report a post-batch resume token from 4.0.7 onwards. Older
// servers return no token and this returns an error.
func (m *mongoCDC) getCurrentResumeToken(ctx context.Context) (bson.Raw, error) {
	filter := []bson.M{{"$match": bson.M{
		"ns.coll": bson.M{"$in": slices.Clone(m.collections)},
	}}}
	stream, err := m.db.Watch(
		ctx,
		filter,
		options.ChangeStream().
			SetBatchSize(int32(0)).
			SetMaxAwaitTime(0*time.Millisecond),
	)
	if err != nil {
		return nil, err
	}
	defer func() {
		// Don't leave the server-side cursor dangling: this stream is only used
		// to read a position, the streaming phase opens its own.
		_ = stream.Close(ctx)
	}()
	if stream.TryNext(ctx) {
		// A batchSize of 0 means the server sends no events in the first batch,
		// so this should be unreachable. If it ever happens the cached token is
		// that event's `_id` and resuming after it would skip the event, so
		// report no token instead: the caller then falls back to the oplog
		// timestamp, or fails when no timestamp exists.
		m.logger.Debugf("change stream returned an event while capturing the start position, ignoring the token to avoid skipping it")
		return nil, errors.New("unable to determine start position prior to snapshot phase: stream returned an event")
	}
	if rt := stream.ResumeToken(); rt != nil {
		// The driver hands back a token aliasing the cursor's batch buffer, so
		// copy it before the stream is closed.
		return bson.Raw(slices.Clone([]byte(rt))), nil
	}
	return nil, errors.New("unable to determine start position prior to snapshot phase")
}

func (m *mongoCDC) readFromStream(ctx context.Context, cp *checkpoint.Capped[bson.Raw], opts *options.ChangeStreamOptionsBuilder) error {
	filter := []bson.M{{"$match": bson.M{
		"ns.coll": bson.M{"$in": slices.Clone(m.collections)},
	}}}
	stream, err := m.db.Watch(ctx, filter, opts)
	if err != nil {
		return fmt.Errorf("error opening change stream: %w", err)
	}
	stream.SetBatchSize(int32(m.readBatchSize))
	var mb service.MessageBatch
	// You'd think that this would be the same as just calling stream.Next(ctx), but surprise! It's not
	// They do something funky where they apply another timeout they probably shouldn't be applying,
	// so work around that by doing the polling loop for the next record ourselves.
	next := func() bool {
		for {
			if stream.TryNext(ctx) {
				return true
			}
			if stream.Err() != nil || stream.ID() == 0 {
				return false
			}
			// If we have no pending batches, then we can accept this resume token as the new checkpoint, this
			// is important to advance our oplog position while the collections we're streaming don't have changes.
			// If there are batches in flight, then we just drop the resume token - we can pick up it back up
			// next time after we poll for changes.
			if cp.Pending() == 0 {
				m.resumeTokenMu.Lock()
				m.resumeToken = stream.ResumeToken()
				if m.checkpointFlusher == nil {
					err := m.checkpoint.Store(ctx, m.resumeToken)
					if err != nil {
						m.logger.Warnf("unable to store checkpoint in cache: %v", err)
					}
				}
				m.resumeTokenMu.Unlock()
			}
		}
	}
	for next() {
		var data bson.M
		if err := stream.Decode(&data); err != nil {
			return fmt.Errorf("unable to decode document: %w", err)
		}
		opType, ok := data["operationType"].(string)
		if !ok {
			return fmt.Errorf("unable to extract operation type from change string, got: %s", data)
		}
		var doc any
		var keyOnly bool // true when doc is documentKey-only or synthetic partial update
		switch opType {
		case "update":
			if m.docMode == documentModePartialUpdate {
				key, ok := data["documentKey"].(bson.M)
				if !ok {
					return fmt.Errorf("missing document key in update, got: %s", data)
				}
				desc, ok := data["updateDescription"].(bson.M)
				if !ok {
					return fmt.Errorf("missing description in update, got: %s", data)
				}
				paths, _ := desc["disambiguatedPaths"].(bson.M)
				if paths == nil {
					paths = bson.M{}
				}
				normalizePath := func(path string) any {
					if unambiguous, ok := paths[path]; ok {
						return unambiguous
					} else {
						return strings.Split(path, ".")
					}
				}
				ops := bson.A{}
				updates, ok := desc["updatedFields"].(bson.M)
				if !ok {
					return fmt.Errorf("unexpected updatedFields in update operation: %s", data)
				}
				for k, v := range updates {
					ops = append(ops, bson.M{
						"path":  normalizePath(k),
						"type":  "set",
						"value": v,
					})
				}
				removals, ok := desc["removedFields"].(bson.A)
				if !ok {
					return fmt.Errorf("unexpected removedFields in update operation: %s", data)
				}
				for _, path := range removals {
					path, ok := path.(string)
					if !ok {
						return fmt.Errorf("unexpected removedFields element in update operation: %s", data)
					}
					ops = append(ops, bson.M{
						"path":  normalizePath(path),
						"type":  "unset",
						"value": nil,
					})
				}
				truncs, ok := desc["truncatedArrays"].(bson.A)
				if !ok {
					return fmt.Errorf("unexpected truncatedArrays in update operation: %s", data)
				}
				for _, truncated := range truncs {
					truncated, ok := truncated.(bson.M)
					if !ok {
						return fmt.Errorf("unexpected truncatedArrays element in update operation: %s", data)
					}
					path, ok := truncated["field"].(string)
					if !ok {
						return fmt.Errorf("unexpected truncatedArrays field in update operation: %s", data)
					}
					ops = append(ops, bson.M{
						"path":  normalizePath(path),
						"type":  "truncatedArray",
						"value": truncated["newSize"],
					})
				}
				key["operations"] = ops
				doc = key
				keyOnly = true // synthetic structure, don't infer schema from it
				break
			}
			fallthrough
		case "insert", "replace":
			afterDoc, afterOk := data["fullDocument"]
			if !afterOk {
				return fmt.Errorf("%s event did not have fullDocument", opType)
			}
			if afterDoc == nil {
				// In update_lookup mode, fullDocument is null when the document
				// is deleted before the post-update lookup completes. Fall back
				// to documentKey so we still emit the event.
				if opType == "update" {
					doc = data["documentKey"]
					keyOnly = true
					break
				}
				return fmt.Errorf("%s event had null fullDocument", opType)
			}
			doc = afterDoc
		case "delete":
			doc = data["fullDocumentBeforeChange"]
			if doc == nil {
				// this is when pre images are not available
				doc = data["documentKey"]
				keyOnly = true
			}
		case "invalidate":
			return errors.New("watch stream invalidated")
		default:
			// Otherwise skip the other kinds of events
			continue
		}
		coll, ok := bsonGetPath(data, "ns", "coll").(string)
		if !ok {
			return fmt.Errorf("unable to extract collection from change stream, got: %s", data)
		}
		optime, ok := data["clusterTime"].(bson.Timestamp)
		if !ok {
			return fmt.Errorf("unable to extract optime from change stream, got: %T", data["clusterTime"])
		}
		msg, err := m.newMongoDBCDCMessage(doc, opType, coll, optime, keyOnly)
		if err != nil {
			return fmt.Errorf("unable to create message from change stream event: %w", err)
		}
		mb = append(mb, msg)
		if stream.RemainingBatchLength() == 0 {
			resolve, err := cp.Track(ctx, stream.ResumeToken(), int64(len(mb)))
			if err != nil {
				return err
			}
			ackFn := func(ctx context.Context, err error) error {
				if err != nil {
					return err
				}
				resumeToken := resolve()
				if resumeToken == nil || *resumeToken == nil {
					return nil
				}
				m.resumeTokenMu.Lock()
				defer m.resumeTokenMu.Unlock()
				m.resumeToken = *resumeToken
				if m.checkpointFlusher == nil {
					return m.checkpoint.Store(ctx, m.resumeToken)
				}
				return nil
			}
			select {
			case m.readChan <- mongoBatch{mb, ackFn}:
			case <-ctx.Done():
			case <-m.shutsig.SoftStopChan():
			}
			mb = nil
		}
	}
	return stream.Err()
}

// newMongoDBCDCMessage creates a service.Message from a BSON document with
// appropriate metadata. When keyOnly is true the document represents only a
// documentKey (e.g. a delete without pre-images) or a synthetic partial-update
// structure — schema inference is skipped and only the cached schema (if any)
// is attached.
func (m *mongoCDC) newMongoDBCDCMessage(doc any, operationType, collectionName string, opTime bson.Timestamp, keyOnly bool) (msg *service.Message, err error) {
	var b []byte
	if doc != nil {
		// Replace Decimal128 values with canonical decimal strings so the
		// emitted body matches the schema.BigDecimal value contract.
		doc = normaliseDecimal128(doc)
		b, err = bson.MarshalExtJSON(doc, m.marshalCanonical, false)
		if err != nil {
			return nil, fmt.Errorf("error marshalling bson to json: %w", err)
		}
	} else {
		b = []byte("null")
	}
	msg = service.NewMessage(b)
	msg.MetaSetMut("operation", operationType)
	msg.MetaSetMut("collection", collectionName)
	// BSON has a special timestamp type for internal MongoDB use and is not associated with the regular Date type.
	// This internal timestamp type is a 64 bit value where:
	// the most significant 32 bits are a time_t value (seconds since the Unix epoch)
	// the least significant 32 bits are an incrementing ordinal for operations within a given second.
	// This is the JSON format for a timestamp, but the normalize serialization stuff doesn't support writing
	// one at the top level.
	msg.MetaSetMut("operation_time", fmt.Sprintf(`{"$timestamp":{"t":%d,"i":%d}}`, opTime.T, opTime.I))

	// Attach schema metadata.
	if docM, ok := doc.(bson.M); ok {
		var s any
		if keyOnly {
			s = m.getCachedSchema(collectionName)
		} else {
			s = m.getOrInferCollectionSchema(collectionName, docM)
		}
		if s != nil {
			msg.MetaSetImmut("schema", service.ImmutableAny{V: s})
		}
	}
	return msg, nil
}

// getOrInferCollectionSchema returns the cached schema if the document's key
// set matches, or infers a new schema and updates the cache.
func (m *mongoCDC) getOrInferCollectionSchema(collectionName string, doc bson.M) any {
	docKeys := sortedMapKeys(doc)

	m.collectionSchemasMu.Lock()
	defer m.collectionSchemasMu.Unlock()

	if cached, ok := m.collectionSchemas[collectionName]; ok && slices.Equal(cached.keys, docKeys) {
		return cached.schema
	}

	// Cache miss or key-set mismatch — (re-)infer.
	s, keys := inferSchemaFromDocument(collectionName, doc)
	m.collectionSchemas[collectionName] = &cachedSchema{schema: s, keys: keys}
	return s
}

// getCachedSchema returns the cached schema for a collection without inference.
// Used for keyOnly documents (deletes with documentKey, partial updates).
func (m *mongoCDC) getCachedSchema(collectionName string) any {
	m.collectionSchemasMu.RLock()
	defer m.collectionSchemasMu.RUnlock()
	if cached, ok := m.collectionSchemas[collectionName]; ok {
		return cached.schema
	}
	return nil
}

func (m *mongoCDC) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	select {
	case mb := <-m.readChan:
		return mb.documents, mb.ackFn, nil
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	case <-m.shutsig.HasStoppedChan():
		return nil, nil, service.ErrNotConnected
	case err := <-m.errorChan:
		return nil, nil, err
	}
}

func (m *mongoCDC) Close(ctx context.Context) error {
	// The disconnect must outlive the shutdown deadline, otherwise an expired
	// ctx makes the driver skip its endSessions handshake.
	disconnectCtx := context.WithoutCancel(ctx)
	defer func() {
		if m.client != nil {
			_ = m.client.Disconnect(disconnectCtx)
			m.client, m.db = nil, nil
		}
	}()
	if m.shutsig == nil {
		return nil
	}
	m.shutsig.TriggerSoftStop()
	ctx, cancel := m.shutsig.HasStoppedCtx(ctx)
	defer cancel()
	<-ctx.Done()
	m.shutsig.TriggerHardStop()
	<-m.shutsig.HasStoppedChan()
	return ctx.Err()
}
