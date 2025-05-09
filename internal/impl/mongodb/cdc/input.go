// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package cdc

import (
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

Each message omitted by this plugin has the following metadata:

- operation: either "create", "replace", "delete" or "update" for changes streamed. Documents from the initial snapshot have the operation set to "read".
- collection: the collection the document was written to.
- operation_time: the oplog time for when this operation occurred.
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
				Description("If true, determine parallel snapshot chunks using `$bucketAuto` instead of the `splitVector` command. This allows parallel collection reading in environments where privledged access to the MongoDB cluster is not allowed such as MongoDB Atlas.").
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
	err := service.RegisterBatchInput("mongodb_cdc", spec(), newMongoCDC)
	if err != nil {
		panic(err)
	}
}

func newMongoCDC(conf *service.ParsedConfig, res *service.Resources) (i service.BatchInput, err error) {
	if err := license.CheckRunningEnterprise(res); err != nil {
		return nil, err
	}
	cdc := &mongoCDC{
		readChan:  make(chan mongoBatch),
		errorChan: make(chan error, 1),
		logger:    res.Logger(),
	}
	var url, username, password, dbName, appName string
	if url, err = conf.FieldString(fieldClientURL); err != nil {
		return
	}
	if username, err = conf.FieldString(fieldClientUsername); err != nil {
		return
	}
	if password, err = conf.FieldString(fieldClientPassword); err != nil {
		return
	}
	if appName, err = conf.FieldString(fieldClientAppName); err != nil {
		return
	}
	if dbName, err = conf.FieldString(fieldClientDatabase); err != nil {
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
			func(ctx context.Context) {
				cdc.resumeTokenMu.Lock()
				defer cdc.resumeTokenMu.Unlock()
				if cdc.resumeToken == nil {
					return
				}
				if err := cdc.checkpoint.Store(ctx, cdc.resumeToken); err != nil {
					res.Logger().Warnf("unable to store checkpoints in cache: %v", err)
				}
			},
		)
	}

	if cdc.checkpointLimit, err = conf.FieldInt(fieldCheckpointLimit); err != nil {
		return
	}

	opts := options.Client().
		SetConnectTimeout(10 * time.Second).
		SetTimeout(30 * time.Second).
		SetServerSelectionTimeout(30 * time.Second).
		ApplyURI(url).
		SetAppName(appName).
		SetBSONOptions(&options.BSONOptions{
			DefaultDocumentM: true,
		})

	if username != "" && password != "" {
		creds := options.Credential{
			Username: username,
			Password: password,
		}
		opts.SetAuth(creds)
	}

	cdc.client, err = mongo.Connect(opts)
	if err != nil {
		return nil, fmt.Errorf("unable to connect to mongo: %w", err)
	}
	cdc.db = cdc.client.Database(dbName)
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
		case <-m.errorChan:
			// drain error channel
		default:
		}
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
	ts, ok := bsonGetPath(helloReply, "lastWrite", "majorityOpTime", "ts").(bson.Timestamp)
	if !ok {
		return fmt.Errorf("unable to get oplog last commit timestamp, got %s", helloReply.String())
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
		var skipSnapshot bool
		func() {
			m.resumeTokenMu.Lock()
			defer m.resumeTokenMu.Unlock()
			if m.resumeToken != nil {
				// TODO: Handle the resume token becoming invalid due to collection rename/drop
				opts = opts.SetResumeAfter(m.resumeToken)
				skipSnapshot = true
			} else {
				// If there are no writes between snapshot and streaming, we want to skip the last
				// document that will be read in the snapshot.
				nextTS := nextTimestamp(ts)
				opts = opts.SetStartAtOperationTime(&nextTS)
			}
		}()
		cp := checkpoint.NewCapped[bson.Raw](int64(m.checkpointLimit))
		if !skipSnapshot {
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

func (m *mongoCDC) readSnapshot(ctx context.Context, coll *mongo.Collection, snapshotTime bson.Timestamp, cp *checkpoint.Capped[bson.Raw]) (err error) {
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

func (m *mongoCDC) readParallelSnapshot(ctx context.Context, coll *mongo.Collection, snapshotTime bson.Timestamp, cp *checkpoint.Capped[bson.Raw]) error {
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

func (m *mongoCDC) readSnapshotRange(ctx context.Context, coll *mongo.Collection, snapshotTime bson.Timestamp, cp *checkpoint.Capped[bson.Raw], start, end any) error {
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
		return fmt.Errorf("failed to read snapshot: %w", err)
	}
	cursor.SetBatchSize(int32(m.readBatchSize))
	defer cursor.Close(ctx)
	var mb service.MessageBatch
	for cursor.Next(ctx) {
		var doc bson.M
		if err := cursor.Decode(&doc); err != nil {
			return fmt.Errorf("unable to decode document: %w", err)
		}
		msg, err := m.newMongoDBCDCMessage(doc, "read", coll.Name(), snapshotTime)
		if err != nil {
			return fmt.Errorf("unable to create message from document: %w", err)
		}
		mb = append(mb, msg)
		if cursor.RemainingBatchLength() == 0 {
			resolve, err := cp.Track(ctx, nil, int64(len(mb)))
			if err != nil {
				return fmt.Errorf("unable to create batch: %w", err)
			}
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
				_ = b.ackFn(ctx, nil)
			}
			mb = nil
		}
	}
	if err := cursor.Err(); err != nil {
		return fmt.Errorf("failed to read snapshot: %w", err)
	}
	return nil
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
	for stream.Next(ctx) {
		var data bson.M
		if err := stream.Decode(&data); err != nil {
			return fmt.Errorf("unable to decode document: %w", err)
		}
		opType, ok := data["operationType"].(string)
		if !ok {
			return fmt.Errorf("unable to extract operation type from change string, got: %s", data)
		}
		var doc any
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
				break
			}
			fallthrough
		case "insert", "replace":
			afterDoc, afterOk := data["fullDocument"]
			if !afterOk {
				return fmt.Errorf("%s event did not have fullDocument", opType)
			}
			doc = afterDoc
		case "delete":
			doc = data["fullDocumentBeforeChange"]
			if doc == nil {
				// this is when pre images are not available
				doc = data["documentKey"]
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
		msg, err := m.newMongoDBCDCMessage(doc, opType, coll, optime)
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
				m.resumeToken = stream.ResumeToken()
				if m.checkpointFlusher == nil {
					return m.checkpoint.Store(ctx, m.resumeToken)
				}
				return nil
			}
			select {
			case m.readChan <- mongoBatch{mb, ackFn}:
			case <-ctx.Done():
			}
			mb = nil
		}
	}
	return stream.Err()
}

func (m *mongoCDC) newMongoDBCDCMessage(doc any, operationType, collectionName string, opTime bson.Timestamp) (msg *service.Message, err error) {
	var b []byte
	if doc != nil {
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
	return msg, nil
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
