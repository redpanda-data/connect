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
	"sync"
	"time"

	"github.com/Jeffail/shutdown"
	"github.com/Masterminds/semver"
	"github.com/dustin/go-humanize"
	"github.com/redpanda-data/benthos/v4/public/bloblang"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/asyncroutine"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
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
	fieldReadBatchSize       = "read_batch_size"
	fieldReadMaxWait         = "read_max_wait"
	fieldUsePreAndPostImages = "use_pre_and_post_images"
	fieldJSONMarshalMode     = "json_marshal_mode"

	marshalModeCanonical string = "canonical"
	marshalModeRelaxed   string = "relaxed"
)

func spec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Summary(`Streams changes from a MongoDB replica set.`).
		Description(`Read from a MongoDB replica set using https://www.mongodb.com/docs/manual/changeStreams/[^Change Streams]. It's only possible to watch for changes when using a sharded MongoDB or a MongoDB cluster running as a replica set.

By default MongoDB does not propagate changes in all cases. In order to capture all changes (including deletes) in a MongoDB cluster one needs to enable pre and post image saving and the collection needs to also enable saving these pre and post images. For more information see https://www.mongodb.com/docs/manual/changeStreams/#change-streams-with-document-pre--and-post-images[^MongoDB documentation].`).
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
			service.NewBoolField(fieldUsePreAndPostImages).
				Description("If true, enables uses pre and post images stored in MongoDB to ensure that updates and deletes have the full document. Otherwise update operations fetch the full current document and deletes have `null` payloads.").
				Default(false).
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
	var collections []string
	if collections, err = conf.FieldStringList(fieldCollections); err != nil {
		return
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
	if cdc.usePreAndPostImages, err = conf.FieldBool(fieldUsePreAndPostImages); err != nil {
		return
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
				cdc.resumeTokensMu.Lock()
				defer cdc.resumeTokensMu.Unlock()
				err := cdc.checkpoint.Store(ctx, cdc.resumeTokens)
				if err != nil {
					res.Logger().Warnf("unable to store checkpoints in cache: %v", err)
				}
			},
		)
	}
	opts := options.Client().
		SetConnectTimeout(10 * time.Second).
		SetTimeout(30 * time.Second).
		SetServerSelectionTimeout(30 * time.Second).
		ApplyURI(url).
		SetAppName(appName)

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
	for _, coll := range collections {
		cdc.collections = append(cdc.collections, cdc.db.Collection(coll))
	}
	if len(cdc.collections) != 1 {
		return nil, errors.New("exactly one collection must be specified")
	}
	return service.AutoRetryNacksBatchedToggled(conf, cdc)
}

type mongoBatch struct {
	documents service.MessageBatch
	ackFn     service.AckFunc
}

type mongoCDC struct {
	client      *mongo.Client
	db          *mongo.Database
	collections []*mongo.Collection
	logger      *service.Logger

	shutsig   *shutdown.Signaller
	readChan  chan mongoBatch
	errorChan chan error

	readBatchSize       int
	streamMaxWait       time.Duration
	usePreAndPostImages bool
	marshalCanonical    bool

	snapshotParallelism    int // if > 0 then enabled
	snapshotSemaphore      *semaphore.Weighted
	useAutoBucketSnapshots bool

	checkpoint        *checkpointCache
	checkpointFlusher *asyncroutine.Periodic

	resumeTokens   map[string]bson.Raw
	resumeTokensMu sync.Mutex
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
		return fmt.Errorf("unable to determine mongodb version")
	}
	version, err := semver.NewVersion(versionStr)
	if err != nil {
		return fmt.Errorf("unable to parse mongodb version: %w", err)
	}
	m.resumeTokens, err = m.checkpoint.Load(ctx)
	if err != nil {
		return fmt.Errorf("unable to load checkpoints from cache: %w", err)
	}
	// Set the stream start when starting fresh to be the current oplog end time.
	r = m.db.RunCommand(ctx, bson.M{"hello": 1})
	if r.Err() != nil {
		return fmt.Errorf("unable to determine replication info (is your mongodb instance running as a replication set?): %w", r.Err())
	}
	var helloReply bson.D
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
		var wg sync.WaitGroup
		for i := range m.collections {
			wg.Add(1)
			coll := m.collections[i]
			opts := options.ChangeStream().
				SetBatchSize(int32(m.readBatchSize)).
				SetMaxAwaitTime(m.streamMaxWait)
			if !m.usePreAndPostImages {
				opts = opts.SetFullDocument(options.UpdateLookup)
			} else {
				opts = opts.SetFullDocument(options.Required)
				if version.Major() >= 6 {
					opts = opts.SetFullDocumentBeforeChange(options.Required)
				}
			}
			var skipSnapshot bool
			func() {
				m.resumeTokensMu.Lock()
				defer m.resumeTokensMu.Unlock()
				if token, ok := m.resumeTokens[coll.Name()]; ok {
					// TODO: Handle the resume token becoming invalid due to collection rename/drop
					opts = opts.SetResumeAfter(token)
					skipSnapshot = true
				} else {
					// If there are no writes between snapshot and streaming, we want to skip the last
					// document that will be read in the snapshot.
					nextTs := nextTimestamp(ts)
					opts = opts.SetStartAtOperationTime(&nextTs)
				}
			}()
			go func() {
				defer wg.Done()
				if !skipSnapshot {
					if err := m.readSnapshot(ctx, coll); err != nil {
						select {
						case m.errorChan <- fmt.Errorf("error watching change stream for %s: %w", coll.Name(), err):
						default:
						}
						return
					}
				}
				if err := m.readFromStream(ctx, coll, opts); err != nil {
					select {
					case m.errorChan <- fmt.Errorf("error watching change stream for %s: %w", coll.Name(), err):
					default:
					}
				}
			}()
		}
		wg.Wait()
		func() {
			ctx, cancel := shutsig.HardStopCtx(context.Background())
			defer cancel()
			m.resumeTokensMu.Lock()
			defer m.resumeTokensMu.Unlock()
			if err := m.checkpoint.Store(ctx, m.resumeTokens); err != nil {
				m.logger.Warnf("unable to store checkpoint before stopping `mongodb_cdc`: %v", err)
			}
		}()
	}()
	return nil
}

func (m *mongoCDC) readSnapshot(ctx context.Context, coll *mongo.Collection) error {
	if m.snapshotParallelism == 0 {
		return nil
	}
	if m.snapshotParallelism > 1 {
		return m.readParallelSnapshot(ctx, coll)
	}
	return m.readSnapshotRange(ctx, coll, bson.MinKey{}, bson.MaxKey{})
}

func getCollectionSize(ctx context.Context, collection *mongo.Collection) (int64, error) {
	cmd := bson.M{"collStats": collection.Name()}
	var result bson.D
	if err := collection.Database().RunCommand(ctx, cmd).Decode(&result); err != nil {
		return 0, fmt.Errorf("error estimating collection size: %w", err)
	}
	size, err := bloblang.ValueAsInt64(bsonGetPath(result, "size"))
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
	var result bson.D
	if err := m.db.RunCommand(ctx, command).Decode(&result); err != nil {
		return nil, err
	}
	splitKeys, ok := bsonGetPath(result, "splitKeys").(bson.A)
	if !ok {
		return nil, fmt.Errorf("unexpected splitVector result format: %s", result.String())
	}
	var prev any = bson.MinKey{}
	ranges := [][2]any{}
	for i := range splitKeys {
		v, ok := splitKeys[i].(bson.D)
		if !ok {
			return nil, fmt.Errorf("unexpected splitVector result format: %s", result.String())
		}
		id := bsonGetPath(v, "_id")
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
			Value: bson.M{
				"groupBy": "$_id",
				"buckets": m.snapshotParallelism,
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
		var bucket bson.D
		if err := cursor.Decode(&bucket); err != nil {
			return nil, fmt.Errorf("unable to extract bucket: %w", err)
		}

		ranges = append(ranges, [2]any{
			bsonGetPath(bucket, "_id", "min"),
			bsonGetPath(bucket, "_id", "max"),
		})
	}
	if cursor.Err() != nil {
		return nil, fmt.Errorf("unable to compute buckets: %w", err)
	}
	if len(ranges) == 0 {
		return [][2]any{{bson.MinKey{}, bson.MaxKey{}}}, nil
	}
	ranges[0][0] = bson.MinKey{}
	ranges[len(ranges)-1][1] = bson.MaxKey{}
	return ranges, nil
}

func (m *mongoCDC) readParallelSnapshot(ctx context.Context, coll *mongo.Collection) error {
	begin := time.Now()
	ranges, err := m.getParallelRanges(ctx, coll)
	if err != nil {
		m.logger.Infof("unable to determine split points for queries over %s, falling back to sequential scan due to: %v", coll.Name(), err)
		return m.readSnapshotRange(ctx, coll, bson.MinKey{}, bson.MaxKey{})
	}
	m.logger.Debugf("determined collection split points in %v", time.Since(begin))
	g, ctx := errgroup.WithContext(ctx)
	for _, r := range ranges {
		minKey := r[0]
		maxKey := r[1]
		g.Go(func() error {
			return m.readSnapshotRange(ctx, coll, minKey, maxKey)
		})
	}
	return g.Wait()
}

func noopAckFn(context.Context, error) error {
	return nil
}

func (m *mongoCDC) readSnapshotRange(ctx context.Context, coll *mongo.Collection, start, end any) error {
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
		b, err := bson.MarshalExtJSON(doc, m.marshalCanonical, false)
		if err != nil {
			return fmt.Errorf("error marshalling bson to json: %w", err)
		}
		msg := service.NewMessage(b)
		msg.MetaSetMut("operation", "read")
		msg.MetaSetMut("collection", coll.Name())
		mb = append(mb, msg)
		if cursor.RemainingBatchLength() == 0 {
			select {
			case m.readChan <- mongoBatch{mb, noopAckFn}:
			case <-ctx.Done():
			}
			mb = nil
		}
	}
	if cursor.Err() != nil {
		return fmt.Errorf("failed to read snapshot: %w", err)
	}
	return nil
}

func (m *mongoCDC) readFromStream(ctx context.Context, coll *mongo.Collection, opts *options.ChangeStreamOptionsBuilder) error {
	stream, err := coll.Watch(ctx, mongo.Pipeline{}, opts)
	if err != nil {
		return fmt.Errorf("error opening change stream for %s: %w", coll.Name(), err)
	}
	stream.SetBatchSize(int32(m.readBatchSize))
	var mb service.MessageBatch
	for stream.Next(ctx) {
		var data bson.M
		if err := stream.Decode(&data); err != nil {
			return fmt.Errorf("unable to decode document: %w", err)
		}
		opType := data["operationType"]
		var doc any
		switch opType {
		case "insert", "replace", "update":
			afterDoc, afterOk := data["fullDocument"]
			if !afterOk {
				return fmt.Errorf("%s event did not have fullDocument", opType)
			}
			doc = afterDoc
		case "delete":
			doc = data["fullDocumentBeforeChange"]
		case "invalidate":
			return errors.New("watch stream invalidated")
		default:
			// Otherwise skip the other kinds of events
			continue
		}
		var b []byte
		if doc != nil {
			b, err = bson.MarshalExtJSON(doc, m.marshalCanonical, false)
			if err != nil {
				return fmt.Errorf("error marshalling bson to json: %w", err)
			}
		} else {
			b = []byte("null")
		}
		msg := service.NewMessage(b)
		msg.MetaSetMut("operation", opType)
		msg.MetaSetMut("collection", coll.Name())
		mb = append(mb, msg)
		if stream.RemainingBatchLength() == 0 {
			ackFn := func(ctx context.Context, err error) error {
				if err != nil {
					return err
				}
				m.resumeTokensMu.Lock()
				defer m.resumeTokensMu.Unlock()
				m.resumeTokens[coll.Name()] = stream.ResumeToken()
				if m.checkpointFlusher == nil {
					return m.checkpoint.Store(ctx, m.resumeTokens)
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
