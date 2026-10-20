// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package cdc

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readconcern"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/replication/incrementalsnapshot"
)

type (
	// incCoordinator is the incremental snapshot coordinator for MongoDB:
	// positions are cluster times, as carried by every change event.
	incCoordinator = incrementalsnapshot.Coordinator[bson.Timestamp, clusterTimeWatermark]
	// incCoordinatorConfig configures an incCoordinator.
	incCoordinatorConfig = incrementalsnapshot.CoordinatorConfig[bson.Timestamp, clusterTimeWatermark]
)

// clusterTimeWatermark is a cluster time read either side of a chunk read.
//
// Positions handed to the coordinator mean "the stream is complete through
// this cluster time, inclusive", so the chunk is safe to release once the
// stream is through T: every write the read could see has streamed, and any
// that touched a buffered document has already removed it.
//
// MongoDB exposes no list of in-flight transactions, so unlike Postgres there
// is nothing to compare for the window opening beyond T itself.
type clusterTimeWatermark struct {
	T bson.Timestamp
	// quiesced records that the stream was already through T when the
	// watermark was read, so nothing the read saw is still to stream.
	quiesced bool
}

// newClusterTimeWatermark builds the watermark for a cluster time read while
// the stream was complete through streamedThrough.
func newClusterTimeWatermark(at, streamedThrough bson.Timestamp) clusterTimeWatermark {
	return clusterTimeWatermark{T: at, quiesced: !streamedThrough.Before(at)}
}

// OpensAt reports whether the stream is through T.
func (w clusterTimeWatermark) OpensAt(pos bson.Timestamp) bool {
	return !pos.Before(w.T)
}

// ClosesAt reports whether the stream is through T, so every write the chunk
// read could have seen has already streamed.
func (w clusterTimeWatermark) ClosesAt(pos bson.Timestamp) bool {
	return !pos.Before(w.T)
}

// Quiesced reports whether the stream was through T when the watermark was
// read. An equal, quiesced pair lets the coordinator release the chunk
// without waiting for another streamed position.
func (w clusterTimeWatermark) Quiesced() bool {
	return w.quiesced
}

// prevTimestamp returns the cluster time just before ts. A change event at ts
// only proves the stream is through the tick before it: the events of one
// transaction share a cluster time, and the rest may not have streamed yet.
func prevTimestamp(ts bson.Timestamp) bson.Timestamp {
	switch {
	case ts.I > 0:
		return bson.Timestamp{T: ts.T, I: ts.I - 1}
	case ts.T > 0:
		return bson.Timestamp{T: ts.T - 1, I: math.MaxUint32}
	default:
		return bson.Timestamp{}
	}
}

// keyStringTimestampType is the KeyString type byte for a BSON timestamp,
// which leads every change stream resume token's _data.
const keyStringTimestampType = 0x82

// resumeTokenTimestamp extracts the cluster time a resume token points at.
//
// The token is opaque by contract, but its _data has carried a KeyString
// starting with the cluster time since change streams were introduced: a hex
// string from 4.2 on, a binary value on 4.0. MongoDB's own Kafka connector
// decodes it the same way. Only the post-batch token of an empty poll needs
// this; change events carry their cluster time directly.
func resumeTokenTimestamp(token bson.Raw) (bson.Timestamp, error) {
	if token == nil {
		return bson.Timestamp{}, errors.New("no resume token")
	}
	data, err := token.LookupErr("_data")
	if err != nil {
		return bson.Timestamp{}, fmt.Errorf("resume token has no _data: %w", err)
	}

	var keyString []byte
	switch data.Type {
	case bson.TypeString:
		if keyString, err = hex.DecodeString(data.StringValue()); err != nil {
			return bson.Timestamp{}, fmt.Errorf("decoding resume token _data: %w", err)
		}
	case bson.TypeBinary:
		_, keyString = data.Binary()
	default:
		return bson.Timestamp{}, fmt.Errorf("unexpected resume token _data type %s", data.Type)
	}

	const timestampLen = 1 + 8
	if len(keyString) < timestampLen || keyString[0] != keyStringTimestampType {
		return bson.Timestamp{}, errors.New("resume token does not start with a cluster time")
	}
	return bson.Timestamp{
		T: binary.BigEndian.Uint32(keyString[1:5]),
		I: binary.BigEndian.Uint32(keyString[5:9]),
	}, nil
}

// documentKey turns an _id into the coordinator's key: one canonical
// Extended JSON string.
//
// Text survives the coordinator's State JSON with its BSON type intact
// (ObjectIds, decimals and binary included), which a raw Go value would not:
// an ObjectId bound restored as a plain string would select a different BSON
// type range entirely. Canonical form also keeps the type in the window's
// identity, so "1" and 1 never collide, and taking it from the raw value
// keeps an embedded document's field order, which _id equality depends on.
func documentKey(id bson.RawValue) (incrementalsnapshot.PrimaryKey, error) {
	if id.Type == bson.Type(0) {
		return nil, errors.New("document has no _id")
	}
	b, err := bson.MarshalExtJSON(bson.D{{Key: "_id", Value: id}}, true, false)
	if err != nil {
		return nil, fmt.Errorf("encoding _id: %w", err)
	}
	return incrementalsnapshot.PrimaryKey{string(b)}, nil
}

// documentKeyValue reverses documentKey, returning the _id ready to use as a
// query value.
func documentKeyValue(pk incrementalsnapshot.PrimaryKey) (bson.RawValue, error) {
	if len(pk) != 1 {
		return bson.RawValue{}, fmt.Errorf("expected a single _id key, got %d parts", len(pk))
	}
	text, ok := pk[0].(string)
	if !ok {
		return bson.RawValue{}, fmt.Errorf("expected an Extended JSON _id key, got %T", pk[0])
	}
	var doc bson.Raw
	if err := bson.UnmarshalExtJSON([]byte(text), true, &doc); err != nil {
		return bson.RawValue{}, fmt.Errorf("decoding _id key %q: %w", text, err)
	}
	id, err := doc.LookupErr("_id")
	if err != nil {
		return bson.RawValue{}, fmt.Errorf("decoding _id key %q: %w", text, err)
	}
	return id, nil
}

// chunkDoc is one document a chunk read returned: its _id, raw so it keeps its
// exact BSON form, and the document decoded for the message body.
type chunkDoc struct {
	id  bson.RawValue
	doc bson.M
}

// chunkReader is the database access fetchChunk needs, separated so the bound
// handling can be tested without a server.
type chunkReader interface {
	// findRange returns up to limit documents in _id order from lower
	// (inclusive; zero RawValue for the collection start) up to upper
	// (exclusive), using index bounds so no BSON type is skipped.
	findRange(ctx context.Context, coll string, lower, upper bson.RawValue, limit int) ([]chunkDoc, error)
	// findByID returns the document with this _id, or nil when there is none.
	findByID(ctx context.Context, coll string, id bson.RawValue) (*chunkDoc, error)
}

// fetchChunk reads the chunk after lower, up to and including upper, in _id
// order: the coordinator's FetchChunk contract.
//
// The range is read through index bounds rather than $gt/$lte. A comparison
// operator only matches values of the bound's own BSON type, so a collection
// with mixed _id types would silently lose every document of the other types.
// Index bounds have the opposite shape (min inclusive, max exclusive), so the
// lower bound is dropped when it comes back and the upper key is looked up on
// its own once the range is exhausted.
func fetchChunk(ctx context.Context, r chunkReader, table incrementalsnapshot.TableID, lower, upper incrementalsnapshot.PrimaryKey, limit int) ([]incrementalsnapshot.Row, error) {
	var lowerID bson.RawValue
	if lower != nil {
		var err error
		if lowerID, err = documentKeyValue(lower); err != nil {
			return nil, err
		}
	}
	upperID, err := documentKeyValue(upper)
	if err != nil {
		return nil, err
	}
	if lower != nil && lowerID.Equal(upperID) {
		// The previous chunk ended on upper, so the collection is done. A
		// resume asks once more, since the checkpoint does not record that,
		// and the server rejects an index range whose bounds are equal.
		return nil, nil
	}

	// One extra, since the lower bound itself may come back first.
	docs, err := r.findRange(ctx, table.Table, lowerID, upperID, limit+1)
	if err != nil {
		return nil, err
	}
	if lower != nil && len(docs) > 0 && docs[0].id.Equal(lowerID) {
		docs = docs[1:]
	}
	if len(docs) > limit {
		docs = docs[:limit]
	}

	// The range stops short of upper, so a short read means only upper itself
	// can be left. A full read leaves it for the next chunk.
	if len(docs) < limit {
		last, err := r.findByID(ctx, table.Table, upperID)
		if err != nil {
			return nil, err
		}
		if last != nil {
			docs = append(docs, *last)
		}
	}

	rows := make([]incrementalsnapshot.Row, 0, len(docs))
	for _, d := range docs {
		pk, err := documentKey(d.id)
		if err != nil {
			return nil, err
		}
		rows = append(rows, incrementalsnapshot.Row{Table: table, PK: pk, Data: d.doc})
	}
	return rows, nil
}

// incSnapshotDeps is the coordinator's Deps over a live database. Methods run
// on the stream goroutine, which is the only goroutine that advances
// streamedThrough, so the position read while planning is stable.
type incSnapshotDeps struct {
	db *mongo.Database
	// watermarkCollection is any watched collection: the watermark read only
	// needs a namespace to address, not its contents.
	watermarkCollection string
	// streamedThrough reports how far the change stream has been consumed.
	streamedThrough func() bson.Timestamp
}

var _ incrementalsnapshot.Deps[clusterTimeWatermark] = (*incSnapshotDeps)(nil)

// collection addresses coll for a chunk read. Reads are majority, matching the
// watermarks and the change stream: a local read could see a write that is not
// yet majority-committed, whose change only streams after the chunk is
// released (moving the document back to its older states first), or that a
// failover rolls back, leaving downstream a state that never durably existed.
func (d *incSnapshotDeps) collection(coll string) *mongo.Collection {
	return d.db.Collection(coll, options.Collection().SetReadConcern(readconcern.Majority()))
}

// retryable marks failures that a later attempt can be expected to clear, such
// as an election or a dropped connection, so the coordinator defers the read
// instead of failing the stream.
func retryable(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) {
		// The input is stopping, which no retry outlives.
		return err
	}
	if mongo.IsNetworkError(err) || mongo.IsTimeout(err) {
		return fmt.Errorf("%w: %w", incrementalsnapshot.ErrRetryable, err)
	}
	return err
}

// ResolvePrimaryKey reports _id, the only key every document is guaranteed to
// have, after checking the collection is one a chunk read can address.
func (d *incSnapshotDeps) ResolvePrimaryKey(ctx context.Context, table incrementalsnapshot.TableID) ([]string, error) {
	specs, err := d.db.ListCollectionSpecifications(ctx, bson.D{{Key: "name", Value: table.Table}})
	if err != nil {
		return nil, retryable(fmt.Errorf("listing collection %s: %w", table, err))
	}
	if len(specs) == 0 {
		return nil, fmt.Errorf("%w: collection %s does not exist", incrementalsnapshot.ErrTableUnusable, table)
	}
	if specs[0].Type == "view" {
		return nil, fmt.Errorf("%w: %s is a view", incrementalsnapshot.ErrTableUnusable, table)
	}
	return []string{"_id"}, nil
}

// ResolveMaxKey returns the collection's largest _id. Sorting compares across
// BSON types, unlike a range query, so this is the true maximum.
func (d *incSnapshotDeps) ResolveMaxKey(ctx context.Context, table incrementalsnapshot.TableID, _ []string) (incrementalsnapshot.PrimaryKey, error) {
	raw, err := d.collection(table.Table).FindOne(ctx, bson.D{},
		options.FindOne().
			SetSort(bson.D{{Key: "_id", Value: -1}}).
			SetProjection(bson.D{{Key: "_id", Value: 1}}),
	).Raw()
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, nil
	}
	if err != nil {
		return nil, retryable(fmt.Errorf("reading max _id: %w", err))
	}
	return documentKey(raw.Lookup("_id"))
}

// ResolveWatermark reads the cluster time of a majority read. Every write a
// majority chunk read started after it can see is at or before it, which is
// what the window needs. Any command would carry operationTime; a find keeps it
// to the read path, and majority matches what the change stream reports.
func (d *incSnapshotDeps) ResolveWatermark(ctx context.Context) (clusterTimeWatermark, error) {
	streamed := d.streamedThrough()
	var reply bson.Raw
	err := d.db.RunCommand(ctx, bson.D{
		{Key: "find", Value: d.watermarkCollection},
		{Key: "limit", Value: 1},
		{Key: "projection", Value: bson.D{{Key: "_id", Value: 1}}},
		{Key: "readConcern", Value: bson.D{{Key: "level", Value: "majority"}}},
	}).Decode(&reply)
	if err != nil {
		return clusterTimeWatermark{}, retryable(fmt.Errorf("reading cluster time: %w", err))
	}
	t, i, ok := reply.Lookup("operationTime").TimestampOK()
	if !ok {
		return clusterTimeWatermark{}, errors.New("reply carries no operationTime: incremental snapshots need a replica set or sharded cluster")
	}
	return newClusterTimeWatermark(bson.Timestamp{T: t, I: i}, streamed), nil
}

// Prepare has nothing to do: a cluster time needs no connection state.
func (*incSnapshotDeps) Prepare(context.Context) error {
	return nil
}

// FetchChunk implements the coordinator's chunk read. Refer to fetchChunk.
func (d *incSnapshotDeps) FetchChunk(ctx context.Context, table incrementalsnapshot.TableID, _ []string, lower, upper incrementalsnapshot.PrimaryKey, limit int) ([]incrementalsnapshot.Row, error) {
	return fetchChunk(ctx, d, table, lower, upper, limit)
}

func (d *incSnapshotDeps) findRange(ctx context.Context, coll string, lower, upper bson.RawValue, limit int) ([]chunkDoc, error) {
	var minID any = bson.MinKey{}
	if lower.Type != bson.Type(0) {
		minID = lower
	}
	cursor, err := d.collection(coll).Find(ctx, bson.D{},
		options.Find().
			SetHint(bson.D{{Key: "_id", Value: 1}}).
			SetMin(bson.D{{Key: "_id", Value: minID}}).
			SetMax(bson.D{{Key: "_id", Value: upper}}).
			SetSort(bson.D{{Key: "_id", Value: 1}}).
			SetLimit(int64(limit)).
			SetBatchSize(int32(limit)),
	)
	if err != nil {
		return nil, retryable(fmt.Errorf("reading chunk: %w", err))
	}
	defer cursor.Close(ctx)

	var docs []chunkDoc
	for cursor.Next(ctx) {
		d, err := decodeChunkDoc(cursor)
		if err != nil {
			return nil, err
		}
		docs = append(docs, d)
	}
	if err := cursor.Err(); err != nil {
		return nil, retryable(fmt.Errorf("reading chunk: %w", err))
	}
	return docs, nil
}

func (d *incSnapshotDeps) findByID(ctx context.Context, coll string, id bson.RawValue) (*chunkDoc, error) {
	res := d.collection(coll).FindOne(ctx, bson.D{{Key: "_id", Value: id}})
	raw, err := res.Raw()
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, nil
	}
	if err != nil {
		return nil, retryable(fmt.Errorf("reading chunk's last document: %w", err))
	}
	var doc bson.M
	if err := res.Decode(&doc); err != nil {
		return nil, fmt.Errorf("unable to decode document: %w", err)
	}
	return &chunkDoc{id: cloneRawValue(raw.Lookup("_id")), doc: doc}, nil
}

func decodeChunkDoc(cursor *mongo.Cursor) (chunkDoc, error) {
	var doc bson.M
	if err := cursor.Decode(&doc); err != nil {
		return chunkDoc{}, fmt.Errorf("unable to decode document: %w", err)
	}
	// Current aliases the cursor's batch buffer, so the _id is copied out.
	return chunkDoc{id: cloneRawValue(cursor.Current.Lookup("_id")), doc: doc}, nil
}

func cloneRawValue(v bson.RawValue) bson.RawValue {
	v.Value = slices.Clone(v.Value)
	return v
}

// incSnapshotRetryCooldown holds the coordinator off after a chunk read
// failed retryably, so an election or a dropped connection is not retried on
// every streamed change.
const incSnapshotRetryCooldown = 5 * time.Second

// incSnapshotRun is one connection's incremental snapshot. Everything but
// construction runs on the stream goroutine, which the coordinator requires.
type incSnapshotRun struct {
	coord  *incCoordinator
	deps   *incSnapshotDeps
	dbName string
	logger *service.Logger
	now    func() time.Time

	// through is how far the change stream has been consumed: every change
	// at or before it has streamed. The watermark reads it while planning.
	through      bson.Timestamp
	heldOffUntil time.Time
	// lastHigh is the high watermark of the chunk most recently read, which
	// is the one emit releases.
	lastHigh bson.Timestamp
	idle     bool

	// encodedState caches state's encoding until stateDirty.
	encodedState []byte
	stateDirty   bool
}

// newIncSnapshotRun builds the coordinator for one connection. resume is the
// stored progress, nil on a fresh start; every configured collection is then
// queued, and on a resume only those the progress does not know about.
func newIncSnapshotRun(db *mongo.Database, collections []string, chunkSize int, resume *incrementalsnapshot.State, logger *service.Logger) (*incSnapshotRun, error) {
	run := &incSnapshotRun{
		dbName:     db.Name(),
		logger:     logger,
		now:        time.Now,
		stateDirty: true,
	}
	run.deps = &incSnapshotDeps{
		db:                  db,
		watermarkCollection: collections[0],
		streamedThrough:     func() bson.Timestamp { return run.through },
	}

	coord, err := incrementalsnapshot.NewCoordinator(incCoordinatorConfig{
		ChunkSize: chunkSize,
		Deps:      &watermarkRecorder{incSnapshotDeps: run.deps, run: run},
		OnTableDropped: func(table incrementalsnapshot.TableID, err error) {
			logger.Warnf("Skipping incremental snapshot of collection %s: %v", table.Table, err)
		},
		OnPlanDeferred: func(err error) {
			run.heldOffUntil = run.now().Add(incSnapshotRetryCooldown)
			logger.Warnf("Incremental snapshot chunk read failed, retrying in %v: %v", incSnapshotRetryCooldown, err)
		},
	}, pruneResume(resume, run.tables(collections), logger))
	if err != nil {
		return nil, err
	}
	run.coord = coord
	if added := coord.AddTables(run.tables(collections)); len(added) > 0 {
		logger.Infof("Incremental snapshot queued collections %v", tableNames(added))
	}
	return run, nil
}

// watermarkRecorder remembers each high watermark, so emitted documents can
// carry the cluster time they were read at.
type watermarkRecorder struct {
	*incSnapshotDeps
	run *incSnapshotRun
}

func (w *watermarkRecorder) ResolveWatermark(ctx context.Context) (clusterTimeWatermark, error) {
	wm, err := w.incSnapshotDeps.ResolveWatermark(ctx)
	if err == nil {
		// The coordinator reads low then high, so the last read is the high
		// watermark of the chunk now buffered.
		w.run.lastHigh = wm.T
	}
	return wm, err
}

func (r *incSnapshotRun) table(collection string) incrementalsnapshot.TableID {
	return incrementalsnapshot.TableID{Schema: r.dbName, Table: collection}
}

func (r *incSnapshotRun) tables(collections []string) []incrementalsnapshot.TableID {
	tables := make([]incrementalsnapshot.TableID, 0, len(collections))
	for _, c := range collections {
		tables = append(tables, r.table(c))
	}
	return tables
}

func tableNames(tables []incrementalsnapshot.TableID) []string {
	names := make([]string, 0, len(tables))
	for _, t := range tables {
		names = append(names, t.Table)
	}
	return names
}

// state encodes the coordinator's checkpointable progress.
//
// It is read for every streamed change, long after the snapshot has gone
// idle, so the encoding is cached and only redone once the progress may have
// moved: after a commit the coordinator acted on, a queued request, or a
// released chunk.
func (r *incSnapshotRun) state() ([]byte, error) {
	if !r.stateDirty && r.encodedState != nil {
		return r.encodedState, nil
	}
	b, err := json.Marshal(r.coord.State())
	if err != nil {
		return nil, fmt.Errorf("encoding incremental snapshot progress: %w", err)
	}
	r.encodedState, r.stateDirty = b, false
	return b, nil
}

// releasedState is state for a chunk being released: the coordinator has just
// advanced its progress past the chunk, so the cached encoding is stale.
func (r *incSnapshotRun) releasedState() ([]byte, error) {
	r.stateDirty = true
	return r.state()
}

// advance records that the stream is complete through pos and gives the
// coordinator the chance to release or read a chunk. Positions only move
// forward: a stale one would let a watermark claim quiescence it has not seen.
func (r *incSnapshotRun) advance(ctx context.Context, pos bson.Timestamp, emit incrementalsnapshot.EmitFunc) error {
	if pos.After(r.through) {
		r.through = pos
	}
	if r.now().Before(r.heldOffUntil) {
		return nil
	}
	changed, err := r.coord.OnCommit(ctx, r.through, emit)
	if changed {
		r.stateDirty = true
	}
	if err != nil {
		return err
	}
	if idle := r.coord.Idle(); idle != r.idle {
		r.idle = idle
		if idle {
			r.logger.Infof("Incremental snapshot complete, every queued collection has been read")
		}
	}
	return nil
}

// streamedChange removes a buffered document the stream just changed, so the
// chunk cannot overwrite the newer state on release.
func (r *incSnapshotRun) streamedChange(collection string, id bson.RawValue) error {
	table := r.table(collection)
	if !r.coord.Snapshotting(table) {
		return nil
	}
	key, err := documentKey(id)
	if err != nil {
		return err
	}
	r.coord.OnStreamedRow(table, key)
	return nil
}

// request queues collections a snapshot-execute signal named, read already or
// not, reporting the ones it queued: a collection still queued or being read
// is left alone.
func (r *incSnapshotRun) request(collections []string) []string {
	added := r.coord.RequeueTables(r.tables(collections))
	if len(added) > 0 {
		r.stateDirty = true
	}
	return tableNames(added)
}

// pruneResume drops the collections stored progress still has queued, or is
// part-way through, that are no longer configured. The stream no longer
// watches them, so nothing could remove a buffered document a later change
// supersedes, and their documents would be emitted for a collection the
// pipeline has stopped reading.
func pruneResume(resume *incrementalsnapshot.State, configured []incrementalsnapshot.TableID, logger *service.Logger) *incrementalsnapshot.State {
	resume = resume.Clone()
	if resume == nil {
		return nil
	}
	var dropped []incrementalsnapshot.TableID
	if resume.CurrentTable != nil && !slices.Contains(configured, *resume.CurrentTable) {
		dropped = append(dropped, *resume.CurrentTable)
		resume.CurrentTable, resume.LastSentPK, resume.MaxPK = nil, nil, nil
	}
	remaining := resume.RemainingTables[:0]
	for _, table := range resume.RemainingTables {
		if slices.Contains(configured, table) {
			remaining = append(remaining, table)
		} else if !slices.Contains(dropped, table) {
			dropped = append(dropped, table)
		}
	}
	resume.RemainingTables = remaining
	if len(dropped) > 0 {
		logger.Warnf("Abandoning the incremental snapshot of collections %v, which are no longer in %s", tableNames(dropped), fieldCollections)
	}
	return resume
}
