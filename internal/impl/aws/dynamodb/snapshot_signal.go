// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package dynamodb

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
	smithytime "github.com/aws/smithy-go/time"
	"github.com/cenkalti/backoff/v4"

	"github.com/redpanda-data/benthos/v4/public/service"

	incsnapshot "github.com/redpanda-data/connect/v4/internal/impl/aws/dynamodb/incrementalsnapshot"
	"github.com/redpanda-data/connect/v4/internal/replication"
)

// decodeSignalRecord interprets a signal table stream record. Only INSERTs
// carrying a NewImage are signals (isSignal false otherwise, with all other
// results zero). The raw data attribute is returned so the caller can parse a
// snapshot payload. A malformed signal wraps replication.ErrSignalRejected.
func decodeSignalRecord(rec types.Record) (sig *replication.ControlSignal, data []byte, isSignal bool, err error) {
	if rec.EventName != types.OperationTypeInsert || rec.Dynamodb == nil || len(rec.Dynamodb.NewImage) == 0 {
		return nil, nil, false, nil
	}
	image := rec.Dynamodb.NewImage

	id := signalID(image)

	typ, err := signalStringAttr(image, "type")
	if err != nil {
		return nil, nil, true, err
	}
	dataStr, err := signalStringAttr(image, "data")
	if err != nil {
		return nil, nil, true, err
	}

	data = []byte(dataStr)
	sig, err = replication.DecodeSignal(id, typ, data)
	if err != nil {
		return nil, nil, true, err
	}
	return sig, data, true, nil
}

// signalID renders a signal item's id attribute for logs, "" when absent.
func signalID(image map[string]types.AttributeValue) string {
	v, exists := image["id"]
	if !exists {
		return ""
	}
	return fmt.Sprintf("%v", convertAttributeValue(v))
}

// isReusedSignalID reports whether rec is a MODIFY of an item that carries a
// signal type: a signal written with an id already in the table, which
// DynamoDB streams as a MODIFY and so is not acted on.
func isReusedSignalID(rec types.Record) bool {
	if rec.EventName != types.OperationTypeModify || rec.Dynamodb == nil {
		return false
	}
	_, exists := rec.Dynamodb.NewImage["type"]
	return exists
}

func signalStringAttr(image map[string]types.AttributeValue, name string) (string, error) {
	v, exists := image[name]
	if !exists {
		return "", fmt.Errorf("%w: signal record has no %q attribute", replication.ErrSignalRejected, name)
	}
	s, ok := v.(*types.AttributeValueMemberS)
	if !ok {
		return "", fmt.Errorf("%w: signal %q attribute must be a string", replication.ErrSignalRejected, name)
	}
	return s.Value, nil
}

// handleSignalRecords acts on the control signals in a page read from the
// signal table. Non-signal records are skipped and rejected signals are
// logged, so a bad record never blocks the rest of the page. A
// snapshot-execute signal is retried with backoff until its request is
// durable. It returns false only when ctx ends during those retries; the
// reader must then drop the batch unsent, so the signal is read again.
func (d *dynamoDBCDCInput) handleSignalRecords(ctx context.Context, records []types.Record) bool {
	for _, rec := range records {
		sig, data, isSignal, err := decodeSignalRecord(rec)
		if err != nil {
			d.log.With("id", signalID(rec.Dynamodb.NewImage)).Errorf("Decoding control signal record: %v", err)
			continue
		}
		if !isSignal {
			if isReusedSignalID(rec) {
				d.log.With("id", signalID(rec.Dynamodb.NewImage)).Warn("Control signal ignored: only inserts are signals, so reusing a signal id has no effect; write each signal with a new id")
			}
			continue
		}
		log := d.log.With("id", sig.ID, "type", sig.SignalType)
		switch {
		case sig.SignalType == replication.LogSignalType:
			log.Info(sig.Message)
		case sig.SignalType == replication.SnapshotSignalType:
			if !d.retrySnapshotSignal(ctx, log, data) {
				return false
			}
		case !replication.IsKnownSignalType(sig.SignalType):
			log.Warnf("Control signal %q received but not a recognized type", sig.SignalType)
		}
	}
	return true
}

// retrySnapshotSignal calls handleSnapshotSignal until it returns nil,
// backing off between attempts. It returns false once ctx ends.
func (d *dynamoDBCDCInput) retrySnapshotSignal(ctx context.Context, log *service.Logger, data []byte) bool {
	// The backoff mirrors the reader's hardcoded GetRecords backoff. It only
	// applies while a checkpoint read or write for the signal is failing, and
	// never gives up: the signal is held until the write succeeds or the
	// input stops.
	boff := backoff.NewExponentialBackOff()
	boff.InitialInterval = 200 * time.Millisecond
	boff.MaxInterval = 5 * time.Second
	boff.MaxElapsedTime = 0 // Never give up
	for {
		err := d.handleSnapshotSignal(ctx, log, data)
		if err == nil {
			return true
		}
		if ctx.Err() != nil {
			return false
		}
		wait := boff.NextBackOff()
		log.Errorf("Failed to record snapshot signal, retrying in %v: %v", wait, err)
		if smithytime.SleepWithContext(ctx, wait) != nil {
			return false
		}
	}
}

// handleSnapshotSignal records and queues the incremental snapshots a
// snapshot-execute signal asks for. Rejected requests are logged and return
// nil. A non-nil error means a checkpoint read or write failed, so the
// request could not be judged and the caller must retry it. log carries the
// signal's id and type.
func (d *dynamoDBCDCInput) handleSnapshotSignal(ctx context.Context, log *service.Logger, data []byte) error {
	if d.conf.snapshot.mode != snapshotModeIncremental {
		log.Warn(fmt.Errorf("%w: set incremental_snapshot.enabled to true", replication.ErrSnapshotDisabled).Error())
		return nil
	}
	if d.backfills == nil {
		// Unreachable while connect creates the queue before any reader
		// starts; kept so a signal can never crash the process.
		log.Warn("Snapshot signal ignored: the incremental snapshot queue is not running")
		return nil
	}
	sig, err := replication.ParseSnapshotSignal(data)
	if err != nil {
		log.Errorf("Parsing snapshot signal: %v", err)
		return nil
	}
	// A table named twice is validated, recorded and logged once.
	sig.Tables = incsnapshot.DedupeTables(sig.Tables)

	// Resolve every name under the read lock, then release it: the
	// checkpoint calls below go to AWS.
	streams := make([]*tableStream, len(sig.Tables))
	var bad []string
	d.mu.RLock()
	for i, name := range sig.Tables {
		ts, exists := d.tableStreams[name]
		switch {
		case !exists:
			bad = append(bad, name+" (not streamed by this input)")
		case ts.isSignalTable:
			bad = append(bad, name+" (is the signal table, which is never snapshotted)")
		case ts.incremental == nil:
			bad = append(bad, name+" (has no incremental snapshot state)")
		default:
			if err := incsnapshot.RequireNewImage(name, ts.streamSpec); err != nil {
				bad = append(bad, name+" ("+err.Error()+")")
			}
		}
		streams[i] = ts
	}
	d.mu.RUnlock()
	if len(bad) > 0 {
		// The signal is all or nothing, so one bad table rejects it whole.
		log.With("tables", strings.Join(sig.Tables, ",")).Errorf("Snapshot signal: %v", fmt.Errorf("%w, no table was queued: cannot snapshot %s", replication.ErrSignalRejected, strings.Join(bad, "; ")))
		return nil
	}

	var added, restarted, running []string
	for i, name := range sig.Tables {
		// A table being backfilled finishes that backfill and is not
		// queued again, as in postgres_cdc: the signal cannot restart a
		// run midway, and queuing it would only find the run complete.
		if d.backfillRetries.isRunning(name) {
			running = append(running, name)
			continue
		}
		ts := streams[i]
		progress, err := ts.checkpointer.SnapshotProgress(ctx)
		if err != nil {
			return fmt.Errorf("getting snapshot progress for table %s: %w", name, err)
		}
		// The requested row is written before the table is queued, so a
		// crash after the signal is acked still finds the request. For a
		// completed table it is also written before the reset, which keeps
		// it: a crash midway leaves the signal unacked, so it is read again.
		if err := ts.checkpointer.MarkSnapshotRequested(ctx); err != nil {
			return fmt.Errorf("recording snapshot request for table %s: %w", name, err)
		}
		if progress.IsComplete() {
			// A completed backfill is read again from the start. Its run has
			// waited for its acks and marked the snapshot complete, so no
			// write of that run can land after the reset.
			if err := ts.checkpointer.ResetSnapshotProgress(ctx); err != nil {
				return fmt.Errorf("resetting snapshot progress for table %s: %w", name, err)
			}
			restarted = append(restarted, name)
		}
		// A table already queued is not queued twice.
		d.backfills.Push(name)
		added = append(added, name)
	}

	if len(running) > 0 {
		log.Warnf("Incremental snapshot: signal ignored for %v: already being backfilled, so nothing was queued for it", running)
	}
	if len(added) > 0 {
		log.Infof("Incremental snapshot: signal queued %d table(s) for backfill: %v", len(added), added)
	}
	if len(restarted) > 0 {
		log.Infof("Incremental snapshot: signal restarts the completed backfill of %v from the start", restarted)
	}
	return nil
}
