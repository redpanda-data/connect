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
	"fmt"
	"sync"

	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"cloud.google.com/go/bigquery/storage/managedwriter"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/descriptorpb"
)

// maxAppendRowsBytes is a conservative chunk-size limit for AppendRows. The
// API's hard limit is 10MB per request; we leave headroom for proto framing
// and gRPC overhead.
const maxAppendRowsBytes = 9 * 1024 * 1024

// pendingStreamWriter writes a single batch to BigQuery using a freshly
// allocated pending stream. The lifecycle per WriteBatch is:
//
//  1. CreateWriteStream(Pending) — fresh stream per batch.
//  2. AppendRows for each chunk with sequential offsets.
//  3. FinalizeWriteStream.
//  4. BatchCommitWriteStreams — atomic commit.
//
// Failure semantics: any error before commit leaves an uncommitted stream
// (data not visible). benthos retries via a fresh stream, giving clean
// at-least-once semantics with exactly-once-within-stream guarantees on the
// data that does land. A successful commit whose response is lost on the
// wire produces a duplicate-commit retry — this is a fundamental limit of
// the BQ Storage Write API exactly-once contract.
type pendingStreamWriter struct {
	storage *managedwriter.Client
	// inflight tracks in-progress Write calls so Close can wait for them to
	// finish before the underlying storage client is torn down. Without this,
	// a benthos shutdown that races with a pending Write would tear the gRPC
	// connection mid-Finalize/BatchCommit and surface a permanent batch error
	// instead of a clean retry.
	inflight sync.WaitGroup
}

// Begin registers an intent to run Write, returning a release function the
// caller must defer. It exists so the caller can register the in-flight
// operation while still holding the lock that synchronises Close — Write
// itself can only Add(1) once it actually runs, which leaves a window where
// Close.Wait() observes inflight=0 between the lock release and Write entry.
// Callers should always do:
//
//	pending := snapshot under connMu.RLock
//	done := pending.Begin()
//	defer done()
//	connMu.RUnlock
//	... pending.Write(...)
func (p *pendingStreamWriter) Begin() func() {
	p.inflight.Add(1)
	return p.inflight.Done
}

// Wait blocks until every in-flight Write returns. Intended for output.Close
// to call before tearing down the underlying managedwriter.Client. Safe to
// call repeatedly.
func (p *pendingStreamWriter) Wait() {
	p.inflight.Wait()
}

// Write executes the pending-stream lifecycle for a single batch. parent is
// the BigQuery resource path of the destination table
// (`projects/{p}/datasets/{d}/tables/{t}`). descriptorProto must match the
// rows' serialised proto encoding.
//
// The caller is expected to have invoked Begin() before this call so Close
// can correctly observe the in-flight count. Write does not register itself
// because the registration must happen under the caller's connMu RLock.
func (p *pendingStreamWriter) Write(
	ctx context.Context,
	parent string,
	descriptorProto *descriptorpb.DescriptorProto,
	rows [][]byte,
) error {
	if len(rows) == 0 {
		return nil
	}

	stream, err := p.storage.CreateWriteStream(ctx, &storagepb.CreateWriteStreamRequest{
		Parent: parent,
		WriteStream: &storagepb.WriteStream{
			Type: storagepb.WriteStream_PENDING,
		},
	})
	if err != nil {
		return fmt.Errorf("creating pending write stream: %w", err)
	}

	ms, err := p.storage.NewManagedStream(ctx,
		managedwriter.WithStreamName(stream.GetName()),
		managedwriter.WithSchemaDescriptor(descriptorProto),
	)
	if err != nil {
		return fmt.Errorf("opening managed stream for %s: %w", stream.GetName(), err)
	}
	defer func() { _ = ms.Close() }()

	chunks := chunkRowsByBytes(rows, maxAppendRowsBytes)
	var offset int64
	for _, chunk := range chunks {
		result, err := ms.AppendRows(ctx, chunk, managedwriter.WithOffset(offset))
		if err != nil {
			return fmt.Errorf("AppendRows at offset %d: %w", offset, err)
		}
		if _, err := result.GetResult(ctx); err != nil {
			return fmt.Errorf("AppendRows ack at offset %d: %w", offset, err)
		}
		offset += int64(len(chunk))
	}

	if _, err := ms.Finalize(ctx); err != nil {
		return fmt.Errorf("FinalizeWriteStream %s: %w", stream.GetName(), err)
	}

	// BatchCommitWriteStreams can return a non-nil response with an empty
	// CommitTime and a populated StreamErrors slice when the RPC succeeds at
	// the transport layer but the server refuses to commit (stream not
	// finalized, schema mismatch at commit time, already committed, etc.).
	// In that case the data is not visible in BigQuery, so we must surface it
	// as a write error rather than reporting the batch as successfully
	// delivered.
	resp, err := p.storage.BatchCommitWriteStreams(ctx, &storagepb.BatchCommitWriteStreamsRequest{
		Parent:       parent,
		WriteStreams: []string{stream.GetName()},
	})
	if err != nil {
		return fmt.Errorf("committing pending stream %s for table %s: %w", stream.GetName(), parent, err)
	}
	if errs := resp.GetStreamErrors(); len(errs) > 0 {
		// Wrap the first StorageError so handleWriteError/classifyBQError can
		// classify it as permanent or retriable. Multiple stream errors are
		// rare with a single-stream request; include their count in the
		// message for debugging.
		first := errs[0]
		return fmt.Errorf("BatchCommitWriteStreams %s rejected (%d stream error(s)): %w",
			stream.GetName(), len(errs), grpcstatus.Error(codes.Code(first.GetCode()), first.GetErrorMessage()))
	}
	return nil
}

// chunkRowsByBytes splits a row slice into smaller slices such that each
// chunk's total payload byte size stays under maxBytes. A single row that
// already exceeds maxBytes is emitted in its own chunk; the API will reject
// it server-side and the caller surfaces that as a permanent error.
func chunkRowsByBytes(rows [][]byte, maxBytes int) [][][]byte {
	if len(rows) == 0 {
		return nil
	}
	var (
		chunks       [][][]byte
		current      [][]byte
		currentBytes int
	)
	for _, r := range rows {
		if len(current) > 0 && currentBytes+len(r) > maxBytes {
			chunks = append(chunks, current)
			current = nil
			currentBytes = 0
		}
		current = append(current, r)
		currentBytes += len(r)
	}
	if len(current) > 0 {
		chunks = append(chunks, current)
	}
	return chunks
}
