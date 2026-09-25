// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package logminer

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

type redoVolumeStrategy struct {
	selector *logFileSelector
	// maxRedoLogSizeInBytes is fetched once, lazily; 0 means "not yet
	// fetched" (never legitimately 0 on a running database).
	maxRedoLogSizeInBytes uint64
	maxRedoSizeStmt       *sql.Stmt
	openThreadsStmt       *sql.Stmt
	// lastCapped is what selectSession's most recent call returned, read
	// back by resetIfUncapped so miningCycle doesn't need to carry it as
	// a local between the two calls.
	lastCapped bool
}

func newRedoVolumeStrategy(minCount, growthMax int) *redoVolumeStrategy {
	return &redoVolumeStrategy{selector: &logFileSelector{minCount: minCount, growthMax: growthMax}}
}

func (rv *redoVolumeStrategy) selectSession(ctx context.Context, conn *sql.Conn, logCollector *LogFileCollector, currentSCN, dbCurrentSCN uint64) (files []*LogFile, endSCN uint64, err error) {
	if rv.maxRedoLogSizeInBytes == 0 {
		size, err := rv.GetMaxRedoLogSize(ctx, conn)
		if err != nil {
			return nil, 0, fmt.Errorf("fetching max redo log size for logminer: %w", err)
		}
		if size == 0 {
			return nil, 0, errors.New("database reported a max redo log size of 0 bytes across V$LOG - cannot size the redo_volume byte budget")
		}
		rv.maxRedoLogSizeInBytes = size
	}

	candidates, err := logCollector.GetLogsBySCNRange(ctx, conn, currentSCN, dbCurrentSCN)
	if err != nil {
		return nil, 0, fmt.Errorf("collecting redo logs for logminer: %w", err)
	}
	openThreads, err := rv.GetOpenThreads(ctx, conn)
	if err != nil {
		return nil, 0, fmt.Errorf("collecting open redo threads for logminer: %w", err)
	}
	var capped bool
	if files, endSCN, capped, err = rv.selector.selectForSession(candidates, openThreads, dbCurrentSCN, rv.maxRedoLogSizeInBytes); err != nil {
		return nil, 0, fmt.Errorf("selecting log files for session: %w", err)
	}
	rv.lastCapped = capped
	return files, endSCN, nil
}

// resetIfUncapped resets the budget to its minimum once selectSession's most
// recent call catches up within it, rather than staying grown from an
// earlier stalled cycle.
func (rv *redoVolumeStrategy) resetIfUncapped() {
	if !rv.lastCapped {
		rv.selector.count = rv.selector.minCount
	}
}

// GetMaxRedoLogSize returns the largest configured online redo log size, in
// bytes, across every redo group. The redo_volume window strategy uses this
// as the unit its byte budget is denominated in (N x this size), since
// online redo log groups are always provisioned to a uniform size, unlike
// archived log files (see LogFile.SizeBytes).
func (rv *redoVolumeStrategy) GetMaxRedoLogSize(ctx context.Context, conn *sql.Conn) (uint64, error) {
	if rv.maxRedoSizeStmt == nil {
		stmt, err := conn.PrepareContext(ctx, "SELECT MAX(BYTES) FROM V$LOG")
		if err != nil {
			return 0, fmt.Errorf("preparing max redo log size query: %w", err)
		}
		rv.maxRedoSizeStmt = stmt
	}

	var maxBytes uint64
	if err := rv.maxRedoSizeStmt.QueryRowContext(ctx).Scan(&maxBytes); err != nil {
		return 0, fmt.Errorf("querying max redo log size: %w", err)
	}
	return maxBytes, nil
}

// GetOpenThreads returns the redo thread numbers Oracle currently reports as
// OPEN. The redo_volume window strategy uses this on RAC databases to check
// that every open thread actually has log files in a GetLogsBySCNRange
// result - an open thread with none means the collector query missed
// something, not that the thread has nothing to mine.
func (rv *redoVolumeStrategy) GetOpenThreads(ctx context.Context, conn *sql.Conn) ([]int, error) {
	if rv.openThreadsStmt == nil {
		stmt, err := conn.PrepareContext(ctx, `SELECT THREAD# FROM V$THREAD WHERE STATUS = 'OPEN'`)
		if err != nil {
			return nil, fmt.Errorf("preparing open redo threads query: %w", err)
		}
		rv.openThreadsStmt = stmt
	}

	rows, err := rv.openThreadsStmt.QueryContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("querying open redo threads: %w", err)
	}
	defer rows.Close()

	var threads []int
	for rows.Next() {
		var thread int
		if err := rows.Scan(&thread); err != nil {
			return nil, fmt.Errorf("scanning open redo thread row: %w", err)
		}
		threads = append(threads, thread)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return threads, nil
}

// Close releases the prepared GetMaxRedoLogSize and GetOpenThreads statements, if any.
func (rv *redoVolumeStrategy) Close() error {
	var errs []error
	if rv.maxRedoSizeStmt != nil {
		if err := rv.maxRedoSizeStmt.Close(); err != nil {
			errs = append(errs, err)
		}
		rv.maxRedoSizeStmt = nil
	}
	if rv.openThreadsStmt != nil {
		if err := rv.openThreadsStmt.Close(); err != nil {
			errs = append(errs, err)
		}
		rv.openThreadsStmt = nil
	}
	return errors.Join(errs...)
}
