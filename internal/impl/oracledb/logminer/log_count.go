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

// logCountStrategy owns the state and DB-glue methods for the log_count
// window strategy: the byte-budget file selector, the max redo log size it's
// denominated in, and the prepared statements used to fetch that size and
// the currently open redo threads.
type logCountStrategy struct {
	selector *logFileSelector
	// maxRedoLogSizeInBytes is fetched once, lazily; 0 means "not yet fetched" (never legitimately 0 on a running database).
	maxRedoLogSizeInBytes uint64
	maxRedoSizeStmt       *sql.Stmt
	openThreadsStmt       *sql.Stmt
}

func newLogCountStrategy(minCount, growthMax int) *logCountStrategy {
	return &logCountStrategy{selector: &logFileSelector{minCount: minCount, growthMax: growthMax}}
}

func (lc *logCountStrategy) selectSession(ctx context.Context, conn *sql.Conn, logCollector *LogFileCollector, currentSCN, dbCurrentSCN uint64) (files []*LogFile, endSCN uint64, capped bool, err error) {
	if lc.maxRedoLogSizeInBytes == 0 {
		size, err := lc.GetMaxRedoLogSize(ctx, conn)
		if err != nil {
			return nil, 0, false, fmt.Errorf("fetching max redo log size for logminer: %w", err)
		}
		if size == 0 {
			return nil, 0, false, errors.New("database reported a max redo log size of 0 bytes across V$LOG - cannot size the log_count byte budget")
		}
		lc.maxRedoLogSizeInBytes = size
	}

	candidates, err := logCollector.GetLogsBySCNRange(ctx, conn, currentSCN, dbCurrentSCN)
	if err != nil {
		return nil, 0, false, fmt.Errorf("collecting redo logs for logminer: %w", err)
	}
	openThreads, err := lc.GetOpenThreads(ctx, conn)
	if err != nil {
		return nil, 0, false, fmt.Errorf("collecting open redo threads for logminer: %w", err)
	}
	if files, endSCN, capped, err = lc.selector.selectForSession(candidates, openThreads, dbCurrentSCN, lc.maxRedoLogSizeInBytes); err != nil {
		return nil, 0, false, fmt.Errorf("selecting log files for session: %w", err)
	}
	return files, endSCN, capped, nil
}

func (lc *logCountStrategy) resetIfUncapped(capped bool) {
	if !capped {
		lc.selector.count = lc.selector.minCount
	}
}

// GetMaxRedoLogSize returns the largest configured online redo log size, in
// bytes, across every redo group. The log_count window strategy uses this as
// the unit its file-count budget is denominated in (N x this size) rather
// than a literal file count, since online redo log groups are always
// provisioned to a uniform size, unlike archived log files (see LogFile.SizeBytes).
func (lc *logCountStrategy) GetMaxRedoLogSize(ctx context.Context, conn *sql.Conn) (uint64, error) {
	if lc.maxRedoSizeStmt == nil {
		stmt, err := conn.PrepareContext(ctx, "SELECT MAX(BYTES) FROM V$LOG")
		if err != nil {
			return 0, fmt.Errorf("preparing max redo log size query: %w", err)
		}
		lc.maxRedoSizeStmt = stmt
	}

	var maxBytes uint64
	if err := lc.maxRedoSizeStmt.QueryRowContext(ctx).Scan(&maxBytes); err != nil {
		return 0, fmt.Errorf("querying max redo log size: %w", err)
	}
	return maxBytes, nil
}

// GetOpenThreads returns the redo thread numbers Oracle currently reports as
// OPEN. The log_count window strategy uses this on RAC databases to check
// that every open thread actually has log files in a GetLogsBySCNRange
// result - an open thread with none means the collector query missed
// something, not that the thread has nothing to mine.
func (lc *logCountStrategy) GetOpenThreads(ctx context.Context, conn *sql.Conn) ([]int, error) {
	if lc.openThreadsStmt == nil {
		stmt, err := conn.PrepareContext(ctx, `SELECT THREAD# FROM V$THREAD WHERE STATUS = 'OPEN'`)
		if err != nil {
			return nil, fmt.Errorf("preparing open redo threads query: %w", err)
		}
		lc.openThreadsStmt = stmt
	}

	rows, err := lc.openThreadsStmt.QueryContext(ctx)
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
func (lc *logCountStrategy) Close() error {
	var errs []error
	if lc.maxRedoSizeStmt != nil {
		if err := lc.maxRedoSizeStmt.Close(); err != nil {
			errs = append(errs, err)
		}
		lc.maxRedoSizeStmt = nil
	}
	if lc.openThreadsStmt != nil {
		if err := lc.openThreadsStmt.Close(); err != nil {
			errs = append(errs, err)
		}
		lc.openThreadsStmt = nil
	}
	return errors.Join(errs...)
}
