// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/redpanda-data/benthos/v4/public/service"
	"golang.org/x/sync/errgroup"
)

// parallelSnapshotSet owns the shared *sql.DB and a pool of per-worker Snapshot
// instances. Every worker in the set holds its own *sql.Conn and its own
// REPEATABLE READ / CONSISTENT SNAPSHOT transaction, but all transactions were
// opened within a single FLUSH TABLES ... WITH READ LOCK window so they view
// identical state at the same binlog position.
type parallelSnapshotSet struct {
	db      *sql.DB
	workers []*Snapshot
	logger  *service.Logger
}

// prepareParallelSnapshotSet opens workerCount reader connections that all
// share a single globally-consistent MySQL snapshot:
//
//  1. Acquire a single lock connection and FLUSH TABLES <tables> WITH READ LOCK.
//  2. Open workerCount snapshot connections, each starting a REPEATABLE READ
//     transaction followed by START TRANSACTION WITH CONSISTENT SNAPSHOT.
//  3. Capture the binlog position once (all workers share this position).
//  4. Release the table locks and return.
//
// The returned set's workers can each be read from in parallel without
// coordination: they are independent connections/transactions observing the
// same historical state. The caller is responsible for invoking release then
// close once snapshot reading is finished.
//
// Ownership: this function takes ownership of db. On success the returned set
// closes db when set.close() is called. On error db is closed before the
// function returns (along with any partially-opened conns/txns) and the
// caller must not reuse it.
func prepareParallelSnapshotSet(ctx context.Context, logger *service.Logger, db *sql.DB, tables []string, workerCount int) (*parallelSnapshotSet, *position, error) {
	if workerCount < 1 {
		_ = db.Close()
		return nil, nil, fmt.Errorf("parallel snapshot worker count must be >= 1, got %d", workerCount)
	}
	if len(tables) == 0 {
		_ = db.Close()
		return nil, nil, errors.New("no tables provided")
	}
	// Never open more workers than tables: extra workers would sit idle and
	// waste a connection for the duration of the snapshot.
	if workerCount > len(tables) {
		workerCount = len(tables)
	}

	set := &parallelSnapshotSet{db: db, logger: logger}
	// failWith closes the partially-built set (which closes db) and returns
	// the combined error. Use this on every error path below.
	failWith := func(errs ...error) (*parallelSnapshotSet, *position, error) {
		errs = append(errs, set.close())
		return nil, nil, errors.Join(errs...)
	}

	lockConn, err := db.Conn(ctx)
	if err != nil {
		return failWith(fmt.Errorf("create lock connection: %w", err))
	}
	// The lock conn is only needed to bracket the BEGINs below. Always return
	// it to the pool on exit; the lock itself is released via UNLOCK TABLES.
	defer func() {
		_ = lockConn.Close()
	}()

	lockQuery := buildFlushAndLockTablesQuery(tables)
	logger.Infof("Acquiring table-level read locks for parallel snapshot (%d workers): %s", workerCount, lockQuery)
	if _, err := lockConn.ExecContext(ctx, lockQuery); err != nil {
		return failWith(fmt.Errorf("acquire table-level read locks: %w", err))
	}
	unlockTables := func() error {
		if _, err := lockConn.ExecContext(ctx, "UNLOCK TABLES"); err != nil {
			return fmt.Errorf("release table-level read locks: %w", err)
		}
		return nil
	}

	for idx := 0; idx < workerCount; idx++ {
		conn, err := db.Conn(ctx)
		if err != nil {
			return failWith(fmt.Errorf("open snapshot connection %d: %w", idx, err), unlockTables())
		}
		tx, err := conn.BeginTx(ctx, &sql.TxOptions{
			ReadOnly:  true,
			Isolation: sql.LevelRepeatableRead,
		})
		if err != nil {
			_ = conn.Close()
			return failWith(fmt.Errorf("begin snapshot transaction %d: %w", idx, err), unlockTables())
		}
		// NOTE: this is a little sneaky because we're actually implicitly
		// closing the transaction started with BeginTx above and replacing it
		// with this one. We have to do this because the database/sql driver
		// does not support WITH CONSISTENT SNAPSHOT directly.
		if _, err := tx.ExecContext(ctx, "START TRANSACTION WITH CONSISTENT SNAPSHOT"); err != nil {
			_ = tx.Rollback()
			_ = conn.Close()
			return failWith(fmt.Errorf("start consistent snapshot %d: %w", idx, err), unlockTables())
		}
		// Each worker is a "bare" Snapshot: no db (the set owns it), no
		// lockConn (released at the end of this function). close() on each
		// worker will rollback its tx and close its conn, which is what we
		// want.
		set.workers = append(set.workers, &Snapshot{
			tx:           tx,
			snapshotConn: conn,
			logger:       logger,
		})
	}

	// Capture binlog position while still under lock, from any worker. All
	// workers are at the same snapshot so this single position applies to all
	// of them.
	pos, err := set.workers[0].getCurrentBinlogPosition(ctx)
	if err != nil {
		return failWith(fmt.Errorf("get binlog position: %w", err), unlockTables())
	}

	if err := unlockTables(); err != nil {
		return failWith(err)
	}

	return set, &pos, nil
}

// release commits every worker's snapshot transaction. Analogous to
// Snapshot.releaseSnapshot for the sequential path.
func (p *parallelSnapshotSet) release(ctx context.Context) error {
	var errs []error
	for _, w := range p.workers {
		if err := w.releaseSnapshot(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// close rolls back any still-open transactions, closes every worker
// connection, then closes the shared *sql.DB.
func (p *parallelSnapshotSet) close() error {
	var errs []error
	for _, w := range p.workers {
		if err := w.close(); err != nil {
			errs = append(errs, err)
		}
	}
	if p.db != nil {
		if err := p.db.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close db: %w", err))
		}
		p.db = nil
	}
	return errors.Join(errs...)
}

// readSnapshotParallel distributes i.tables across set.workers and reads them
// concurrently using an errgroup. Any worker error cancels siblings and
// returns from Wait (matching the existing fail-halt semantics of the
// sequential path).
func (i *mysqlStreamInput) readSnapshotParallel(ctx context.Context, set *parallelSnapshotSet) error {
	return distributeTablesToWorkers(ctx, i.tables, len(set.workers), func(gctx context.Context, workerIdx int, table string) error {
		return i.readSnapshotTable(gctx, set.workers[workerIdx], table)
	})
}

// distributeTablesToWorkers fans out tables across workerCount goroutines,
// calling readFn(ctx, workerIdx, table) exactly once per table. It uses an
// errgroup: the first error cancels the shared context and is returned from
// Wait. Exposed for unit-testing the fan-out independently of MySQL.
func distributeTablesToWorkers(ctx context.Context, tables []string, workerCount int, readFn func(context.Context, int, string) error) error {
	if workerCount < 1 {
		return fmt.Errorf("workerCount must be >= 1, got %d", workerCount)
	}
	if workerCount > len(tables) {
		workerCount = len(tables)
	}
	if workerCount == 0 {
		// No tables at all. Nothing to do.
		return nil
	}

	g, gctx := errgroup.WithContext(ctx)
	tableCh := make(chan string)

	g.Go(func() error {
		defer close(tableCh)
		for _, t := range tables {
			select {
			case tableCh <- t:
			case <-gctx.Done():
				return gctx.Err()
			}
		}
		return nil
	})

	for w := 0; w < workerCount; w++ {
		workerIdx := w
		g.Go(func() error {
			for table := range tableCh {
				if err := readFn(gctx, workerIdx, table); err != nil {
					return err
				}
			}
			return nil
		})
	}

	return g.Wait()
}
