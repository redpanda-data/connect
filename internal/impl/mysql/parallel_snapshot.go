// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"golang.org/x/sync/errgroup"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// parallelSnapshotSet owns the shared *sql.DB and a pool of per-worker
// Snapshot instances. Every worker holds its own connection and its own
// REPEATABLE READ / CONSISTENT SNAPSHOT transaction, but all transactions
// were opened within a single FLUSH TABLES ... WITH READ LOCK window so
// they view identical state at the same binlog position.
type parallelSnapshotSet struct {
	db      *sql.DB
	workers []*Snapshot
	logger  *service.Logger
}

// prepareParallelSnapshotSet opens workerCount reader connections that all
// share a single globally-consistent MySQL snapshot:
//
//  1. Acquire FLUSH TABLES <tables> WITH READ LOCK on a dedicated connection.
//  2. Open workerCount snapshot connections, each starting a REPEATABLE READ
//     transaction followed by START TRANSACTION WITH CONSISTENT SNAPSHOT.
//  3. Capture the binlog position once (all workers share this position).
//  4. Release the table locks and return.
//
// Takes ownership of db. On error db is closed before returning.
func prepareParallelSnapshotSet(ctx context.Context, logger *service.Logger, db *sql.DB, tables []string, workerCount int) (*parallelSnapshotSet, *position, error) {
	if workerCount < 1 {
		_ = db.Close()
		return nil, nil, fmt.Errorf("parallel snapshot worker count must be >= 1, got %d", workerCount)
	}
	if len(tables) == 0 {
		_ = db.Close()
		return nil, nil, errors.New("no tables provided")
	}

	set := &parallelSnapshotSet{db: db, logger: logger}
	failWith := func(errs ...error) (*parallelSnapshotSet, *position, error) {
		errs = append(errs, set.close())
		return nil, nil, errors.Join(errs...)
	}

	lockConn, err := db.Conn(ctx)
	if err != nil {
		return failWith(fmt.Errorf("create lock connection: %w", err))
	}
	defer func() { _ = lockConn.Close() }()

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

	for idx := range workerCount {
		conn, err := db.Conn(ctx)
		if err != nil {
			return failWith(fmt.Errorf("open snapshot connection %d: %w", idx, err), unlockTables())
		}
		tx, err := conn.BeginTx(ctx, &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelRepeatableRead})
		if err != nil {
			_ = conn.Close()
			return failWith(fmt.Errorf("begin snapshot transaction %d: %w", idx, err), unlockTables())
		}
		// NOTE: this is a little sneaky because we're implicitly closing the
		// transaction started with BeginTx above and replacing it with this
		// one. We have to do this because the database/sql driver does not
		// support WITH CONSISTENT SNAPSHOT directly.
		if _, err := tx.ExecContext(ctx, "START TRANSACTION WITH CONSISTENT SNAPSHOT"); err != nil {
			_ = tx.Rollback()
			_ = conn.Close()
			return failWith(fmt.Errorf("start consistent snapshot %d: %w", idx, err), unlockTables())
		}
		set.workers = append(set.workers, &Snapshot{
			tx:           tx,
			snapshotConn: conn,
			logger:       logger,
		})
	}

	pos, err := set.workers[0].getCurrentBinlogPosition(ctx)
	if err != nil {
		return failWith(fmt.Errorf("get binlog position: %w", err), unlockTables())
	}
	if err := unlockTables(); err != nil {
		return failWith(err)
	}
	return set, &pos, nil
}

func (p *parallelSnapshotSet) release(ctx context.Context) error {
	var errs []error
	for _, w := range p.workers {
		if err := w.releaseSnapshot(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

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

// distributeWorkToWorkers fans out items across workerCount goroutines via an
// errgroup. The first error cancels the shared context and is returned from
// Wait. Exposed as a generic helper so the fan-out logic can be unit-tested
// independently of MySQL types.
func distributeWorkToWorkers[T any](ctx context.Context, items []T, workerCount int, readFn func(context.Context, int, T) error) error {
	if workerCount < 1 {
		return fmt.Errorf("workerCount must be >= 1, got %d", workerCount)
	}
	if workerCount > len(items) {
		workerCount = len(items)
	}
	if workerCount == 0 {
		return nil
	}

	g, gctx := errgroup.WithContext(ctx)
	itemCh := make(chan T)

	g.Go(func() error {
		defer close(itemCh)
		for _, it := range items {
			select {
			case itemCh <- it:
			case <-gctx.Done():
				return gctx.Err()
			}
		}
		return nil
	})

	for w := 0; w < workerCount; w++ {
		workerIdx := w
		g.Go(func() error {
			for item := range itemCh {
				if err := readFn(gctx, workerIdx, item); err != nil {
					return err
				}
			}
			return nil
		})
	}

	return g.Wait()
}
