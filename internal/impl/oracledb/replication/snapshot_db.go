// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package replication

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff/v4"
	goora "github.com/sijms/go-ora/v2/network"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// connectionCapacityErrCodes are the ORA codes returned when the database has no room for another connection.
var connectionCapacityErrCodes = []int{
	18,    // ORA-00018: maximum number of sessions exceeded
	20,    // ORA-00020: maximum number of processes exceeded
	12516, // ORA-12516: listener could not find available handler with matching protocol stack
	12518, // ORA-12518: listener could not hand off client connection
	12519, // ORA-12519: no appropriate service handler found
	12520, // ORA-12520: listener could not find available handler for requested type of server
}

// containerResetTimeout bounds switching a snapshot connection back to the root container. The
// switch takes milliseconds, and this only stops a hung database blocking shutdown, so needn't be configurable.
const containerResetTimeout = 10 * time.Second

// DefaultSnapshotConnBackoff returns the default backoff used while waiting for a snapshot connection.
func DefaultSnapshotConnBackoff() *backoff.ExponentialBackOff {
	b := backoff.NewExponentialBackOff()
	b.InitialInterval = 100 * time.Millisecond
	b.MaxInterval = 5 * time.Second
	b.MaxElapsedTime = 3 * time.Minute
	return b
}

// snapshotDB hands snapshot readers PDB-switched, NLS-configured connections, waiting with backoff when the database is at capacity.
type snapshotDB struct {
	pool     *sql.DB
	pdbName  string
	backoff  backoff.ExponentialBackOff // copied per acquire
	log      *service.Logger
	warnOnce sync.Once    // capacity warning
	inUse    atomic.Int32 // connections held
}

// newSnapshotDB opens the connection pool, using DefaultSnapshotConnBackoff when connBackoff is nil.
func newSnapshotDB(ctx context.Context, connStr, pdbName string, connBackoff *backoff.ExponentialBackOff, log *service.Logger) (*snapshotDB, error) {
	pool, err := sql.Open("oracle", connStr)
	if err != nil {
		return nil, fmt.Errorf("connecting to oracle database for snapshotting: %w", err)
	}
	if connBackoff == nil {
		connBackoff = DefaultSnapshotConnBackoff()
	}
	d := &snapshotDB{pool: pool, pdbName: pdbName, backoff: *connBackoff, log: log}
	// Check a connection can be opened and configured now, waiting with backoff like any other reader.
	if err := d.withConn(ctx, func(*sql.Conn) error { return nil }); err != nil {
		pool.Close()
		return nil, err
	}
	return d, nil
}

// setWorkers keeps n connections idle so chunks reuse them instead of logging in again.
func (d *snapshotDB) setWorkers(n int) {
	d.pool.SetMaxIdleConns(n)
}

// withConn runs fn on a dedicated connection switched into the PDB (if set) with NLS settings applied, which keyset pagination needs to compare bounds correctly.
func (d *snapshotDB) withConn(ctx context.Context, fn func(conn *sql.Conn) error) (err error) {
	var conn *sql.Conn
	if conn, err = d.acquire(ctx); err != nil {
		return fmt.Errorf("acquiring snapshot connection: %w", withConnectionCapacityHint(err))
	}
	defer func() {
		d.inUse.Add(-1)
		if closeErr := conn.Close(); closeErr != nil {
			d.log.Errorf("Closing snapshot connection: %v", closeErr)
		}
	}()

	if d.pdbName != "" {
		if _, err = conn.ExecContext(ctx, "ALTER SESSION SET CONTAINER = "+d.pdbName); err != nil {
			return fmt.Errorf("switching session to PDB '%s' for snapshot: %w", d.pdbName, err)
		}
		defer func() {
			// Reset even when ctx is done, so the pooled connection is left in the root, but don't
			// let a hung database block shutdown.
			resetCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), containerResetTimeout)
			defer cancel()
			if _, cerr := conn.ExecContext(resetCtx, "ALTER SESSION SET CONTAINER = CDB$ROOT"); cerr != nil {
				d.log.Errorf("Switching session back to root container: %v", cerr)
			}
		}()
	}

	if err = ApplyNLSSettings(ctx, conn); err != nil {
		return fmt.Errorf("configuring nls for snapshot connection: %w", err)
	}
	if err = applyBinaryCollation(ctx, conn); err != nil {
		return fmt.Errorf("configuring collation for snapshot connection: %w", err)
	}

	return fn(conn)
}

// withReadOnlyTxn runs fn in a READ ONLY transaction, whose queries all read as of its start, on a connection from withConn.
func (d *snapshotDB) withReadOnlyTxn(ctx context.Context, fn func(tx *sql.Tx) error) error {
	return d.withConn(ctx, func(conn *sql.Conn) (err error) {
		// context.Background() avoids database/sql's awaitDone goroutine, which races with Rollback in go-ora.
		tx, err := conn.BeginTx(context.Background(), nil)
		if err != nil {
			return fmt.Errorf("beginning snapshot transaction: %w", err)
		}

		if _, err = tx.ExecContext(ctx, "SET TRANSACTION READ ONLY"); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("setting transaction read-only: %w", err)
		}
		defer func() {
			if err != nil {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					d.log.Errorf("Failed to rollback snapshot transaction: %v", rbErr)
				}
			}
		}()

		if err = fn(tx); err != nil {
			return err
		}

		if rbErr := tx.Rollback(); rbErr != nil {
			d.log.Errorf("Failed rollback snapshot transaction: %v", rbErr)
		}
		return nil
	})
}

// acquire gets a pooled connection and counts it as held, retrying with backoff while the database is at capacity and giving up only once the backoff expires with no connections held.
func (d *snapshotDB) acquire(ctx context.Context) (*sql.Conn, error) {
	retry := d.backoff
	retry.Reset()
	for {
		conn, err := d.pool.Conn(ctx)
		if err == nil {
			// Count the connection before returning it, so another reader's give-up check can't miss it.
			d.inUse.Add(1)
			return conn, nil
		}
		if !isConnectionCapacityError(err) {
			return nil, err
		}
		delay := retry.NextBackOff()
		if delay == backoff.Stop {
			if d.inUse.Load() == 0 {
				// Try once more, in case the last reader released its connection after the failed attempt.
				if conn, err = d.pool.Conn(ctx); err == nil {
					d.inUse.Add(1)
					return conn, nil
				}
				return nil, err
			}
			retry.Reset()
			delay = retry.NextBackOff()
		}
		d.warnOnce.Do(func() {
			d.log.Warnf("Database has no capacity for another snapshot connection, so snapshot workers will wait for one to be released; lower max_parallel_snapshot_tables to stay within the database's processes and sessions limits: %v", err)
		})
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// currentSCN returns the database's current SCN and server time.
func (d *snapshotDB) currentSCN(ctx context.Context) (scn SCN, ts time.Time, err error) {
	err = d.withConn(ctx, func(conn *sql.Conn) error {
		return conn.QueryRowContext(ctx, `SELECT CURRENT_SCN, SYSTIMESTAMP FROM V$DATABASE`).Scan(&scn, &ts)
	})
	return scn, ts, err
}

// Close closes the pool and its connections.
func (d *snapshotDB) Close() error {
	if err := d.pool.Close(); err != nil {
		return fmt.Errorf("closing database connection: %w", err)
	}
	return nil
}

// withConnectionCapacityHint adds advice to lower max_parallel_snapshot_tables to a connection
// capacity error, which acquire only returns once no other reader holds a connection.
func withConnectionCapacityHint(err error) error {
	switch {
	case isHandshakeEOF(err):
		// With no other connection held, an EOF may mean the database is down rather than full.
		return fmt.Errorf("%w\n\nThe database closed the snapshot connection while it was logging in, and no other snapshot reader held a connection. "+
			"Either the database has no capacity for another connection, so lower max_parallel_snapshot_tables to stay within its processes and sessions limits, "+
			"or the database is down or restarting", err)
	case isConnectionCapacityError(err):
		return fmt.Errorf("%w\n\nThe database has no capacity for another snapshot connection. Each parallel snapshot reader holds its own connection, so lower max_parallel_snapshot_tables to stay within the database's processes and sessions limits", err)
	default:
		return err
	}
}

// isConnectionCapacityError reports whether err means the database has no room for another connection, including an EOF mid-handshake.
func isConnectionCapacityError(err error) bool {
	var oraErr *goora.OracleError
	return isHandshakeEOF(err) || (errors.As(err, &oraErr) && slices.Contains(connectionCapacityErrCodes, oraErr.ErrCode))
}

// isHandshakeEOF reports whether err is the database closing the connection mid-handshake without an ORA code.
func isHandshakeEOF(err error) bool {
	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)
}

// applyBinaryCollation sorts and compares keys in binary order, so a linguistic NLS_SORT or NLS_COMP can't make keyset pages and chunk bounds skip or repeat rows.
func applyBinaryCollation(ctx context.Context, db dbExecer) error {
	for _, stmt := range []string{"ALTER SESSION SET NLS_SORT = BINARY", "ALTER SESSION SET NLS_COMP = BINARY"} {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("running %s: %w", stmt, err)
		}
	}
	return nil
}
