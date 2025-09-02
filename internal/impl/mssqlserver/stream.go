// Copyright 2025 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package mssqlserver

import (
	"bytes"
	"container/heap"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/redpanda-data/benthos/v4/public/service"
)

type heapItem struct{ iter *changeTableRowIter }

// rowIteratorMinHeap is used for sorting iterators by LSN to ensure they're in order across tables
type rowIteratorMinHeap []*heapItem

func (h rowIteratorMinHeap) Len() int { return len(h) }
func (h rowIteratorMinHeap) Less(i, j int) bool {
	// Compare LSNs as byte slices. CDC LSNs are fixed-length varbinary(10) so lexicographic == numeric order.
	// TODO: I think we also need to order by CommandID, add test to verify
	return bytes.Compare(h[i].iter.current.StartLSN, h[j].iter.current.StartLSN) < 0
}
func (h rowIteratorMinHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *rowIteratorMinHeap) Push(x any)   { *h = append(*h, x.(*heapItem)) }
func (h *rowIteratorMinHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

// changeTable is a valid, working change table configured by MS SQL Server that has change tracking enabled.
type changeTable struct {
	captureInstance string
	startLSN        []byte
}

// change represents a logical change row from the change table set in Table.
type change struct {
	Table     string
	StartLSN  []byte // varbinary(10)
	Operation int    // 1=delete, 2=insert, 3=update (before), 4=update (after), 5=merge
	SeqVal    []byte
	CommandID int
	Columns   map[string]any
}

type changeTableRowIter struct {
	table   string
	rows    *sql.Rows
	cols    []string
	current *change
}

// newChangeTableRowIter returns an custom row iterator for the given changeTable.
func newChangeTableRowIter(ctx context.Context, db *sql.DB, changeTable string, fromLSN, toLSN []byte) (*changeTableRowIter, error) {
	// Note: LSN is varbinary type so can sort correctly for LSNs
	// Inspired by Debezium https://github.com/debezium/debezium/blob/main/debezium-connector-sqlserver/src/main/java/io/debezium/connector/sqlserver/SqlServerConnection.java?plain=1#L177

	// "Sequence of the operation as represented in the transaction log. Should not be used for ordering. Instead, use the __$command_id column"
	// source: https://learn.microsoft.com/en-us/sql/relational-databases/system-tables/cdc-capture-instance-ct-transact-sql?view=sql-server-ver17
	q := fmt.Sprintf("SELECT * FROM cdc.%s_CT WITH (NOLOCK) WHERE (? IS NULL OR [__$start_lsn] > ?) AND (? IS NULL OR [__$start_lsn] <= ?) ORDER BY [__$start_lsn] ASC, [__$command_id] ASC, [__$seqval] ASC, [__$operation] ASC", changeTable)
	rows, err := db.QueryContext(ctx, q, fromLSN, fromLSN, toLSN, toLSN)
	if err != nil {
		return nil, err
	}

	cols, err := rows.Columns()
	if err != nil {
		rows.Close()
		return nil, err
	}

	itor := &changeTableRowIter{table: changeTable, rows: rows, cols: cols}
	// Prime the iterator by loading the first row
	if err := itor.next(); err != nil {
		// Already exhausted iterator
		err := itor.Close()
		return nil, err
	}

	return itor, nil
}

func (ct *changeTableRowIter) next() error {
	if !ct.rows.Next() {
		return sql.ErrNoRows
	}

	vals := make([]any, len(ct.cols))
	for i := range vals {
		var v any
		vals[i] = &v
	}
	if err := ct.rows.Scan(vals...); err != nil {
		return err
	}

	ct.current = ct.valsToChange(vals)
	return nil
}

func (ct *changeTableRowIter) Close() error {
	return ct.rows.Close()
}

func (ct *changeTableRowIter) valsToChange(vals []any) *change {
	// TODO: We should be able to remove this allocation
	ch := &change{
		Table:   ct.table,
		Columns: make(map[string]any, len(ct.cols)),
	}
	for i, c := range ct.cols {
		v := *(vals[i].(*any))
		switch c {
		case "__$start_lsn":
			if b, ok := v.([]byte); ok {
				ch.StartLSN = b
			}
		case "__$operation":
			switch x := v.(type) {
			case int64:
				ch.Operation = int(x)
			case int32:
				ch.Operation = int(x)
			default:
			}
		case "__$command_id":
			switch x := v.(type) {
			case int64:
				ch.CommandID = int(x)
			case int32:
				ch.CommandID = int(x)
			default:
			}
		case "__$seqval":
			if b, ok := v.([]byte); ok {
				ch.SeqVal = b
			}
		default:
			ch.Columns[c] = v
		}
	}
	return ch
}

type changeTableStream struct {
	logger *service.Logger

	trackedTables map[string]changeTable
	resCh         chan asyncMessage
}

func (r *changeTableStream) verifyChangeTables(ctx context.Context, db *sql.DB, configTables []string) error {
	rows, err := db.QueryContext(ctx, "SELECT capture_instance, start_lsn FROM cdc.change_tables")
	if err != nil {
		return fmt.Errorf("fetching change tables: %w", err)
	}

	var changeTables []changeTable
	for rows.Next() {
		var t changeTable
		if err := rows.Scan(&t.captureInstance, &t.startLSN); err != nil {
			return fmt.Errorf("loading change table: %w", err)
		}
		changeTables = append(changeTables, t)
	}

	for _, t := range configTables {
		for _, ct := range changeTables {
			if strings.HasSuffix(ct.captureInstance, "dbo_"+t) {
				r.trackedTables[ct.captureInstance] = ct
				r.logger.Debugf("Found change table '%s'", ct.captureInstance)
				goto next
			}
		}
		r.logger.Warnf("Change table for table '%s' not found", t)
	next:
	}

	if len(r.trackedTables) != len(configTables) {
		return fmt.Errorf("could not find all change tables")
	}

	return nil
}

func (r *changeTableStream) read(ctx context.Context, db *sql.DB, handle func(c *change) ([]byte, error)) error {
	var (
		fromLSN []byte // load last checkpoint; nil means start from beginning in tables
		toLSN   []byte // often set to fn_cdc_get_max_lsn(); nil means no upper bound
		lastLSN []byte
	)

	for {
		// Fetch a upper bound so the run is repeatable
		if err := db.QueryRowContext(ctx, `SELECT sys.fn_cdc_get_max_lsn()`).Scan(&toLSN); err != nil {
			return err
		}

		// Create an iterator per table, table LSNs can be ordred but we need to create a global
		// ordering by merging them (which we do using a using a (min) heap).
		h := &rowIteratorMinHeap{}
		heap.Init(h)

		iters := make([]*changeTableRowIter, 0, len(r.trackedTables))
		for _, inst := range r.trackedTables {
			it, err := newChangeTableRowIter(ctx, db, inst.captureInstance, fromLSN, toLSN)
			if err != nil {
				return fmt.Errorf("initialising iterator for change table %s: %w", inst.captureInstance, err)
			}

			if it != nil && it.current != nil {
				iters = append(iters, it)
				heap.Push(h, &heapItem{iter: it})
			} else if it != nil {
				it.Close()
			}
		}

		for h.Len() > 0 {
			// Pop the smallest LSN change
			item := heap.Pop(h).(*heapItem)
			cur := item.iter.current

			if lsn, err := handle(cur); err != nil {
				// Clean up before returning error
				for _, it := range iters {
					_ = it.Close()
				}
				return err
			} else {
				// TODO: This should be set via the ackFn
				lastLSN = lsn
			}

			// Advance the iterator and push back on heap to be sorted
			if err := item.iter.next(); err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					r.logger.Debugf("Reached end of rows for table %s", item.iter.table)
				}
				// exhausted all rows
				item.iter.Close()
			} else {
				// put back advanced on the heap to sort it again
				heap.Push(h, item)
			}
		}

		if lastLSN != nil {
			fromLSN = lastLSN
		}
	}
}
