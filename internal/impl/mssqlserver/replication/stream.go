// Copyright 2025 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package replication

import (
	"bytes"
	"container/heap"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/confx"
	"github.com/redpanda-data/connect/v4/internal/sqlutil"
)

type heapItem struct{ iter *changeTableRowIter }

// rowIteratorMinHeap is used for sorting iterators by LSN to ensure they're in order across tables.
type rowIteratorMinHeap []*heapItem

func (h rowIteratorMinHeap) Len() int { return len(h) }

func (h rowIteratorMinHeap) Less(i, j int) bool {
	// Compare LSNs as byte slices. CDC LSNs are fixed-length varbinary(10) so lexicographic == numeric order.
	// We also need to order by command_id, see below for more details:
	// https://learn.microsoft.com/en-us/sql/relational-databases/system-tables/cdc-capture-instance-ct-transact-sql?view=sql-server-ver17
	// First compare LSNs
	if cmp := bytes.Compare(h[i].iter.current.startLSN, h[j].iter.current.startLSN); cmp != 0 {
		return cmp < 0
	}
	// If LSN equal, compare command_id
	if h[i].iter.current.commandID != h[j].iter.current.commandID {
		return h[i].iter.current.commandID < h[j].iter.current.commandID
	}
	// If command_id equal, compare operation
	return h[i].iter.current.operation < h[j].iter.current.operation
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

// change represents a logical change row from the change table.
type change struct {
	startLSN   LSN // varbinary(10)
	endLSN     LSN // varbinary(10)
	operation  OpType
	updateMask []byte
	seqVal     []byte
	commandID  int
	columns    map[string]any
}

func (c *change) reset() {
	if c != nil {
		for k := range c.columns {
			delete(c.columns, k)
		}
		c.startLSN = nil
		c.endLSN = nil
		c.updateMask = nil
		c.seqVal = nil
		c.operation = 0
		c.commandID = 0
	}
}

// changeTableRowIter is responsible for handling the iteration of change table records, row by row.
// It moves to the next row, sorts them by min-heap based on LSN ordering criteria,
// parses the data and sends it for processing.
type changeTableRowIter struct {
	table    UserDefinedTable
	rows     *sql.Rows
	cols     []string
	colTypes []*sql.ColumnType
	current  *change
	log      *service.Logger

	vals []any

	// userColNames and userColTypes are the user-defined columns only,
	// excluding MSSQL system columns (those with __$ prefix).
	userColNames []string
	userColTypes []*sql.ColumnType
}

// newChangeTableRowIter returns an custom row iterator for the given changeTable.
func newChangeTableRowIter(
	ctx context.Context,
	db *sql.DB,
	changeTable UserDefinedTable,
	fromLSN, toLSN LSN,
	logger *service.Logger,
) (*changeTableRowIter, error) {
	// Note: LSN is varbinary type so can sort correctly for LSNs
	// Inspired by Debezium https://github.com/debezium/debezium/blob/main/debezium-connector-sqlserver/src/main/java/io/debezium/connector/sqlserver/SqlServerConnection.java?plain=1#L177

	// "Sequence of the operation as represented in the transaction log. Should not be used for ordering. Instead, use the __$command_id column"
	// source: https://learn.microsoft.com/en-us/sql/relational-databases/system-tables/cdc-capture-instance-ct-transact-sql?view=sql-server-ver17
	q := fmt.Sprintf("SELECT * FROM %s WITH (NOLOCK) WHERE (? IS NULL OR [__$start_lsn] > ?) AND (? IS NULL OR [__$start_lsn] <= ?) ORDER BY [__$start_lsn] ASC, [__$command_id] ASC, [__$operation] ASC", changeTable.ToChangeTable())
	rows, err := db.QueryContext(ctx, q, fromLSN, fromLSN, toLSN, toLSN) //nolint:rowserrcheck
	if err != nil {
		return nil, err
	}

	cols, err := rows.Columns()
	if err != nil {
		rows.Close()
		return nil, err
	}

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		rows.Close()
		return nil, err
	}

	// Compute user-defined column lists by filtering out MSSQL system columns
	// (those with the __$ prefix, e.g. __$start_lsn, __$operation, etc.).
	userColNames := make([]string, 0, len(cols))
	userColTypes := make([]*sql.ColumnType, 0, len(cols))
	for i, c := range cols {
		if !strings.HasPrefix(c, "__$") {
			userColNames = append(userColNames, c)
			userColTypes = append(userColTypes, colTypes[i])
		}
	}

	// Pre-allocate scan targets. For DECIMAL/NUMERIC and MONEY/SMALLMONEY
	// columns we scan into a string-shaped target so the driver hands back
	// the lossless text representation; everything else scans into a bare
	// any and lets the driver pick its native Go type.
	vals := make([]any, len(cols))
	for i := range vals {
		switch strings.ToUpper(colTypes[i].DatabaseTypeName()) {
		case "DECIMAL", "NUMERIC", "MONEY", "SMALLMONEY":
			vals[i] = new(sql.NullString)
		default:
			var v any
			vals[i] = &v
		}
	}

	iter := &changeTableRowIter{
		table:        changeTable,
		rows:         rows,
		cols:         cols,
		colTypes:     colTypes,
		vals:         vals,
		log:          logger,
		userColNames: userColNames,
		userColTypes: userColTypes,
	}
	// Prime the iterator by loading the first row
	if err := iter.next(); err != nil {
		// Already exhausted iterator
		closeErr := iter.Close()
		return nil, errors.Join(err, closeErr)
	}

	return iter, nil
}

func (ct *changeTableRowIter) next() error {
	if !ct.rows.Next() {
		// consult iterator error result before we can infer it's due to no rows.
		if err := ct.rows.Err(); err != nil {
			return err
		}
		return sql.ErrNoRows
	}

	// read row into ct.vals, reusing pre-allocated slice of pointer
	if err := ct.rows.Scan(ct.vals...); err != nil {
		return err
	}

	if ct.current == nil {
		ct.current = &change{columns: make(map[string]any, len(ct.cols))}
	} else {
		ct.current.reset()
	}

	if err := ct.mapValsToChange(ct.vals, ct.current); err != nil {
		return fmt.Errorf("mapping change table columns to iterator row: %w", err)
	}

	return nil
}

func (ct *changeTableRowIter) Close() error {
	return ct.rows.Close()
}

// mapValsToChange maps the values from vals to the dst out parameter.
func (ct *changeTableRowIter) mapValsToChange(vals []any, dst *change) error {
	for i, c := range ct.cols {
		v := unwrapScanTarget(vals[i])
		switch c {
		case "__$start_lsn":
			if b, ok := v.([]byte); ok {
				dst.startLSN = b
			} else {
				return errors.New("mapping 'start_lsn' column from change table")
			}
		case "__$end_lsn":
			// "In SQL Server 2012 (11.x), this column is always NULL."
			// https://learn.microsoft.com/en-us/sql/relational-databases/system-tables/cdc-capture-instance-ct-transact-sql?view=sql-server-ver16
			if b, ok := v.([]byte); ok {
				dst.endLSN = b
			} else if v == nil {
				dst.endLSN = nil
			} else {
				ct.log.Warnf("failed to map 'end_lsn' column from change table")
			}
		case "__$update_mask":
			if b, ok := v.([]byte); ok {
				dst.updateMask = b
			} else {
				return errors.New("mapping 'update_mask' column from change table")
			}
		case "__$operation":
			switch x := v.(type) {
			case int64:
				dst.operation = OpType(x)
			case int32:
				dst.operation = OpType(x)
			default:
				return errors.New("mapping 'operation' column from change table")
			}
		case "__$command_id":
			switch x := v.(type) {
			case int64:
				dst.commandID = int(x)
			case int32:
				dst.commandID = int(x)
			default:
				return errors.New("mapping 'command_id' column from change table")
			}
		case "__$seqval":
			if b, ok := v.([]byte); ok {
				dst.seqVal = b
			} else {
				return errors.New("mapping 'seqval' column from change table")
			}
		default:
			if ct.colTypes[i] != nil {
				dst.columns[c] = mapScannedValue(v, ct.colTypes[i])
			} else {
				dst.columns[c] = v
			}
		}
	}
	return nil
}

// unwrapScanTarget pulls the underlying value out of a slot pre-allocated by
// the streaming iterator. DECIMAL/NUMERIC/MONEY/SMALLMONEY columns scan into
// *sql.NullString to keep the driver from coercing to a lossy float64;
// everything else scans into *any.
func unwrapScanTarget(slot any) any {
	switch s := slot.(type) {
	case *sql.NullString:
		if !s.Valid {
			return nil
		}
		return []byte(s.String)
	case *any:
		return *s
	default:
		return slot
	}
}

// mapScannedValue takes an already-scanned value and column type, and converts it
// to the appropriate Go type for JSON marshaling.
func mapScannedValue(val any, colType *sql.ColumnType) any {
	if val == nil {
		return nil
	}

	typeName := colType.DatabaseTypeName()
	switch typeName {
	case "DECIMAL", "NUMERIC":
		// Decimals come as []byte from the driver. When precision/scale is
		// known, normalise to the canonical decimal string contract for
		// schema.Decimal columns. Without precision info, fall back to the
		// raw text — same shape as the column's String mapping.
		b, ok := val.([]byte)
		if !ok {
			return val
		}
		precision, scale, hasSize := colType.DecimalSize()
		if !hasSize {
			return string(b)
		}
		canonical, err := sqlutil.CanonicaliseDecimalBytes(b, int32(precision), int32(scale))
		if err != nil {
			return string(b)
		}
		return canonical
	case "MONEY", "SMALLMONEY":
		// MONEY/SMALLMONEY remain String-typed (see schema.go) so the
		// emitted wire form is a quoted canonical decimal string, matching
		// the schema's String declaration.
		if b, ok := val.([]byte); ok {
			canonical, err := sqlutil.CanonicaliseBigDecimalBytes(b)
			if err != nil {
				return string(b)
			}
			return canonical
		}
	}

	return val
}

// ChangePublisher is responsible for handling and processing of a replication.MessageEvent.
type ChangePublisher interface {
	Publish(ctx context.Context, msg MessageEvent) error
}

// ChangeTableStream tracks and streams all change events from the configured change
// tables tracked in tables.
type ChangeTableStream struct {
	tables          []UserDefinedTable
	backoffInterval time.Duration
	publisher       ChangePublisher
	log             *service.Logger
}

// NewChangeTableStream creates a new instance of NewChangeTableStream, responsible
// for paging through change events based on the tables param.
func NewChangeTableStream(tables []UserDefinedTable, publisher ChangePublisher, backoffInterval time.Duration, logger *service.Logger) *ChangeTableStream {
	s := &ChangeTableStream{
		tables:          tables,
		publisher:       publisher,
		backoffInterval: backoffInterval,
		log:             logger,
	}
	return s
}

// ReadChangeTables streams the change events from the configured SQL Server change tables.
func (r *ChangeTableStream) ReadChangeTables(ctx context.Context, db *sql.DB, startPos LSN) error {
	r.log.Infof("Starting streaming %d change table(s)", len(r.tables))
	var (
		startLSN LSN // load last checkpoint; nil means start from beginning in tables
		endLSN   LSN // often set to fn_cdc_get_max_lsn(); nil means no upper bound
		lastLSN  LSN
	)

	if len(startPos) != 0 {
		startLSN = startPos
		lastLSN = startPos
		r.log.Infof("Resuming from recorded LSN position '%s'", startPos)
	}

	for {
		// We have the "from" position, now fetch the "to" upper bound
		if err := db.QueryRowContext(ctx, "SELECT sys.fn_cdc_get_max_lsn()").Scan(&endLSN); err != nil {
			return err
		}

		// Create an iterator per table, table LSNs can be ordred but we need to create a global
		// ordering by merging them (which we do using a using a (min) heap).
		h := &rowIteratorMinHeap{}
		heap.Init(h)

		iters := make([]*changeTableRowIter, 0, len(r.tables))
		for _, changeTable := range r.tables {
			if len(startLSN) == 0 {
				// if no previous LSN is set, start from beginning dictated by tracking table
				startLSN = changeTable.startLSN
			}

			it, err := newChangeTableRowIter(ctx, db, changeTable, startLSN, endLSN, r.log)
			if err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					// No data means we can skip adding row iterator to the heap below
					r.log.Debugf("Exhausted all changes for change table '%s'", changeTable.ToChangeTable())
					continue
				}
				return fmt.Errorf("initialising iterator for change table '%s': %w", changeTable.ToChangeTable(), err)
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

			msg := MessageEvent{
				Table:       item.iter.table.Name,
				Schema:      item.iter.table.Schema,
				Data:        cur.columns,
				LSN:         cur.startLSN,
				Operation:   cur.operation.String(),
				ColumnNames: item.iter.userColNames,
				ColumnTypes: item.iter.userColTypes,
			}

			if err := r.publisher.Publish(ctx, msg); err != nil {
				// Clean up before returning error
				for _, it := range iters {
					_ = it.Close()
				}
				return err
			} else {
				// next page
				lastLSN = cur.startLSN
			}

			// Advance the iterator and push back on heap to be sorted
			if err := item.iter.next(); err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					r.log.Debugf("Reached end of rows for change table '%s'", item.iter.table.ToChangeTable())
				}
				// exhausted all rows
				item.iter.Close()
			} else {
				// put back advanced on the heap to sort it again
				heap.Push(h, item)
			}
		}

		if len(lastLSN) != 0 {
			if !bytes.Equal(startLSN, lastLSN) {
				startLSN = lastLSN
			} else {
				r.log.Debug("No more changes across all change tables, backing off...")
				time.Sleep(r.backoffInterval)
			}
		}
	}
}

// UserDefinedTable represents a found user's SQL Server table (called a user-defined table) in SQL.
type UserDefinedTable struct {
	Schema          string
	Name            string
	CaptureInstance string
	startLSN        LSN
}

// ToChangeTable returns a string in the SQL Server change table format of cdc.<capture_instance>_CT.
func (t *UserDefinedTable) ToChangeTable() string {
	return fmt.Sprintf("cdc.%s_CT", t.CaptureInstance)
}

// FullName returns a string of the table name including the schema (ie dbo.<tablename>).
func (t *UserDefinedTable) FullName() string {
	return fmt.Sprintf("%s.%s", t.Schema, t.Name)
}

// captureInstance is one row of cdc.change_tables for a given source table.
type captureInstance struct {
	name     string
	startLSN LSN
}

// VerifyUserDefinedTables verifies underlying user defined tables based on
// supplied include/exclude filters, resolving each one's CDC capture
// instance(s) via cdc.change_tables. capInstanceOverride optionally names a
// literal capture instance to prefer for a table with two capture instances;
// see resolveCaptureInstance for the full precedence.
func VerifyUserDefinedTables(ctx context.Context, db *sql.DB, tableFilter *confx.RegexpFilter, capInstanceOverride string, log *service.Logger) ([]UserDefinedTable, error) {
	q := `
	SELECT s.name AS SchemaName, t.name AS TableName, ct.capture_instance, ct.start_lsn
	FROM sys.tables t
	INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
	LEFT JOIN cdc.change_tables ct ON ct.source_object_id = t.object_id
	WHERE s.name != 'cdc'
	ORDER BY s.name, t.name, ct.capture_instance;`
	rows, err := db.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("fetching user defined tables and capture instances for verification: %w", err)
	}
	defer rows.Close()

	var (
		order     []string
		tables    = map[string]UserDefinedTable{}
		instances = map[string][]captureInstance{}
	)
	for rows.Next() {
		var (
			schema, name string
			instanceName sql.NullString
			startLSN     LSN
		)
		if err := rows.Scan(&schema, &name, &instanceName, &startLSN); err != nil {
			return nil, fmt.Errorf("scanning sys.tables row for user defined tables: %w", err)
		}

		fullName := fmt.Sprintf("%s.%s", schema, name)
		if !tableFilter.Matches(fullName) {
			continue
		}
		if _, ok := tables[fullName]; !ok {
			tables[fullName] = UserDefinedTable{Schema: schema, Name: name}
			order = append(order, fullName)
		}
		if instanceName.Valid {
			instances[fullName] = append(instances[fullName], captureInstance{name: instanceName.String, startLSN: startLSN})
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating through sys.tables for user defined tables: %w", err)
	}

	if len(order) == 0 {
		return nil, errors.New("no user defined tables found for given include and exclude filters")
	}

	userTables := make([]UserDefinedTable, 0, len(order))
	for _, fullName := range order {
		tbl := tables[fullName]
		if err := resolveCaptureInstance(&tbl, instances[fullName], capInstanceOverride, log); err != nil {
			return nil, err
		}
		if len(tbl.startLSN) == 0 {
			return nil, fmt.Errorf("field 'start_lsn' in change table '%s' expected to be set but was not", tbl.ToChangeTable())
		}
		userTables = append(userTables, tbl)
	}

	for _, t := range userTables {
		log.Infof("Found table '%s' and change table '%s'", t.FullName(), t.ToChangeTable())
	}

	return userTables, nil
}

func resolveCaptureInstance(tbl *UserDefinedTable, instances []captureInstance, override string, log *service.Logger) error {
	switch len(instances) {
	case 0:
		return fmt.Errorf("no change table found for table '%s': is CDC enabled for this table?", tbl.FullName())
	case 1:
		tbl.CaptureInstance = instances[0].name
		tbl.startLSN = instances[0].startLSN
		return nil
	}

	names := make([]string, len(instances))
	for i, inst := range instances {
		names[i] = inst.name
	}

	if override != "" {
		for _, inst := range instances {
			if inst.name == override {
				tbl.CaptureInstance = inst.name
				tbl.startLSN = inst.startLSN
				return nil
			}
		}
	}

	conventionName := fmt.Sprintf("%s_%s", tbl.Schema, tbl.Name)
	for _, inst := range instances {
		if inst.name != conventionName {
			continue
		}
		if override != "" {
			log.Warnf("Table '%s' has multiple CDC capture instances (%s); configured capture_instance '%s' does not match either, falling back to the default-named instance '%s'. "+
				"If this is a mid-migration cutover, check capture_instance matches the new instance's actual name.",
				tbl.FullName(), strings.Join(names, ", "), override, conventionName)
		} else {
			log.Warnf("Table '%s' has multiple CDC capture instances (%s); preferring the default-named instance '%s'. "+
				"If this is a mid-migration cutover, set capture_instance to the new instance's name, or drop the old one once the migration completes.",
				tbl.FullName(), strings.Join(names, ", "), conventionName)
		}
		tbl.CaptureInstance = inst.name
		tbl.startLSN = inst.startLSN
		return nil
	}

	if override != "" {
		return fmt.Errorf("table '%s' has multiple CDC capture instances (%s) and configured capture_instance '%s' does not match either", tbl.FullName(), strings.Join(names, ", "), override)
	}
	return fmt.Errorf("table '%s' has multiple CDC capture instances (%s): unable to determine which one to stream from", tbl.FullName(), strings.Join(names, ", "))
}
