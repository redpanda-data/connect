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
	"strings"

	"github.com/redpanda-data/connect/v4/internal/impl/db2/db2cli"
)

// SnapshotConfig holds configuration for the initial full-table snapshot phase.
// The snapshot reads all rows from each configured table inside a single
// read-only serializable (DB2 RR) transaction, ensuring a consistent view.
type SnapshotConfig struct {
	// Schemas is the list of DB2 source schemas (TABSCHEMA) whose tables are snapshotted.
	// Must have at least one element; each entry must be a valid DB2 identifier
	// (uppercase alphanumeric+underscore). For single-schema operation pass a one-element slice.
	Schemas []string

	// Tables is the list of table names (without schema prefix) to snapshot.
	// If empty, the snapshot is a no-op.
	Tables []string

	// AsnCDCSchema is the CDC control schema that owns IBMSNAP_REGISTER.
	// Defaults to "ASNCDC" if empty. Used to capture the starting CSN watermark
	// from ASNCDC.IBMSNAP_REGISTER before reading the first row.
	//
	// IBM reference: https://www.ibm.com/docs/en/db2/11.5?topic=tables-ibmsnap-register
	AsnCDCSchema string

	// BatchSize is the number of rows fetched per keyset-paginated query.
	// Reducing BatchSize lowers memory usage; increasing it improves throughput.
	BatchSize int

	// TableFilter narrows which tables in Tables are snapshotted. When nil all
	// tables in Tables are included. This allows table_include_regex /
	// table_exclude_regex from the connector config to apply to snapshot tables.
	TableFilter func(string) bool

	// Parallelism is the number of tables to snapshot concurrently.
	// 0 or 1 means sequential (default). Maximum is 16.
	// Note: all goroutines share the same read-only transaction, so this is safe
	// for correctness but each goroutine holds its own statement handle.
	Parallelism int

	// IsolationLevel is the SQL isolation level for the snapshot transaction.
	// Defaults to sql.LevelSerializable (DB2 "RR" — Repeatable Read, no phantoms).
	// sql.LevelReadCommitted (DB2 "CS") lowers lock overhead but does not guarantee
	// snapshot consistency across multiple tables.
	IsolationLevel sql.IsolationLevel
}

// effectiveIsolationLevel returns the configured isolation level, defaulting to
// sql.LevelSerializable (DB2 RR) when the field is zero.
func (c *SnapshotConfig) effectiveIsolationLevel() sql.IsolationLevel {
	if c.IsolationLevel == 0 {
		return sql.LevelSerializable
	}
	return c.IsolationLevel
}

// schemasINClause returns a SQL IN clause fragment like "'S1', 'S2'".
// Each schema name is validated before embedding so that callers that bypass
// Snapshot() cannot inject SQL via an unvalidated Schemas value.
func (c *SnapshotConfig) schemasINClause() string {
	quoted := make([]string, 0, len(c.Schemas))
	for _, s := range c.Schemas {
		if !isValidDB2IdentifierInternal(s) {
			continue
		}
		quoted = append(quoted, "'"+s+"'")
	}
	return strings.Join(quoted, ", ")
}

// asncdcSchema returns the CDC control schema, defaulting to "ASNCDC".
func (c *SnapshotConfig) asncdcSchema() string {
	if c.AsnCDCSchema != "" {
		return c.AsnCDCSchema
	}
	return "ASNCDC"
}

// Snapshotter performs the initial full-table read of all monitored tables
// before the CDC streaming phase begins.
//
// All tables are read inside a single read-only REPEATABLE READ transaction so
// that rows from different tables are consistent with each other. The CDC log
// position (CSN) is captured from ASNCDC.IBMSNAP_REGISTER *before* the first
// row is read; streaming resumes from this CSN, ensuring no changes are missed.
//
// Large tables are paginated using keyset pagination (ORDER BY pk / WHERE pk >
// lastKey FETCH FIRST N ROWS ONLY), which is efficient regardless of table size
// and avoids OFFSET-based scans.
type Snapshotter struct {
	db      *sql.DB
	config  SnapshotConfig
	version Version
}

// NewSnapshotter creates a Snapshotter that will use db to read rows, config
// to determine which tables to snapshot and what isolation level to use, and
// version to apply any DB2-version-specific behaviour.
func NewSnapshotter(db *sql.DB, config SnapshotConfig, version Version) *Snapshotter {
	return &Snapshotter{
		db:      db,
		config:  config,
		version: version,
	}
}

// Snapshot reads all rows from each configured table and returns the CSN that
// streaming should resume from (captured before the first table is read).
func (s *Snapshotter) Snapshot(ctx context.Context, handler func(event ChangeEvent) error) (CSN, error) {
	for _, schema := range s.config.Schemas {
		if !isValidDB2IdentifierInternal(schema) {
			return CSN{}, fmt.Errorf("invalid schema %q: must be non-empty uppercase alphanumeric+underscore", schema)
		}
	}

	// sql.LevelSerializable maps to DB2 "RR" (Repeatable Read — no phantoms).
	// sql.LevelRepeatableRead maps to DB2 "RS" (Read Stability — phantoms allowed),
	// which is weaker than intended. Use Serializable for correct snapshot isolation.
	// Full fix (SET CURRENT ISOLATION RR per statement) is tracked separately.
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{
		Isolation: s.config.effectiveIsolationLevel(),
		ReadOnly:  true,
	})
	if err != nil {
		return CSN{}, fmt.Errorf("beginning snapshot transaction: %w", err)
	}
	// Note: this transaction holds the sole connection in db's pool (SetMaxOpenConns(1)).
	// Any concurrent caller (schema-change polling, signal processing) blocks until this
	// transaction commits. Full fix requires a separate db handle for snapshot queries.
	defer tx.Rollback() //nolint:errcheck

	startCSN, err := s.captureCurrentCSN(ctx, tx)
	if err != nil {
		return CSN{}, fmt.Errorf("capturing starting CSN: %w", err)
	}

	if err = s.snapshotTablesSequential(ctx, tx, handler); err != nil {
		return CSN{}, fmt.Errorf("snapshot failed: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return CSN{}, fmt.Errorf("committing snapshot transaction: %w", err)
	}

	return startCSN, nil
}

// captureCurrentCSN reads MAX(SYNCHPOINT) from ASNCDC.IBMSNAP_REGISTER for the
// monitored schema. SYNCHPOINT is the CDC log position the capture daemon has
// confirmed writing; streaming will resume from this value after the snapshot.
//
// SYNCHPOINT (not CURRENT TIMESTAMP or a DB2 log-position function) is used
// because it reflects the capture daemon's confirmed write position — the same
// value that change-table poll queries compare against.
//
// For freshly registered tables SYNCHPOINT may be NULL (the capture daemon has not
// yet processed any transactions). CD_NEW_SYNCHPOINT is always populated at
// ASNCDC.ADDTABLE time, so the UNION over both columns mirrors getUpperBound in
// stream.go and guarantees a non-null watermark even for brand-new registrations.
func (s *Snapshotter) captureCurrentCSN(ctx context.Context, tx *sql.Tx) (CSN, error) {
	cdcSchema := s.config.asncdcSchema()
	schemasIN := s.config.schemasINClause()

	// Schema names are validated as uppercase alphanumeric + underscore at config-parse
	// time (isValidDB2Identifier in input_db2_cdc.go), so direct embedding is safe.
	query := fmt.Sprintf(`
		SELECT MAX(t.SYNCHPOINT) FROM (
			SELECT CD_NEW_SYNCHPOINT AS SYNCHPOINT
			  FROM %s.IBMSNAP_REGISTER
			 WHERE SOURCE_OWNER IN (%s)
			UNION ALL
			SELECT SYNCHPOINT
			  FROM %s.IBMSNAP_REGISTER
			 WHERE SOURCE_OWNER IN (%s)
		) t`,
		cdcSchema, schemasIN,
		cdcSchema, schemasIN,
	)

	var synchpointRaw any
	err := tx.QueryRowContext(ctx, query).Scan(&synchpointRaw)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) || isObjectNotFoundError(err) {
			// CDC tables not yet set up — return a null CSN so the caller
			// treats this as "no watermark" rather than "watermark at 0".
			return NullCSN(), nil
		}
		return CSN{}, fmt.Errorf("capturing CSN from IBMSNAP_REGISTER: %w", err)
	}

	if synchpointRaw == nil {
		return NullCSN(), nil
	}

	// SYNCHPOINT is VARCHAR(16) FOR BIT DATA; the DB2 CLI driver may return
	// it as a hex-encoded string — getBitDataBytes hex-decodes it if needed.
	synchpointBytes := getBitDataBytes(&synchpointRaw)
	if len(synchpointBytes) == 0 {
		return NullCSN(), nil
	}

	return NewCSNFromDBValue(synchpointBytes), nil
}

// isObjectNotFoundError returns true when err indicates that a table, view, or
// other named object does not exist in the catalog. Checks the structured
// *db2cli.DB2Error SQLSTATE first (exact match, locale-independent); falls back
// to substring matching for errors that have been wrapped into plain strings.
//
// Relevant SQLSTATE codes:
//
//	42704 — DB2 "undefined name" (object not in catalog)
//	42S02 — ODBC "base table or view not found"
func isObjectNotFoundError(err error) bool {
	if db2Err, ok := errors.AsType[*db2cli.DB2Error](err); ok {
		return db2Err.SQLState == "42704" || db2Err.SQLState == db2cli.SQLSTATE_TABLE_OR_VIEW_NOT_FOUND
	}
	// Fallback: match the DB2 error string format "DB2 Error [42704] ..." for
	// heavily-wrapped errors where AsType cannot unwrap to *db2cli.DB2Error.
	// We match only the bracketed SQLSTATE form rather than free English phrases
	// ("does not exist", "not found") to avoid swallowing unrelated errors that
	// happen to contain those words in their message text.
	msg := err.Error()
	return strings.Contains(msg, "[42704]") || strings.Contains(msg, "[42S02]")
}

// snapshotTablesSequential snapshots tables one at a time in the order they
// appear in config.Tables. Parallel snapshotting is not supported because
// sharing a *sql.Tx across goroutines is a data race on the ODBC handle;
// maxSnapshotParallelism enforces this at the config layer.
func (s *Snapshotter) snapshotTablesSequential(ctx context.Context, tx *sql.Tx, handler func(event ChangeEvent) error) error {
	for _, schema := range s.config.Schemas {
		for _, t := range s.config.Tables {
			if s.config.TableFilter != nil && !s.config.TableFilter(t) {
				continue
			}
			if err := s.snapshotTable(ctx, tx, schema, t, handler); err != nil {
				return fmt.Errorf("snapshotting table %s.%s: %w", schema, t, err)
			}
		}
	}
	return nil
}

// snapshotTable reads all rows of one table using keyset pagination.
func (s *Snapshotter) snapshotTable(ctx context.Context, tx *sql.Tx, schema, tableName string, handler func(event ChangeEvent) error) error {
	pks, err := s.discoverPrimaryKeys(ctx, tx, schema, tableName)
	if err != nil {
		return fmt.Errorf("discovering primary keys: %w", err)
	}

	if len(pks) == 0 {
		return fmt.Errorf("table %s.%s has no primary key (required for CDC snapshot)", schema, tableName)
	}
	for _, pk := range pks {
		if !isValidDB2IdentifierInternal(pk) {
			return fmt.Errorf("catalog returned unsafe PK column name %q for %s.%s", pk, schema, tableName)
		}
	}

	columns, err := s.getTableColumns(ctx, tx, schema, tableName)
	if err != nil {
		return fmt.Errorf("getting table columns: %w", err)
	}
	for _, col := range columns {
		if !isValidDB2IdentifierInternal(col) {
			return fmt.Errorf("catalog returned unsafe column name %q for %s.%s", col, schema, tableName)
		}
	}

	// Pre-compute per-table SQL queries once; avoids repeated quoting/joining across all batch iterations.
	quotedCols := make([]string, len(columns))
	for i, col := range columns {
		quotedCols[i] = QuoteDB2Identifier(col)
	}
	quotedPKs := make([]string, len(pks))
	for i, pk := range pks {
		quotedPKs[i] = QuoteDB2Identifier(pk)
	}
	orderByClause := strings.Join(quotedPKs, ", ")
	baseSelect := fmt.Sprintf(`SELECT %s FROM %s.%s`,
		strings.Join(quotedCols, ", "),
		QuoteDB2Identifier(schema),
		QuoteDB2Identifier(tableName),
	)
	suffix := fmt.Sprintf(" ORDER BY %s FETCH FIRST %d ROWS ONLY", orderByClause, s.config.BatchSize)
	queryNoBounds := baseSelect + suffix
	placeholders := strings.TrimSuffix(strings.Repeat("?, ", len(pks)), ", ")
	queryWithBounds := baseSelect + fmt.Sprintf(" WHERE (%s) > (%s)", orderByClause, placeholders) + suffix

	lastKeyValues := make([]any, len(pks)) // allocated once; reused across batch iterations
	var hasLastKey bool
	var colKindCache []db2ColumnKind // nil on first batch; populated from ColumnTypes() and reused

	for {
		var batchKeyArg []any
		if hasLastKey {
			batchKeyArg = lastKeyValues
		}
		batchRows, err := s.fetchBatch(ctx, tx, queryNoBounds, queryWithBounds, columns, batchKeyArg, &colKindCache)
		if err != nil {
			return fmt.Errorf("fetching batch: %w", err)
		}

		if len(batchRows) == 0 {
			break
		}

		for _, row := range batchRows {
			event := ChangeEvent{
				Schema:    schema,
				Table:     tableName,
				Operation: OpTypeRead,
				CSN:       NullCSN(),
				Data:      row,
				PKColumns: pks,
			}

			if err := handler(event); err != nil {
				return fmt.Errorf("handler error: %w", err)
			}
		}

		lastRow := batchRows[len(batchRows)-1]
		for i, pk := range pks {
			v := lastRow[pk]
			if v == nil {
				// NULL in a PK column would produce (pk > NULL) in the next batch's
				// WHERE clause, which DB2 evaluates as UNKNOWN — returning zero rows
				// and silently truncating the snapshot mid-table.
				return fmt.Errorf("snapshot keyset cursor: NULL value in PK column %q for %s.%s; snapshot aborted to prevent row loss", pk, schema, tableName)
			}
			lastKeyValues[i] = v
		}
		hasLastKey = true

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}

	return nil
}

// discoverPrimaryKeys returns the primary key column names for a table, ordered by sequence.
func (*Snapshotter) discoverPrimaryKeys(ctx context.Context, tx *sql.Tx, schema, tableName string) ([]string, error) {
	query := `
		SELECT COLNAME
		FROM SYSCAT.KEYCOLUSE
		WHERE TABSCHEMA = ?
		  AND TABNAME = ?
		  AND CONSTNAME = (
		    SELECT CONSTNAME
		    FROM SYSCAT.TABCONST
		    WHERE TABSCHEMA = ?
		      AND TABNAME = ?
		      AND TYPE = 'P'
		  )
		ORDER BY COLSEQ
	`

	rows, err := tx.QueryContext(ctx, query, schema, tableName, schema, tableName)
	if err != nil {
		return nil, fmt.Errorf("querying primary keys: %w", err)
	}
	defer rows.Close()

	var pks []string
	for rows.Next() {
		var colName string
		if err := rows.Scan(&colName); err != nil {
			return nil, err
		}
		pks = append(pks, strings.TrimSpace(colName))
	}

	return pks, rows.Err()
}

// getTableColumns returns all column names for a table, in column order.
func (*Snapshotter) getTableColumns(ctx context.Context, tx *sql.Tx, schema, tableName string) ([]string, error) {
	query := `
		SELECT COLNAME
		FROM SYSCAT.COLUMNS
		WHERE TABSCHEMA = ?
		  AND TABNAME = ?
		ORDER BY COLNO
	`

	rows, err := tx.QueryContext(ctx, query, schema, tableName)
	if err != nil {
		return nil, fmt.Errorf("querying columns: %w", err)
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var colName string
		if err := rows.Scan(&colName); err != nil {
			return nil, err
		}
		columns = append(columns, strings.TrimSpace(colName))
	}

	if len(columns) == 0 {
		return nil, fmt.Errorf("table %s.%s has no columns", schema, tableName)
	}

	return columns, rows.Err()
}

// fetchBatch fetches one page of rows using keyset pagination.
// queryNoBounds and queryWithBounds must be pre-computed by the caller (snapshotTable)
// so this hot-path function avoids per-batch string allocation.
// colKindCache is a write-through pointer: nil on the first call, populated after the
// first call and reused on all subsequent calls so SQLDescribeCol is only invoked once per table.
func (s *Snapshotter) fetchBatch(ctx context.Context, tx *sql.Tx, queryNoBounds, queryWithBounds string, columns []string, lastKeyValues []any, colKindCache *[]db2ColumnKind) ([]map[string]any, error) {
	var query string
	var args []any
	if len(lastKeyValues) > 0 {
		query = queryWithBounds
		args = lastKeyValues
	} else {
		query = queryNoBounds
	}

	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("executing batch query: %w", err)
	}
	defer rows.Close()

	// colKinds: computed once on the first batch via SQLDescribeCol and cached for
	// all subsequent pages of the same table (column schema is stable across pages).
	colKinds := *colKindCache
	if colKinds == nil {
		columnTypes, err := rows.ColumnTypes()
		if err != nil {
			return nil, fmt.Errorf("getting column types: %w", err)
		}
		colKinds = make([]db2ColumnKind, len(columnTypes))
		for i, ct := range columnTypes {
			colKinds[i] = columnKind(ct.DatabaseTypeName())
		}
		*colKindCache = colKinds
	}

	result := make([]map[string]any, 0, s.config.BatchSize)

	// Reuse scanDest and scanPtrs across rows to avoid one allocation per column per row.
	scanDest := make([]any, len(columns))
	scanPtrs := make([]any, len(columns))
	for i := range scanDest {
		scanPtrs[i] = &scanDest[i]
	}

	for rows.Next() {
		// Clear previous row values before scanning (avoids stale data on nil columns).
		clear(scanDest)

		if err := rows.Scan(scanPtrs...); err != nil {
			return nil, fmt.Errorf("scanning row: %w", err)
		}

		rowMap := make(map[string]any, len(columns))
		for i, col := range columns {
			rowMap[col] = convertDB2Value(scanDest[i], colKinds[i])
		}

		result = append(result, rowMap)
	}

	return result, rows.Err()
}

// convertDB2Value converts a DB2 driver value to a JSON-serializable Go type.
// kind must be pre-computed from the column's DatabaseTypeName() before the row loop.
func convertDB2Value(value any, kind db2ColumnKind) any {
	if value == nil {
		return nil
	}

	if b, ok := value.([]byte); ok {
		if kind == db2ColumnKindText {
			return string(b)
		}
		// Binary / unknown types: return raw bytes (JSON-marshalled as base64).
		return b
	}

	return value
}
