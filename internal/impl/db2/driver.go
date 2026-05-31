// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

// Package db2 provides a database/sql driver for IBM DB2 implemented without
// CGO. The driver is named "db2-cli" and registered via database/sql.Register
// in the package's init function.
//
// The driver loads the IBM DB2 CLI shared library at runtime using
// github.com/ebitengine/purego (dlopen/LoadLibrary), so no C compiler or CGO
// toolchain is required at build time. The CLI (ODBC-compatible C API) call
// sequence is: SQLAllocHandle → SQLDriverConnect → SQLPrepare →
// SQLBindParameter → SQLExecute → SQLFetch → SQLGetData → SQLFreeHandle.
//
// Parameter binding uses SQL_C_CHAR / SQL_VARCHAR for all types. CDC stream
// queries embed binary CSN values as hex literals (X'...') to avoid needing
// SQL_C_BINARY for CHAR FOR BIT DATA columns.
//
// Memory safety: bindParams returns both the data buffers (bufs) and the
// indicator lengths (inds). Both must be kept alive via runtime.KeepAlive until
// after SQLExecute returns. The Go GC does not track C-side pointer references,
// so any local variable whose address is passed to SQLBindParameter must be
// heap-allocated (via a slice) and explicitly retained.
package db2

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"unsafe"

	"golang.org/x/text/encoding/unicode"

	"github.com/redpanda-data/connect/v4/internal/impl/db2/db2cli"
)

// utf16LE is cached at package level so decodeUTF16LE avoids reconstructing
// the encoding descriptor on every GRAPHIC/VARGRAPHIC column value.
var utf16LE = unicode.UTF16(unicode.LittleEndian, unicode.IgnoreBOM)

// connSecretRE matches credential keywords in DB2 connection-string fragments
// that some driver versions include verbatim in error messages. Matches empty
// values (PWD=;) and the UID keyword in addition to PWD.
var connSecretRE = regexp.MustCompile(`(?i)\b(PWD|PASSWORD|UID|USERNAME|SECURITY)=[^\s;]*`)

// sanitizeConnErr replaces any credential keyword values in a DB2 connection
// error message with "***" before the error is wrapped and propagated to logs.
func sanitizeConnErr(err error) error {
	if err == nil {
		return nil
	}
	sanitized := connSecretRE.ReplaceAllStringFunc(err.Error(), func(m string) string {
		// Preserve the keyword, replace only the value.
		idx := strings.IndexByte(m, '=')
		return m[:idx+1] + "***"
	})
	if sanitized == err.Error() {
		return err
	}
	return errors.New(sanitized)
}

// cTypeForSQLType returns the ODBC C transfer type for a given SQL type.
// Using the wrong C type causes silent data corruption:
//   - SQL_C_CHAR on BLOB columns truncates at the first zero byte.
//   - SQL_C_CHAR on VARGRAPHIC/GRAPHIC returns raw UTF-16LE bytes that look
//     like garbage when interpreted as UTF-8.
func cTypeForSQLType(sqlType db2cli.SQLSMALLINT) db2cli.SQLSMALLINT {
	switch sqlType {
	case db2cli.SQL_BINARY, db2cli.SQL_VARBINARY, db2cli.SQL_LONGVARBINARY, db2cli.SQL_BLOB:
		return db2cli.SQL_C_BINARY
	case db2cli.SQL_WCHAR, db2cli.SQL_WVARCHAR, db2cli.SQL_WLONGVARCHAR,
		db2cli.SQL_GRAPHIC, db2cli.SQL_VARGRAPHIC, db2cli.SQL_LONGVARGRAPHIC, db2cli.SQL_DBCLOB:
		return db2cli.SQL_C_WCHAR
	default:
		return db2cli.SQL_C_CHAR
	}
}

// db2Driver implements database/sql/driver.Driver using purego DB2 CLI bindings.
type db2Driver struct{}

func init() {
	sql.Register("db2-cli", &db2Driver{})
}

func (*db2Driver) Open(dsn string) (driver.Conn, error) {
	if err := db2cli.LoadLibrary(); err != nil {
		return nil, fmt.Errorf("loading DB2 CLI library: %w", err)
	}

	var henv db2cli.SQLHENV
	ret := db2cli.SQLAllocHandle(db2cli.SQL_HANDLE_ENV, db2cli.SQLHANDLE(db2cli.SQL_NULL_HENV), (*db2cli.SQLHANDLE)(&henv))
	if ret != db2cli.SQL_SUCCESS && ret != db2cli.SQL_SUCCESS_WITH_INFO {
		return nil, errors.New("allocating environment handle")
	}

	ret = db2cli.SQLSetEnvAttr(henv, db2cli.SQL_ATTR_ODBC_VERSION, db2cli.SQL_OV_ODBC3, 0)
	if ret != db2cli.SQL_SUCCESS && ret != db2cli.SQL_SUCCESS_WITH_INFO {
		db2cli.SQLFreeHandle(db2cli.SQL_HANDLE_ENV, db2cli.SQLHANDLE(henv))
		return nil, errors.New("setting ODBC version")
	}

	var hdbc db2cli.SQLHDBC
	ret = db2cli.SQLAllocHandle(db2cli.SQL_HANDLE_DBC, db2cli.SQLHANDLE(henv), (*db2cli.SQLHANDLE)(&hdbc))
	if ret != db2cli.SQL_SUCCESS && ret != db2cli.SQL_SUCCESS_WITH_INFO {
		db2cli.SQLFreeHandle(db2cli.SQL_HANDLE_ENV, db2cli.SQLHANDLE(henv))
		return nil, errors.New("allocating connection handle")
	}

	_, ret = db2cli.SQLDriverConnect(hdbc, dsn)
	if ret != db2cli.SQL_SUCCESS && ret != db2cli.SQL_SUCCESS_WITH_INFO {
		err := db2cli.GetLastError(db2cli.SQL_HANDLE_DBC, db2cli.SQLHANDLE(hdbc))
		db2cli.SQLFreeHandle(db2cli.SQL_HANDLE_DBC, db2cli.SQLHANDLE(hdbc))
		db2cli.SQLFreeHandle(db2cli.SQL_HANDLE_ENV, db2cli.SQLHANDLE(henv))
		return nil, fmt.Errorf("connecting: %w", sanitizeConnErr(err))
	}

	return &db2Conn{henv: henv, hdbc: hdbc}, nil
}

// db2Conn implements driver.Conn.
type db2Conn struct {
	henv db2cli.SQLHENV
	hdbc db2cli.SQLHDBC
}

func (c *db2Conn) Prepare(query string) (driver.Stmt, error) {
	var hstmt db2cli.SQLHSTMT
	ret := db2cli.SQLAllocHandle(db2cli.SQL_HANDLE_STMT, db2cli.SQLHANDLE(c.hdbc), (*db2cli.SQLHANDLE)(&hstmt))
	if ret != db2cli.SQL_SUCCESS && ret != db2cli.SQL_SUCCESS_WITH_INFO {
		return nil, errors.New("allocating statement handle")
	}

	ret = db2cli.SQLPrepare(hstmt, query)
	if ret != db2cli.SQL_SUCCESS && ret != db2cli.SQL_SUCCESS_WITH_INFO {
		err := db2cli.GetLastError(db2cli.SQL_HANDLE_STMT, db2cli.SQLHANDLE(hstmt))
		db2cli.SQLFreeHandle(db2cli.SQL_HANDLE_STMT, db2cli.SQLHANDLE(hstmt))
		return nil, fmt.Errorf("preparing statement: %w", err)
	}

	return &db2Stmt{hstmt: hstmt, conn: c}, nil
}

func (c *db2Conn) Close() error {
	if c.hdbc != 0 {
		db2cli.SQLDisconnect(c.hdbc)
		db2cli.SQLFreeHandle(db2cli.SQL_HANDLE_DBC, db2cli.SQLHANDLE(c.hdbc))
		c.hdbc = 0
	}
	if c.henv != 0 {
		db2cli.SQLFreeHandle(db2cli.SQL_HANDLE_ENV, db2cli.SQLHANDLE(c.henv))
		c.henv = 0
	}
	return nil
}

func (c *db2Conn) Begin() (driver.Tx, error) {
	ret := db2cli.SQLSetConnectAttr(c.hdbc, db2cli.SQL_ATTR_AUTOCOMMIT, db2cli.SQL_AUTOCOMMIT_OFF, 0)
	if ret != db2cli.SQL_SUCCESS && ret != db2cli.SQL_SUCCESS_WITH_INFO {
		return nil, errors.New("beginning transaction")
	}
	return &db2Tx{conn: c}, nil
}

// BeginTx implements driver.ConnBeginTx so that sql.TxOptions.Isolation is honoured.
// Without this, database/sql falls back to Begin() and silently drops the isolation level.
func (c *db2Conn) BeginTx(_ context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if opts.Isolation != driver.IsolationLevel(sql.LevelDefault) {
		isoConst, err := isolationLevelToDB2(sql.IsolationLevel(opts.Isolation))
		if err != nil {
			return nil, err
		}
		ret := db2cli.SQLSetConnectAttr(c.hdbc, db2cli.SQL_ATTR_TXN_ISOLATION, isoConst, 0)
		if ret != db2cli.SQL_SUCCESS && ret != db2cli.SQL_SUCCESS_WITH_INFO {
			return nil, fmt.Errorf("setting transaction isolation level: %w",
				db2cli.GetLastError(db2cli.SQL_HANDLE_DBC, db2cli.SQLHANDLE(c.hdbc)))
		}
	}
	ret := db2cli.SQLSetConnectAttr(c.hdbc, db2cli.SQL_ATTR_AUTOCOMMIT, db2cli.SQL_AUTOCOMMIT_OFF, 0)
	if ret != db2cli.SQL_SUCCESS && ret != db2cli.SQL_SUCCESS_WITH_INFO {
		return nil, errors.New("beginning transaction")
	}
	return &db2Tx{conn: c}, nil
}

// QueryContext implements driver.QueryerContext so that context cancellation
// interrupts an in-flight SQL query via SQLCancel. Without this, a blocked
// SQLExecute cannot be interrupted, causing goroutine leaks on context cancel.
func (c *db2Conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	vals := make([]driver.Value, len(args))
	for i, a := range args {
		vals[i] = a.Value
	}
	stmt, err := c.Prepare(query)
	if err != nil {
		return nil, err
	}
	db2s := stmt.(*db2Stmt)

	type result struct {
		rows driver.Rows
		err  error
	}
	done := make(chan result, 1)
	go func() {
		rows, e := db2s.Query(vals)
		done <- result{rows, e}
	}()

	select {
	case res := <-done:
		if res.err != nil {
			_ = stmt.Close()
		}
		return res.rows, res.err
	case <-ctx.Done():
		db2cli.SQLCancel(db2s.hstmt)
		// Drain the result so the goroutine can exit, then discard any
		// successfully-produced rows to avoid leaking the 32 KB colBuf.
		res := <-done
		if res.rows != nil {
			_ = res.rows.Close()
		}
		_ = stmt.Close()
		return nil, ctx.Err()
	}
}

// ExecContext implements driver.ExecerContext so that context cancellation
// interrupts an in-flight DML/DDL statement via SQLCancel. Without this,
// database/sql falls back to Prepare+Exec which blocks without cancel support.
func (c *db2Conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	vals := make([]driver.Value, len(args))
	for i, a := range args {
		vals[i] = a.Value
	}
	stmt, err := c.Prepare(query)
	if err != nil {
		return nil, err
	}
	db2s := stmt.(*db2Stmt)

	type result struct {
		res driver.Result
		err error
	}
	done := make(chan result, 1)
	go func() {
		res, e := db2s.Exec(vals)
		done <- result{res, e}
	}()

	select {
	case res := <-done:
		// Always free the statement handle — db2Result holds no reference to
		// the stmt, so without this call the SQLHSTMT leaks until connection
		// close. Under high-frequency ExecContext call patterns (e.g. bulk INSERTs)
		// this exhausts the CLI driver's handle pool (HY014 CLI0129E).
		_ = stmt.Close()
		return res.res, res.err
	case <-ctx.Done():
		db2cli.SQLCancel(db2s.hstmt)
		<-done // wait for goroutine to finish after cancel
		_ = stmt.Close()
		return nil, ctx.Err()
	}
}

// isolationLevelToDB2 maps a sql.IsolationLevel to the corresponding DB2 CLI constant.
func isolationLevelToDB2(level sql.IsolationLevel) (uintptr, error) {
	switch level {
	case sql.LevelReadUncommitted:
		return db2cli.SQL_TXN_READ_UNCOMMITTED, nil
	case sql.LevelReadCommitted:
		return db2cli.SQL_TXN_READ_COMMITTED, nil
	case sql.LevelRepeatableRead:
		return db2cli.SQL_TXN_REPEATABLE_READ, nil
	case sql.LevelSerializable:
		return db2cli.SQL_TXN_SERIALIZABLE, nil
	default:
		return 0, fmt.Errorf("unsupported transaction isolation level: %v", level)
	}
}

// db2Tx implements driver.Tx.
type db2Tx struct {
	conn *db2Conn
}

func (tx *db2Tx) Commit() error {
	ret := db2cli.SQLEndTran(db2cli.SQL_HANDLE_DBC, db2cli.SQLHANDLE(tx.conn.hdbc), db2cli.SQL_COMMIT)
	// Capture the commit error BEFORE calling SQLSetConnectAttr: the subsequent
	// ODBC call overwrites the connection's diagnostic record, so GetLastError
	// would return the wrong error if called after the autocommit restore.
	var commitErr error
	if ret != db2cli.SQL_SUCCESS && ret != db2cli.SQL_SUCCESS_WITH_INFO {
		commitErr = db2cli.GetLastError(db2cli.SQL_HANDLE_DBC, db2cli.SQLHANDLE(tx.conn.hdbc))
	}
	// Always restore AUTOCOMMIT regardless of SQLEndTran outcome (MI-10):
	// if SQLEndTran fails the connection is in an unknown state but must still
	// be usable for future queries — leaving AUTOCOMMIT off would corrupt it.
	restoreRet := db2cli.SQLSetConnectAttr(tx.conn.hdbc, db2cli.SQL_ATTR_AUTOCOMMIT, db2cli.SQL_AUTOCOMMIT_ON, 0)
	if commitErr != nil {
		return fmt.Errorf("committing transaction: %w", commitErr)
	}
	if restoreRet != db2cli.SQL_SUCCESS && restoreRet != db2cli.SQL_SUCCESS_WITH_INFO {
		return fmt.Errorf("re-enabling autocommit after commit: %w",
			db2cli.GetLastError(db2cli.SQL_HANDLE_DBC, db2cli.SQLHANDLE(tx.conn.hdbc)))
	}
	return nil
}

func (tx *db2Tx) Rollback() error {
	ret := db2cli.SQLEndTran(db2cli.SQL_HANDLE_DBC, db2cli.SQLHANDLE(tx.conn.hdbc), db2cli.SQL_ROLLBACK)
	// Capture rollback error before autocommit restore (see Commit for rationale).
	var rollbackErr error
	if ret != db2cli.SQL_SUCCESS && ret != db2cli.SQL_SUCCESS_WITH_INFO {
		rollbackErr = db2cli.GetLastError(db2cli.SQL_HANDLE_DBC, db2cli.SQLHANDLE(tx.conn.hdbc))
	}
	// Always restore AUTOCOMMIT regardless of SQLEndTran outcome (MI-10).
	restoreRet := db2cli.SQLSetConnectAttr(tx.conn.hdbc, db2cli.SQL_ATTR_AUTOCOMMIT, db2cli.SQL_AUTOCOMMIT_ON, 0)
	if rollbackErr != nil {
		return fmt.Errorf("rolling back transaction: %w", rollbackErr)
	}
	if restoreRet != db2cli.SQL_SUCCESS && restoreRet != db2cli.SQL_SUCCESS_WITH_INFO {
		return fmt.Errorf("re-enabling autocommit after rollback: %w",
			db2cli.GetLastError(db2cli.SQL_HANDLE_DBC, db2cli.SQLHANDLE(tx.conn.hdbc)))
	}
	return nil
}

// db2Stmt implements driver.Stmt.
type db2Stmt struct {
	hstmt db2cli.SQLHSTMT
	conn  *db2Conn
}

func (s *db2Stmt) Close() error {
	if s.hstmt != 0 {
		db2cli.SQLFreeHandle(db2cli.SQL_HANDLE_STMT, db2cli.SQLHANDLE(s.hstmt))
		s.hstmt = 0
	}
	return nil
}

func (*db2Stmt) NumInput() int {
	return -1
}

// driverValueToString converts a driver.Value to its SQL string representation.
//
// Special-casing float64 is necessary because JSON unmarshal produces float64
// for all JSON numbers, so primary-key values restored from the incremental
// snapshot checkpoint (LastEmittedPK) arrive as float64. fmt.Sprintf("%v",
// float64(1234567890)) produces "1.23456789e+09" which is not a valid integer
// literal for a SQL WHERE clause. Instead: if the float64 value is a whole
// number and within the int64 range, format it as a plain decimal integer.
func driverValueToString(v driver.Value) string {
	switch val := v.(type) {
	case json.Number:
		return val.String()
	case float64:
		if val == math.Trunc(val) && val >= math.MinInt64 && val <= math.MaxInt64 {
			return strconv.FormatInt(int64(val), 10)
		}
		return strconv.FormatFloat(val, 'f', -1, 64)
	case bool:
		if val {
			return "1"
		}
		return "0"
	case []byte:
		// DECIMAL/NUMERIC and CHAR/VARCHAR columns arrive from the DB2 ODBC driver as
		// []byte. Return the raw string so keyset-pagination cursors round-trip
		// correctly — fmt.Sprintf("%v", []byte{49,50}) would produce "[49 50]", not "12".
		return string(val)
	case string:
		return val
	default:
		return fmt.Sprintf("%v", v)
	}
}

// bindParams binds each driver.Value to the prepared statement as SQL_C_CHAR.
// Returns (bufs, inds): both slices must stay alive via runtime.KeepAlive until
// after SQLExecute returns. bufs holds the null-terminated data buffers;
// inds holds the length indicators. The Go GC does not track C-side pointer
// references, so local variables whose addresses are passed to SQLBindParameter
// must live in heap-allocated slices that are explicitly retained.
func (s *db2Stmt) bindParams(args []driver.Value) (bufs [][]byte, inds []db2cli.SQLLEN, err error) {
	bufs = make([][]byte, len(args))
	inds = make([]db2cli.SQLLEN, len(args))

	for i, arg := range args {
		pos := db2cli.SQLUSMALLINT(i + 1)

		if arg == nil {
			inds[i] = db2cli.SQLLEN(db2cli.SQL_NULL_DATA)
			ret := db2cli.SQLBindParameter(
				s.hstmt, pos,
				db2cli.SQL_PARAM_INPUT, db2cli.SQL_C_CHAR, db2cli.SQL_VARCHAR,
				0, 0, nil, 0, &inds[i],
			)
			if ret != db2cli.SQL_SUCCESS && ret != db2cli.SQL_SUCCESS_WITH_INFO {
				return nil, nil, fmt.Errorf("binding NULL parameter %d: %w", i+1,
					db2cli.GetLastError(db2cli.SQL_HANDLE_STMT, db2cli.SQLHANDLE(s.hstmt)))
			}
			continue
		}

		strVal := driverValueToString(arg)
		// Null-terminate so DB2 CLI can treat it as a C string.
		// Pre-allocate len+1 to avoid the two-allocation pattern of
		// append([]byte(strVal), 0) which copies then grows.
		buf := make([]byte, len(strVal)+1)
		copy(buf, strVal)
		bufs[i] = buf

		inds[i] = db2cli.SQLLEN(len(strVal))
		ret := db2cli.SQLBindParameter(
			s.hstmt, pos,
			db2cli.SQL_PARAM_INPUT, db2cli.SQL_C_CHAR, db2cli.SQL_VARCHAR,
			db2cli.SQLULEN(len(strVal)), 0,
			db2cli.SQLPOINTER(unsafe.Pointer(&buf[0])),
			db2cli.SQLLEN(len(buf)),
			&inds[i],
		)
		if ret != db2cli.SQL_SUCCESS && ret != db2cli.SQL_SUCCESS_WITH_INFO {
			return nil, nil, fmt.Errorf("binding parameter %d: %w", i+1,
				db2cli.GetLastError(db2cli.SQL_HANDLE_STMT, db2cli.SQLHANDLE(s.hstmt)))
		}
	}

	return bufs, inds, nil
}

func (s *db2Stmt) Exec(args []driver.Value) (driver.Result, error) {
	bufs, inds, err := s.bindParams(args)
	if err != nil {
		return nil, fmt.Errorf("binding parameters: %w", err)
	}

	ret := db2cli.SQLExecute(s.hstmt)
	runtime.KeepAlive(bufs) // keep data buffers alive until SQLExecute returns
	runtime.KeepAlive(inds) // keep indicator lengths alive until SQLExecute returns

	if ret != db2cli.SQL_SUCCESS && ret != db2cli.SQL_SUCCESS_WITH_INFO {
		err := db2cli.GetLastError(db2cli.SQL_HANDLE_STMT, db2cli.SQLHANDLE(s.hstmt))
		return nil, fmt.Errorf("executing statement: %w", err)
	}

	var rowCount db2cli.SQLLEN
	if r := db2cli.SQLRowCount(s.hstmt, &rowCount); r != db2cli.SQL_SUCCESS {
		rowCount = 0
	}

	return &db2Result{rowsAffected: int64(rowCount)}, nil
}

func (s *db2Stmt) Query(args []driver.Value) (driver.Rows, error) {
	bufs, inds, err := s.bindParams(args)
	if err != nil {
		return nil, fmt.Errorf("binding parameters: %w", err)
	}

	ret := db2cli.SQLExecute(s.hstmt)
	runtime.KeepAlive(bufs) // keep data buffers alive until SQLExecute returns
	runtime.KeepAlive(inds) // keep indicator lengths alive until SQLExecute returns

	if ret != db2cli.SQL_SUCCESS && ret != db2cli.SQL_SUCCESS_WITH_INFO {
		err := db2cli.GetLastError(db2cli.SQL_HANDLE_STMT, db2cli.SQLHANDLE(s.hstmt))
		return nil, fmt.Errorf("executing query: %w", err)
	}

	var colCount db2cli.SQLSMALLINT
	if r := db2cli.SQLNumResultCols(s.hstmt, &colCount); r != db2cli.SQL_SUCCESS {
		return nil, errors.New("getting column count")
	}

	return &db2Rows{stmt: s, colCount: int(colCount)}, nil
}

// db2Result implements driver.Result.
type db2Result struct {
	rowsAffected int64
}

func (*db2Result) LastInsertId() (int64, error) {
	return 0, errors.New("LastInsertId not supported")
}

func (r *db2Result) RowsAffected() (int64, error) {
	return r.rowsAffected, nil
}

// db2Rows implements driver.Rows.
type db2Rows struct {
	stmt         *db2Stmt
	colCount     int
	cols         []string             // cached column names, populated on first Columns() call
	colDataTypes []db2cli.SQLSMALLINT // SQL type per column, populated by Columns()
	colBuf       [32 * 1024]byte      // reused across column reads; one allocation per rows-object
}

func (r *db2Rows) Columns() []string {
	if r.cols != nil {
		return r.cols
	}
	cols := make([]string, r.colCount)
	colDataTypes := make([]db2cli.SQLSMALLINT, r.colCount)
	for i := 0; i < r.colCount; i++ {
		colName := make([]byte, 256)
		var nameLen, dataType, decimalDigits, nullable db2cli.SQLSMALLINT
		var colSize db2cli.SQLULEN

		ret := db2cli.SQLDescribeCol(
			r.stmt.hstmt,
			db2cli.SQLUSMALLINT(i+1),
			(*db2cli.SQLCHAR)(&colName[0]),
			256,
			&nameLen,
			&dataType,
			&colSize,
			&decimalDigits,
			&nullable,
		)
		if ret == db2cli.SQL_SUCCESS || ret == db2cli.SQL_SUCCESS_WITH_INFO {
			// Clamp nameLen to [0, bufsize]: per ODBC spec nameLen is the required
			// length before truncation; non-conformant drivers may return negative.
			n := max(0, min(int(nameLen), len(colName)))
			cols[i] = string(colName[:n])
		} else {
			cols[i] = fmt.Sprintf("col%d", i+1)
		}
		colDataTypes[i] = dataType
	}
	r.cols = cols
	r.colDataTypes = colDataTypes
	return r.cols
}

func (r *db2Rows) Close() error {
	ret := db2cli.SQLCloseCursor(r.stmt.hstmt)
	var closeErr error
	if ret != db2cli.SQL_SUCCESS && ret != db2cli.SQL_SUCCESS_WITH_INFO {
		// SQLSTATE 24000 = "invalid cursor state": cursor was never opened or is
		// already closed. This is normal after a zero-row result set; treat as no-op.
		diags := db2cli.GetDiagnostics(db2cli.SQL_HANDLE_STMT, db2cli.SQLHANDLE(r.stmt.hstmt))
		var invalidCursor bool
		for _, d := range diags {
			if d.SQLState == db2cli.SQLSTATE_INVALID_CURSOR_STATE {
				invalidCursor = true
				break
			}
		}
		if !invalidCursor {
			closeErr = errors.New("closing cursor")
		}
	}
	// Always free the statement handle so SQLHSTMT resources are returned to DB2
	// immediately rather than accumulating until connection close.
	_ = r.stmt.Close()
	return closeErr
}

func (r *db2Rows) Next(dest []driver.Value) error {
	ret := db2cli.SQLFetch(r.stmt.hstmt)
	if ret == db2cli.SQL_NO_DATA {
		return io.EOF
	}
	if ret != db2cli.SQL_SUCCESS && ret != db2cli.SQL_SUCCESS_WITH_INFO {
		err := db2cli.GetLastError(db2cli.SQL_HANDLE_STMT, db2cli.SQLHANDLE(r.stmt.hstmt))
		return fmt.Errorf("fetching row: %w", err)
	}

	// Ensure colDataTypes is populated (Columns() may not have been called yet).
	if r.colDataTypes == nil {
		r.Columns()
	}

	for i := 0; i < r.colCount; i++ {
		cType := cTypeForSQLType(r.colDataTypes[i])
		val, isNull, err := r.readColumnValue(db2cli.SQLUSMALLINT(i+1), cType)
		if err != nil {
			return fmt.Errorf("reading column %d: %w", i+1, err)
		}
		if isNull {
			dest[i] = nil
		} else {
			dest[i] = val
		}
	}

	return nil
}

// readColumnValue drains one column by calling SQLGetData in a loop.
// cType controls the ODBC transfer type:
//   - SQL_C_CHAR  — text columns: returns string
//   - SQL_C_BINARY — BLOB/BINARY columns: returns []byte (preserves zero bytes)
//   - SQL_C_WCHAR  — DBCS/VARGRAPHIC columns: returns string decoded from UTF-16LE
//
// When a value exceeds the initial buffer DB2 returns SQL_SUCCESS_WITH_INFO and
// sets indicator > bufSize; subsequent calls retrieve the remainder. This avoids
// both silent truncation and buf[:indicator] out-of-bounds panics for large columns.
//
// maxColumnValueBytes caps LOB accumulation to prevent OOM when a BLOB/CLOB column
// contains an unusually large value. DB2 BLOBs can theoretically reach 2 GB.
const maxColumnValueBytes = 64 * 1024 * 1024 // 64 MiB

func (r *db2Rows) readColumnValue(colIdx db2cli.SQLUSMALLINT, cType db2cli.SQLSMALLINT) (driver.Value, bool, error) {
	buf := r.colBuf[:] // reuse the per-rows buffer; avoids 32 KB heap alloc per column per row
	var acc []byte     // accumulator for multi-chunk reads

	for {
		var indicator db2cli.SQLLEN
		ret := db2cli.SQLGetData(
			r.stmt.hstmt, colIdx,
			cType,
			db2cli.SQLPOINTER(unsafe.Pointer(&buf[0])),
			db2cli.SQLLEN(len(buf)),
			&indicator,
		)
		// Keep buf alive across each FFI call: the GC must not relocate the backing
		// array while DB2 CLI holds the unsafe.Pointer reference.
		runtime.KeepAlive(buf)

		switch {
		case ret == db2cli.SQL_NO_DATA:
			// All data was drained in a previous iteration.
			return valueFromAccumulator(acc, cType), false, nil
		case ret != db2cli.SQL_SUCCESS && ret != db2cli.SQL_SUCCESS_WITH_INFO:
			return nil, false, db2cli.GetLastError(db2cli.SQL_HANDLE_STMT, db2cli.SQLHANDLE(r.stmt.hstmt))
		case indicator == db2cli.SQL_NULL_DATA:
			return nil, true, nil
		}

		// Determine how many bytes DB2 wrote into buf.
		// If indicator >= len(buf) the value was truncated; DB2 wrote exactly len(buf) bytes
		// (possibly including a null-terminator at buf[len(buf)-1]).
		// If indicator < 0 (SQL_NO_TOTAL) consume until the null-terminator.
		var written int
		if indicator >= 0 && int(indicator) < len(buf) {
			written = int(indicator)
		} else if cType == db2cli.SQL_C_WCHAR {
			// UTF-16LE: each codepoint is 2 bytes; stripping single 0x00 bytes would
			// corrupt the last character. Strip only aligned 2-byte null pairs instead.
			written = len(buf)
			for written >= 2 && buf[written-1] == 0 && buf[written-2] == 0 {
				written -= 2
			}
		} else if cType == db2cli.SQL_C_BINARY {
			// Binary data has no null-terminator convention; all bytes in buf are
			// significant (e.g. BLOB payloads legitimately end with 0x00).
			written = len(buf)
		} else {
			// Character types: scan back past the null-terminator DB2 appends.
			written = len(buf)
			for written > 0 && buf[written-1] == 0 {
				written--
			}
		}
		if len(acc)+written > maxColumnValueBytes {
			return nil, false, fmt.Errorf("column value exceeds maximum size limit of %d bytes (LOB truncation safeguard)", maxColumnValueBytes)
		}
		acc = append(acc, buf[:written]...)

		if ret == db2cli.SQL_SUCCESS {
			// Full value retrieved in one shot.
			return valueFromAccumulator(acc, cType), false, nil
		}
		// SQL_SUCCESS_WITH_INFO → more data pending; loop.
	}
}

// valueFromAccumulator converts accumulated raw bytes to the appropriate driver.Value.
func valueFromAccumulator(acc []byte, cType db2cli.SQLSMALLINT) driver.Value {
	switch cType {
	case db2cli.SQL_C_BINARY:
		// Return a defensive copy so the caller cannot mutate colBuf via the slice.
		out := make([]byte, len(acc))
		copy(out, acc)
		return out
	case db2cli.SQL_C_WCHAR:
		return decodeUTF16LE(acc)
	default:
		return string(acc)
	}
}

// decodeUTF16LE converts a UTF-16LE byte slice to a UTF-8 Go string.
// DB2 returns GRAPHIC/VARGRAPHIC/DBCLOB data as UTF-16LE when fetched with SQL_C_WCHAR.
// Uses golang.org/x/text/encoding/unicode (already a project dependency) so we
// don't need to roll our own byte-to-uint16 conversion.
func decodeUTF16LE(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	out, err := utf16LE.NewDecoder().Bytes(b)
	if err != nil {
		// Fallback: return raw bytes as a best-effort string rather than silently dropping data.
		return string(b)
	}
	return string(out)
}
